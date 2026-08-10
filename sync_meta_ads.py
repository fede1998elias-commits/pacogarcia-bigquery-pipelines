"""
sync_meta_ads.py
Carga datos de Meta Ads (Graph API v21.0) a BigQuery — dos tablas particionadas por fecha.

    meta_ads.ad_insights        level=ad       métricas sumables (spend, clicks, purchases, revenue…)
    meta_ads.campaign_insights  level=campaign SOLO por reach/frequency (Meta deduplica usuarios;
                                               no se pueden derivar sumando desde ad_insights)

Uso:
    python sync_meta_ads.py                # corrida diaria: ventana móvil últimos 30 días
    python sync_meta_ads.py --backfill     # histórico completo: 2026-02-19 → ayer (bloques de 30 días)
    python sync_meta_ads.py --since 2026-03-01 --until 2026-03-31   # rango manual

INSERCIÓN: load_table_from_json (BATCH — NO streaming inserts — costo $0 en BQ),
con WRITE_TRUNCATE sobre la partición de cada fecha (tabla$YYYYMMDD): reemplaza
solo ese día, nunca la tabla entera. Necesario porque Meta reprocesa atribución
hasta 28 días hacia atrás.
"""
import os
import sys
import time
import argparse
from datetime import datetime, timedelta, date, timezone
from collections import defaultdict

import requests
from dotenv import load_dotenv

load_dotenv()
sys.stdout.reconfigure(encoding="utf-8")

# ── Configuración ─────────────────────────────────────────────────────────────
SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
GCP_PROJECT     = os.environ.get("GCP_PROJECT")
BQ_DATASET      = "meta_ads"
TABLE_ADS       = "ad_insights"
TABLE_CAMPAIGNS = "campaign_insights"
GRAPH_BASE      = "https://graph.facebook.com/v21.0"
# Creación de la cuenta publicitaria — no hay datos antes; sólo acota el --backfill.
ACCOUNT_CREATED = os.environ.get("META_ACCOUNT_CREATED", "2026-02-19")
CHUNK_DAYS      = 30             # días por llamada a la Graph API
CHUNK_PAUSE_S   = 20             # pausa entre bloques en backfill (rate limit de Meta; con 5s el backfill 2026-07-14 disparó un bloqueo #200)

# Compras: SOLO omni_purchase (misma clave que meta_ads.py). "purchase",
# "omni_purchase" y "offsite_conversion.fb_pixel_purchase" reportan la MISMA
# venta por canales de conteo distintos — sumarlos triplica compras y revenue.
# omni_purchase es lo que el Ads Manager muestra como "Compras", ya deduplicado.
# Validado contra el panel el 2026-07-14 (2.361 compras reales vs 7.113 que
# reportaba la suma de los tres).
PURCHASE_ACTIONS = {"omni_purchase"}

# ── Clientes ──────────────────────────────────────────────────────────────────
from google.oauth2 import service_account
from google.cloud import bigquery


def _meta_token() -> str:
    t = os.environ.get("META_ACCESS_TOKEN", "").strip()
    if not t:
        raise SystemExit("❌ Variable de entorno META_ACCESS_TOKEN no definida")
    return t


def _meta_account() -> str:
    acc = os.environ.get("META_AD_ACCOUNT_ID", "").strip()
    if not acc:
        raise SystemExit("❌ Variable de entorno META_AD_ACCOUNT_ID no definida")
    return acc if acc.startswith("act_") else f"act_{acc}"


def _bq_client() -> bigquery.Client:
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/cloud-platform",
        ],
    )
    return bigquery.Client(credentials=creds, project=GCP_PROJECT)


# ── Schemas BQ ────────────────────────────────────────────────────────────────
SCHEMA_ADS = [
    bigquery.SchemaField("date",          "DATE",      mode="REQUIRED"),
    bigquery.SchemaField("campaign_id",   "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("campaign_name", "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("adset_id",      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("adset_name",    "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("ad_id",         "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("ad_name",       "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("spend",         "FLOAT",     mode="NULLABLE"),
    bigquery.SchemaField("impressions",   "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("clicks",        "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("ctr",           "FLOAT",     mode="NULLABLE"),
    bigquery.SchemaField("cpc",           "FLOAT",     mode="NULLABLE"),
    bigquery.SchemaField("cpm",           "FLOAT",     mode="NULLABLE"),
    bigquery.SchemaField("purchases",     "FLOAT",     mode="NULLABLE"),
    bigquery.SchemaField("revenue",       "FLOAT",     mode="NULLABLE"),
    bigquery.SchemaField("synced_at",     "TIMESTAMP", mode="NULLABLE"),
]

SCHEMA_CAMPAIGNS = [
    bigquery.SchemaField("date",          "DATE",      mode="REQUIRED"),
    bigquery.SchemaField("campaign_id",   "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("campaign_name", "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("spend",         "FLOAT",     mode="NULLABLE"),
    bigquery.SchemaField("impressions",   "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("reach",         "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("frequency",     "FLOAT",     mode="NULLABLE"),
    bigquery.SchemaField("synced_at",     "TIMESTAMP", mode="NULLABLE"),
]


# ── Setup dataset y tablas ────────────────────────────────────────────────────
def setup_bq(bq: bigquery.Client) -> tuple[str, str]:
    dataset_ref = bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}")
    dataset_ref.location = "US"
    bq.create_dataset(dataset_ref, exists_ok=True)

    refs = []
    for table_name, schema in ((TABLE_ADS, SCHEMA_ADS), (TABLE_CAMPAIGNS, SCHEMA_CAMPAIGNS)):
        table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}"
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",
            expiration_ms=None,
        )
        bq.create_table(table, exists_ok=True)
        refs.append(table_ref)
    return refs[0], refs[1]


# ── Graph API ─────────────────────────────────────────────────────────────────
def graph_get(path: str, params: dict, retries: int = 5) -> list[dict]:
    """
    GET a la Graph API con paginación completa (sigue data.paging.next hasta agotar).
    Errores de Meta:
        190      → token expirado/ inválido: abortar con mensaje claro
        200      → permisos insuficientes: abortar con mensaje claro
        4 / 17   → rate limit: backoff exponencial y reintento
        otros    → RuntimeError con código + mensaje textual de Meta
    """
    params = dict(params, access_token=_meta_token())
    url = f"{GRAPH_BASE}/{path}"
    rows: list[dict] = []
    attempt = 0

    while url:
        try:
            resp = requests.get(url, params=params, timeout=60)
            payload = resp.json()
        except (requests.RequestException, ValueError) as e:
            attempt += 1
            if attempt > retries:
                raise RuntimeError(f"Error de red contra Graph API (agotados {retries} intentos): {e}")
            wait = min(30 * 2 ** (attempt - 1), 300)
            print(f"  ⚠️  Error de red intento {attempt}/{retries}, retry en {wait}s: {e}", flush=True)
            time.sleep(wait)
            continue

        if "error" in payload:
            err  = payload["error"]
            code = err.get("code")
            msg  = err.get("message", str(err))
            if code == 190:
                raise SystemExit(f"❌ (#190) Token de Meta expirado o inválido. Regenerar META_ACCESS_TOKEN. Detalle: {msg}")
            if code == 200:
                raise SystemExit(f"❌ (#200) Permisos insuficientes sobre la cuenta publicitaria. Detalle: {msg}")
            if code in (4, 17):
                attempt += 1
                if attempt > retries:
                    raise RuntimeError(f"(#{code}) Rate limit de Meta (agotados {retries} intentos): {msg}")
                wait = min(60 * 2 ** (attempt - 1), 300)
                print(f"  ⚠️  (#{code}) Rate limit intento {attempt}/{retries}, retry en {wait}s", flush=True)
                time.sleep(wait)
                continue
            raise RuntimeError(f"(#{code}) {msg}")

        attempt = 0
        rows.extend(payload.get("data", []))
        url = payload.get("paging", {}).get("next")
        params = {}  # `next` ya incluye todos los parámetros en la URL

    return rows


def _flatten_purchases(row: dict) -> tuple[float, float]:
    """Aplana actions/action_values (listas anidadas de Meta) a purchases y revenue."""
    purchases = sum(
        float(a.get("value", 0))
        for a in (row.get("actions") or [])
        if a.get("action_type") in PURCHASE_ACTIONS
    )
    revenue = sum(
        float(v.get("value", 0))
        for v in (row.get("action_values") or [])
        if v.get("action_type") in PURCHASE_ACTIONS
    )
    return purchases, revenue


def _f(row: dict, key: str) -> float:
    return round(float(row.get(key, 0) or 0), 4)


def fetch_chunk(chunk_start: str, chunk_end: str) -> tuple[dict[str, list[dict]], dict[str, list[dict]]]:
    """
    Trae insights diarios (time_increment=1) del rango en dos niveles.
    Retorna (ad_rows_by_date, campaign_rows_by_date).
    """
    time_range = f'{{"since":"{chunk_start}","until":"{chunk_end}"}}'
    account = _meta_account()
    synced_at = datetime.now(timezone.utc).isoformat()

    # nivel ad — métricas sumables
    raw_ads = graph_get(f"{account}/insights", {
        "level": "ad",
        "fields": "campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,"
                  "spend,impressions,clicks,ctr,cpc,cpm,actions,action_values",
        "time_increment": "1",
        "time_range": time_range,
        "limit": 500,
    })

    ads_by_date: dict[str, list[dict]] = defaultdict(list)
    for row in raw_ads:
        purchases, revenue = _flatten_purchases(row)
        ads_by_date[row["date_start"]].append({
            "date":          row["date_start"],
            "campaign_id":   row.get("campaign_id", ""),
            "campaign_name": row.get("campaign_name", ""),
            "adset_id":      row.get("adset_id", ""),
            "adset_name":    row.get("adset_name", ""),
            "ad_id":         row.get("ad_id", ""),
            "ad_name":       row.get("ad_name", ""),
            "spend":         round(float(row.get("spend", 0) or 0), 2),
            "impressions":   int(row.get("impressions", 0) or 0),
            "clicks":        int(row.get("clicks", 0) or 0),
            "ctr":           _f(row, "ctr"),
            "cpc":           _f(row, "cpc"),
            "cpm":           _f(row, "cpm"),
            "purchases":     round(purchases, 1),
            "revenue":       round(revenue, 2),
            "synced_at":     synced_at,
        })

    # nivel campaign — SOLO por reach/frequency (no derivables sumando ads);
    # spend/impressions se repiten únicamente para cruzar consistencia
    raw_camps = graph_get(f"{account}/insights", {
        "level": "campaign",
        "fields": "campaign_id,campaign_name,spend,impressions,reach,frequency",
        "time_increment": "1",
        "time_range": time_range,
        "limit": 500,
    })

    camps_by_date: dict[str, list[dict]] = defaultdict(list)
    for row in raw_camps:
        camps_by_date[row["date_start"]].append({
            "date":          row["date_start"],
            "campaign_id":   row.get("campaign_id", ""),
            "campaign_name": row.get("campaign_name", ""),
            "spend":         round(float(row.get("spend", 0) or 0), 2),
            "impressions":   int(row.get("impressions", 0) or 0),
            "reach":         int(row.get("reach", 0) or 0),
            "frequency":     _f(row, "frequency"),
            "synced_at":     synced_at,
        })

    return dict(ads_by_date), dict(camps_by_date)


# ── BQ load (batch, WRITE_TRUNCATE por partición) ────────────────────────────
def bq_load_partition(bq: bigquery.Client, table_ref: str, schema: list, ds: str, rows: list[dict]) -> str | None:
    """
    Carga filas de UNA fecha con load_table_from_json (BATCH) sobre la partición
    `tabla$YYYYMMDD` con WRITE_TRUNCATE: reemplaza solo esa partición.
    Retorna None si OK, mensaje de error si falla.
    """
    partition_ref = f"{table_ref}${ds.replace('-', '')}"
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )
    job = bq.load_table_from_json(rows, partition_ref, job_config=job_config)
    try:
        job.result()
    except Exception as e:
        return str(e)
    if job.errors:
        return str(job.errors)
    return None


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", action="store_true",
                        help=f"Histórico completo: {ACCOUNT_CREATED} → ayer, en bloques de {CHUNK_DAYS} días")
    parser.add_argument("--since", type=str, default=None, help="Fecha inicial YYYY-MM-DD (rango manual)")
    parser.add_argument("--until", type=str, default=None, help="Fecha final YYYY-MM-DD (rango manual)")
    args = parser.parse_args()

    yesterday = (datetime.today() - timedelta(days=1)).date()

    if args.backfill:
        start = date.fromisoformat(ACCOUNT_CREATED)
        end   = yesterday
        mode  = "BACKFILL"
    elif args.since or args.until:
        if not (args.since and args.until):
            parser.error("--since y --until van juntos")
        start = date.fromisoformat(args.since)
        end   = min(date.fromisoformat(args.until), yesterday)
        mode  = "MANUAL"
    else:
        end   = yesterday
        start = end - timedelta(days=29)   # ventana móvil 30 días (Meta reatribuye hasta 28 días atrás)
        mode  = "DIARIA (ventana móvil 30 días)"

    start = max(start, date.fromisoformat(ACCOUNT_CREATED))  # no hay datos antes de la creación de la cuenta
    if start > end:
        raise SystemExit(f"❌ Rango vacío: {start} > {end}")

    all_dates = []
    cur = start
    while cur <= end:
        all_dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    # Fallar temprano y con mensaje claro si falta configuración, en vez de
    # reventar a mitad de camino o escribir en un proyecto "None".
    _meta_token()
    account = _meta_account()
    for var, val in (("GOOGLE_SERVICE_ACCOUNT_FILE", SERVICE_ACCOUNT_FILE),
                     ("GCP_PROJECT", GCP_PROJECT)):
        if not val:
            raise SystemExit(f"❌ Variable de entorno {var} no definida")

    print("🔄 Sync Meta Ads → BigQuery")
    print(f"   INSERCIÓN : load_table_from_json  ← BATCH, WRITE_TRUNCATE por partición")
    print(f"   Modo      : {mode}")
    print(f"   Período   : {start} → {end} ({len(all_dates)} días)")
    print(f"   Cuenta    : {account}")
    print(f"   Destino   : {GCP_PROJECT}.{BQ_DATASET}.{{{TABLE_ADS}, {TABLE_CAMPAIGNS}}}")
    print()

    bq = _bq_client()
    ref_ads, ref_camps = setup_bq(bq)

    chunks = [all_dates[i:i + CHUNK_DAYS] for i in range(0, len(all_dates), CHUNK_DAYS)]

    rows_ads    = 0
    rows_camps  = 0
    dates_ok    = 0
    dates_empty = 0
    api_errors  = 0
    bq_errors   = 0
    processed   = 0

    for n, chunk in enumerate(chunks, 1):
        chunk_start, chunk_end = chunk[0], chunk[-1]
        print(f"  📥 Graph API  {chunk_start} → {chunk_end} ({len(chunk)} días)...", flush=True)

        try:
            ads_by_date, camps_by_date = fetch_chunk(chunk_start, chunk_end)
        except RuntimeError as e:
            api_errors += len(chunk)
            processed  += len(chunk)
            print(f"  ❌ {chunk_start} → {chunk_end}: error API → {e}  ({processed}/{len(all_dates)})")
            continue

        for ds in chunk:
            processed += 1
            a_rows = ads_by_date.get(ds, [])
            c_rows = camps_by_date.get(ds, [])

            if not a_rows and not c_rows:
                dates_empty += 1
                print(f"  ⚪ {ds}: sin actividad  ({processed}/{len(all_dates)})")
                continue

            failed = False
            for table_ref, schema, rows in (
                (ref_ads,   SCHEMA_ADS,       a_rows),
                (ref_camps, SCHEMA_CAMPAIGNS, c_rows),
            ):
                if not rows:
                    continue
                err = bq_load_partition(bq, table_ref, schema, ds, rows)
                if err:
                    print(f"  ❌ {ds}: BQ error en {table_ref.split('.')[-1]} → {err}")
                    bq_errors += 1
                    failed = True

            if not failed:
                rows_ads   += len(a_rows)
                rows_camps += len(c_rows)
                dates_ok   += 1
                print(f"  ✅ {ds}: {len(a_rows)} anuncios, {len(c_rows)} campañas  ({processed}/{len(all_dates)})")

        if args.backfill and n < len(chunks):
            time.sleep(CHUNK_PAUSE_S)   # pausa entre bloques — rate limit de Meta

    print()
    print(f"🎉 Listo.")
    print(f"   Fechas cargadas    : {dates_ok}")
    print(f"   Fechas sin datos   : {dates_empty}")
    print(f"   Filas ad_insights  : {rows_ads:,}")
    print(f"   Filas campaign_insights: {rows_camps:,}")
    print(f"   Errores API Meta   : {api_errors}")
    print(f"   Errores BQ         : {bq_errors}")

    if api_errors or bq_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
