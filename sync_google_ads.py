"""
sync_google_ads.py
Carga datos de Google Ads a BigQuery — tabla por día y campaña.

IDEMPOTENCIA: toda fecha que ya tenga filas se borra (DELETE por fecha) justo
antes de recargarse. Correr la misma fecha N veces deja siempre el mismo
resultado. Nunca duplica.

VENTANA MÓVIL: los últimos --refresh-days días se recargan SIEMPRE, aunque ya
estén en BQ. Sin esto cada fecha se capturaba una sola vez a D+1 y quedaba
congelada, pero Google sigue atribuyendo conversiones ~2 semanas hacia atrás
(ventana de clic de 30 días + remodelado DDA): las conversiones quedaban
subestimadas ~31% y el ROAS daba 6,92 contra 9,77 real. El costo NO se mueve.

Uso:
    python sync_google_ads.py                              # 30 días (+ refresh de 21)
    python sync_google_ads.py --days 30 --refresh-days 21  # lo que corre el daily_sync
    python sync_google_ads.py --days 1095 --refresh-days 0 # histórico, sin refrescar
    python sync_google_ads.py --reprocess --days 60        # backfill correctivo

INSERCIÓN: load_table_from_json (BATCH — NO streaming inserts).
"""
import os
import sys
import time
import argparse
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()
sys.stdout.reconfigure(encoding="utf-8")

# ── Configuración ─────────────────────────────────────────────────────────────
SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
YAML_FILE   = os.path.join(os.path.dirname(__file__), "google-ads.yaml")
CUSTOMER_ID = (os.environ.get("GOOGLE_ADS_CUSTOMER_ID") or "").strip() or None
GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET  = "google_ads_data"
BQ_TABLE    = "daily_campaigns"
CHUNK_DAYS  = 30   # días por llamada a la API de Ads

# ── Clientes ──────────────────────────────────────────────────────────────────
from google.oauth2 import service_account
from google.cloud import bigquery
from google.ads.googleads.client import GoogleAdsClient


def _ads_client() -> GoogleAdsClient:
    return GoogleAdsClient.load_from_storage(YAML_FILE)


def _bq_client() -> bigquery.Client:
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/cloud-platform",
        ],
    )
    return bigquery.Client(credentials=creds, project=GCP_PROJECT)


# ── Schema BQ ─────────────────────────────────────────────────────────────────
SCHEMA = [
    bigquery.SchemaField("date",              "DATE",    mode="REQUIRED"),
    bigquery.SchemaField("campaign_id",       "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("campaign_name",     "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("channel_type",      "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("cost",              "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("impressions",       "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("clicks",            "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("ctr",               "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("avg_cpc",           "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("conversions",       "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("conversions_value", "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("roas",              "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("synced_at",         "TIMESTAMP", mode="NULLABLE"),
]


# ── Setup dataset y tabla ─────────────────────────────────────────────────────
def setup_bq(bq: bigquery.Client) -> str:
    dataset_ref = bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}")
    dataset_ref.location = "US"
    bq.create_dataset(dataset_ref, exists_ok=True)

    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    table = bigquery.Table(table_ref, schema=SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date",
        expiration_ms=None,      # sin expiración — requiere billing habilitado
    )
    bq.create_table(table, exists_ok=True)
    return table_ref


# ── Fechas ya cargadas ────────────────────────────────────────────────────────
def get_loaded_dates(bq: bigquery.Client, table_ref: str) -> set:
    try:
        sql = f"SELECT DISTINCT CAST(date AS STRING) FROM `{table_ref}`"
        return {row[0] for row in bq.query(sql).result()}
    except Exception as e:
        print(f"  WARN  get_loaded_dates error (continuando desde cero): {e}")
        return set()


# ── Fetch Google Ads por rango (CHUNK_DAYS días por llamada) ──────────────────
def fetch_chunk(
    ads: GoogleAdsClient,
    chunk_start: str,
    chunk_end: str,
    retries: int = 3,
) -> tuple[dict[str, list[dict]], bool]:
    """
    Consulta Google Ads para el rango chunk_start → chunk_end.
    Filtra cost > 0 (per reglas del proyecto).
    Retorna (dict keyed por date_str, had_error).
    """
    svc = ads.get_service("GoogleAdsService")
    query = f"""
        SELECT
            segments.date,
            campaign.id,
            campaign.name,
            campaign.advertising_channel_type,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.ctr,
            metrics.average_cpc,
            metrics.conversions,
            metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{chunk_start}' AND '{chunk_end}'
          AND metrics.cost_micros > 0
        ORDER BY segments.date, metrics.cost_micros DESC
    """
    for attempt in range(1, retries + 1):
        try:
            response = svc.search(customer_id=CUSTOMER_ID, query=query)
            rows_raw = list(response)   # materializa el iterator (API call real aquí)
            break
        except Exception as e:
            if attempt < retries:
                wait = 2 ** attempt
                print(f"  WARN  Ads API error intento {attempt}/{retries}, retry en {wait}s: {e}")
                time.sleep(wait)
            else:
                print(f"  ERROR  Ads API error (agotados {retries} intentos): {e}")
                return {}, True

    synced_at = datetime.utcnow().isoformat()
    by_date: dict[str, list[dict]] = defaultdict(list)

    for row in rows_raw:
        cost = row.metrics.cost_micros / 1_000_000
        conv_value = row.metrics.conversions_value
        roas = round(conv_value / cost, 4) if cost > 0 else 0.0

        by_date[row.segments.date].append({
            "date":              row.segments.date,
            "campaign_id":       row.campaign.id,
            "campaign_name":     row.campaign.name,
            "channel_type":      row.campaign.advertising_channel_type.name,
            "cost":              round(cost, 2),
            "impressions":       row.metrics.impressions,
            "clicks":            row.metrics.clicks,
            "ctr":               round(row.metrics.ctr * 100, 4),
            "avg_cpc":           round(row.metrics.average_cpc / 1_000_000, 2),
            "conversions":       round(row.metrics.conversions, 4),
            "conversions_value": round(conv_value, 2),
            "roas":              roas,
            "synced_at":         synced_at,
        })

    return dict(by_date), False


# ── BQ load (batch, no streaming) ────────────────────────────────────────────
def bq_load(bq: bigquery.Client, table_ref: str, rows: list[dict]) -> str | None:
    """
    Carga filas con load_table_from_json (BATCH).
    Retorna None si OK, mensaje de error si falla.
    """
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition="WRITE_APPEND",
    )
    job = bq.load_table_from_json(rows, table_ref, job_config=job_config)
    try:
        job.result()
    except Exception as e:
        return str(e)
    if job.errors:
        return str(job.errors)
    return None


def delete_date(bq: bigquery.Client, table_ref: str, ds: str) -> str | None:
    """Borra las filas de una fecha. Hace la recarga idempotente: nunca duplica.
    La tabla está particionada por `date`, así que el filtro poda la partición y
    el DELETE procesa ~24 bytes."""
    try:
        bq.query(
            f"DELETE FROM `{table_ref}` WHERE date = @d",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("d", "DATE", ds)]
            ),
        ).result()
        return None
    except Exception as e:
        return str(e)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--days", type=int, default=30,
        help="Días hacia atrás (default: 30 | histórico completo: 1095)",
    )
    parser.add_argument(
        "--refresh-days", type=int, default=21,
        help="Ventana móvil que SIEMPRE se recarga, aunque ya esté en BQ "
             "(default: 21). Imprescindible: Google sigue atribuyendo "
             "conversiones hacia atrás, así que una fecha capturada a D+1 queda "
             "subestimada ~31%%. La curva se aplana a los ~15 días; 21 cubre la "
             "cola y tolera un retraso del pipeline. 0 = desactivar.",
    )
    parser.add_argument(
        "--reprocess", action="store_true",
        help="Reprocesa TODAS las fechas del rango, incluso las ya cargadas: "
             "borra cada fecha y la recarga (borrado por-fecha, no masivo).",
    )
    args = parser.parse_args()

    end_date   = datetime.today() - timedelta(days=1)   # Ads tiene lag de 1 día
    start_date = end_date - timedelta(days=args.days - 1)

    print("SYNC Sync Google Ads → BigQuery")
    print(f"   INSERCIÓN : load_table_from_json  ← BATCH")
    print(f"   Período   : {start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')} ({args.days} días)")
    print(f"   Cliente   : {CUSTOMER_ID}")
    print(f"   Destino   : {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")

    # Ventana móvil que se recarga siempre. Una fecha capturada a D+1 tiene sólo
    # ~2/3 de sus conversiones finales: Google sigue atribuyendo durante semanas.
    # Refrescarla hasta que madure es lo que hace que el ROAS de BQ cuadre.
    refresh_from = (end_date - timedelta(days=args.refresh_days - 1)
                    if args.refresh_days > 0 else None)

    if args.reprocess:
        print(f"   MODO      : REPROCESO (borra y recarga cada fecha del rango)")
    elif refresh_from:
        print(f"   Ventana móvil: {refresh_from.strftime('%Y-%m-%d')} → "
              f"{end_date.strftime('%Y-%m-%d')} (se recarga siempre, {args.refresh_days} días)")
    print()

    ads = _ads_client()
    bq  = _bq_client()

    table_ref = setup_bq(bq)
    # loaded se calcula SIEMPRE: además de decidir qué saltear, dice qué fechas
    # hay que borrar antes de recargar para no duplicar filas.
    loaded    = get_loaded_dates(bq, table_ref)
    print(f"   Fechas ya en BQ: {len(loaded)}")

    # Fechas pendientes
    dates_pending = []
    cur = start_date
    while cur <= end_date:
        ds = cur.strftime("%Y-%m-%d")
        nueva      = ds not in loaded
        en_ventana = refresh_from is not None and cur >= refresh_from
        if args.reprocess or nueva or en_ventana:
            dates_pending.append(ds)
        cur += timedelta(days=1)

    if not dates_pending:
        print("OK Todo al día — no hay fechas nuevas.")
        return

    skipped = args.days - len(dates_pending)
    print(f"   Fechas a cargar: {len(dates_pending)} "
          f"(skipping {skipped} ya cargadas y maduras)")
    print()

    # Dividir en chunks de CHUNK_DAYS días
    chunks = [
        dates_pending[i : i + CHUNK_DAYS]
        for i in range(0, len(dates_pending), CHUNK_DAYS)
    ]

    total_rows  = 0
    dates_ok    = 0
    dates_empty = 0
    api_errors  = 0
    bq_errors   = 0
    processed   = 0

    for chunk in chunks:
        chunk_start, chunk_end = chunk[0], chunk[-1]
        print(f"  FETCH Ads API  {chunk_start} → {chunk_end} ({len(chunk)} días)...", flush=True)

        by_date, had_error = fetch_chunk(ads, chunk_start, chunk_end)

        if had_error:
            api_errors += len(chunk)
            for ds in chunk:
                processed += 1
                print(f"  ERROR {ds}: error API  ({processed}/{len(dates_pending)})")
            continue

        for ds in chunk:
            processed += 1
            rows = by_date.get(ds, [])

            if not rows:
                dates_empty += 1
                print(f"  SKIP {ds}: sin campañas con costo  ({processed}/{len(dates_pending)})")
                continue

            # Borrar JUSTO antes de recargar, con las filas nuevas ya en memoria:
            # así la carga es idempotente y un fallo de la API de Ads (que corta
            # más arriba, en had_error) nunca puede dejar la fecha vacía.
            # Si la fecha vuelve sin filas se saltea arriba y NO se borra: el
            # costo nunca se revisa hacia atrás, así que una fecha con gasto no
            # puede quedarse sin campañas — sería un problema de la API, no un
            # dato real, y borrar destruiría data buena.
            if ds in loaded:
                del_err = delete_date(bq, table_ref, ds)
                if del_err:
                    print(f"  ERROR {ds}: DELETE error → {del_err}  ({processed}/{len(dates_pending)})")
                    bq_errors += 1
                    continue

            err = bq_load(bq, table_ref, rows)
            if err:
                print(f"  ERROR {ds}: BQ error → {err}  ({processed}/{len(dates_pending)})")
                bq_errors += 1
            else:
                print(f"  OK {ds}: {len(rows)} campañas  ({processed}/{len(dates_pending)})")
                total_rows += len(rows)
                dates_ok   += 1

    print()
    print(f"Done. Listo.")
    print(f"   Fechas cargadas : {dates_ok}")
    print(f"   Fechas sin datos: {dates_empty}")
    print(f"   Filas totales   : {total_rows:,}")
    print(f"   Errores API Ads : {api_errors}")
    print(f"   Errores BQ      : {bq_errors}")


if __name__ == "__main__":
    main()
