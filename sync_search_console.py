"""
sync_search_console.py
Carga datos de Search Console a BigQuery — tabla por día.

Uso:
    python sync_search_console.py            # últimos 30 días
    python sync_search_console.py --days 90  # últimos 90 días (backfill)
    python sync_search_console.py --days 480 # máximo histórico (~16 meses)

Programar en Windows Task Scheduler para correr diariamente.
"""
import os
import sys
import time
import argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
sys.stdout.reconfigure(encoding="utf-8")

# ── Configuración ─────────────────────────────────────────────────────────────
SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
SITE_URL             = os.environ.get("SEARCH_CONSOLE_SITE_URL")
GCP_PROJECT          = os.environ.get("GCP_PROJECT")
BQ_DATASET           = "search_console_data"
BQ_TABLE             = "daily_performance"

# ── Clientes ──────────────────────────────────────────────────────────────────
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery

def _sc_client():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/webmasters.readonly"],
    )
    return build("searchconsole", "v1", credentials=creds)


def _bq_client():
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
    bigquery.SchemaField("date",        "DATE",    mode="REQUIRED"),
    bigquery.SchemaField("query",       "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("page",        "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("device",      "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("country",     "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("search_type", "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("clicks",      "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("impressions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("ctr",         "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("position",    "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("synced_at",   "TIMESTAMP", mode="NULLABLE"),
]

SEARCH_TYPES = ["web", "image", "video", "news"]


# ── Setup dataset y tabla ─────────────────────────────────────────────────────
def setup_bq(bq):
    """Crea dataset y tabla si no existen. Si ya existen, no hace nada."""
    dataset_ref = bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}")
    dataset_ref.location = "US"
    bq.create_dataset(dataset_ref, exists_ok=True)

    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    table = bigquery.Table(table_ref, schema=SCHEMA)

    # Particionado por fecha — permite queries eficientes por rango de fechas
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date",
    )
    bq.create_table(table, exists_ok=True)
    return table_ref


# ── Combinaciones ya cargadas (evita duplicados) ──────────────────────────────
def get_loaded_keys(bq, table_ref) -> set:
    """Retorna set de 'date|search_type' ya cargados en BQ."""
    try:
        query = f"SELECT DISTINCT CAST(date AS STRING), search_type FROM `{table_ref}`"
        return {f"{row[0]}|{row[1]}" for row in bq.query(query).result()}
    except Exception as e:
        print(f"  WARN  get_loaded_keys error (continuando desde cero): {e}")
        return set()


# ── Fetch Search Console por día y tipo (con retry) ──────────────────────────
def fetch_day(sc, date_str: str, search_type: str, retries: int = 3) -> tuple[list[dict], bool]:
    """
    Trae datos de un día + search_type desde Search Console.
    Dimensiones: query, page, device, country.
    Retorna (filas, hubo_error_api).
    """
    body = {
        "startDate":  date_str,
        "endDate":    date_str,
        "type":       search_type,
        "dimensions": ["query", "page", "device", "country"],
        "rowLimit":   25000,
    }
    for attempt in range(1, retries + 1):
        try:
            response = sc.searchanalytics().query(siteUrl=SITE_URL, body=body).execute()
            break
        except Exception as e:
            if attempt < retries:
                wait = 2 ** attempt
                print(f"  WARN  SC error {date_str} [{search_type}] (intento {attempt}/{retries}, retry en {wait}s): {e}")
                time.sleep(wait)
            else:
                print(f"  ERROR  SC error {date_str} [{search_type}] (agotados {retries} intentos): {e}")
                return [], True

    rows = []
    synced_at = datetime.utcnow().isoformat()
    for row in response.get("rows", []):
        keys = row.get("keys", [])
        rows.append({
            "date":        date_str,
            "query":       keys[0] if len(keys) > 0 else None,
            "page":        keys[1] if len(keys) > 1 else None,
            "device":      keys[2] if len(keys) > 2 else None,
            "country":     keys[3] if len(keys) > 3 else None,
            "search_type": search_type,
            "clicks":      int(row.get("clicks", 0)),
            "impressions": int(row.get("impressions", 0)),
            "ctr":         round(float(row.get("ctr", 0)) * 100, 4),
            "position":    round(float(row.get("position", 0)), 2),
            "synced_at":   synced_at,
        })
    return rows, False


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30,
                        help="Cuántos días hacia atrás cargar (default: 30, max útil: 480)")
    args = parser.parse_args()

    # Search Console tiene lag de 2 días
    end_date   = datetime.today() - timedelta(days=2)
    start_date = end_date - timedelta(days=args.days - 1)

    print(f"SYNC Sync Search Console → BigQuery")
    print(f"   Período: {start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')} ({args.days} días)")
    print(f"   Dataset: {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")
    print()

    sc = _sc_client()
    bq = _bq_client()

    table_ref = setup_bq(bq)
    loaded    = get_loaded_keys(bq, table_ref)
    print(f"   Combinaciones ya en BQ: {len(loaded)}")

    # Preflight: verificar que SC tiene datos en la fecha más temprana
    preflight_date = start_date.strftime("%Y-%m-%d")
    print(f"   Preflight SC check en {preflight_date} [web]...", end=" ", flush=True)
    probe, err = fetch_day(sc, preflight_date, "web")
    if err:
        print(f"ERROR Error de API — abortando.")
        return
    print(f"{'OK — ' + str(len(probe)) + ' filas' if probe else 'sin datos (fecha fuera del rango SC)'}")
    print()

    # Generar lista de (fecha, search_type) a procesar
    combos = []
    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        for st in SEARCH_TYPES:
            if f"{date_str}|{st}" not in loaded:
                combos.append((date_str, st))
        current += timedelta(days=1)

    if not combos:
        print("OK Todo al día — no hay combinaciones nuevas para cargar.")
        return

    total_combos = args.days * len(SEARCH_TYPES)
    print(f"   Combinaciones a cargar: {len(combos)} (skipping {total_combos - len(combos)} ya cargadas)")
    print()

    total_rows  = 0
    api_errors  = 0
    bq_errors   = 0
    empty_days  = 0
    for i, (date_str, search_type) in enumerate(combos, 1):
        rows, had_error = fetch_day(sc, date_str, search_type)
        if had_error:
            api_errors += 1
            continue
        if rows:
            job_config = bigquery.LoadJobConfig(
                schema=SCHEMA,
                write_disposition="WRITE_APPEND",
            )
            job = bq.load_table_from_json(rows, table_ref, job_config=job_config)
            try:
                job.result()
            except Exception as e:
                print(f"  ERROR {date_str} [{search_type}]: BQ job failed → {e}")
                bq_errors += 1
                continue
            if job.errors:
                print(f"  ERROR {date_str} [{search_type}]: BQ error → {job.errors}")
                bq_errors += 1
            else:
                print(f"  OK {date_str} [{search_type}]: {len(rows)} filas  ({i}/{len(combos)})")
                total_rows += len(rows)
        else:
            empty_days += 1
            print(f"  SKIP {date_str} [{search_type}]: sin datos  ({i}/{len(combos)})")

    print()
    print(f"Done. Listo. {total_rows:,} filas cargadas | "
          f"{empty_days} sin datos | {api_errors} errores SC | {bq_errors} errores BQ")


if __name__ == "__main__":
    main()
