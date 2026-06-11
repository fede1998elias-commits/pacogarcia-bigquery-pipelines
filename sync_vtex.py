"""
sync_vtex.py — carga órdenes e items de VTEX a BigQuery.

Tablas destino:
  vtex_data.daily_orders  — una fila por orden
  vtex_data.order_items   — una fila por item de cada orden

INSERCIÓN: load_table_from_json (BATCH — NO streaming inserts — costo $0 en BQ).

Uso:
    python sync_vtex.py                          # últimos 365 días
    python sync_vtex.py --days 730               # últimos 2 años
    python sync_vtex.py --start-date 2021-01-01  # histórico completo desde esa fecha
"""
import os
import sys
import time
import argparse
from datetime import datetime, timedelta, timezone, date
from dotenv import load_dotenv

load_dotenv()
sys.stdout.reconfigure(encoding="utf-8")

# ── Configuración ─────────────────────────────────────────────────────────────
SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
VTEX_ACCOUNT = os.environ.get("VTEX_ACCOUNT")
GCP_PROJECT  = os.environ.get("GCP_PROJECT")
BQ_DATASET   = "vtex_data"
TABLE_ORDERS = "daily_orders"
TABLE_ITEMS  = "order_items"

# Argentina = UTC-3 sin DST
ARS_OFFSET = timedelta(hours=3)

import requests
from google.oauth2 import service_account
from google.cloud import bigquery
from vtex import get_product_detail   # usa product_cache.json

# ── Headers VTEX ──────────────────────────────────────────────────────────────
def _vtex_headers() -> dict:
    return {
        "X-VTEX-API-AppKey":   os.environ["VTEX_APP_KEY"],
        "X-VTEX-API-AppToken": os.environ["VTEX_APP_TOKEN"],
        "Accept":              "application/json",
        "Content-Type":        "application/json",
    }

def _vtex_url(path: str) -> str:
    return f"https://{VTEX_ACCOUNT}.vtexcommercestable.com.br{path}"


# ── Clientes ──────────────────────────────────────────────────────────────────
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
SCHEMA_ORDERS = [
    bigquery.SchemaField("order_id",        "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("creation_date",   "DATE",      mode="REQUIRED"),
    bigquery.SchemaField("status",          "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("total_value_ars", "FLOAT",     mode="NULLABLE"),
    bigquery.SchemaField("payment_name",    "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("utm_source",      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("utm_campaign",    "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("items_count",     "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("shipping_state",  "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("shipping_method", "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("seller_id",       "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("synced_at",       "TIMESTAMP", mode="NULLABLE"),
]

SCHEMA_ITEMS = [
    bigquery.SchemaField("order_id",      "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("creation_date", "DATE",      mode="REQUIRED"),
    bigquery.SchemaField("product_id",    "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("product_name",  "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("category",      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("brand",         "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("quantity",      "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("price",         "FLOAT",     mode="NULLABLE"),
    bigquery.SchemaField("synced_at",     "TIMESTAMP", mode="NULLABLE"),
]


# ── Helpers de fecha ──────────────────────────────────────────────────────────
def _ars_day_utc_range(ars_date: date) -> tuple[str, str]:
    """Rango UTC para un día calendario de Argentina (UTC-3)."""
    utc_start = datetime(ars_date.year, ars_date.month, ars_date.day, 3, 0, 0)
    utc_end   = utc_start + timedelta(days=1) - timedelta(seconds=1)
    return utc_start.strftime("%Y-%m-%dT%H:%M:%S"), utc_end.strftime("%Y-%m-%dT%H:%M:%S")


def _vtex_date_to_ars_date(vtex_date: str) -> str:
    """Convierte fecha UTC de VTEX a fecha de Argentina (YYYY-MM-DD)."""
    clean = vtex_date[:19]   # '2023-01-15T17:30:00'
    dt_utc = datetime.strptime(clean, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    dt_ars = dt_utc - ARS_OFFSET
    return dt_ars.strftime("%Y-%m-%d")


# ── Setup BQ ──────────────────────────────────────────────────────────────────
def setup_bq(bq: bigquery.Client) -> tuple[str, str]:
    ds_ref = bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}")
    ds_ref.location = "US"
    bq.create_dataset(ds_ref, exists_ok=True)

    ref_orders = f"{GCP_PROJECT}.{BQ_DATASET}.{TABLE_ORDERS}"
    ref_items  = f"{GCP_PROJECT}.{BQ_DATASET}.{TABLE_ITEMS}"

    for ref, schema in ((ref_orders, SCHEMA_ORDERS), (ref_items, SCHEMA_ITEMS)):
        t = bigquery.Table(ref, schema=schema)
        t.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="creation_date",
            expiration_ms=None,
        )
        bq.create_table(t, exists_ok=True)

    return ref_orders, ref_items


# ── Dedup ─────────────────────────────────────────────────────────────────────
def get_loaded_dates(bq: bigquery.Client, table_ref: str) -> set:
    try:
        sql = f"SELECT DISTINCT CAST(creation_date AS STRING) FROM `{table_ref}`"
        return {row[0] for row in bq.query(sql).result()}
    except Exception as e:
        print(f"  WARN  get_loaded_dates error (desde cero): {e}")
        return set()


# ── Fetch VTEX — paginación con split recursivo ───────────────────────────────
def _parse_raw_order(o: dict) -> dict:
    marketing = o.get("marketingData") or {}
    return {
        "orderId":      o.get("orderId"),
        "creationDate": o.get("creationDate"),
        "status":       o.get("status"),
        "totalValue":   o.get("totalValue"),
        "paymentNames": o.get("paymentNames"),
        "utmSource":    marketing.get("utmSource"),
        "utmCampaign":  marketing.get("utmCampaign"),
    }


def _paginate_window(
    utc_start: str, utc_end: str, retries: int = 3
) -> tuple[list[dict] | None, bool]:
    """
    Pagina órdenes en un rango UTC.
    Retorna (orders, hit_vtex_limit).
    hit_vtex_limit=True cuando la API rechaza la página 31 (límite 30 × 100 = 3.000).
    orders puede ser partial (las 3.000 ya cargadas) cuando hit_vtex_limit=True.
    """
    orders: list[dict] = []
    page = 1

    while True:
        data = None
        for attempt in range(1, retries + 1):
            try:
                resp = requests.get(
                    _vtex_url("/api/oms/pvt/orders"),
                    headers=_vtex_headers(),
                    params={
                        "f_creationDate": f"creationDate:[{utc_start} TO {utc_end}]",
                        "page":           page,
                        "per_page":       100,
                        "orderBy":        "creationDate,asc",
                    },
                    timeout=30,
                )
                if resp.status_code == 400 and page > 1:
                    # VTEX rechaza página 31+ — límite alcanzado, devolver parcial
                    return orders, True
                resp.raise_for_status()
                data = resp.json()
                break
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 400 and page > 1:
                    return orders, True   # límite de paginación
                if attempt < retries:
                    time.sleep(2 ** attempt)
                else:
                    return None, False    # error real de API
            except Exception as e:
                if attempt < retries:
                    time.sleep(2 ** attempt)
                else:
                    return None, False

        if data is None:
            return None, False

        for o in data.get("list", []):
            orders.append(_parse_raw_order(o))

        paging = data.get("paging", {})
        if page >= paging.get("pages", 1):
            return orders, False   # todo cargado
        page += 1


def _fetch_recursive(
    utc_start: str, utc_end: str, retries: int = 3, depth: int = 0
) -> list[dict] | None:
    """
    Trae órdenes en un rango UTC dividiéndose recursivamente si supera el límite.
    Máximo depth=5 → ventanas mínimas de ~45 min, soporte hasta ~96.000 ord/día.
    """
    orders, hit_limit = _paginate_window(utc_start, utc_end, retries)

    if orders is None:
        return None    # error real

    if not hit_limit:
        return orders  # éxito total en esta ventana

    if depth >= 5:
        # Límite de recursión: devolver lo que hay y loguear
        print(f"\n    WARN  Ventana aún supera 3.000 ord en depth={depth} — cargando parcial")
        return orders

    # Dividir la ventana en dos mitades y recurrir
    start_dt = datetime.strptime(utc_start, "%Y-%m-%dT%H:%M:%S")
    end_dt   = datetime.strptime(utc_end,   "%Y-%m-%dT%H:%M:%S")
    mid_dt   = start_dt + (end_dt - start_dt) // 2
    mid_str  = mid_dt.strftime("%Y-%m-%dT%H:%M:%S")
    mid1_str = (mid_dt + timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S")

    left  = _fetch_recursive(utc_start, mid_str,  retries, depth + 1)
    right = _fetch_recursive(mid1_str,  utc_end,  retries, depth + 1)

    if left is None or right is None:
        return None
    return left + right


def fetch_orders_for_date(ars_date: date, retries: int = 3) -> list[dict] | None:
    """
    Trae todos los pedidos de un día calendario de Argentina.
    Maneja el límite de 3.000 órdenes de VTEX via split recursivo.
    Deduplica por orderId al final (los sub-rangos pueden solapar).
    """
    utc_start, utc_end = _ars_day_utc_range(ars_date)
    expected = ars_date.isoformat()

    # Probe rápido (10 filas) para detectar días sin órdenes reales antes del
    # split recursivo caro. VTEX devuelve datos fallback cuando el filtro de
    # fecha no encuentra resultados, así que verificamos que al menos 1 orden
    # pertenezca al día solicitado.
    try:
        probe_resp = requests.get(
            _vtex_url("/api/oms/pvt/orders"),
            headers=_vtex_headers(),
            params={
                "f_creationDate": f"creationDate:[{utc_start} TO {utc_end}]",
                "page": 1, "per_page": 10, "orderBy": "creationDate,asc",
            },
            timeout=30,
        )
        probe_resp.raise_for_status()
        probe_list = probe_resp.json().get("list", [])
        if not probe_list:
            return []   # día sin órdenes (respuesta limpia)
        in_range = [o for o in probe_list
                    if o.get("creationDate")
                    and _vtex_date_to_ars_date(o["creationDate"]) == expected]
        if not in_range:
            return []   # fallback VTEX — ninguna orden corresponde a este día
    except Exception as e:
        print(f"    WARN  Probe falló ({e}), intentando igual", flush=True)

    raw = _fetch_recursive(utc_start, utc_end, retries)
    if raw is None:
        return None

    # Dedup por orderId — los sub-rangos de tiempo pueden devolver solapados
    seen: set[str] = set()
    unique: list[dict] = []
    for o in raw:
        oid = o.get("orderId")
        if oid and oid not in seen:
            seen.add(oid)
            unique.append(o)

    dupes = len(raw) - len(unique)
    if dupes:
        print(f"    INFO  {dupes} duplicados eliminados", flush=True)

    # Validar que las órdenes realmente pertenecen a este día en ARS.
    # VTEX devuelve las órdenes más recientes como fallback cuando no hay
    # resultados para la fecha pedida (ej: feriados con 0 órdenes).
    expected = ars_date.isoformat()
    valid = [
        o for o in unique
        if o.get("creationDate") and _vtex_date_to_ars_date(o["creationDate"]) == expected
    ]

    out_of_range = len(unique) - len(valid)
    if out_of_range:
        print(f"    INFO  {out_of_range} órdenes fuera de rango filtradas (fallback VTEX)", flush=True)

    return valid


def fetch_order_detail_raw(order_id: str, retries: int = 3) -> dict | None:
    """
    Detalle completo de un pedido (incluye items con precio y cantidad).
    Retorna None si falla tras todos los intentos.
    """
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(
                _vtex_url(f"/api/oms/pvt/orders/{order_id}"),
                headers=_vtex_headers(),
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                print(f"    WARN  Detalle {order_id} falló: {e}")
                return None


# ── BQ load (batch, no streaming) ────────────────────────────────────────────
def bq_load(bq: bigquery.Client, table_ref: str, schema, rows: list[dict]) -> str | None:
    """load_table_from_json — BATCH, no streaming. Retorna None si OK, error si falla."""
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",
    )
    job = bq.load_table_from_json(rows, table_ref, job_config=job_config)
    try:
        job.result()
    except Exception as e:
        return str(e)
    return str(job.errors) if job.errors else None


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days",       type=int, default=365,
                        help="Días hacia atrás desde hoy (default: 365)")
    parser.add_argument("--start-date", type=str, default=None,
                        help="Fecha de inicio explícita YYYY-MM-DD (override --days)")
    args = parser.parse_args()

    today    = date.today()
    end_date = today

    if args.start_date:
        start_date = date.fromisoformat(args.start_date)
    else:
        start_date = today - timedelta(days=args.days - 1)

    total_days = (end_date - start_date).days + 1

    print("SYNC Sync VTEX → BigQuery")
    print(f"   INSERCIÓN  : load_table_from_json  ← BATCH (costo $0)")
    print(f"   Período    : {start_date} → {end_date} ({total_days} días)")
    print(f"   Cuenta VTEX: {VTEX_ACCOUNT}")
    print(f"   Tablas     : {GCP_PROJECT}.{BQ_DATASET}.{TABLE_ORDERS}")
    print(f"              : {GCP_PROJECT}.{BQ_DATASET}.{TABLE_ITEMS}")
    print()

    bq = _bq_client()
    ref_orders, ref_items = setup_bq(bq)

    loaded = get_loaded_dates(bq, ref_orders)
    print(f"   Fechas ya en BQ: {len(loaded)}")

    dates_pending = []
    cur = start_date
    while cur <= end_date:
        ds = cur.isoformat()
        if ds not in loaded:
            dates_pending.append(cur)
        cur += timedelta(days=1)

    if not dates_pending:
        print("OK Todo al día — no hay fechas nuevas.")
        return

    skipped = total_days - len(dates_pending)
    print(f"   Fechas a cargar: {len(dates_pending)} (skipping {skipped} ya cargadas)")
    print()

    total_orders  = 0
    total_items   = 0
    dates_ok      = 0
    dates_empty   = 0
    api_errors    = 0
    bq_errors     = 0
    detail_errors = 0
    processed     = 0

    for ars_date in dates_pending:
        processed += 1
        ds = ars_date.isoformat()
        synced_at = datetime.utcnow().isoformat()

        print(f"  FETCH {ds} ...", end=" ", flush=True)

        orders_raw = fetch_orders_for_date(ars_date)
        if orders_raw is None:
            api_errors += 1
            print(f"ERROR error API  ({processed}/{len(dates_pending)})")
            continue

        if not orders_raw:
            dates_empty += 1
            print(f"SKIP sin órdenes  ({processed}/{len(dates_pending)})")
            continue

        print(f"{len(orders_raw)} órdenes → detalle...", end=" ", flush=True)

        order_rows = []
        item_rows  = []

        for order in orders_raw:
            order_id = order["orderId"]
            ars_date_str = _vtex_date_to_ars_date(order["creationDate"]) if order.get("creationDate") else ds

            # Detalle completo (para items e items_count)
            detail = fetch_order_detail_raw(order_id)
            time.sleep(0.2)   # rate limiting suave

            if detail is None:
                detail_errors += 1
                # Cargar orden sin items (items_count=0) para no perder la orden
                items_raw = []
            else:
                items_raw = detail.get("items") or []

            # Fila para daily_orders
            payment = (
                (detail or {})
                .get("paymentData", {})
                .get("transactions", [{}])[0]
                .get("payments", [{}])
            )
            payment_name = payment[0].get("paymentSystemName") if payment else order.get("paymentNames")

            # Extraer provincia y método de envío del detalle
            shipping_state  = None
            shipping_method = None
            seller_id       = None
            if detail:
                try:
                    logistics = detail.get("shippingData", {}).get("logisticsInfo", [{}])
                    if logistics:
                        shipping_method = logistics[0].get("selectedSla") or logistics[0].get("slas", [{}])[0].get("id") if logistics[0].get("slas") else None
                    address = detail.get("shippingData", {}).get("address", {})
                    shipping_state = address.get("state")
                    sellers = detail.get("sellers", [{}])
                    if sellers:
                        seller_id = sellers[0].get("id")
                except Exception:
                    pass

            order_rows.append({
                "order_id":        order_id,
                "creation_date":   ars_date_str,
                "status":          order.get("status"),
                "total_value_ars": round((order.get("totalValue") or 0) / 100, 2),
                "payment_name":    payment_name,
                "utm_source":      order.get("utmSource"),
                "utm_campaign":    order.get("utmCampaign"),
                "items_count":     len(items_raw),
                "shipping_state":  shipping_state,
                "shipping_method": shipping_method,
                "seller_id":       seller_id,
                "synced_at":       synced_at,
            })

            # Filas para order_items
            for item in items_raw:
                product_id = str(item.get("productId", "")) or None
                enriched   = get_product_detail(product_id) if product_id else None
                item_rows.append({
                    "order_id":     order_id,
                    "creation_date": ars_date_str,
                    "product_id":   product_id,
                    "product_name": (enriched or {}).get("name") or item.get("name"),
                    "category":     (enriched or {}).get("category"),
                    "brand":        (enriched or {}).get("brand") or
                                    item.get("additionalInfo", {}).get("brandName"),
                    "quantity":     item.get("quantity"),
                    "price":        round((item.get("price") or 0) / 100, 2),
                    "synced_at":    synced_at,
                })

        # Cargar a BQ (batch)
        err_o = bq_load(bq, ref_orders, SCHEMA_ORDERS, order_rows)
        if err_o:
            print(f"\n  ERROR {ds}: BQ orders error → {err_o}  ({processed}/{len(dates_pending)})")
            bq_errors += 1
            continue

        if item_rows:
            err_i = bq_load(bq, ref_items, SCHEMA_ITEMS, item_rows)
            if err_i:
                print(f"\n  ERROR {ds}: BQ items error → {err_i}  ({processed}/{len(dates_pending)})")
                bq_errors += 1
                continue

        total_orders += len(order_rows)
        total_items  += len(item_rows)
        dates_ok     += 1
        print(f"OK {len(order_rows)} órdenes / {len(item_rows)} items  ({processed}/{len(dates_pending)})")

    print()
    print("Done. Listo.")
    print(f"   Fechas cargadas     : {dates_ok}")
    print(f"   Fechas sin órdenes  : {dates_empty}")
    print(f"   Órdenes cargadas    : {total_orders:,}")
    print(f"   Items cargados      : {total_items:,}")
    print(f"   Errores API VTEX    : {api_errors}")
    print(f"   Errores detalle ord.: {detail_errors}")
    print(f"   Errores BQ          : {bq_errors}")


if __name__ == "__main__":
    main()
