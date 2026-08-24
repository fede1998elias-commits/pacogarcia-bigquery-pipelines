"""
vtex_stock.py — snapshot de inventario VTEX al grano SKU × DEPÓSITO.

Tabla destino:
  vtex_data.stock_snapshot   — una fila por (sku_id, warehouse_id, snapshot_date)

POR QUÉ EL GRANO ES SKU × DEPÓSITO
El depósito es una dimensión del inventario, NO un atributo del SKU: el mismo
SKU tiene balance en 1_1 (web) y en 1_3 (Mercado Libre) al mismo tiempo. Si se
colapsa a un número por SKU, el stock "vendible" de la web queda inflado con
unidades que no le corresponden (~1% en la práctica). Por eso la tabla guarda
una fila por depósito y las vistas deciden qué depósito mirar:

  vw_stock_vendible  → is_active = true AND warehouse_id = '1_1' AND
                       available_quantity > 0.  ESTE es el stock real de la web
                       y es el número que hay que usar: lo que se puede vender
                       hoy, ya sin los SKUs agotados.
  vw_stock_por_deposito → 1_1 y 1_3 en columnas separadas, sin colapsar. SÍ
                       incluye los SKUs en cero, porque existe para control de
                       totales.

FLUJO (todo lo caro escala con PRODUCTOS, no con SKUs)
  1. GetProductAndSkuIds (paginado)          → universo de productos (~17.563)
  2. stockkeepingunitbyproductid/{productId} → SKUs del producto: Id, Name
     (= TALLE), RefId, IsActive
     /api/catalog/pvt/product/{productId}    → Name y RefId del PRODUCTO
  3. /api/logistics/pvt/inventory/skus/{id}  → balance por depósito, 1 por SKU

Por qué dos llamadas en el paso 2: stockkeepingunitbyproductid devuelve la
proyección "catálogo" y deja NameComplete, ProductName y ProductRefId en null
SIEMPRE (verificado 2026-08-11). El endpoint que sí los trae,
stockkeepingunitbyid/{skuId}, cuesta una llamada POR SKU (~106k en el modo
completo); el detalle por producto cuesta una POR PRODUCTO (~17.5k) y da lo
mismo salvo NameComplete, que se reconstruye como "<producto> <talle>".

NOMBRES: sku_name es el TALLE crudo ("2", "XL") — es lo que se agrupa para
analizar curva de talles. sku_name_completo es la concatenación reconstruida.

NOTA sobre sku_name para análisis de talles (NO afecta a esta tabla, que va
toda por sku_id): agrupar por sku_name no es confiable en todo el catálogo.
Parte de los productos no codifican el talle en el SKU sino en el nombre del
producto, y el SKU queda con un valor de relleno. Ejemplo real de la corrida
del 2026-08-11: sku_id 130574, sku_name "0", product_name "Canillera Ush
Juveniles Talle S Unisex" — el talle verdadero es la S del nombre. Tenerlo en
cuenta si alguna vez se cruza esta tabla con una guía de talles; para el stock
en sí es irrelevante.

El paso 3 es el caro (una llamada por SKU). --active-only lo limita a los SKUs
con IsActive=true (~18k, ~30 min); sin el flag corre sobre todos (~106k, ~2h,
incluye histórico de productos dados de baja).

INSERCIÓN BQ: load_table_from_json (BATCH — no streaming — costo $0).
IDEMPOTENCIA: DELETE de la snapshot_date + append, igual que sync_vtex.py.
Correr N veces el mismo día deja siempre el mismo resultado.

STOCK INFINITO: has_unlimited_quantity=true → available_quantity queda NULL y
fuera de todas las sumas. Un SKU infinito no es "muchas unidades", es "no
aplica"; sumarlo como 0 o como 9999 miente en las dos direcciones.

Uso:
    python vtex_stock.py --active-only --limit 200      # prueba, CSV local
    python vtex_stock.py --active-only                  # ~18k SKUs, ~30 min
    python vtex_stock.py --active-only --load-bq        # + carga a BigQuery
    python vtex_stock.py --resume                       # retoma corrida cortada
    python vtex_stock.py --concurrency 20 --active-only
"""
import argparse
import csv
import json
import os
import random
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# Auth VTEX: se reutiliza vtex.py en vez de duplicar headers. Esa versión ya
# hace .strip() sobre key y token — sin eso, el \n que arrastran los secrets de
# GitHub rompe con "Invalid header value" (commit c1a1cdd).
from vtex import _base, _headers

load_dotenv()
sys.stdout.reconfigure(encoding="utf-8")

# NO hay default para VTEX_ACCOUNT (se sacó el setdefault al pasar a GitHub
# Actions): un default silencioso en CI apunta a la cuenta equivocada sin avisar.
# La cuenta llega por el secret VTEX_ACCOUNT, y su ausencia tiene que romper el
# run. Todas las credenciales salen del entorno — secrets en Actions, .env en
# local — y este módulo no conoce ninguna ruta fuera de su propio repo.
# La validación vive en main() y no acá a propósito: verify_stock.py importa este
# módulo, y un exit a nivel import lo rompería sin necesitar credenciales VTEX.

# ── Configuración ─────────────────────────────────────────────────────────────
SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE",
                                      "e-coomerce-484513-633cb3db894a.json")
GCP_PROJECT = os.environ.get("GCP_PROJECT", "e-coomerce-484513")
BQ_DATASET  = "vtex_data"
BQ_LOCATION = "US"
TABLE_STOCK = "stock_snapshot"

VIEW_VENDIBLE = "vw_stock_vendible"
VIEW_POR_DEPOSITO = "vw_stock_por_deposito"

# Depósito de la web. Ver docstring: NO es intercambiable con el total.
WAREHOUSE_WEB = "1_1"
WAREHOUSE_ML  = "1_3"

# Argentina = UTC-3 sin DST
ARS_OFFSET = timedelta(hours=3)

# GetProductAndSkuIds acepta ventanas de 50 como máximo.
_PRODUCT_PAGE = 50
_LOG_EVERY    = 500

# IDs que el índice GetProductAndSkuIds devuelve pero que NO existen en el
# catálogo: stockkeepingunitbyproductid/{id} responde 404 siempre. Cuatro son
# basura evidente (1111111112/4/7/9) y el resto son IDs anómalos, fuera del rango
# del catálogo real. Verificados uno por uno el 2026-08-11: los 13 dan 404
# determinístico, mientras productos reales del mismo catálogo dan 200 con SKUs.
#
# Se descartan en el listado y no al fallar: intentarlos suma 13 entradas a
# failed_products, y verify_stock.py trata failed_products como error duro SIN
# umbral de tolerancia, así que dejaba la auditoría en rojo permanente por 13
# productos que no existen. Lista explícita a propósito: un heurístico por
# longitud o por dígitos repetidos podría descartar productos legítimos en
# silencio. Si VTEX los saca del índice, borrarlos de acá no cambia nada.
PRODUCT_IDS_FANTASMA = frozenset({
    "111134", "111242", "111382", "111385", "111742", "115458",
    "118715849", "133673001", "1342378504",
    "1111111112", "1111111114", "1111111117", "1111111119",
})

OUTPUT_DIR     = Path(__file__).parent / "output"
CHECKPOINT_FILE = OUTPUT_DIR / ".stock_checkpoint.json"
ROWS_FILE       = OUTPUT_DIR / ".stock_rows.jsonl"


# ── Fecha ─────────────────────────────────────────────────────────────────────
def snapshot_date_ars() -> date:
    """Fecha calendario de Argentina (UTC-3, sin DST)."""
    return (datetime.now(timezone.utc) - ARS_OFFSET).date()


# ── HTTP con backoff ──────────────────────────────────────────────────────────
def _retry_after_seconds(resp) -> float | None:
    """Retry-After en segundos. Ignora el formato HTTP-date: no lo usa VTEX."""
    raw = resp.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return max(0.0, float(raw.strip()))
    except ValueError:
        return None


def _get(path: str, params: dict | None = None, timeout: int = 30,
         retries: int = 5) -> tuple[object | None, str | None]:
    """
    GET a VTEX con backoff exponencial.

    Reintenta 429 y 5xx (respetando Retry-After cuando viene) y los errores de
    red. Los 4xx distintos de 429 NO se reintentan: un 404 o un 403 no mejoran
    esperando, y reintentarlos sólo alarga una corrida de 18k llamadas.

    Retorna (response, None) o (None, motivo).
    """
    delay = 1.0
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(_base(path), headers=_headers(),
                                params=params, timeout=timeout)
        except requests.RequestException as e:
            if attempt == retries:
                return None, f"red: {e}"
            time.sleep(delay + random.uniform(0, 0.3))
            delay = min(delay * 2, 60)
            continue

        if resp.status_code == 200:
            return resp, None

        if resp.status_code == 429 or resp.status_code >= 500:
            if attempt == retries:
                return None, f"HTTP {resp.status_code}"
            wait = _retry_after_seconds(resp)
            time.sleep((wait if wait is not None else delay) + random.uniform(0, 0.3))
            delay = min(delay * 2, 60)
            continue

        return None, f"HTTP {resp.status_code}"

    return None, "sin intentos"


# ── Paso 1: universo de productos ─────────────────────────────────────────────
def fetch_product_ids(limit: int | None = None) -> list[str]:
    """
    Lista los productId del catálogo vía GetProductAndSkuIds (paginado de a 50).
    limit corta la lista en N productos — sirve para pruebas, no para muestreo
    representativo (los IDs vienen ordenados, no al azar).

    Los IDs de PRODUCT_IDS_FANTASMA se descartan acá y se reportan por pantalla:
    nunca se intentan, así no ensucian failed_products. El descarte NO altera la
    paginación — frm avanza de a _PRODUCT_PAGE sin importar cuántos se filtren.
    """
    product_ids: list[str] = []
    omitidos: list[str] = []
    frm = 1

    def _reportar_omitidos() -> None:
        if omitidos:
            print(f"   ⏭️  {len(omitidos)} IDs fantasma omitidos del índice"
                  f" (404 conocidos): {', '.join(sorted(omitidos))}", flush=True)

    while True:
        resp, err = _get("/api/catalog_system/pvt/products/GetProductAndSkuIds",
                         params={"_from": frm, "_to": frm + _PRODUCT_PAGE - 1})
        if resp is None:
            print(f"  ⚠️  GetProductAndSkuIds falló en _from={frm}: {err}", flush=True)
            break

        payload = resp.json() or {}
        data = payload.get("data") or {}
        if not data:
            break

        for pid in data:
            pid = str(pid)
            if pid in PRODUCT_IDS_FANTASMA:
                omitidos.append(pid)
            else:
                product_ids.append(pid)

        if limit is not None and len(product_ids) >= limit:
            _reportar_omitidos()
            return product_ids[:limit]

        total = (payload.get("range") or {}).get("total")
        frm += _PRODUCT_PAGE
        if total is not None and frm > total:
            break

    _reportar_omitidos()
    return product_ids


# ── Paso 2: SKUs por producto ─────────────────────────────────────────────────
def _first_nonempty(*vals) -> str | None:
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return None


def _sku_meta(raw: dict) -> dict:
    """
    Normaliza un SKU de stockkeepingunitbyproductid.

    OJO con lo que este endpoint NO devuelve. Su respuesta es la forma
    "catálogo": Id, ProductId, Name, RefId, IsActive y medidas. NameComplete,
    ProductName, ProductRefId y AlternateIds vienen SIEMPRE null acá — no es que
    falten en algunos SKUs, no existen en esta proyección. Verificado contra el
    producto 5426 el 2026-08-11. Esos campos se completan en
    fetch_product_detail(), una llamada por producto.

    Name es el TALLE ("2", "XL"), no el nombre del producto.
    """
    alt = raw.get("AlternateIds") or {}
    return {
        "sku_id":     str(raw.get("Id")) if raw.get("Id") is not None else None,
        "product_id": str(raw.get("ProductId")) if raw.get("ProductId") is not None else None,
        # alt.RefId es null en esta proyección; queda como red por si VTEX cambia.
        "sku_ref_id": _first_nonempty(raw.get("RefId"), alt.get("RefId")),
        "sku_name":   _first_nonempty(raw.get("Name")),
        "is_active":  bool(raw.get("IsActive")),
    }


def fetch_product_detail(product_id: str) -> tuple[dict | None, str | None]:
    """
    Nombre y RefId del producto. UNA llamada por PRODUCTO.

    Se usa este endpoint y no stockkeepingunitbyid/{skuId} (que traería
    NameComplete exacto) porque éste escala con la cantidad de productos
    (~17.5k) y el otro con la de SKUs (~106k en el modo completo).
    """
    resp, err = _get(f"/api/catalog/pvt/product/{product_id}")
    if resp is None:
        return None, err
    d = resp.json() or {}
    return {
        "product_name":   _first_nonempty(d.get("Name")),
        "product_ref_id": _first_nonempty(d.get("RefId")),
    }, None


def _name_completo(product_name: str | None, sku_name: str | None) -> str | None:
    """
    Reconstruye el NameComplete de VTEX: "<producto> <talle>".

    Es una RECONSTRUCCIÓN, no el campo original — el endpoint por producto no lo
    devuelve. Coincide con el real en los casos verificados ("Rodillera Procer
    Lisa" + "2" = "Rodillera Procer Lisa 2"), pero si algún SKU usa
    ComplementName puede diferir. El talle crudo queda aparte en sku_name, así
    que agrupar por talle nunca depende de este campo.
    """
    partes = [p for p in (product_name, sku_name) if p]
    return " ".join(partes) if partes else None


def fetch_skus_for_product(product_id: str) -> tuple[list[dict] | None, str | None]:
    """SKUs de un producto (sin datos del producto). (None, motivo) si falla."""
    resp, err = _get(f"/api/catalog_system/pvt/sku/stockkeepingunitbyproductid/{product_id}")
    if resp is None:
        return None, err

    payload = resp.json()
    if isinstance(payload, dict):        # producto sin SKUs devuelve objeto vacío
        payload = []

    skus = [_sku_meta(s) for s in payload]
    return [s for s in skus if s["sku_id"]], None


def fetch_product_bundle(product_id: str) -> tuple[list[dict] | None, str | None, str | None]:
    """
    SKUs de un producto + nombre/RefId del producto. DOS llamadas por PRODUCTO
    — nunca por SKU.

    Retorna (skus, error_de_skus, error_de_detalle). Que falle el detalle NO
    descarta los SKUs: perder el nombre del producto es molesto, perder el stock
    es grave. En ese caso los SKUs salen con product_name/product_ref_id NULL y
    el fallo queda contado aparte.
    """
    skus, err = fetch_skus_for_product(product_id)
    if skus is None:
        return None, err, None

    detail, derr = fetch_product_detail(product_id)
    detail = detail or {"product_name": None, "product_ref_id": None}

    for s in skus:
        s["product_name"]      = detail["product_name"]
        s["product_ref_id"]    = detail["product_ref_id"]
        s["sku_name_completo"] = _name_completo(detail["product_name"], s["sku_name"])

    return skus, None, derr


# ── Paso 3: inventario por SKU ────────────────────────────────────────────────
def fetch_inventory(sku_id: str) -> tuple[list[dict] | None, str | None]:
    """Balance por depósito de un SKU. (None, motivo) si falla."""
    resp, err = _get(f"/api/logistics/pvt/inventory/skus/{sku_id}")
    if resp is None:
        return None, err
    return (resp.json() or {}).get("balance") or [], None


def _inventory_rows(sku: dict, balance: list[dict], ds: str, synced_at: str) -> list[dict]:
    """Una fila por depósito. Sin balance no se emite fila: no hay dato, no se inventa."""
    rows = []
    for b in balance:
        unlimited = bool(b.get("hasUnlimitedQuantity"))
        total     = b.get("totalQuantity")
        reserved  = b.get("reservedQuantity")
        total     = int(total)    if total    is not None else None
        reserved  = int(reserved) if reserved is not None else None

        # Stock infinito → available NULL y afuera de las sumas (ver docstring).
        # oversold sigue la misma regla: sin dato firme no se computa.
        if unlimited or total is None or reserved is None:
            available = None
            oversold  = None
        else:
            available = max(0, total - reserved)
            # reserved > total pasa de verdad (1,5% de las filas en la muestra
            # del 2026-08-11): pedidos comprometidos sin mercadería. available
            # clampeado a 0 es correcto para "cuánto puedo vender", pero se come
            # esa señal; oversold la deja explícita en vez de perderla.
            oversold  = max(0, reserved - total)

        rows.append({
            "sku_id":                 sku["sku_id"],
            "product_id":             sku["product_id"],
            "sku_ref_id":             sku["sku_ref_id"],
            "product_ref_id":         sku["product_ref_id"],
            "sku_name":               sku["sku_name"],
            "sku_name_completo":      sku["sku_name_completo"],
            "product_name":           sku["product_name"],
            "is_active":              sku["is_active"],
            "warehouse_id":           _first_nonempty(b.get("warehouseId")),
            "warehouse_name":         _first_nonempty(b.get("warehouseName")),
            "total_quantity":         total,
            "reserved_quantity":      reserved,
            "available_quantity":     available,
            "oversold_quantity":      oversold,
            "has_unlimited_quantity": unlimited,
            "snapshot_date":          ds,
            "synced_at":              synced_at,
        })
    return rows


# ── Checkpoint resumable ──────────────────────────────────────────────────────
def _checkpoint_key(args, ds: str) -> dict:
    """Identidad de la corrida. Si cambia, el checkpoint viejo no sirve."""
    return {"snapshot_date": ds, "active_only": bool(args.active_only), "limit": args.limit}


def save_checkpoint(key: dict, product_ids: list[str], skus: list[dict],
                    failed_products: list[dict], failed_details: list[dict]) -> None:
    """
    Guarda TAMBIÉN la lista de IDs, no sólo el progreso: sin ella, retomar
    obligaría a re-listar los ~17.5k productos y podría arrancar de un universo
    distinto al de la corrida original (el catálogo cambia entre medio).
    """
    OUTPUT_DIR.mkdir(exist_ok=True)
    CHECKPOINT_FILE.write_text(json.dumps({
        "key":             key,
        "product_ids":     product_ids,
        "skus":            skus,
        "failed_products": failed_products,
        "failed_details":  failed_details,
    }, ensure_ascii=False), encoding="utf-8")


def load_checkpoint(key: dict) -> dict | None:
    if not CHECKPOINT_FILE.exists():
        return None
    try:
        cp = json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if cp.get("key") != key:
        return None
    cp.setdefault("failed_details", [])   # checkpoints previos al detalle por producto
    return cp


def load_done_rows() -> tuple[list[dict], set[str]]:
    """
    Filas ya escritas en la corrida anterior + los sku_id ya consultados.

    Un SKU sin balance en ningún depósito no genera filas, así que se marca con
    una línea centinela: sin eso quedaría fuera de done_ids y --resume lo
    volvería a consultar en cada corrida.
    """
    if not ROWS_FILE.exists():
        return [], set()
    rows, done = [], set()
    with ROWS_FILE.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue        # línea cortada por un kill a mitad de escritura
            done.add(r["sku_id"])
            if not r.get("_sin_balance"):
                rows.append(r)
    return rows, done


def clear_checkpoint() -> None:
    for f in (CHECKPOINT_FILE, ROWS_FILE):
        if f.exists():
            f.unlink()


# ── BigQuery ──────────────────────────────────────────────────────────────────
SCHEMA_STOCK = [
    bigquery.SchemaField("sku_id",                 "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("product_id",             "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("sku_ref_id",             "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("product_ref_id",         "STRING",    mode="NULLABLE"),
    # sku_name = TALLE crudo ("2", "XL"). sku_name_completo = "<producto> <talle>",
    # reconstruido (ver _name_completo). Agrupar por talle usa sku_name.
    bigquery.SchemaField("sku_name",               "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("sku_name_completo",      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("product_name",           "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("is_active",              "BOOL",      mode="NULLABLE"),
    bigquery.SchemaField("warehouse_id",           "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("warehouse_name",         "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("total_quantity",         "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("reserved_quantity",      "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("available_quantity",     "INTEGER",   mode="NULLABLE"),
    # max(0, reserved - total). > 0 significa reservas sin mercadería detrás.
    bigquery.SchemaField("oversold_quantity",      "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("has_unlimited_quantity", "BOOL",      mode="NULLABLE"),
    bigquery.SchemaField("snapshot_date",          "DATE",      mode="REQUIRED"),
    bigquery.SchemaField("synced_at",              "TIMESTAMP", mode="NULLABLE"),
]

CSV_COLUMNS = [f.name for f in SCHEMA_STOCK]


def _bq_client() -> bigquery.Client:
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/cloud-platform",
        ],
    )
    return bigquery.Client(credentials=creds, project=GCP_PROJECT)


def table_ref() -> str:
    return f"{GCP_PROJECT}.{BQ_DATASET}.{TABLE_STOCK}"


def setup_bq(bq: bigquery.Client) -> str:
    ds = bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}")
    ds.location = BQ_LOCATION
    bq.create_dataset(ds, exists_ok=True)

    ref = table_ref()
    t = bigquery.Table(ref, schema=SCHEMA_STOCK)
    t.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="snapshot_date",
        expiration_ms=None,
    )
    bq.create_table(t, exists_ok=True)
    return ref


def delete_snapshot(bq: bigquery.Client, ds: str) -> str | None:
    try:
        bq.query(
            f"DELETE FROM `{table_ref()}` WHERE snapshot_date = @d",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("d", "DATE", ds)]
            ),
        ).result()
        return None
    except Exception as e:
        return str(e)


def bq_load(bq: bigquery.Client, rows: list[dict]) -> str | None:
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA_STOCK,
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    job = bq.load_table_from_json(rows, table_ref(), job_config=job_config)
    try:
        job.result()
    except Exception as e:
        return str(e)
    return str(job.errors) if job.errors else None


def ensure_views(bq: bigquery.Client) -> None:
    """
    Vistas siempre al MAX(snapshot_date): quien las consulta quiere la foto de
    hoy, no un promedio sobre el histórico de particiones.
    """
    tbl = f"`{table_ref()}`"

    sql_vendible = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT}.{BQ_DATASET}.{VIEW_VENDIBLE}` AS
    -- Stock REAL de la web: sólo SKUs activos y sólo el depósito {WAREHOUSE_WEB}.
    -- Sumar todos los depósitos mete el stock de Mercado Libre ({WAREHOUSE_ML})
    -- e infla el vendible ~1%.
    --
    -- El available_quantity > 0 va en un wrapper de AFUERA, no en el WHERE de
    -- adentro: así filtra sobre el SUM ya agregado, que es el disponible real
    -- del SKU en {WAREHOUSE_WEB}. Adentro filtraría fila por fila, antes de
    -- agregar, y es otra pregunta. "Vendible" es lo que se puede vender hoy,
    -- no el catálogo activo: los agotados quedan afuera.
    SELECT * FROM (
      SELECT
        snapshot_date,
        sku_id,
        ANY_VALUE(product_id)     AS product_id,
        ANY_VALUE(sku_ref_id)     AS sku_ref_id,
        ANY_VALUE(product_ref_id) AS product_ref_id,
        ANY_VALUE(sku_name)       AS sku_name,
        ANY_VALUE(product_name)   AS product_name,
        SUM(total_quantity)       AS total_quantity,
        SUM(reserved_quantity)    AS reserved_quantity,
        SUM(available_quantity)   AS available_quantity,
        LOGICAL_OR(has_unlimited_quantity) AS has_unlimited_quantity
      FROM {tbl}
      WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {tbl})
        AND is_active
        AND warehouse_id = '{WAREHOUSE_WEB}'
      GROUP BY snapshot_date, sku_id
    )
    WHERE available_quantity > 0
    """

    sql_por_deposito = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT}.{BQ_DATASET}.{VIEW_POR_DEPOSITO}` AS
    -- Un SKU tiene balance en {WAREHOUSE_WEB} y {WAREHOUSE_ML} a la vez: van en
    -- columnas separadas, no colapsados. disponible_todos_depositos existe para
    -- control de totales — NO es el stock vendible de la web (ese es
    -- disponible_{WAREHOUSE_WEB} filtrando is_active y > 0, o {VIEW_VENDIBLE}).
    SELECT
      snapshot_date,
      sku_id,
      ANY_VALUE(product_id)     AS product_id,
      ANY_VALUE(sku_ref_id)     AS sku_ref_id,
      ANY_VALUE(product_ref_id) AS product_ref_id,
      ANY_VALUE(sku_name)       AS sku_name,
      ANY_VALUE(product_name)   AS product_name,
      LOGICAL_OR(is_active)     AS is_active,
      SUM(IF(warehouse_id = '{WAREHOUSE_WEB}', available_quantity, 0)) AS disponible_{WAREHOUSE_WEB},
      SUM(IF(warehouse_id = '{WAREHOUSE_ML}',  available_quantity, 0)) AS disponible_{WAREHOUSE_ML},
      SUM(IF(warehouse_id NOT IN ('{WAREHOUSE_WEB}', '{WAREHOUSE_ML}'),
             available_quantity, 0))                                   AS disponible_otros,
      SUM(available_quantity)                                          AS disponible_todos_depositos,
      -- Sobreventa: reservas sin mercadería detrás. Va por depósito igual que
      -- lo disponible — un SKU sobrevendido en la web no lo está en ML. Mismo
      -- tratamiento de NULL: los SKUs de stock infinito traen oversold NULL y
      -- SUM los ignora, no los cuenta como 0.
      SUM(IF(warehouse_id = '{WAREHOUSE_WEB}', oversold_quantity, 0))  AS sobreventa_{WAREHOUSE_WEB},
      SUM(IF(warehouse_id = '{WAREHOUSE_ML}',  oversold_quantity, 0))  AS sobreventa_{WAREHOUSE_ML},
      SUM(IF(warehouse_id NOT IN ('{WAREHOUSE_WEB}', '{WAREHOUSE_ML}'),
             oversold_quantity, 0))                                    AS sobreventa_otros,
      SUM(oversold_quantity)                                           AS sobreventa_todos_depositos,
      LOGICAL_OR(has_unlimited_quantity)                               AS has_unlimited_quantity
    FROM {tbl}
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {tbl})
    GROUP BY snapshot_date, sku_id
    """

    for sql in (sql_vendible, sql_por_deposito):
        bq.query(sql).result()


# ── Salida local ──────────────────────────────────────────────────────────────
def csv_path(ds: str) -> Path:
    return OUTPUT_DIR / f"stock_snapshot_{ds.replace('-', '')}.csv"


def meta_path(ds: str) -> Path:
    return OUTPUT_DIR / f"stock_snapshot_{ds.replace('-', '')}_meta.json"


def write_csv(rows: list[dict], ds: str) -> Path:
    OUTPUT_DIR.mkdir(exist_ok=True)
    path = csv_path(ds)
    # utf-8-sig: el CSV se abre en Excel para validar contra el informe manual.
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        w.writeheader()
        w.writerows(rows)
    return path


# ── Resumen de control ────────────────────────────────────────────────────────
def build_summary(rows: list[dict], skus: list[dict], done_ids: set[str],
                  failed_skus: list[dict], failed_products: list[dict],
                  failed_details: list[dict], product_ids: list[str]) -> dict:
    listed_ids = {s["sku_id"] for s in skus}

    by_wh_units  = defaultdict(int)
    by_wh_rows   = defaultdict(int)
    vendible     = 0
    unlimited    = 0
    oversold_u   = 0
    oversold_f   = 0
    for r in rows:
        wh = r["warehouse_id"] or "(sin id)"
        by_wh_rows[wh] += 1
        if r["has_unlimited_quantity"]:
            unlimited += 1
            continue
        av = r["available_quantity"] or 0
        by_wh_units[wh] += av
        ov = r["oversold_quantity"] or 0
        if ov:
            oversold_u += ov
            oversold_f += 1
        if r["is_active"] and wh == WAREHOUSE_WEB:
            vendible += av

    activos   = sum(1 for s in skus if s["is_active"])
    inactivos = len(skus) - activos

    # Duplicados esperados = 0. Se cuentan grupos con más de un sku_id distinto,
    # no filas: la tabla tiene varias filas por SKU (una por depósito) y contar
    # filas daría "duplicados" que son sólo el grano de la tabla.
    por_talle = defaultdict(set)
    por_refid = defaultdict(set)
    for s in skus:
        if s["sku_name"]:
            por_talle[(s["product_id"], s["sku_name"])].add(s["sku_id"])
        if s["sku_ref_id"]:
            por_refid[s["sku_ref_id"]].add(s["sku_id"])

    dup_talle = {k: sorted(v) for k, v in por_talle.items() if len(v) > 1}
    dup_refid = {k: sorted(v) for k, v in por_refid.items() if len(v) > 1}

    # Cobertura de los campos que vienen del detalle por producto. Se mide
    # porque exactamente acá estuvo el bug de la primera corrida: salían vacíos
    # y ni el CSV ni el resumen lo gritaban.
    con_product_name   = sum(1 for s in skus if s.get("product_name"))
    con_product_ref_id = sum(1 for s in skus if s.get("product_ref_id"))
    con_sku_ref_id     = sum(1 for s in skus if s.get("sku_ref_id"))
    con_sku_name       = sum(1 for s in skus if s.get("sku_name"))
    n = len(skus) or 1

    return {
        "productos_listados":   len(product_ids),
        "productos_fallidos":   len(failed_products),
        "productos_sin_detalle": len(failed_details),
        "cobertura": {
            "product_name":   round(con_product_name   / n * 100, 2),
            "product_ref_id": round(con_product_ref_id / n * 100, 2),
            "sku_ref_id":     round(con_sku_ref_id     / n * 100, 2),
            "sku_name":       round(con_sku_name       / n * 100, 2),
        },
        "skus_listados":        len(listed_ids),
        "skus_traidos":         len(done_ids),
        "skus_fallidos":        len(failed_skus),
        "skus_activos":         activos,
        "skus_inactivos":       inactivos,
        "filas":                len(rows),
        "filas_por_deposito":   dict(sorted(by_wh_rows.items())),
        "unidades_por_deposito": dict(sorted(by_wh_units.items())),
        "unidades_total":       sum(by_wh_units.values()),
        "unidades_vendibles":   vendible,
        "unidades_sobreventa":  oversold_u,
        "filas_sobreventa":     oversold_f,
        "filas_stock_infinito": unlimited,
        "warehouses_distintos": sorted(by_wh_rows),
        "dup_talle":            len(dup_talle),
        "dup_refid":            len(dup_refid),
        "dup_talle_ejemplos":   [f"{k[0]} / {k[1]}" for k in list(dup_talle)[:5]],
        "dup_refid_ejemplos":   list(dup_refid)[:5],
    }


def print_summary(s: dict, ds: str, path: Path, loaded_bq: bool) -> None:
    print()
    print("── Resumen de control ──")
    print(f"   snapshot_date          : {ds}  (UTC-3)")
    print(f"   Productos listados     : {s['productos_listados']:,}"
          f"   fallidos: {s['productos_fallidos']:,}"
          f"   sin detalle: {s['productos_sin_detalle']:,}")
    print(f"   SKUs listados          : {s['skus_listados']:,}")
    print(f"   SKUs con inventario    : {s['skus_traidos']:,}")
    print(f"   SKUs fallidos          : {s['skus_fallidos']:,}")
    print(f"   SKUs activos/inactivos : {s['skus_activos']:,} / {s['skus_inactivos']:,}")
    print(f"   Filas (SKU × depósito) : {s['filas']:,}")
    print()
    print(f"   Warehouses distintos   : {', '.join(s['warehouses_distintos']) or '(ninguno)'}")
    for wh in s["warehouses_distintos"]:
        etiqueta = " ← web" if wh == WAREHOUSE_WEB else (" ← ML" if wh == WAREHOUSE_ML else "")
        print(f"     {wh:<10} {s['unidades_por_deposito'].get(wh, 0):>10,} u"
              f"   ({s['filas_por_deposito'].get(wh, 0):,} filas){etiqueta}")
    print(f"   Unidades TOTAL         : {s['unidades_total']:,}"
          f"   (todos los depósitos — no es el vendible)")
    print(f"   Unidades VENDIBLES     : {s['unidades_vendibles']:,}"
          f"   (is_active + {WAREHOUSE_WEB})  ← stock real de la web")
    print(f"   Unidades EN SOBREVENTA : {s['unidades_sobreventa']:,}"
          f"   (reserved > total, en {s['filas_sobreventa']:,} filas)")
    print(f"   Filas stock infinito   : {s['filas_stock_infinito']:,}"
          f"   (available NULL, fuera de las sumas)")
    print()
    print("   Cobertura de campos    : (esperado 100% — si baja, el detalle por producto falló)")
    for campo, pct in s["cobertura"].items():
        marca = "  ⚠️" if pct < 99.0 else ""
        print(f"     {campo:<16} {pct:>6.2f}%{marca}")
    print()
    print(f"   Talles duplicados      : {s['dup_talle']}   (esperado 0)"
          + (f"  ej: {', '.join(s['dup_talle_ejemplos'])}" if s["dup_talle"] else ""))
    print(f"   RefIds duplicados      : {s['dup_refid']}   (esperado 0)"
          + (f"  ej: {', '.join(s['dup_refid_ejemplos'])}" if s["dup_refid"] else ""))
    print()
    print(f"   CSV                    : {path}")
    print(f"   BigQuery               : {'cargado en ' + table_ref() if loaded_bq else 'NO cargado (sin --load-bq)'}")


# ── Validación de entorno ─────────────────────────────────────────────────────
def _exigir_entorno(*names: str) -> None:
    """
    Corta antes de empezar si falta una credencial, en vez de descubrirlo a mitad
    de una corrida de 60 minutos. Sin VTEX_ACCOUNT el KeyError de vtex.py sale
    recién en la primera request, y dentro del ThreadPoolExecutor de la fase ②
    quedaría contado como "producto fallido" en lugar de romper el run.
    """
    faltan = [n for n in names if not (os.environ.get(n) or "").strip()]
    if faltan:
        sys.exit(f"❌ Faltan variables de entorno: {', '.join(faltan)}. "
                 f"En GitHub Actions vienen de los secrets del repo; "
                 f"en local, del .env.")


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser(description="Snapshot de stock VTEX por SKU × depósito")
    ap.add_argument("--concurrency", type=int, default=12,
                    help="Hilos concurrentes contra la API VTEX (default: 12)")
    ap.add_argument("--load-bq", action="store_true",
                    help="Carga a BigQuery además del CSV (default: NO carga)")
    ap.add_argument("--resume", action="store_true",
                    help="Retoma una corrida cortada usando el checkpoint")
    ap.add_argument("--active-only", action="store_true",
                    help="Inventario sólo de SKUs IsActive=true (~18k, ~30 min). "
                         "Sin el flag: todos (~106k, ~2h, incluye histórico de baja)")
    ap.add_argument("--limit", type=int, default=None,
                    help="Corta el universo en N productos (pruebas)")
    args = ap.parse_args()

    _exigir_entorno("VTEX_ACCOUNT", "VTEX_APP_KEY", "VTEX_APP_TOKEN")
    # Con --load-bq, validar el service account ANTES de las ~18k llamadas a
    # VTEX: si falta, la corrida moriría en la fase ④, después de una hora.
    if args.load_bq and not Path(SERVICE_ACCOUNT_FILE).is_file():
        sys.exit(f"❌ --load-bq pero no existe el service account "
                 f"{SERVICE_ACCOUNT_FILE!r} (GOOGLE_SERVICE_ACCOUNT_FILE, "
                 f"relativo al directorio de trabajo).")

    ds        = snapshot_date_ars().isoformat()
    synced_at = datetime.now(timezone.utc).isoformat()
    key       = _checkpoint_key(args, ds)

    print("📦 Snapshot de stock VTEX")
    print(f"   snapshot_date : {ds}  (UTC-3)")
    print(f"   Cuenta VTEX   : {os.environ['VTEX_ACCOUNT']}")
    print(f"   Alcance       : {'sólo activos' if args.active_only else 'TODOS los SKUs (incluye de baja)'}"
          + (f"   límite: {args.limit} productos" if args.limit else ""))
    print(f"   Concurrencia  : {args.concurrency}")
    print(f"   BigQuery      : {'SÍ → ' + table_ref() if args.load_bq else 'no (sólo CSV)'}")
    print()

    OUTPUT_DIR.mkdir(exist_ok=True)

    # Sin --resume la corrida empieza de cero, así que hay que barrer el
    # checkpoint viejo ANTES de nada: el .jsonl de filas se abre en modo append
    # y si no se limpia, una corrida nueva queda mezclada con la anterior. El
    # CSV no lo notaría (se arma desde memoria), pero el próximo --resume leería
    # filas de dos corridas distintas como si fueran una.
    if not args.resume:
        clear_checkpoint()

    # ── Fases 1 y 2: universo de productos y SKUs ─────────────────────────────
    cp = load_checkpoint(key) if args.resume else None
    if cp:
        product_ids       = cp["product_ids"]
        skus              = cp["skus"]
        failed_products   = cp["failed_products"]
        failed_details    = cp["failed_details"]
        print(f"♻️  Checkpoint: {len(product_ids):,} productos / {len(skus):,} SKUs ya listados")
    else:
        if args.resume:
            print("⚠️  Checkpoint ausente o de otra corrida (fecha/flags distintos) — se empieza de cero")
            clear_checkpoint()

        print("① Listando productos ...", flush=True)
        product_ids = fetch_product_ids(limit=args.limit)
        print(f"   {len(product_ids):,} productos", flush=True)

        print("② Listando SKUs y datos de producto ...", flush=True)
        skus, failed_products, failed_details = [], [], []
        done_p = 0
        lock = threading.Lock()
        with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
            futs = {ex.submit(fetch_product_bundle, p): p for p in product_ids}
            for fut in as_completed(futs):
                pid = futs[fut]
                try:
                    got, err, derr = fut.result()
                except Exception as e:
                    got, err, derr = None, f"excepción: {e}", None
                with lock:
                    done_p += 1
                    if got is None:
                        failed_products.append({"product_id": pid, "error": err})
                    else:
                        skus.extend(got)
                        # El detalle fallido no descarta los SKUs: se cuenta aparte.
                        if derr:
                            failed_details.append({"product_id": pid, "error": derr})
                    if done_p % _LOG_EVERY == 0:
                        print(f"   {done_p:,}/{len(product_ids):,} productos"
                              f" — {len(skus):,} SKUs", flush=True)
        print(f"   {len(skus):,} SKUs listados"
              f" ({len(failed_products):,} productos fallidos,"
              f" {len(failed_details):,} sin detalle)", flush=True)
        save_checkpoint(key, product_ids, skus, failed_products, failed_details)

    # Dedup defensivo: un sku_id repetido acá duplicaría sus filas de inventario.
    by_id = {}
    for s in skus:
        by_id.setdefault(s["sku_id"], s)
    skus = list(by_id.values())

    objetivo = [s for s in skus if s["is_active"]] if args.active_only else list(skus)
    print(f"   A consultar inventario: {len(objetivo):,} SKUs"
          f" ({'sólo activos' if args.active_only else 'todos'})")

    # ── Fase 3: inventario ────────────────────────────────────────────────────
    rows, done_ids = ([], set())
    if args.resume:
        rows, done_ids = load_done_rows()
        if done_ids:
            print(f"♻️  {len(done_ids):,} SKUs ya tenían inventario — se saltean")

    pendientes = [s for s in objetivo if s["sku_id"] not in done_ids]
    failed_skus: list[dict] = []

    print(f"③ Consultando inventario de {len(pendientes):,} SKUs ...", flush=True)
    lock = threading.Lock()
    done_n = 0
    rows_fh = ROWS_FILE.open("a", encoding="utf-8")
    try:
        with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
            futs = {ex.submit(fetch_inventory, s["sku_id"]): s for s in pendientes}
            for fut in as_completed(futs):
                sku = futs[fut]
                try:
                    balance, err = fut.result()
                except Exception as e:
                    balance, err = None, f"excepción: {e}"

                with lock:
                    done_n += 1
                    if balance is None:
                        # Un SKU fallido NO frena la corrida: se registra y sigue.
                        failed_skus.append({"sku_id": sku["sku_id"], "error": err})
                    else:
                        new_rows = _inventory_rows(sku, balance, ds, synced_at)
                        rows.extend(new_rows)
                        done_ids.add(sku["sku_id"])
                        if new_rows:
                            for r in new_rows:
                                rows_fh.write(json.dumps(r, ensure_ascii=False) + "\n")
                        else:
                            # Consultado y sin balance: centinela para --resume.
                            rows_fh.write(json.dumps(
                                {"sku_id": sku["sku_id"], "_sin_balance": True}) + "\n")
                        rows_fh.flush()
                    if done_n % _LOG_EVERY == 0:
                        print(f"   {done_n:,}/{len(pendientes):,} SKUs"
                              f" — {len(rows):,} filas, {len(failed_skus):,} fallidos", flush=True)
    finally:
        rows_fh.close()

    print(f"   {len(rows):,} filas ({len(failed_skus):,} SKUs fallidos)")

    # ── Salida ────────────────────────────────────────────────────────────────
    path = write_csv(rows, ds)
    summary = build_summary(rows, objetivo, done_ids, failed_skus,
                            failed_products, failed_details, product_ids)

    # Guardrail: bq_load hace DELETE de la snapshot_date y recién después LOAD.
    # Con rows vacío eso borra la carga del día y no la repone. Es alcanzable sin
    # que salte ninguna excepción: si GetProductAndSkuIds devuelve 403/404,
    # fetch_product_ids avisa y devuelve [], y la corrida sigue con 0 filas. En un
    # job diario desatendido eso sería pérdida de datos silenciosa.
    if args.load_bq and not rows:
        print("\n❌ 0 filas: no se carga a BigQuery, para no borrar la snapshot "
              "del día con un DELETE seguido de un LOAD vacío.")
        return 1

    # ── BigQuery ──────────────────────────────────────────────────────────────
    loaded_bq = False
    if args.load_bq:
        print()
        print("④ Cargando a BigQuery ...", flush=True)
        bq = _bq_client()
        setup_bq(bq)
        err = delete_snapshot(bq, ds)      # idempotencia: borrar la fecha y recargar
        if err:
            print(f"   ❌ DELETE falló: {err}")
            return 1
        err = bq_load(bq, rows)
        if err:
            print(f"   ❌ Carga falló: {err}")
            return 1
        ensure_views(bq)
        loaded_bq = True
        print(f"   ✅ {len(rows):,} filas en {table_ref()}")
        print(f"   ✅ Vistas {VIEW_VENDIBLE} y {VIEW_POR_DEPOSITO} actualizadas")

    meta_path(ds).write_text(json.dumps({
        "snapshot_date":   ds,
        "active_only":     bool(args.active_only),
        "limit":           args.limit,
        "loaded_bq":       loaded_bq,
        "csv":             str(path),
        "listed_sku_ids":  sorted(s["sku_id"] for s in objetivo),
        "fetched_sku_ids": sorted(done_ids),
        "failed_skus":     failed_skus,
        "failed_details":  failed_details,
        "failed_products": failed_products,
        "summary":         summary,
    }, ensure_ascii=False), encoding="utf-8")

    print_summary(summary, ds, path, loaded_bq)

    # El checkpoint se borra sólo si no quedó nada pendiente: con SKUs fallidos
    # conviene conservarlo para poder --resume y reintentar sólo esos.
    if not failed_skus and not failed_products:
        clear_checkpoint()
    else:
        print(f"\n   ℹ️  Checkpoint conservado — reintentar los fallidos con --resume")

    return 0


if __name__ == "__main__":
    sys.exit(main())
