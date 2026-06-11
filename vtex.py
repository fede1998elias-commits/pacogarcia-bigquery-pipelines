"""
Cliente VTEX — OMS y Catálogo.
Credenciales vía variables de entorno: VTEX_ACCOUNT, VTEX_APP_KEY, VTEX_APP_TOKEN
"""
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

_CACHE_FILE = Path(__file__).parent / "product_cache.json"
_CACHE_TTL  = timedelta(hours=24)

_ACCOUNT   = lambda: os.environ["VTEX_ACCOUNT"]
_APP_KEY   = lambda: os.environ["VTEX_APP_KEY"]
_APP_TOKEN = lambda: os.environ["VTEX_APP_TOKEN"]

_PAGE_SIZE = 100  # máximo permitido por la OMS API


def _headers() -> dict:
    return {
        "X-VTEX-API-AppKey":   _APP_KEY(),
        "X-VTEX-API-AppToken": _APP_TOKEN(),
        "Accept":              "application/json",
        "Content-Type":        "application/json",
    }


def _base(path: str) -> str:
    return "https://" + _ACCOUNT() + ".vtexcommercestable.com.br" + path


def _extract_items(raw_items: list) -> list:
    return [
        {
            "productId": i.get("productId"),
            "quantity":  i.get("quantity"),
            "price":     i.get("price"),
        }
        for i in (raw_items or [])
    ]


def fetch_order_detail(order_id: str) -> dict:
    """Devuelve el detalle completo de un pedido, incluyendo items."""
    resp = requests.get(_base(f"/api/oms/pvt/orders/{order_id}"), headers=_headers(), timeout=30)
    resp.raise_for_status()
    o = resp.json()
    marketing = o.get("marketingData") or {}
    return {
        "orderId":      o.get("orderId"),
        "creationDate": o.get("creationDate"),
        "status":       o.get("status"),
        "totalValue":   o.get("value"),
        "paymentNames": o.get("paymentData", {}).get("transactions", [{}])[0].get("payments", [{}])[0].get("paymentSystemName"),
        "utmSource":    marketing.get("utmSource"),
        "utmCampaign":  marketing.get("utmCampaign"),
        "items":        _extract_items(o.get("items")),
    }


def fetch_orders(days: int = 30, with_items: bool = False) -> list[dict]:
    """
    Trae pedidos de los últimos N días desde la OMS API (paginado).
    with_items=True hace una llamada extra por pedido para traer los items
    — usarlo solo con rangos cortos (days<=7) para evitar timeouts.
    """
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S")
    orders = []
    page = 1

    while True:
        resp = requests.get(
            _base("/api/oms/pvt/orders"),
            headers=_headers(),
            params={
                "f_creationDate": "creationDate:[" + since + " TO 2999-01-01T00:00:00]",
                "page":           page,
                "per_page":       _PAGE_SIZE,
                "orderBy":        "creationDate,desc",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        raw_list = data.get("list", [])
        if not raw_list:
            break

        for o in raw_list:
            marketing = o.get("marketingData") or {}
            orders.append({
                "orderId":      o.get("orderId"),
                "creationDate": o.get("creationDate"),
                "status":       o.get("status"),
                "totalValue":   o.get("totalValue"),
                "paymentNames": o.get("paymentNames"),
                "utmSource":    marketing.get("utmSource"),
                "utmCampaign":  marketing.get("utmCampaign"),
                "items":        [],
            })

        paging = data.get("paging", {})
        if page >= paging.get("pages", 1):
            break
        page += 1

    if with_items:
        for order in orders:
            try:
                detail = fetch_order_detail(order["orderId"])
                order["items"] = detail["items"]
            except Exception:
                pass

    return orders


def _load_cache() -> dict:
    if _CACHE_FILE.exists():
        try:
            return json.loads(_CACHE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_cache(cache: dict) -> None:
    _CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def get_product_detail(product_id: str) -> dict | None:
    """
    Devuelve name, category, brand, categoryId, brandId para un producto.
    Usa product_cache.json como caché local con TTL de 24 horas.
    """
    product_id = str(product_id)
    cache = _load_cache()
    entry = cache.get(product_id)

    if entry:
        cached_at = datetime.fromisoformat(entry["cached_at"])
        if datetime.now(timezone.utc) - cached_at < _CACHE_TTL:
            return {k: entry[k] for k in ("name", "category", "brand", "categoryId", "brandId")}

    for attempt in range(1, 4):
        try:
            resp = requests.get(
                _base("/api/catalog_system/pub/products/search"),
                headers=_headers(),
                params={"fq": "productId:" + product_id},
                timeout=15,
            )
            break
        except Exception:
            if attempt < 3:
                time.sleep(2 ** attempt)
            else:
                return None
    if not resp.ok or not resp.json():
        return None

    p = resp.json()[0]
    detail = {
        "name":       p.get("productName"),
        "category":   p.get("categories", [""])[0].lstrip("/") if p.get("categories") else None,
        "brand":      p.get("brand"),
        "categoryId": p.get("categoryId"),
        "brandId":    p.get("brandId"),
    }

    cache[product_id] = {**detail, "cached_at": datetime.now(timezone.utc).isoformat()}
    _save_cache(cache)
    return detail


def enrich_orders(orders: list[dict]) -> list[dict]:
    """
    Recibe la lista de fetch_orders(with_items=True) y agrega
    productName, category y brand a cada item.
    """
    for order in orders:
        for item in order.get("items") or []:
            detail = get_product_detail(item.get("productId"))
            if detail:
                item["productName"] = detail["name"]
                item["category"]    = detail["category"]
                item["brand"]       = detail["brand"]
            else:
                item["productName"] = None
                item["category"]    = None
                item["brand"]       = None
    return orders


def fetch_products(page: int = 1, page_size: int = 50) -> list[dict]:
    """
    Trae productos del catálogo con productId, name, categoryId.
    Usa /api/catalog/pvt/product/{id} para el detalle.
    """
    resp = requests.get(
        _base("/api/catalog_system/pvt/products/GetProductAndSkuIds"),
        headers=_headers(),
        params={
            "_from": (page - 1) * page_size + 1,
            "_to":   page * page_size,
        },
        timeout=30,
    )
    resp.raise_for_status()
    raw = resp.json().get("data", {})

    products = []
    for product_id in raw:
        detail_resp = requests.get(
            _base("/api/catalog/pvt/product/" + str(product_id)),
            headers=_headers(),
            timeout=30,
        )
        if not detail_resp.ok:
            products.append({"productId": product_id, "name": None, "categoryId": None})
            continue
        detail = detail_resp.json()
        products.append({
            "productId":  product_id,
            "name":       detail.get("Name"),
            "categoryId": detail.get("CategoryId"),
        })

    return products
