#!/usr/bin/env python
"""
Auditoría de integridad de la snapshot de stock: verifica que la corrida de
vtex_stock.py haya traído TODO lo que se propuso traer, y que lo que quedó en
BigQuery sea exactamente lo que quedó en el CSV.

Existe porque una snapshot de inventario falla en silencio de una forma que la
de órdenes no: vtex_stock.py está diseñado para que un SKU fallido no frene la
corrida (con ~18k llamadas, cortar en el primer 429 sería inusable). Esa
tolerancia es correcta durante la corrida y peligrosa después: el CSV se
escribe igual, el resumen se imprime igual, y un stock vendible 5% más bajo de
lo real se ve perfectamente normal. Nada en la salida grita. Este script grita.

Chequea cuatro cosas:
  1. CONJUNTOS de sku_id: los listados en el catálogo vs los que efectivamente
     tienen inventario. Se comparan como sets, no como cantidades: si un SKU
     faltara y otro sobrara los conteos cuadrarían y el agujero pasaría
     desapercibido.
  2. failed_skus: cualquier SKU que la API no devolvió.
  3. CSV vs BigQuery: filas y conjunto de sku_id de la snapshot_date.
  4. Coherencia del grano y de las reglas de negocio (avisos).

Criterio de falla dura (exit 1):
  - SKUs listados que no llegaron al CSV por encima del umbral --tolerancia
  - failed_skus > 0
  - filas o sku_id del CSV que no coinciden con BigQuery

Criterio de aviso (no rompe):
  - SKUs en el CSV que ya no estaban en la lista original (catálogo que cambió
    entre el listado y la consulta de inventario — no es stock perdido)
  - talles o RefIds duplicados
  - depósitos inesperados además de 1_1 / 1_3
  - available_quantity NULL por stock infinito

El chequeo 3 se SALTEA, y se marca explícitamente como salteado, cuando la
corrida no usó --load-bq. Un chequeo que no corrió no es un chequeo que pasó.

Uso:
    python verify_stock.py                          # última snapshot (hoy, UTC-3)
    python verify_stock.py --date 2026-08-11
    python verify_stock.py --tolerancia 0.5         # % de SKUs faltantes tolerado
    python verify_stock.py --warn-only              # reporta pero no falla
"""
import argparse
import csv
import json
import sys

# Reutiliza la lógica REAL de la sync (cliente BQ, refs de tabla, rutas de
# salida, depósitos). Reimplementarlas acá haría que el verificador y lo
# verificado pudieran divergir sin que nadie se entere.
from vtex_stock import (
    WAREHOUSE_ML,
    WAREHOUSE_WEB,
    _bq_client,
    csv_path,
    meta_path,
    snapshot_date_ars,
    table_ref,
)


def _leer_csv(path):
    """Devuelve (filas, sku_ids). Las cantidades vienen como str desde el CSV."""
    with path.open(encoding="utf-8-sig", newline="") as fh:
        filas = list(csv.DictReader(fh))
    return filas, {f["sku_id"] for f in filas}


def _int_o_none(v):
    if v is None or v == "":
        return None
    try:
        return int(v)
    except ValueError:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", type=str, default=None,
                    help="snapshot_date a auditar YYYY-MM-DD (default: hoy UTC-3)")
    ap.add_argument("--tolerancia", type=float, default=0.5,
                    help="%% de SKUs listados que puede faltar sin romper (default: 0.5)")
    ap.add_argument("--warn-only", action="store_true",
                    help="Reporta pero no falla (exit 0 siempre)")
    args = ap.parse_args()

    ds = args.date or snapshot_date_ars().isoformat()

    mp = meta_path(ds)
    cp = csv_path(ds)

    print("🔍 Auditoría de stock_snapshot")
    print(f"   snapshot_date : {ds}")
    print(f"   CSV           : {cp}")
    print(f"   Tabla         : {table_ref()}")
    print()

    if not mp.exists() or not cp.exists():
        falta = mp if not mp.exists() else cp
        print(f"❌ No hay corrida para {ds}: falta {falta}")
        print(f"   Correr primero: python vtex_stock.py --active-only")
        return 0 if args.warn_only else 1

    meta = json.loads(mp.read_text(encoding="utf-8"))
    filas_csv, skus_csv = _leer_csv(cp)

    listados = set(meta["listed_sku_ids"])
    fallidos = meta["failed_skus"]
    fallidos_prod = meta["failed_products"]
    fallidos_det  = meta.get("failed_details", [])
    cargo_bq = bool(meta["loaded_bq"])

    duro = []      # rompe el run
    aviso = []     # sólo se reporta

    # ── 1. Conjuntos de sku_id: catálogo vs CSV ───────────────────────────────
    # sin_inventario incluye tanto los que fallaron como los que la API devolvió
    # sin balance en ningún depósito. Los dos son SKUs sobre los que la snapshot
    # no dice nada, y para el que consulte el stock son indistinguibles.
    sin_inventario = listados - skus_csv
    de_mas         = skus_csv - listados
    pct = (len(sin_inventario) / len(listados) * 100) if listados else 0.0

    print("① SKUs listados vs con inventario")
    print(f"   Listados en catálogo : {len(listados):,}")
    print(f"   Presentes en el CSV  : {len(skus_csv):,}")
    print(f"   Sin inventario       : {len(sin_inventario):,}  ({pct:.2f}%)")
    if sin_inventario:
        print(f"     ej: {', '.join(sorted(sin_inventario)[:5])}")
    if pct > args.tolerancia:
        duro.append(f"{len(sin_inventario):,} SKUs ({pct:.2f}%) sin inventario, "
                    f"supera la tolerancia de {args.tolerancia}%")
    if de_mas:
        # No es stock perdido: el catálogo pudo cambiar entre el paso 2 y el 3.
        aviso.append(f"{len(de_mas):,} sku_id en el CSV que no estaban en la lista original")
    print()

    # ── 2. Fallos de API ──────────────────────────────────────────────────────
    print("② Fallos de API")
    print(f"   SKUs fallidos      : {len(fallidos):,}")
    print(f"   Productos fallidos : {len(fallidos_prod):,}")
    print(f"   Productos s/detalle: {len(fallidos_det):,}")
    if fallidos:
        motivos = {}
        for f in fallidos:
            motivos[f["error"]] = motivos.get(f["error"], 0) + 1
        for m, n in sorted(motivos.items(), key=lambda x: -x[1])[:5]:
            print(f"     {n:>6,} × {m}")
        duro.append(f"{len(fallidos):,} SKUs quedaron sin consultar por error de API")
    if fallidos_prod:
        duro.append(f"{len(fallidos_prod):,} productos no se pudieron listar — "
                    f"sus SKUs no están ni en la lista original")
    if fallidos_det:
        # No es stock perdido: el inventario de esos SKUs está, les falta el
        # nombre del producto. Por eso avisa y no rompe.
        aviso.append(f"{len(fallidos_det):,} productos sin detalle — sus SKUs quedaron "
                     f"con product_name/product_ref_id NULL (el stock sí está). "
                     f"--resume NO los reintenta: hay que borrar el checkpoint y recorrer")
    print()

    # ── 2b. Cobertura de campos del detalle por producto ──────────────────────
    # Chequeo agregado después de la primera corrida del 2026-08-11, donde
    # product_name y product_ref_id salieron vacíos en el 100% de las filas
    # porque el endpoint no los devolvía. El CSV se escribió igual y el resumen
    # dio todo verde: nada avisó. Esto avisa.
    print("②b Cobertura de campos")
    campos = ["sku_id", "product_id", "sku_ref_id", "product_ref_id",
              "sku_name", "sku_name_completo", "product_name"]
    total = len(filas_csv) or 1
    for c in campos:
        if c not in (filas_csv[0] if filas_csv else {}):
            duro.append(f"la columna {c} no existe en el CSV")
            continue
        llenas = sum(1 for f in filas_csv if (f[c] or "").strip())
        pct = llenas / total * 100
        marca = "  ⚠️" if pct < 99.0 else ""
        print(f"   {c:<18} {pct:>6.2f}%{marca}")
        # sku_ref_id es legítimamente opcional en VTEX; el resto no debería faltar.
        if pct < 99.0 and c != "sku_ref_id":
            aviso.append(f"{c} vacío en el {100 - pct:.2f}% de las filas")
        if pct == 0.0:
            duro.append(f"{c} vacío en el 100% de las filas — la fuente del campo "
                        f"no está devolviendo el dato")
    print()

    # ── 3. CSV vs BigQuery ────────────────────────────────────────────────────
    print("③ CSV vs BigQuery")
    if not cargo_bq:
        print("   ⏭️  SALTEADO — la corrida no usó --load-bq.")
        print("      NO se verificó BigQuery. Este chequeo no corrió; no dice que esté bien.")
    else:
        bq = _bq_client()
        rows = list(bq.query(f"""
            SELECT sku_id, warehouse_id
            FROM `{table_ref()}`
            WHERE snapshot_date = '{ds}'
        """).result())
        skus_bq  = {r["sku_id"] for r in rows}
        print(f"   Filas CSV : {len(filas_csv):,}")
        print(f"   Filas BQ  : {len(rows):,}")
        print(f"   sku_id CSV: {len(skus_csv):,}   sku_id BQ: {len(skus_bq):,}")

        if len(rows) != len(filas_csv):
            duro.append(f"filas BQ ({len(rows):,}) ≠ filas CSV ({len(filas_csv):,})")
        faltan_bq = skus_csv - skus_bq
        sobran_bq = skus_bq - skus_csv
        if faltan_bq:
            duro.append(f"{len(faltan_bq):,} sku_id del CSV no están en BQ "
                        f"(ej: {', '.join(sorted(faltan_bq)[:3])})")
        if sobran_bq:
            duro.append(f"{len(sobran_bq):,} sku_id en BQ que no están en el CSV — "
                        f"el DELETE de la snapshot_date no limpió del todo")
    print()

    # ── 4. Grano y reglas de negocio ──────────────────────────────────────────
    print("④ Grano y reglas de negocio")
    por_deposito = {}
    grano = {}
    infinitos = 0
    vendible = 0
    oversold = 0
    oversold_filas = 0
    for f in filas_csv:
        wh = f["warehouse_id"] or "(sin id)"
        clave = (f["sku_id"], wh)
        grano[clave] = grano.get(clave, 0) + 1
        if f["has_unlimited_quantity"] in ("True", "true", "1"):
            infinitos += 1
            continue
        av = _int_o_none(f["available_quantity"]) or 0
        por_deposito[wh] = por_deposito.get(wh, 0) + av
        ov = _int_o_none(f.get("oversold_quantity")) or 0
        if ov:
            oversold += ov
            oversold_filas += 1
        # Coherencia del cálculo: available y oversold salen de los mismos dos
        # números y son excluyentes. Que los dos den > 0 en la misma fila sería
        # un error de cómputo, no un dato raro de VTEX.
        if av > 0 and ov > 0:
            duro.append(f"sku {f['sku_id']} dep {wh}: available={av} y oversold={ov} "
                        f"a la vez — el cálculo está mal")
        if f["is_active"] in ("True", "true", "1") and wh == WAREHOUSE_WEB:
            vendible += av

    dup_grano = {k: v for k, v in grano.items() if v > 1}
    print(f"   Depósitos          : {', '.join(sorted(por_deposito)) or '(ninguno)'}")
    for wh in sorted(por_deposito):
        print(f"     {wh:<10} {por_deposito[wh]:>10,} u")
    print(f"   Unidades vendibles : {vendible:,}  (is_active + {WAREHOUSE_WEB})")
    print(f"   En sobreventa      : {oversold:,} u en {oversold_filas:,} filas "
          f"(reserved > total)")
    print(f"   Filas duplicadas   : {len(dup_grano)}  (mismo sku_id × depósito, esperado 0)")
    print(f"   Stock infinito     : {infinitos:,} filas con available NULL")

    if dup_grano:
        # Esto sí es duro: rompe el grano de la tabla y dobla las sumas.
        ej = list(dup_grano)[:3]
        duro.append(f"{len(dup_grano)} pares (sku_id, warehouse_id) repetidos "
                    f"— el grano de la tabla está roto (ej: {ej})")

    inesperados = set(por_deposito) - {WAREHOUSE_WEB, WAREHOUSE_ML}
    if inesperados:
        aviso.append(f"depósitos inesperados además de {WAREHOUSE_WEB}/{WAREHOUSE_ML}: "
                     f"{', '.join(sorted(inesperados))} — revisar si entran en el vendible")
    if WAREHOUSE_WEB not in por_deposito:
        duro.append(f"no hay ninguna fila del depósito {WAREHOUSE_WEB} — "
                    f"el stock vendible da 0 y no es real")
    if infinitos:
        aviso.append(f"{infinitos:,} filas con stock infinito (available NULL, "
                     f"fuera de las sumas)")

    s = meta["summary"]
    if s["dup_talle"]:
        aviso.append(f"{s['dup_talle']} talles duplicados (esperado 0): "
                     f"{', '.join(s['dup_talle_ejemplos'])}")
    if s["dup_refid"]:
        aviso.append(f"{s['dup_refid']} RefIds duplicados (esperado 0): "
                     f"{', '.join(s['dup_refid_ejemplos'])}")

    # ── Veredicto ─────────────────────────────────────────────────────────────
    print()
    print("── Resumen ──")
    for a in aviso:
        print(f"   ⚠️  {a}")
    for d in duro:
        print(f"   ❌ {d}")
    if not cargo_bq:
        print(f"   ⏭️  Chequeo CSV-vs-BigQuery NO ejecutado (corrida sin --load-bq)")

    if not duro:
        if aviso:
            print("\n✅ Integridad OK. Hay avisos — ver detalle arriba.")
        else:
            print("\n✅ Integridad OK: la snapshot está completa y coherente.")
        return 0

    print("\n❌ INTEGRIDAD ROTA")
    if fallidos or len(sin_inventario):
        print(f"   Reintentar sólo lo que falta: python vtex_stock.py --resume"
              + (" --active-only" if meta["active_only"] else ""))
    if args.warn_only:
        print("   (--warn-only: no se marca el run como fallido)")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
