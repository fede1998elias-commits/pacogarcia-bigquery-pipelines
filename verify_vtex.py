#!/usr/bin/env python
"""
Auditoría de integridad de vtex_data: compara órdenes reales en VTEX contra lo
cargado en BigQuery, día por día, y FALLA (exit 1) si falta alguna venta.

Existe porque el 2026-07-27 se detectó que la sync sólo cargaba cada fecha una
vez, a media mañana, dejando afuera todas las órdenes del resto del día (827
órdenes perdidas entre 2026-06-10 y 2026-07-27). El fix es la ventana móvil de
sync_vtex.py; este script es la red que avisa si el problema vuelve por
cualquier otra vía.

Chequea tres cosas:
  1. Conteo VTEX vs BQ por fecha (días CERRADOS: hoy queda excluido porque
     todavía está recibiendo órdenes y es normal que esté parcial).
  2. Que no haya order_id duplicados.
  3. Que las órdenes con items_count > 0 tengan sus filas en order_items.

Criterio de falla: FALTANTES (VTEX > BQ), duplicados y órdenes sin items rompen
el run. Filas de MÁS en BQ (BQ > VTEX) sólo avisan: no son ventas perdidas y
pueden venir de una orden que VTEX dejó de listar, así que no justifican cortar
el pipeline.

Una fecha que VTEX no responde también rompe el run (una fecha sin verificar es
una fecha sin garantía), pero recién después de reintentarla con una pausa larga:
el run #68 del daily_sync (2026-08-15) se cayó por un 429 pasajero en una sola
fecha, con 0 órdenes faltantes en las 30. Ese caso ya no rompe nada.

Uso:
    python verify_vtex.py --days 7                    # ventana a auditar
    python verify_vtex.py --days 30 --warn-only       # no falla, sólo reporta
    python verify_vtex.py --start-date 2026-06-10 --end-date 2026-07-28
    python verify_vtex.py --days 7 --include-today    # incluye el día en curso
"""
import argparse
import sys
import time
from datetime import date, timedelta

# Reutiliza la MISMA lógica de ventana ARS y de filtrado de la sync, para que la
# comparación no dependa de una reimplementación que pueda divergir.
from sync_vtex import (
    _bq_client,
    fetch_orders_for_date,
    GCP_PROJECT,
    BQ_DATASET,
    TABLE_ORDERS,
    TABLE_ITEMS,
)

# Throttle propio contra el rate limit de VTEX. Ver el comentario del loop
# principal: sin esto la auditoría se autoinflige 429s.
PAUSA_ENTRE_DIAS = 1.0
PAUSA_REINTENTO  = 60.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7,
                    help="Días hacia atrás a auditar (default: 7)")
    ap.add_argument("--start-date", type=str, default=None,
                    help="Inicio explícito YYYY-MM-DD (override --days)")
    ap.add_argument("--end-date", type=str, default=None,
                    help="Fin explícito YYYY-MM-DD (default: ayer, o hoy con --include-today)")
    ap.add_argument("--include-today", action="store_true",
                    help="Incluye el día en curso. Sólo para inspección manual: a media "
                         "tarde el día todavía no cerró y va a aparecer como faltante.")
    ap.add_argument("--warn-only", action="store_true",
                    help="Reporta pero no falla (exit 0 siempre)")
    args = ap.parse_args()

    ref_orders = f"{GCP_PROJECT}.{BQ_DATASET}.{TABLE_ORDERS}"
    ref_items  = f"{GCP_PROJECT}.{BQ_DATASET}.{TABLE_ITEMS}"
    bq = _bq_client()

    today = date.today()
    # El día en curso se excluye por defecto: a esta hora VTEX sigue tomando pedidos.
    if args.end_date:
        last = date.fromisoformat(args.end_date)
    else:
        last = today if args.include_today else today - timedelta(days=1)

    if args.start_date:
        first = date.fromisoformat(args.start_date)
    else:
        first = last - timedelta(days=args.days - 1)

    print("🔍 Auditoría VTEX vs BigQuery")
    print(f"   Rango auditado: {first} → {last}"
          f"{'  (incluye el día en curso, parcial por definición)' if last >= today else ''}")
    print(f"   Tabla: {ref_orders}")
    print()

    # Se comparan CONJUNTOS de order_id, no cantidades: si un día tuviera una
    # orden faltante y otra de más, los conteos cuadrarían y el bug pasaría
    # desapercibido. Con sets eso no puede pasar.
    rows = bq.query(f"""
        SELECT creation_date AS d, order_id
        FROM `{ref_orders}`
        WHERE creation_date BETWEEN '{first}' AND '{last}'
    """).result()
    en_bq: dict[str, set] = {}
    for r in rows:
        en_bq.setdefault(r["d"].isoformat(), set()).add(r["order_id"])

    faltantes = {}
    sobrantes = {}

    def verificar_dia(ds: str) -> bool:
        """
        Compara una fecha VTEX vs BQ y anota el resultado en faltantes/sobrantes.
        Retorna False sólo si VTEX no respondió (fecha SIN verificar); True si se
        pudo comparar, haya cuadrado o no.

        Está extraída del loop para que la segunda pasada pueda reusarla y
        reintentar las fechas que se cayeron por rate limit. Las escrituras a
        faltantes/sobrantes son por asignación, así que reprocesar una fecha es
        idempotente.
        """
        orders = fetch_orders_for_date(date.fromisoformat(ds))
        if orders is None:
            print(f"  ⚠️  {ds}: error de API VTEX — no se pudo verificar")
            return False

        ids_vtex = {o["orderId"] for o in orders if o.get("orderId")}
        ids_bq   = en_bq.get(ds, set())
        v, b     = len(ids_vtex), len(ids_bq)

        sin_cargar = ids_vtex - ids_bq
        de_mas     = ids_bq - ids_vtex

        # No es elif: un día puede tener faltantes Y sobrantes a la vez, y los
        # dos tienen que quedar registrados.
        if sin_cargar:
            faltantes[ds] = len(sin_cargar)
            muestra = ", ".join(sorted(sin_cargar)[:3])
            print(f"  ❌ {ds}: VTEX {v} vs BQ {b} → FALTAN {len(sin_cargar)}  (ej: {muestra})")
        if de_mas:
            sobrantes[ds] = len(de_mas)
            print(f"  ⚠️  {ds}: VTEX {v} vs BQ {b} → BQ tiene {len(de_mas)} order_id que VTEX ya no lista")
        if not sin_cargar and not de_mas:
            print(f"  ✅ {ds}: {v} = {b}")
        return True

    errores_api = []
    cur = first
    while cur <= last:
        if not verificar_dia(cur.isoformat()):
            errores_api.append(cur.isoformat())
        cur += timedelta(days=1)
        # Pausa entre días: este script le pide a VTEX los MISMOS 30 días que la
        # sync acabó de traer, y arranca en el mismo segundo en que la sync
        # termina. Sin pausa VTEX rate-limitea a mitad de la auditoría — 25
        # respuestas 429 en el run #68 (2026-08-15), que dejó una fecha sin
        # verificar y pintó el run de rojo con 0 órdenes faltantes. 30 s de
        # espera total al lado de los ~70 s que ya tarda la ventana es ruido.
        if cur <= last:
            time.sleep(PAUSA_ENTRE_DIAS)

    # Segunda pasada. Un 429 no es un agujero de datos, pero una fecha sin
    # verificar tampoco es una fecha garantizada: se reintenta una vez con una
    # pausa larga, y sólo lo que sobrevive al reintento cuenta como ciego. El
    # estándar no baja — abajo, errores_api sigue rompiendo el run.
    if errores_api:
        print(f"\n  ⏳ {len(errores_api)} fecha(s) sin verificar — reintento en "
              f"{PAUSA_REINTENTO:.0f} s para dejar pasar el rate limit de VTEX")
        time.sleep(PAUSA_REINTENTO)
        errores_api = [ds for ds in errores_api if not verificar_dia(ds)]

    dups = list(bq.query(f"""
        SELECT order_id, COUNT(*) n
        FROM `{ref_orders}`
        WHERE creation_date BETWEEN '{first}' AND '{last}'
        GROUP BY order_id HAVING COUNT(*) > 1
    """).result())

    huerfanas = list(bq.query(f"""
        SELECT o.order_id, o.items_count
        FROM `{ref_orders}` o
        LEFT JOIN (SELECT DISTINCT order_id FROM `{ref_items}`) i USING (order_id)
        WHERE o.creation_date BETWEEN '{first}' AND '{last}'
          AND o.items_count > 0 AND i.order_id IS NULL
    """).result())

    print()
    print("── Resumen ──")
    print(f"   Órdenes faltantes    : {sum(faltantes.values())} en {len(faltantes)} fechas")
    print(f"   Filas de más en BQ   : {sum(sobrantes.values())} en {len(sobrantes)} fechas")
    print(f"   order_id duplicados  : {len(dups)}")
    print(f"   Órdenes sin items    : {len(huerfanas)}")
    print(f"   Fechas no verificadas: {len(errores_api)} (error de API, tras reintento)")

    # Sobrantes NO rompen el run: no son ventas perdidas. Se reportan arriba.
    # errores_api SÍ rompen: una fecha que no se pudo verificar es una fecha sin
    # garantía, y el sentido de este script es que no queden agujeros ciegos.
    # A esta altura ya pasaron por el reintento de arriba, así que un 429 suelto
    # no llega hasta acá: lo que queda es un problema real de acceso a VTEX.
    problemas = bool(faltantes or dups or huerfanas or errores_api)
    if not problemas:
        if sobrantes:
            print("\n✅ Sin ventas perdidas. (Hay filas de más en BQ — ver detalle arriba.)")
        else:
            print("\n✅ Integridad OK: BigQuery coincide con VTEX en toda la ventana.")
        return 0

    print("\n❌ INTEGRIDAD ROTA")
    if faltantes:
        print(f"   Faltan ventas en BigQuery en {len(faltantes)} fechas.")
        print(f"   Arreglar con: python sync_vtex.py --reprocess "
              f"--start-date {min(faltantes)} --end-date {max(faltantes)}")
        print(f"   (fechas afectadas: {', '.join(sorted(faltantes))})")
    if dups:
        print(f"   Duplicados en {len(dups)} order_id — revisar el borrado por fecha.")
    if huerfanas:
        print(f"   {len(huerfanas)} órdenes con items_count > 0 sin filas en order_items "
              f"— la carga de items falló después de cargar las órdenes.")
    if errores_api:
        print(f"   {len(errores_api)} fechas quedaron SIN VERIFICAR por error de API VTEX, "
              f"incluso tras el reintento: {', '.join(errores_api)}")
        print(f"   Ojo: esto NO dice que falten ventas — dice que no se pudo comprobar. "
              f"Reintentar a mano con: python verify_vtex.py "
              f"--start-date {min(errores_api)} --end-date {max(errores_api)}")

    if args.warn_only:
        print("   (--warn-only: no se marca el run como fallido)")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
