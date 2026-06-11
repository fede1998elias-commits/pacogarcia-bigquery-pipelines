# pacogarcia-bigquery-pipelines

Scripts de sincronización de datos de ecommerce hacia BigQuery (GCP).  
Fuentes: **Google Search Console**, **Google Ads** y **VTEX OMS**.

---

## Scripts

### `sync_search_console.py`
Carga datos de rendimiento de búsqueda orgánica desde Google Search Console a BigQuery.

- **Tabla destino:** `search_console_data.daily_performance`
- **Particionado:** por `date` (DAY)
- **Dimensiones:** query, página, dispositivo, país, tipo de búsqueda
- **Métricas:** clicks, impresiones, CTR, posición
- **Deduplicación:** no recarga combinaciones `(date, search_type)` ya cargadas
- **Lag:** 2 días (límite de la API de Search Console)

```bash
python sync_search_console.py            # últimos 30 días
python sync_search_console.py --days 90  # backfill 90 días
python sync_search_console.py --days 480 # máximo histórico (~16 meses)
```

---

### `sync_google_ads.py`
Carga métricas por campaña y día desde Google Ads a BigQuery.

- **Tabla destino:** `google_ads_data.daily_campaigns`
- **Particionado:** por `date` (DAY)
- **Métricas:** costo, impresiones, clicks, CTR, CPC promedio, conversiones, valor de conversiones, ROAS
- **Filtro:** solo campañas con `cost > 0`
- **Inserción:** batch (`load_table_from_json`, sin streaming inserts)
- **Chunk size:** 30 días por llamada a la API

```bash
python sync_google_ads.py              # últimos 365 días
python sync_google_ads.py --days 1095  # ~3 años (histórico completo)
```

---

### `sync_vtex.py`
Carga órdenes e items de VTEX OMS a BigQuery. Maneja el límite de 3.000 órdenes por llamada mediante split recursivo de ventanas de tiempo.

- **Tablas destino:**
  - `vtex_data.daily_orders` — una fila por orden
  - `vtex_data.order_items` — una fila por ítem de cada orden
- **Particionado:** por `creation_date` (DAY) en ambas tablas
- **Timezone:** fechas convertidas a hora Argentina (UTC-3, sin DST)
- **Inserción:** batch (`load_table_from_json`, sin streaming inserts)
- **Cache de productos:** `product_cache.json` (TTL 24 h) vía `vtex.py`

```bash
python sync_vtex.py                          # últimos 365 días
python sync_vtex.py --days 730               # últimos 2 años
python sync_vtex.py --start-date 2021-01-01  # histórico completo desde esa fecha
```

---

### `vtex.py`
Módulo auxiliar con el cliente VTEX. Expone funciones para consultar órdenes, detalle de órdenes y productos del catálogo, con caché local en `product_cache.json`.

No se ejecuta directamente; es importado por `sync_vtex.py`.

---

## Configuración

### 1. Variables de entorno

Crear un archivo `.env` en la raíz del proyecto (no se sube al repo):

```env
# GCP — service account con permisos de BigQuery y Search Console
GOOGLE_SERVICE_ACCOUNT_FILE=ruta/a/tu-service-account.json

# Google Search Console
SEARCH_CONSOLE_SITE_URL=sc-domain:tudominio.com.ar

# Google Ads
GOOGLE_ADS_CUSTOMER_ID=1234567890

# VTEX
VTEX_ACCOUNT=nombre-cuenta-vtex
VTEX_APP_KEY=vtexappkey-...
VTEX_APP_TOKEN=...
```

### 2. Credencial GCP (service account)

El service account necesita los siguientes roles/permisos:
- **BigQuery Data Editor** — para crear datasets, tablas y cargar datos
- **BigQuery Job User** — para ejecutar jobs de carga
- **Search Console** — acceso de lectura en Google Search Console (agregado desde la consola de Search Console)

### 3. Google Ads (`google-ads.yaml`)

Requerido para `sync_google_ads.py`. Formato estándar de la librería `google-ads`:

```yaml
developer_token: TU_DEVELOPER_TOKEN
client_id: TU_CLIENT_ID
client_secret: TU_CLIENT_SECRET
refresh_token: TU_REFRESH_TOKEN
login_customer_id: TU_MCC_ID
use_proto_plus: True
```

> El archivo `google-ads.yaml` no se incluye en el repo (excluido por `.gitignore`).

---

## Instalación

```bash
pip install -r requirements.txt
```

---

## Uso típico (daily sync)

```bash
python sync_search_console.py
python sync_google_ads.py
python sync_vtex.py
```

Los tres scripts son idempotentes: detectan las fechas ya cargadas en BigQuery y solo procesan las que faltan. Se pueden programar con Windows Task Scheduler o cualquier orquestador de tareas.
