# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Services & Ports

Host ports come from `docker-compose.dev.yml`'s `${VAR:-default}` fallbacks, overridden by `.env.dev`. Always start with `--env-file .env.dev` (see gotcha below) — these are the ports you'll actually hit:

| Service | Description | Port |
|---|---|---|
| `front` | Nuxt 3 frontend | 9010 |
| `api` | FastAPI backend | 9011 |
| `db` | PostgreSQL/PostGIS | 9012 |
| `db-ch` | ClickHouse (analytics) | 9013 (HTTP), 9014 (native) |
| `process` | Data pipeline worker | — |

## Common Commands

**Start dev environment:**
```bash
docker compose -f docker-compose.dev.yml --env-file .env.dev up
```
Without `--env-file .env.dev`, compose falls back to the in-file defaults (front 3000, api 4000, db 5432) and can recreate dependent services on the wrong ports.

**Frontend (outside Docker):**
```bash
cd front
npm install
npm run dev        # dev server with HMR
npm run build      # production build
npm run lint       # ESLint
```

**API tests:**
```bash
cd api
pytest tests/
pytest tests/test_extract_timeseries_all.py  # single file
```

**Process CLI** (run inside the `process` container or with uv):
```bash
python MAIN.py check_download
python MAIN.py download [--limit 10] [--date YYYY-MM-DD] [--variable temp]
python MAIN.py compute [--workers 4] [--id <nc_jobs_id>]
python MAIN.py image [--workers 4]
python MAIN.py liveocean_download [--liveocean-date YYYY-MM-DD]
python MAIN.py liveocean_process
python MAIN.py bottom_layer
```

**ClickHouse client:**
```bash
docker compose -f docker-compose.dev.yml exec db-ch clickhouse-client --query "SHOW TABLES"
```

**Deploy frontend:**
```bash
./deploy.sh dev    # deploys to dev branch
./deploy.sh prod   # deploys to master branch
```

## Architecture

### Data Sources
- **SalishSeaCast (SSC)** — daily NetCDF files downloaded from ERDDAP (`salishsea.eos.ubc.ca/erddap`). Covers the Salish Sea with a curvilinear sigma-coordinate grid.
- **LiveOcean (LO)** — daily `layers.nc` files downloaded from S3.

### Storage Layout
Data is mounted at `/opt/data/` in containers (maps to `./data/` locally):
- `SalishSeaCast/nc/` — raw daily NetCDF files
- `SalishSeaCast/images/` — rendered WebP tiles (served by API)
- `LiveOcean/nc/` — LiveOcean NetCDF files
- `sensors/{id}/{sensorCategoryCode}.nc` — compressed sensor NC files
- `cache/` — cached grid lookup NPZ files

### PostgreSQL/PostGIS (`oa` database)
Core relational store for pipeline state and spatial metadata:
- `datasets` / `fields` — ERDDAP dataset registry and variable configuration
- `nc_jobs` — **pipeline state machine** (see below)
- `grid` — SSC curvilinear grid cells (row/col → lat/lon, used for nearest-neighbor lookups)
- `lo_grid` — LiveOcean grid cells
- `sensors` — sensor metadata including `variables` JSONB (maps canonical model variable names to sensor-specific category codes)
- `colormaps` — colormap definitions served to the frontend

### ClickHouse (`db-ch`)
Stores time-series data for fast analytical queries. The API queries it via `api/modules/clickhouse_helpers.py`, which selects between a local instance (`CH_HOST`/`CH_PORT`) and a remote one (`CH_USE_REMOTE=true` + `CH_REMOTE_URL`). Used by `extractTimeseries` when the source is `SalishSeaCast` (reads `grid_SSC` and `SalishSeaCast_hourly` — raw per-hour values, no mean/min/max aggregation).

### `nc_jobs` State Machine
Every NetCDF file/day-variable combination is tracked as a row. States:
```
pending_download → downloading → success_download
  → pending_compute → computing → success_compute
  → pending_image / pending_bottom
  → imaging / bottoming
  → success_image / success_bottom
```
The `process` CLI commands advance rows through these states. `check_download` creates new rows; `download` fetches files; `compute` derives biogeochemical variables; `image` renders WebP tiles via `nc2tile.py`.

### API (`api/`)
FastAPI app in `SERVER.py`. All blocking work runs in a `ProcessPoolExecutor` via `run_in_process()`, limited by `_extract_semaphore` (default 4 concurrent). Key endpoints:

| Endpoint | Module |
|---|---|
| `POST /extractTimeseries` | `modules/extractTimeseries.py` |
| `POST /getProfile` | `modules/extract_profile.py` |
| `POST /sensorTimeseries` | `modules/extractSensorTimeseries.py` |
| `POST /extract_climateTimeseries` | `modules/extract_climate_timeseries.py` |
| `POST /analysis/timeseries` | `modules/ocean_analysis.py` |
| `GET /png/{source}/{var}/{dt}/{depth}` | `modules/pngGenerator.py` (generates on-demand if missing) |
| `GET /variables` | `modules/variables.py` |
| `POST /admin/syncHourly` | `modules/sync_hourly.py` (bearer-token auth via `SYNC_API_TOKEN`; imports a date's Native-format export rsynced in by a remote `process` pipeline — see `SalishSeaCast_sync_log`) |

**Federation**: When `FEDERATION_ENABLED=true` and `REMOTE_B_API_BASE` is set, SSC requests are routed between two servers (A = local, B = remote archive) based on `nc_jobs.misc` ownership flags.

### Process Pipeline (`process/`)
Entry point: `MAIN.py` → `modules/cli.py`. Key modules:
- `modules/downloader.py` — ERDDAP HTTP fetching with backfill
- `modules/compute.py` — biogeochemical derived variables (pH, Ω aragonite) via PyCO2SYS
- `modules/png_worker.py` — renders WebP tiles, advances `nc_jobs` to `success_image`
- `modules/live_ocean.py` — LiveOcean-specific download and processing
- `liveOcean/` — LiveOcean grid init and imaging utilities

Shared between `api` and `process` containers: `shared/nc2tile.py` (curvilinear → Web-Mercator WebP reprojection).

### Frontend (`front/`)
Nuxt 3 + Vuetify + Pinia. Key structure:
- `app/pages/index.vue` — single main page with MapboxGL map
- `app/components/` — map overlays, chart dialogs, time controls, variable/sensor pickers
- `app/stores/` — Pinia stores for app-wide state
- `composables/` — MapboxGL layer logic (`useRasterLayer`, `useVectorTileLayer`, `useBuoyLayer`, `useMapAnimator`, etc.) and data fetching (`useSensorTimeseries`)

Config via `nuxt.config.ts`. Runtime env vars: `NUXT_PUBLIC_API_BASE_URL`, `NUXT_PUBLIC_MAPBOX_TOKEN`.

**UI conventions**: Prefer Vuetify components (`v-btn`, `v-card`, `v-sheet`, etc.) over raw `div`/`button` elements, even when heavily restyled — apply custom look via scoped CSS classes on top of the component (e.g. `selectedInfo.vue`'s `.colorbar` class on a `v-card`) rather than dropping to plain HTML. Use `:deep()` to reach into a component's internal classes (e.g. `.v-btn__content`) when the override needs to target inner markup.

#### Frontend Feature Map

`app/pages/index.vue` hosts a bottom `v-footer` tab rail (`activeTab`, bound to `mainStore.activeBottomTab`) with four tabs:

| Tab | Component | Purpose | Data fetching |
|---|---|---|---|
| Timeseries | `app/components/TimeseriesChart.vue` | Chart of a single point/sensor's raw timeseries over a date range | `composables/useSensorTimeseries.ts` → `POST /extractTimeseries` or `/sensorTimeseries` |
| Model Analysis | `app/components/analytics.vue` ("Analysis Builder") | Analyzes the full historical timeseries for a selected coordinate/depth/variable — view mode (All Years Overlaid / Annual Summary), season filter, statistic (min/mean/max), all-time min/max records, threshold-crossing stats per year | `composables/useAnalysisFetch.ts` → `POST /analysis/timeseries` |
| Comparison | `app/components/sensorComparison.vue` (shown only when a sensor is selected) | Compares sensor vs. model data | — |
| Sensor Analysis | `app/components/sensorAnalytics.vue` (shown only when a sensor is selected) | Same "Analysis Builder" pattern as Model Analysis (view mode / season / statistic), but run against a sensor's own observed timeseries instead of the model | `composables/useSensorAnalysisFetch.ts` (`fetchSensorAnalysisSeries`) |

**Model Analysis advanced mode**: `analytics.vue`'s fullscreen icon opens `app/components/analysis/AdvancedAnalysisDialog.vue`, a fullscreen dialog with 5 sub-tabs under `app/components/analysis/`:
- `ExtremeEvents.vue` — baseline window, min duration, direction
- `CompoundStress.vue` — primary + secondary variable threshold comparison
- `Trend.vue` — Theil-Sen slope, Mann-Kendall test
- `Climatology.vue` — deviation from day-of-year climatological mean
- `Correlation.vue` — 2-4 selectable variables

The dialog fetches one shared "primary series" per point/variable/depth (reused across sub-tabs), plus a memoized `cachedFetch` helper for secondary-variable series used by CompoundStress and Correlation. Client-side stats helpers live in `composables/useAnalysisStatistics.ts`; ECharts dark theme registration in `composables/useEchartsTheme.ts`.

Other chart-related components: `app/components/EchartsLineDialog.vue` — a separate monthly-chart dialog, unrelated to Model Analysis. `app/components/sensorInfo.vue` — despite the name, this is the left-panel searchable/filterable sensor list (mounted in `controlPanel.vue`'s "Sensors" expansion panel); it also bundles the per-sensor metadata dialog (info icon) and a heatmap dialog. Selecting a sensor here or on the map both call `mainStore.selectSensor(id, depth)`, and the list auto-scrolls the selected `v-list-item` into view via a `watch` on `mainStore.selectedSensor`.

## Python Environment

`process/`, `scripts/`, and `clickhouse_test/` use **uv** (see `pyproject.toml` + `uv.lock` in each). `api/` uses pip with `requirements.txt`. Each subproject has its own `.venv`.
