# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Services & Ports

Host ports come from `docker-compose.dev.yml`'s `${VAR:-default}` fallbacks, overridden by `.env.dev`. Always start with `--env-file .env.dev` (see gotcha below) — these are the ports you'll actually hit:

| Service | Description | Port |
|---|---|---|
| `front` | Nuxt 3 frontend | 9010 |
| `api` | FastAPI backend | 9011 |
| `db-ch` | ClickHouse (analytics) | 9013 (HTTP), 9014 (native) |
| `process` | Data pipeline worker | — |

## Common Commands

**Start dev environment:**
```bash
docker compose -f docker-compose.dev.yml --env-file .env.dev up
```
Without `--env-file .env.dev`, compose falls back to the in-file defaults (front 3000, api 4000) and can recreate dependent services on the wrong ports.

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

**Process CLI** (run inside the `process` container or with uv — see Process Pipeline below):
```bash
python -m SSC.cli check       [--date YYYY-MM-DD] [--init-days N]
python -m SSC.cli download    [--date YYYY-MM-DD] [--variable VAR] [--limit N]
python -m SSC.cli compute     [--date YYYY-MM-DD] [--limit N] [--workers N]
python -m SSC.cli image       [--date YYYY-MM-DD] [--variable VAR] [--limit N] [--workers N]
python -m SSC.cli ingest      [--date YYYY-MM-DD] [--limit N]
python -m SSC.cli sync        [--date YYYY-MM-DD] [--limit N]
python -m SSC.cli run         [--date YYYY-MM-DD] [--limit N] [--workers N]  # all steps in order
python -m SSC.cli status      [--date YYYY-MM-DD]
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
- **SalishSeaCast (SSC)** — daily NetCDF files downloaded from ERDDAP (`salishsea.eos.ubc.ca/erddap`). Covers the Salish Sea with a curvilinear sigma-coordinate grid. The only data source currently implemented — LiveOcean support was removed (not yet being implemented).

### Storage Layout
Data is mounted at `/opt/data/` in containers (maps to `./data/` locally):
- `SalishSeaCast/nc/` — raw daily NetCDF files
- `SalishSeaCast/images/` — rendered WebP tiles (served by API)
- `sensors/{id}/{sensorCategoryCode}.nc` — compressed sensor NC files
- `cache/` — cached grid lookup NPZ file (`oa_grid_cache.npz`, populated from ClickHouse's `grid_SSC` table on first use)

### ClickHouse (`db-ch`)
The sole database — stores time-series data, pipeline state, sensor metadata, and the SSC curvilinear grid. The API queries it via `api/modules/clickhouse_helpers.py`, which selects between a local instance (`CH_HOST`/`CH_PORT`) and a remote one (`CH_USE_REMOTE=true` + `CH_REMOTE_URL`). Key tables: `grid_SSC` (curvilinear grid cells — `gridX`/`gridY`/`longitude`/`latitude`, used for nearest-neighbor lookups by both the API and `shared/nc2tile.py`'s tile rendering), `SalishSeaCast_hourly`/`SalishSeaCast_daily` (raw and pre-aggregated timeseries), `SalishSeaCast_status` (the process pipeline's own state machine — see below), `sensors`/`sensor_timeseries` (sensor metadata and observations). Colormap/variable metadata (precision, colormap bounds) lives in `shared/variable_config.yml` + `shared/colormaps.json` instead of a database table.

### API (`api/`)
FastAPI app in `SERVER.py`. All blocking work runs in a `ProcessPoolExecutor` via `run_in_process()`, limited by `_extract_semaphore` (default 4 concurrent). Key endpoints:

| Endpoint | Module |
|---|---|
| `POST /extractTimeseries` | `modules/extractTimeseries.py` |
| `POST /getProfile` | `modules/extract_profile.py` |
| `POST /sensorTimeseries` | `modules/extractSensorTimeseries.py` |
| `POST /extract_climateTimeseries` | `modules/extract_climate_timeseries.py` |
| `POST /analysis/timeseries` | `modules/ocean_analysis.py` |
| `GET /png/{source}/{var}/{dt}/{depth}` | Serves a preprocessed WebP/PNG tile directly (all depths are preprocessed ahead of time by `process/SSC`'s `image` step — this route never generates on-demand, 404s if the file doesn't exist) |
| `GET /variables` | `modules/variables.py` |
| `POST /admin/syncHourly` | `modules/sync_hourly.py` (bearer-token auth via `SYNC_API_TOKEN`; imports a date's Native-format export rsynced in by a remote `process` pipeline — see `SalishSeaCast_sync_log`) |

Most POST endpoints above (all except `/admin/syncHourly` and tile-serving routes) fire a PostHog usage-analytics event on success — see Usage Analytics (PostHog) below.

### Process Pipeline (`process/`)
Entry point: `process/SSC/cli.py` (`python -m SSC.cli <command>`). Every NetCDF file/day-variable
combination is tracked as a row in ClickHouse's `SalishSeaCast_status` table, advancing through:
```
pending_download → downloading → success_download
  → pending_compute → computing → success_compute
  → pending_image → imaging → success_image
  → pending_ingest → success_ingest
  → pending_sync → success_sync
```
Key modules:
- `SSC/downloader.py` — ERDDAP HTTP fetching with backfill
- `SSC/compute.py` — biogeochemical derived variables (pH, Ω aragonite) via PyCO2SYS
- `SSC/imaging.py` — renders WebP tiles via `nc2tile.py`, advances status to `success_image`
- `SSC/sync.py` — exports a date's hourly rows to Native format and rsyncs them + WebP images to the API machine (via `cloudflared access ssh` as a ProxyCommand), triggering `POST /admin/syncHourly`

`run` executes all steps in order: `check → download → check_image → compute → check_image → image → promote → ingest → promote → sync`.

Shared between `api` and `process` containers: `shared/nc2tile.py` (curvilinear → Web-Mercator WebP reprojection). Sources the grid from ClickHouse's `grid_SSC` table (cached locally to an `.npz` file) and variable precision/colormap bounds from `shared/variable_config.py` — no database credentials of its own beyond the standard `CH_*` ClickHouse env vars.

Sensor ingestion (ONC/ERDDAP → ClickHouse) lives in the top-level `sensors/` directory, its own docker-compose service — unrelated to `process/`. An older, Postgres-backed `process/sensors/` subsystem existed before that migration; it's been removed entirely, superseded by `sensors/`.

### Frontend (`front/`)
Nuxt 3 + Vuetify + Pinia. Key structure:
- `app/pages/index.vue` — single main page with MapboxGL map
- `app/components/` — map overlays, chart dialogs, time controls, variable/sensor pickers
- `app/stores/` — Pinia stores for app-wide state
- `composables/` — MapboxGL layer logic (`useRasterLayer`, `useVectorTileLayer`, `useBuoyLayer`, `useMapAnimator`, etc.) and data fetching (`useSensorTimeseries`)

Config via `nuxt.config.ts`. Runtime env vars: `NUXT_PUBLIC_API_BASE_URL`, `NUXT_PUBLIC_MAPBOX_TOKEN`, `NUXT_PUBLIC_POSTHOG_KEY`, `NUXT_PUBLIC_POSTHOG_HOST` (see Usage Analytics below).

**UI conventions**: Prefer Vuetify components (`v-btn`, `v-card`, `v-sheet`, etc.) over raw `div`/`button` elements, even when heavily restyled — apply custom look via scoped CSS classes on top of the component (e.g. `selectedInfo.vue`'s `.colorbar` class on a `v-card`) rather than dropping to plain HTML. Use `:deep()` to reach into a component's internal classes (e.g. `.v-btn__content`) when the override needs to target inner markup.

**Charts/plots**: Always use ECharts for any chart or plot — never hand-roll rendering on a raw `<canvas>` (custom heatmaps, hit-testing, tooltips, zoom, etc. reimplement things ECharts already does correctly, e.g. its `dataZoom` component for zoom/pan). Even non-standard visualizations (variable-height heatmap cells, custom hatching) are buildable as an ECharts `custom` series with `renderItem` — see `app/components/comparison/DepthProfile.vue`'s Model/Sensor/Diff panels. One gotcha: ECharts enables progressive rendering by default for `custom` series above a low item-count threshold, which silently paints only the first chunk of data for larger grids (thousands of cells) — set `progressive: false` on the series when the full render is cheap enough to do in one pass. Register the dark theme via `composables/useEchartsTheme.ts`.

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

Other chart-related components: `app/components/sensorInfo.vue` — despite the name, this is the left-panel searchable/filterable sensor list (mounted in `controlPanel.vue`'s "Sensors" expansion panel); it also bundles the per-sensor metadata dialog (info icon) and a heatmap dialog. Selecting a sensor here or on the map both call `mainStore.selectSensor(id, depth)`, and the list auto-scrolls the selected `v-list-item` into view via a `watch` on `mainStore.selectedSensor`.

### Usage Analytics (PostHog)

Both `api/` and `front/` send usage events to **PostHog Cloud** (not self-hosted — the official self-hosted stack bundles its own Postgres/Redis/ClickHouse/Zookeeper/Kafka/MinIO, which would duplicate what this project already runs; this was a deliberate call). Capture is a silent no-op on either side when its key is unset — safe to leave blank in any environment.

- **API** (`api/modules/posthog_helpers.py`): `capture_event(http_request, event, properties)`, configured via `POSTHOG_API_KEY`/`POSTHOG_HOST`. Called from `sensorTimeseries`, `depthProfile`, `extractTimeseries`, `extract_climateTimeseries`, `getMinMax`, `getProfile`, `getEval`, and `analysis/timeseries` — tile-serving and `/admin/syncHourly` are deliberately excluded (too high-volume / not user behavior).
- **Frontend** (`front/app/plugins/posthog.client.ts` + `front/composables/useAnalytics.ts`'s `trackEvent()`), configured via `NUXT_PUBLIC_POSTHOG_KEY`/`NUXT_PUBLIC_POSTHOG_HOST`. Custom events only — no PostHog autocapture, no session replay (deliberate: avoids noise from this canvas/map-heavy UI and keeps event volume down). Events: `sensor_selected` (list vs. map), `model_point_queried` (map-click point/area model query), `model_eval_requested`, `tab_switched` / `query_mode_changed` (centralized in `main.ts`'s store actions, covering every call site), `variable_changed`, `advanced_analysis_opened` (tagged by `dialog_type`). Deliberately *not* instrumented: `getMinMax`'s `autorange()` and the Model Analysis tab's auto-refetch (`scheduleAutoRun`) — both fire on every map pan/zoom or dependent-state change rather than a discrete user action, and would flood PostHog with near-duplicates of events already captured upstream.
- **Identity correlation**: the frontend plugin sets `axios.defaults.headers.common['X-PostHog-Distinct-Id']` to posthog-js's distinct_id once, globally — this codebase has no shared `axios.create()` instance, so every composable's `axios.post/get` picks it up automatically. `SERVER.py`'s `_stamp_request_start_time` middleware reads that header into `request.state.distinct_id` once per request; `capture_event` prefers it over IP-based attribution, falling back to IP for non-browser callers (curl, health checks, direct API access). `disable_geoip` always stays IP-based regardless of which distinct_id is used.
- **Gotcha**: editing `POSTHOG_API_KEY` / `NUXT_PUBLIC_POSTHOG_KEY` in `.env.dev` does not affect an already-running container — env vars are baked in at container creation, not read live. Use `docker compose up -d --force-recreate front api` (a plain `restart` reuses the old environment). The Vercel-deployed frontend has the equivalent gotcha: env var changes need a new deployment to take effect, and the repo's `vercel.json` sets `git.deploymentEnabled: false`, so git pushes don't auto-deploy — trigger a manual deploy after changing env vars there too.

PostHog is now the **only** usage-analytics tool — a pre-existing self-hosted Umami setup (script tag in `nuxt.config.ts`'s `<head>`, plus a `umami`/`umami-db` service pair in `docker-compose.prod.frontend.yml`) was removed once PostHog covered pageviews and more. If `analytics.oa.cioospacificlabs.ca` still resolves anywhere (external DNS/reverse proxy, outside this repo), that's now a dangling route to clean up separately.

## Python Environment

`process/`, `scripts/`, and `clickhouse_test/` use **uv** (see `pyproject.toml` + `uv.lock` in each). `api/` uses pip with `requirements.txt`. Each subproject has its own `.venv`.
