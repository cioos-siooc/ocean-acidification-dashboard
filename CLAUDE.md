# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Services & Ports

Host ports come from `docker-compose.dev.yml`'s `${VAR:-default}` fallbacks, overridden by `.env.dev`:

| Service | Description | Port |
|---|---|---|
| `front` | Nuxt frontend | 9010 |
| `api` | FastAPI backend | 9011 |
| `db-ch` | ClickHouse | 9013 (HTTP), 9014 (native) |
| `process` | Data pipeline worker (the CLI) — dev only | — |
| `prefect` | Dev-only local Prefect server (login `PREFECT_AUTH_STRING`, default `admin:admin`). Prod uses the shared https://prefect.cioospacific.ca | 9015 |
| `scheduler` | `process` image serving `SSC/flows.py` to Prefect; in prod the only pipeline container | — |
| `sensors-scheduler` | `sensors` image serving `sensors/flows.py` (hourly ERDDAP/ONC ingestion); prod: `docker-compose.prod.api.yml` | — |

## Common Commands

```bash
docker compose -f docker-compose.dev.yml --env-file .env.dev up
```
**Always pass `--env-file`** (dev and prod). Without it, dev falls back to in-file defaults (front 3000, api 4000) and recreates dependent services on wrong ports; prod compose files `:?`-require `PREFECT_API_URL` and fail.

```bash
cd front && npm run dev | npm run build | npm run lint
cd api && pytest tests/                      # or a single file, e.g. tests/test_extract_timeseries_all.py
docker compose -f docker-compose.dev.yml exec db-ch clickhouse-client --query "SHOW TABLES"
./deploy.sh dev | prod                       # frontend → dev / master branch
```

**Process CLI** — dev: the `process` container or uv; prod: `docker compose -f docker-compose.prod.process.yml --env-file .env.process.remote exec scheduler uv run python -m SSC.cli …` (or `run --rm scheduler …` while it's stopped):
```bash
python -m SSC.cli {check|download|compute|image|ingest|sync|run|status} [--date YYYY-MM-DD] [--limit N] [--workers N]
# check: [--init-days N]; download/image: [--variable VAR]; run = all steps in order
```

## Architecture

**Data source:** SalishSeaCast (SSC) only — daily NetCDF from ERDDAP (`salishsea.eos.ubc.ca/erddap`), curvilinear sigma-coordinate grid. LiveOcean was removed.

**Storage** (`/opt/data/` in containers = `./data/` locally): `SalishSeaCast/nc/` raw NetCDF; `SalishSeaCast/images/` WebP tiles; `sensors/{id}/{sensorCategoryCode}.nc`; `cache/oa_grid_cache.npz` (grid, populated from `grid_SSC` on first use).

### ClickHouse (`db-ch`)
The sole database. API access via `api/modules/clickhouse_helpers.py` (local `CH_HOST`/`CH_PORT`, or remote with `CH_USE_REMOTE=true` + `CH_REMOTE_URL`). Key tables: `grid_SSC` (`gridX`/`gridY`/`longitude`/`latitude`; nearest-neighbor lookups + tile rendering), `SalishSeaCast_hourly`/`SalishSeaCast_daily`, `SalishSeaCast_status` (pipeline state machine), `sensors`/`sensor_timeseries`. Variable metadata (precision, colormap bounds) lives in `shared/variable_config.yml` + `shared/colormaps.json`, not a table.

### API (`api/`)
FastAPI app in `SERVER.py`. Blocking work runs in a `ProcessPoolExecutor` via `run_in_process()`, limited by `_extract_semaphore` (default 4). The API never reads NetCDF — all data comes from ClickHouse.

| Endpoint | Module |
|---|---|
| `POST /extractTimeseries` | `modules/extractTimeseries.py` |
| `POST /getProfile` | `modules/extract_profile.py` — vertical profile at a point/time; `bin_mode` hourly/daily/monthly (daily/monthly read `SalishSeaCast_daily` means) |
| `POST /depthProfile` | `modules/extract_depth_profile.py` — time-depth (Hovmöller) grid for `depth/TimeDepthHeatmap.vue`, optionally with a variable-depth sensor's casts binned onto it. Returns `grid` (`lat`/`lon`/`distanceKm` of the answering cell) |
| `POST /getMinMax` | `modules/extractMinMax.py` |
| `POST /sensorTimeseries` | `modules/extractSensorTimeseries.py` |
| `POST /extract_climateTimeseries` | `modules/extract_climate_timeseries.py` |
| `POST /analysis/timeseries` | `modules/ocean_analysis.py` |
| `POST /crossSection` | `modules/extract_cross_section.py` — depth-vs-distance grid along a polyline (`vertices`, ≥2 `(lat, lon)`) at one time. Resamples by arc length, snaps via `shared/grid_lookup.py`'s cached KD-tree in one vectorized query, reads only distinct cells hit |
| `GET /png/{source}/{var}/{dt}/{depth}` | Serves pre-rendered tiles; on a miss, `modules/extract_image.py` renders from ClickHouse (`shared/nc2tile.py`'s `render_tile_from_db`) and caches to the same path. `dt`'s shape selects bin mode: full timestamp = hourly, `YYYY-MM-DD` = daily, `YYYY-MM` = monthly. The "bottom" pseudo-depth isn't supported on the fallback path (404s) |
| `GET /variables` | `modules/variables.py` |
| `POST /admin/syncHourly`, `/admin/syncDaily` | `modules/sync_hourly.py` — bearer auth (`SYNC_API_TOKEN`); imports a date's Native-format export rsynced in by the remote pipeline into `SalishSeaCast_hourly`/`_daily` (logged in `SalishSeaCast_sync_log`) |

**Out-of-domain coordinates.** Point lookups go through `extractTimeseries.py`'s `_find_nearest_grid_point`, which raises `OutsideDomainError` when no `grid_SSC` cell is within `MAX_GRID_DIST_KM` (25 km). `SERVER.py`'s `_outside_domain_response` returns a 400 with a plain-string `detail` (many frontend call sites print it) **and** a structured `error` (`code: "outside_model_domain"`, `distanceKm`, `maxDistanceKm`, `requested`, `nearest`). Wired into `/extractTimeseries`, `/depthProfile`, `/getProfile`, `/extract_climateTimeseries`; frontend reads it via `useDepthProfileFetch.ts`'s `asOutsideDomainError()`. The exception must pass every field through `super().__init__` to survive `ProcessPoolExecutor` pickling. `ocean_analysis.py`'s `lookup_nearest_grid_cell` (used by `/analysis/timeseries`) is a **separate, unguarded** lookup.

### Process Pipeline (`process/`)
Entry: `process/SSC/cli.py`. Each file/day-variable is a row in `SalishSeaCast_status`, advancing:
```
pending_download → downloading → success_download → pending_compute → computing → success_compute
  → pending_image → imaging → success_image → pending_ingest → success_ingest → pending_sync → success_sync
```
- `SSC/downloader.py` — ERDDAP fetching with backfill
- `SSC/compute.py` — derived variables (pH, Ω aragonite) via PyCO2SYS
- `SSC/imaging.py` — WebP tiles via `nc2tile.py`
- `SSC/sync.py` — exports a date's rows to Native format, rsyncs them + images to the API machine (`cloudflared access ssh` ProxyCommand), then calls `POST /admin/syncHourly`

`run` = `check → download → check_image → compute → check_image → image → promote → ingest → promote → sync`, defined once in `cli.pipeline_steps()`. Step helpers raise `cli.PipelineError` (not `sys.exit`) so they can run inside Prefect tasks.

**Prefect.** `SSC/flows.py` is the only module importing Prefect (the CLI works without a server). Flow `oceaneco-ssc-pipeline`, deployment `OceanECO-SSC`, tag `oceaneco` (the server is shared across apps), one task run per step. Served by `scheduler` on cron `RUN_CRON` (default `0 */3 * * *`) with `RUN_LIMIT`/`RUN_WORKERS` (prod 10/30, dev 10/4); overlapping runs are cancelled (`CANCEL_NEW`). `SalishSeaCast.*`/`nc2tile` loggers reach task logs via `PREFECT_LOGGING_EXTRA_LOGGERS`. Sweep stages mark rows `failed_*` without raising; each run ends with an `oceaneco-ssc-status` markdown artifact and is marked Failed if any row failed. Ad-hoc runs (`date`, `force`) via the UI's "Run → custom".
- `RUN_SCHEDULE_PAUSED` is re-applied on every scheduler start — **paused by default in dev** (`.env.dev` points at production ClickHouse). Stop `scheduler` for a pause that holds.
- `prefect==` in `process/pyproject.toml` must match dev's server image tag and be ≤ the shared server's version.
- Prod (`docker-compose.prod.process.yml`) has no `process` service and no local server; `scheduler` (`tools` profile) reports to `PREFECT_API_URL` (`https://prefect.cioospacific.ca/api`, basic auth `PREFECT_AUTH_STRING`). A code update means rebuilding/recreating `scheduler`.
- Don't start a manual `SSC.cli run` while a scheduled one is in progress — both claim the same pending rows.
- Renaming a flow/deployment leaves the old one on the server — delete it in the UI.

**Shared code** (`shared/`, used by `api` and `process`): `nc2tile.py` (curvilinear → Web-Mercator WebP; grid from `grid_SSC` cached to `.npz`, bounds from `variable_config.py`, only `CH_*` env vars needed); `grid_lookup.py` (`cKDTree` over the same cached grid, built once per process, for batch nearest-cell snapping).

**Sensors** (`sensors/`, own compose service, unrelated to `process/`): ONC/ERDDAP → ClickHouse. `sensors/flows.py` (only Prefect importer) defines flows `oceaneco-sensors-erddap`/`-onc`, deployments `OceanECO-sensors-ERDDAP`/`-ONC` (crons `ERDDAP_CRON` `0 * * * *`, `ONC_CRON` `30 * * * *`, same `CANCEL_NEW`/shared-server setup), one task per sensor via `store_sensor()`, which raises `FetchError` on a failed request so the run is marked Failed. Served by `sensors-scheduler` (paused by default in dev via `SENSORS_SCHEDULE_PAUSED`); its `PREFECT_API_URL` is `:?`-required.

### Frontend (`front/`)
Nuxt 4 + Nuxt UI v4 (Tailwind v4 + Reka UI) + Pinia. Vuetify has been fully removed — don't reintroduce `v-*` components.
- `app/pages/index.vue` — main MapboxGL page. Raster/vector-tile and time-animation logic lives **inline** here (no composables for it).
- `app/pages/caseStudy/` — standalone narrative pages built from static chart exports (`public/images/case-studies/`), not live data.
- `app/components/`, `app/stores/main.ts`, `composables/` (map-layer logic like `useBuoyLayer`/`useStationsInteraction`, and data fetching).
- `app/config/app.ts` — domain constants (timezone, map extent/zoom/style); `app/config/palette.ts` — Material palette values charts use. Per-environment values go in `runtimeConfig` (`NUXT_PUBLIC_API_BASE_URL`, `NUXT_PUBLIC_MAPBOX_TOKEN`, `NUXT_PUBLIC_POSTHOG_KEY`, `NUXT_PUBLIC_POSTHOG_HOST`).
- Dark-only: `colorMode` pinned in `nuxt.config.ts`; light mode is unverified.

**UI conventions.** Nuxt UI components (`UButton`, `UModal`, `USelectMenu`, `UInput`, `UTabs`, `UPopover`, `UBadge`, `UAlert`, `USeparator`, `UTable`, `UCalendar`) for anything interactive; plain elements + Tailwind for layout and styled surfaces (no need to force panels into a card component). Icons: `<UIcon name="i-mdi-foo" />` / `icon` props, MDI bundled locally via `@iconify-json/mdi` (never the remote Iconify API). Shared primitives in `app/components/ui/`: `SegmentedControl.vue` (single-select button row with radiogroup semantics + arrow keys — use it instead of hand-rolling one) and `DownloadButton.vue`.

**Naming depths.** The map's raster depth, the chart's model level and a sensor's deployment depth routinely disagree (e.g. `ExplorePanel`'s `snapDepthToData` pulls the chart to the seabed level). **Never render a bare "Depth N m"**: `selectedInfo.vue` is headed `MAP LAYER · <source>` and describes only the raster; `ChartContextBar.vue` takes one `ContextItem` per source (`Field`/`Model`/`Sensor`/`Range`/`Point`) with `tone` `muted` (empty) or `warn` (why the chart looks wrong). Round sensor depths as `sensorInfo.vue`'s `depth2txt` does. `mainStore.modelDomain` carries the last point's coverage verdict (`null` = unknown, treated as in-domain).

**Charts.** Always ECharts — never hand-roll on `<canvas>`. Non-standard visuals are `custom` series with `renderItem` (see `depth/TimeDepthHeatmap.vue`); set `progressive: false` on large `custom` series or only the first chunk paints. Register the dark theme via `composables/useEchartsTheme.ts` (also pins legend swatches to filled rects — series must set `itemStyle.color`, the legend ignores `lineStyle`). Never print a raw float: use `useVariableRegistry()`'s `formatDisplayValue(varId, value, { unit })`, which expects a value **already in the display unit**.

**CSV export.** All downloads go through `composables/useCsvExport.ts` + `ui/DownloadButton.vue` — never ECharts' `toolbox.feature.dataView`.
- A host calls `provideCsvExport(context)` and renders one `DownloadButton`; data-owning components call `useCsvExport()?.register(...)` (unregistered with their scope), so several files share one menu. Hosts: `AnalysisWorkspace`, `ExplorePanel`, `CrossSectionPanel`, `ComparisonWorkspace`. A `v-show`-kept-alive host (Analysis Overview) must return `[]` while inactive or its files leak into the visible tab's menu.
- Build datasets from *source* data (full precision), not table rows or chart state.
- Preamble: `#`-commented `# label,value` lines from `csvMeta()` (source, source_url, variable, depth, separate `latitude`/`longitude` rows via `coord()`, window, season) — readers skip with `comment='#'`. Keep it short; no API routes/variable ids. `omitDatasetLine` drops `# dataset:` for single-file views.
- Units go on a second header row (only when some column has one), never in column names.
- Grids go out long (`time, depth, value`). `csvTimestamp()` writes local ISO with offset.
- `TimeseriesChart.vue` exports one file following the legend (`legendSelected`, clipped to the axis window); `Day/Night` is always excluded (`CSV_EXCLUDED_SERIES`). Series on different x-grids are unioned with blanks, never interpolated.

**Share links.** `ShareButton.vue` (app header, plus icon-only in `AnalysisWorkspace`/`ComparisonWorkspace` headers since those fullscreen modals cover it) encodes the view into `#s=<marker><base64url>` — JSON → `deflate-raw` via `CompressionStream` (marker `z`; `u` = uncompressed). Schema and both directions live in `composables/useShareState.ts` (`captureShareState`/`applyShareState`/`applySharedVariable`, `SHARE_VERSION`); newer-version or undecodable payloads are refused whole. The URL is never rewritten during normal use.
- Capture reads **only** the Pinia store — shareable state must live in `stores/main.ts` (e.g. `analysisTab`/`analysisSeason`, `comparisonTab`/`comparisonSeason`, `mapView`, `exploreWindowEnd`).
- Restore is two-pass: `app.vue`'s `onBeforeMount` applies self-contained state and sets `shareRestorePending` **synchronously** so index.vue's bootstrap click doesn't overwrite the shared point; variable/depth/instant resolve in `getVariables()` against the fetched list, each field degrading independently (nearest depth, nearest timestamp, fallback `temperature`).
- Values without a consumer yet go in one-shot `pending*` slots the consumer takes and nulls: `pendingMapView` (index.vue `jumpTo`s it, with a watcher for late decode), `pendingWindowEnd` (claimed in ExplorePanel's `coverage` watcher, *after* `useTimeDepthWindow`'s reset-to-latest).
- A drawn cross-section must be re-added to mapbox-gl-draw explicitly (`restoreSharedCrossSection` in index.vue).
- Per-tab control state (thresholds, secondary variables, isolated year, chart zoom) goes in `mainStore.viewState` via `composables/useViewState.ts`'s `useViewState(scope)` (drop-in for a local `ref`, works with `v-model`) and `useChartZoom(scope)` (re-supply the extent on every `setOption(…, true)`, which resets `dataZoom`). The whole bag is captured as `vs`, so new controls are shareable without touching `useShareState.ts`. Values must be JSON-serialisable.

#### Frontend Feature Map

`index.vue` has a bottom `<footer>` rail (`activeTab` ↔ `mainStore.activeBottomTab`) with two map-synced panes and two fullscreen workspaces (`UModal fullscreen`). Rule: views that read map context (point/depth/clock, or a drawn line) stay beside the map; views that only take a coordinate as input open fullscreen.

| Pane | Component | Purpose | Data fetching |
|---|---|---|---|
| Explore (footer) | `ExplorePanel.vue` | Timeseries \| Model depth \| Sensor depth (profilers only). Timeseries overlays model + sensor; depth sections render via `depth/TimeDepthHeatmap.vue`, with a vertical-profile drawer (`SelectedVariableDrawer.vue`) following the bin-mode toggle and heatmap clicks | `useSensorTimeseries.ts` → `/extractTimeseries`, `/sensorTimeseries`; `useTimeDepthWindow.ts` + `useModelTimeseries.ts`/`useDepthProfileFetch.ts` → `/depthProfile`; `useClimateTimeseries.ts` → `/extract_climateTimeseries` |
| Cross-Section (footer) | `crossSection/CrossSectionPanel.vue` | Polyline drawn with mapbox-gl-draw (wired in `index.vue`: `crossSectionDraw` + `activeTab` watcher). Same `TimeDepthHeatmap.vue`, vertex markers as plain CSS overlay. "New line" bumps `mainStore.crossSectionRedrawToken` | `useCrossSectionFetch.ts` → `/crossSection` |
| Analysis (fullscreen) | `AnalysisWorkspace.vue` | Tabs: Overview (`analysis/AnalysisBuilder.vue`, `source: 'model' \| 'sensor'`), Extreme Events, Compound Stress, Trend, Climatology Anomaly, Correlation | `useAnalysisFetch.ts`/`useSensorAnalysisFetch.ts` → `/analysis/timeseries`; one shared primary series via `fetchSeriesFor`, memoized `cachedFetch` for secondary variables |
| Comparison (fullscreen) | `ComparisonWorkspace.vue` | Tabs: Timeseries (`sensorComparison.vue`), Depth sections (`comparison/ComparisonSections.vue`, variable-depth sensors only), Scatter, Residuals, Seasonal Cycle — only when a sensor is selected | — |

`selectSensor` in `stores/main.ts` deliberately doesn't switch panes (it would throw a dialog over the map on every buoy click); the sensor overlays on Explore's timeseries instead. `sensorInfo.vue` is, despite its name, the left-panel sensor list in `controlPanel.vue` (plus a metadata dialog and heatmap dialog); it scrolls the selected sensor into view. Client-side stats: `composables/useAnalysisStatistics.ts`.

### Usage Analytics (PostHog)
PostHog **Cloud** (deliberately not self-hosted) is the only analytics tool. Capture is a silent no-op when the key is unset.
- **API** (`api/modules/posthog_helpers.py`, `POSTHOG_API_KEY`/`POSTHOG_HOST`): `capture_event(http_request, event, properties)` on successful `sensorTimeseries`, `depthProfile`, `extractTimeseries`, `extract_climateTimeseries`, `getMinMax`, `getProfile`, `analysis/timeseries`. Tile and `/admin/sync*` routes are excluded.
- **Frontend** (`app/plugins/posthog.client.ts` + `composables/useAnalytics.ts`'s `trackEvent()`): custom events only — no autocapture, no session replay. Events: `sensor_selected`, `model_point_queried`, `model_eval_requested`, `tab_switched` / `query_mode_changed` (in `main.ts` store actions), `variable_changed`, `share_link_created`. Don't instrument auto-firing paths (`getMinMax`'s `autorange()`, Analysis `scheduleAutoRun`).
- **Identity**: the plugin sets `axios.defaults.headers.common['X-PostHog-Distinct-Id']` (no shared axios instance exists, so all calls get it); `SERVER.py`'s `_stamp_request_start_time` middleware stores it in `request.state.distinct_id`, and `capture_event` falls back to IP when absent. `disable_geoip` stays IP-based.
- **Gotcha**: env var changes need `docker compose up -d --force-recreate front api` (a `restart` keeps old env). On Vercel, `vercel.json` disables git auto-deploy — trigger a manual deploy after changing env vars.

## Python Environment
`process/`, `scripts/` use **uv** (`pyproject.toml` + `uv.lock`); `api/` uses pip + `requirements.txt`. Each has its own `.venv`.

## Version Logging
When asked to log a new version or update changes:
- Update `CHANGELOG.md` (Keep a Changelog: Added, Changed, Fixed).
- Bump the version in `package.json` per SemVer.
- Summarize key user-facing updates in ≤3 bullets.
