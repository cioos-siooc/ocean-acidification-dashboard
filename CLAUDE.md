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
| `POST /getProfile` | `modules/extract_profile.py` — single-depth-column vertical profile at a point/time; accepts `bin_mode` (hourly/daily/monthly), daily/monthly read the calendar-day/month mean from `SalishSeaCast_daily` instead of the instantaneous hourly reading |
| `POST /depthProfile` | `modules/extract_depth_profile.py` — the time-depth (Hovmöller) grid behind `depth/TimeDepthHeatmap.vue`, optionally alongside a variable-depth sensor's casts binned onto the same grid. Also returns `grid` (`lat`/`lon`/`distanceKm` of the model cell that answered), so a click that snapped several km can say so |
| `POST /getMinMax` | `modules/extractMinMax.py` |
| `POST /sensorTimeseries` | `modules/extractSensorTimeseries.py` |
| `POST /extract_climateTimeseries` | `modules/extract_climate_timeseries.py` |
| `POST /analysis/timeseries` | `modules/ocean_analysis.py` |
| `POST /crossSection` | `modules/extract_cross_section.py` — the model's depth-vs-distance grid along an arbitrary drawn polyline (`vertices`, ≥2 `(lat, lon)` points) at one time snapshot, the Cross-Section tab's spatial counterpart to `/depthProfile`'s single-point time-depth view. Resamples the line to evenly-spaced points by arc length, snaps each to its nearest `grid_SSC` cell via `shared/grid_lookup.py`'s cached KD-tree (one vectorized nearest-neighbor query, no per-point ClickHouse round trip), then reads only the distinct cells actually hit |
| `GET /png/{source}/{var}/{dt}/{depth}` | Serves a WebP/PNG tile — most are preprocessed ahead of time by `process/SSC`'s `image` step, served directly. On a miss, falls back to `modules/extract_image.py`, which renders the tile on demand straight from ClickHouse (via `shared/nc2tile.py`'s `render_tile_from_db`) and caches it to disk at the same path, for dates the pipeline hasn't covered and for daily/monthly bin modes (which the pipeline never pre-renders at all). `dt`'s shape alone selects the bin mode — a full timestamp for hourly, `YYYY-MM-DD` for daily, `YYYY-MM` for monthly (dash-separated, matching the pipeline's own hourly folder convention) — no separate param. 404s only if ClickHouse itself has no data for that variable/date/depth. The per-cell "bottom" pseudo-depth isn't supported on this fallback path (it's spatially-varying, not a fixed depth), so it still 404s for un-pre-generated dates |
| `GET /variables` | `modules/variables.py` |
| `POST /admin/syncHourly` | `modules/sync_hourly.py` (bearer-token auth via `SYNC_API_TOKEN`; imports a date's Native-format export rsynced in by a remote `process` pipeline — see `SalishSeaCast_sync_log`) |
| `POST /admin/syncDaily` | `modules/sync_hourly.py` (`import_daily_native_file`; counterpart to `/admin/syncHourly` for the pre-aggregated `SalishSeaCast_daily` table and its own sync log — same bearer-token auth and server-derived staging path) |

Most POST endpoints above (all except `/admin/syncHourly`, `/admin/syncDaily`, and tile-serving routes) fire a PostHog usage-analytics event on success — see Usage Analytics (PostHog) below.

**Out-of-domain coordinates.** `shared` point lookups go through `extractTimeseries.py`'s `_find_nearest_grid_point`, which raises `OutsideDomainError` (a `RuntimeError` subclass) when no `grid_SSC` cell is within `MAX_GRID_DIST_KM` (25 km). `SERVER.py`'s `_outside_domain_response` renders it as a **400 carrying both a plain-string `detail` and a structured `error` object** (`code: "outside_model_domain"`, `distanceKm`, `maxDistanceKm`, `requested`, `nearest`) — `detail` stays a sentence because ~9 frontend call sites print it directly, while `error.code` is what lets a caller show this as an informational state instead of a red failure. Wired into `/extractTimeseries`, `/depthProfile`, `/getProfile` and `/extract_climateTimeseries`; the frontend reads it via `composables/useDepthProfileFetch.ts`'s `asOutsideDomainError()`. The exception passes every field through `super().__init__` on purpose so it survives `run_in_process`'s `ProcessPoolExecutor` pickling — `Exception.__reduce__` rebuilds via `cls(*args)` and drops anything set outside the constructor. `ocean_analysis.py`'s `lookup_nearest_grid_cell` is a **separate, unguarded** lookup (silently snaps anywhere inside a 0.5-degree box) still used by `/analysis/timeseries`.

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

Shared between `api` and `process` containers: `shared/nc2tile.py` (curvilinear → Web-Mercator WebP reprojection). Sources the grid from ClickHouse's `grid_SSC` table (cached locally to an `.npz` file) and variable precision/colormap bounds from `shared/variable_config.py` — no database credentials of its own beyond the standard `CH_*` ClickHouse env vars. `shared/grid_lookup.py` reuses that same cached grid load to build a `scipy.spatial.cKDTree` (built once per process) for nearest-cell snapping of arbitrary point batches — currently only `extract_cross_section.py`'s polyline resampling needs this, as opposed to `nc2tile.py`'s own Delaunay/linear interpolation used for tile regridding.

Sensor ingestion (ONC/ERDDAP → ClickHouse) lives in the top-level `sensors/` directory, its own docker-compose service — unrelated to `process/`. An older, Postgres-backed `process/sensors/` subsystem existed before that migration; it's been removed entirely, superseded by `sensors/`.

### Frontend (`front/`)
Nuxt 4 + Nuxt UI (v4, Tailwind v4 + Reka UI) + Pinia. Key structure:
- `app/pages/index.vue` — single main page with MapboxGL map
- `app/pages/caseStudy/` — standalone long-form narrative pages (`index.vue` lists cases, `2021-heat-dome.vue` the first writeup), linked from the top nav (`app.vue`) and opened in a new tab. Built from static exports of the app's own charts (`public/images/case-studies/`), not live-fetched — separate from the map/dashboard's data-fetching architecture below.
- `app/components/` — map overlays, chart dialogs, time controls, variable/sensor pickers
- `app/stores/` — Pinia stores for app-wide state
- `composables/` — buoy/station map-layer logic (`useBuoyLayer`, `useStationsInteraction`) and data fetching (`useSensorTimeseries`, `useAnalysisFetch`, etc.). The raster/vector-tile and time-animation layer logic lives **inline in `index.vue`** (the earlier `useRasterLayer`/`useVectorTileLayer`/`useMapAnimator` composables were abandoned and removed).

Config via `nuxt.config.ts`. Runtime env vars: `NUXT_PUBLIC_API_BASE_URL`, `NUXT_PUBLIC_MAPBOX_TOKEN`, `NUXT_PUBLIC_POSTHOG_KEY`, `NUXT_PUBLIC_POSTHOG_HOST` (see Usage Analytics below).

**UI conventions**: Use Nuxt UI components (`UButton`, `UModal`, `USelectMenu`, `UInput`, `UTabs`, `UPopover`, `UBadge`, `UAlert`, `USeparator`, `UTable`, `UCalendar`) for anything interactive. Plain elements + Tailwind utilities are the right call for pure layout and styled surfaces — Nuxt UI has no `UCard`-shaped answer for every panel, and wrapping a bare surface in one just fights its header/body/footer padding. Icons are Iconify via `@nuxt/icon`: `<UIcon name="i-mdi-foo" />` and `icon`/`leading-icon`/`trailing-icon` props, with the MDI set bundled locally by `@iconify-json/mdi` (never the remote Iconify API).

Shared UI primitives live in `app/components/ui/` — currently `SegmentedControl.vue`, a mandatory single-select built on `UButton` with proper `role="radiogroup"` semantics and arrow-key navigation. It replaced 13 `v-btn-toggle`s; reach for it rather than hand-rolling another button row.

**Naming depths.** Three different depths can be on screen at once — the map's raster layer, the model level the chart is plotting, and a sensor's own deployment depth — and they routinely disagree (a mooring at 1257 m snaps the map to the nearest model level at 441.5 m; `ExplorePanel`'s `snapDepthToData` pulls the chart to the seabed level when the map's depth is below it). **Never render a bare, unattributed "Depth N m"**: `selectedInfo.vue` (the map-corner box) is headed `MAP LAYER · <source>` and describes only the raster layer, and `ChartContextBar.vue` takes one `ContextItem` per source (`Field`/`Model`/`Sensor`/`Range`/`Point`) with a `tone` of `muted` (present but empty) or `warn` (the reason the chart looks wrong). Sensor depths are rounded exactly as `sensorInfo.vue`'s `depth2txt` rounds them, since the user is being asked to compare the two figures. `stores/main.ts`'s `modelDomain` carries the last fetched point's coverage verdict (`null` = not established, which reads as in-domain everywhere) so the map box can say "no model data here" even though `ExplorePanel` is what fetched it.

**CSV export**: every download in the app goes through `composables/useCsvExport.ts` + `app/components/ui/DownloadButton.vue` — never ECharts' `toolbox.feature.dataView`, which only renders a read-only TSV blob into a modal (no file) and degrades to noise for `custom` series. Datasets are built from the *source* data, not from table rows or chart state, so files keep full precision (tables round for display). Files carry a `#`-commented provenance preamble (`csvMeta()` folds in the host's `CsvContext`: source, variable + unit, depth, location, window, season), which readers must skip with `comment='#'`. Keep it short — API routes, variable ids and other machine-facing detail were deliberately cut; a dataset can also drop the automatic `# dataset:` line with `omitDatasetLine` where a view only ever produces one file. A host calls `provideCsvExport(context)` and renders one `DownloadButton` in its header; the components that own the data call `useCsvExport()?.register(...)` and unregister with their scope — so a view offering several files gets one control with a menu, not a download icon per chart. A host kept alive with `v-show` (the Analysis Overview tab) must return `[]` from its getter while inactive, or its files leak into the visible tab's menu. Four hosts provide a registry: `AnalysisWorkspace` (which also registers the shared daily series for every deep-dive tab), `ExplorePanel`, `CrossSectionPanel`, `ComparisonWorkspace`. Grids go out long (`time, depth, value`), never as a wide grid whose headers are the depth levels; `csvTimestamp()` writes local ISO with the offset so a file says what the chart said. `TimeseriesChart.vue` exports **one** file whose contents follow the legend — it tracks `legendselectchanged` in `legendSelected` and drops any series the user switched off, clipping rows to the axis window; `Day/Night` is excluded unconditionally (`CSV_EXCLUDED_SERIES`) since it carries only the dusk/dawn shading and the NOW/MAP marker lines. Series with different x-grids (climatology is one point per calendar day at noon UTC) widen onto a union of timestamps with blanks — never interpolated onto a shared clock.

**Share links**: `ShareButton.vue` — mounted in the app header, and again in `AnalysisWorkspace`/`ComparisonWorkspace`'s own headers (icon-only, beside their `DownloadButton`) since those are fullscreen modals that cover the app header entirely — encodes the whole view into `#s=<marker><base64url>` — JSON → `deflate-raw` via the platform's own `CompressionStream` (marker `z`; `u` = uncompressed fallback), no dependency. The schema and both directions live in one place, `composables/useShareState.ts` (`captureShareState`/`applyShareState`/`applySharedVariable`, `SHARE_VERSION`); a payload from a newer version, or any that fails to decode, is refused whole and the app cold-starts. Capture reads **only** the Pinia store, which is why a few things that were component-local refs now live in `stores/main.ts` (`analysisTab`/`analysisSeason`, `comparisonTab`/`comparisonSeason`, `mapView`, `exploreWindowEnd` — the last two written by index.vue/ExplorePanel, which stay their owners). Restore runs in two passes because most of it can't be applied at decode time: `app.vue`'s `onBeforeMount` applies everything self-contained and flips `shareRestorePending` **synchronously** (the decode itself is async) so index.vue's bootstrap map click waits instead of overwriting the shared coordinate with the default point; the variable/depth/instant half is resolved in `getVariables()` against the freshly fetched list, each field validated and degraded independently (nearest depth level, nearest timestamp, per-field fallback to the `temperature` default). Anything whose consumer doesn't exist yet is parked in a one-shot `pending*` store slot the consumer takes and nulls — `pendingMapView` (index.vue `jumpTo`s it, plus a watcher for a decode that lands after the map is up), `pendingWindowEnd` (claimed in ExplorePanel's `coverage` watcher, *after* `useTimeDepthWindow`'s immediate reset-to-latest would clobber it). A drawn cross-section line has to be handed back to mapbox-gl-draw explicitly (`restoreSharedCrossSection` in index.vue) — `mainStore.crossSectionLine` alone only feeds the panel's fetch. Per-tab control state (an Extreme Events threshold, Compound Stress's secondary variable, a Correlation pair, Climatology's isolated year, every chart's zoom extent) does **not** get its own share key: those values live in `stores/main.ts`'s `viewState` bag, one namespaced scope per view, written through `composables/useViewState.ts`'s `useViewState(scope)` — a drop-in replacement for a component's local `ref` that `v-model` binds to unchanged — and `useChartZoom(scope)`, whose extent has to be re-supplied on every `setOption(…, true)` since a replace resets `dataZoom`. Capture takes the whole bag as `vs`, so adding a control to a tab makes it shareable without touching `useShareState.ts`. Values must stay JSON-serialisable. The URL is never rewritten during normal use: the link is a snapshot built when the popover opens.

App-wide domain constants (timezone, map extent/zoom/style) live in `app/config/app.ts`; the Material palette values the charts still use are in `app/config/palette.ts`. Per-environment values stay in `runtimeConfig`.

Dark-only: `colorMode` is pinned in `nuxt.config.ts`. Light mode is a real option but nothing has been verified in it.

**Charts/plots**: Always use ECharts for any chart or plot — never hand-roll rendering on a raw `<canvas>` (custom heatmaps, hit-testing, tooltips, zoom, etc. reimplement things ECharts already does correctly, e.g. its `dataZoom` component for zoom/pan). Even non-standard visualizations (variable-height heatmap cells, custom hatching) are buildable as an ECharts `custom` series with `renderItem` — see `app/components/depth/TimeDepthHeatmap.vue`. One gotcha: ECharts enables progressive rendering by default for `custom` series above a low item-count threshold, which silently paints only the first chunk of data for larger grids (thousands of cells) — set `progressive: false` on the series when the full render is cheap enough to do in one pass. Register the dark theme via `composables/useEchartsTheme.ts`.

#### Frontend Feature Map

`app/pages/index.vue` hosts a bottom `v-footer` rail (`activeTab`, bound to `mainStore.activeBottomTab`) with **two map-synced panes plus two fullscreen workspaces**. The organizing principle: the map is context (coordinate/depth/clock, or now a drawn line), so only a view that actually reads that context stays beside it — Analysis and Comparison take a coordinate as input and have nothing further to say to the map, so they open as `v-dialog fullscreen` instead of footer tabs. An earlier 5-tab layout (Timeseries / Depth / Model Analysis / Comparison / Sensor Analysis) was collapsed into this shape after Model/Sensor Analysis turned out to be the same builder pointed at two sources and the Timeseries tab's sensor overlay duplicated Comparison. Cross-Section was added later as a second map-synced pane rather than folded into Explore, since it reads a drawn polyline instead of a clicked point — a different-shaped input than ExplorePanel's point/depth/clock triplet.

| Pane | Component | Purpose | Data fetching |
|---|---|---|---|
| Explore (footer) | `app/components/ExplorePanel.vue` | Flat selector — Timeseries \| Model depth \| Sensor depth (the last only for profilers). Timeseries always overlays model + sensor; the depth section is a lens above it, not a replacement — the chart stays the read-out. Depth sections render as `app/components/depth/TimeDepthHeatmap.vue` (Hovmöller heatmap, ECharts `custom` series), with a companion vertical-profile drawer (`SelectedVariableDrawer.vue`) that follows the panel's bin-mode toggle and updates on heatmap cell clicks | `composables/useSensorTimeseries.ts` → `/extractTimeseries`/`/sensorTimeseries`; `useTimeDepthWindow.ts` + `useModelTimeseries.ts`/`useDepthProfileFetch.ts` → `/depthProfile`; `useClimateTimeseries.ts` → `/extract_climateTimeseries` |
| Cross-Section (footer) | `app/components/crossSection/CrossSectionPanel.vue` | Reads a polyline drawn on the map (mapbox-gl-draw, wired up in `index.vue` since it owns the `map`/draw-control instances — see `crossSectionDraw` and the `activeTab` watcher there) instead of a clicked point. Renders the depth-vs-distance grid via the same `TimeDepthHeatmap.vue` used by Explore's depth sections, with drawn-vertex boundary markers overlaid as plain CSS (irregular spacing doesn't fit `TimeDepthHeatmap`'s uniform `gridlineBins`). "New line" bumps `mainStore.crossSectionRedrawToken` to re-arm drawing without a tab round-trip | `composables/useCrossSectionFetch.ts` → `POST /crossSection` |
| Analysis (fullscreen) | `app/components/AnalysisWorkspace.vue` | Tabs: Overview (`analysis/AnalysisBuilder.vue`, `source: 'model' \| 'sensor'` prop), Extreme Events, Compound Stress, Trend, Climatology Anomaly, Correlation | `composables/useAnalysisFetch.ts`/`useSensorAnalysisFetch.ts` → `POST /analysis/timeseries`; sub-tabs share one fetched "primary series" per point/variable/depth via `AnalysisWorkspace.vue`'s `fetchSeriesFor`, plus a memoized `cachedFetch` for secondary-variable series (Compound Stress, Correlation) |
| Comparison (fullscreen) | `app/components/ComparisonWorkspace.vue` | Tabs: Timeseries (`sensorComparison.vue`), Depth sections (`comparison/ComparisonSections.vue`, only for variable-depth sensors), Scatter, Residuals, Seasonal Cycle — shown only when a sensor is selected | — |

Selecting a sensor deliberately does not navigate between panes (`selectSensor` in `stores/main.ts`) — under the fullscreen design a jump would throw a dialog over the map on every buoy click, so the sensor instead appears overlaid on Explore's timeseries. Client-side stats helpers live in `composables/useAnalysisStatistics.ts`; ECharts dark theme registration in `composables/useEchartsTheme.ts`.

Other chart-related components: `app/components/sensorInfo.vue` — despite the name, this is the left-panel searchable/filterable sensor list (mounted in `controlPanel.vue`'s "Sensors" expansion panel); it also bundles the per-sensor metadata dialog (info icon) and a heatmap dialog. Selecting a sensor here or on the map both call `mainStore.selectSensor(id, depth)`, and the list auto-scrolls the selected `v-list-item` into view via a `watch` on `mainStore.selectedSensor`.

### Usage Analytics (PostHog)

Both `api/` and `front/` send usage events to **PostHog Cloud** (not self-hosted — the official self-hosted stack bundles its own Postgres/Redis/ClickHouse/Zookeeper/Kafka/MinIO, which would duplicate what this project already runs; this was a deliberate call). Capture is a silent no-op on either side when its key is unset — safe to leave blank in any environment.

- **API** (`api/modules/posthog_helpers.py`): `capture_event(http_request, event, properties)`, configured via `POSTHOG_API_KEY`/`POSTHOG_HOST`. Called from `sensorTimeseries`, `depthProfile`, `extractTimeseries`, `extract_climateTimeseries`, `getMinMax`, `getProfile`, and `analysis/timeseries` — tile-serving and the `/admin/sync*` routes are deliberately excluded (too high-volume / not user behavior).
- **Frontend** (`front/app/plugins/posthog.client.ts` + `front/composables/useAnalytics.ts`'s `trackEvent()`), configured via `NUXT_PUBLIC_POSTHOG_KEY`/`NUXT_PUBLIC_POSTHOG_HOST`. Custom events only — no PostHog autocapture, no session replay (deliberate: avoids noise from this canvas/map-heavy UI and keeps event volume down). Events: `sensor_selected` (list vs. map), `model_point_queried` (map-click point/area model query), `model_eval_requested`, `tab_switched` / `query_mode_changed` (centralized in `main.ts`'s store actions, covering every call site), `variable_changed`, `share_link_created` (fired when a share link is built, with the tab/variable/depth and whether a sensor or drawn line travels with it). Deliberately *not* instrumented: `getMinMax`'s `autorange()` and the Model Analysis tab's auto-refetch (`scheduleAutoRun`) — both fire on every map pan/zoom or dependent-state change rather than a discrete user action, and would flood PostHog with near-duplicates of events already captured upstream.
- **Identity correlation**: the frontend plugin sets `axios.defaults.headers.common['X-PostHog-Distinct-Id']` to posthog-js's distinct_id once, globally — this codebase has no shared `axios.create()` instance, so every composable's `axios.post/get` picks it up automatically. `SERVER.py`'s `_stamp_request_start_time` middleware reads that header into `request.state.distinct_id` once per request; `capture_event` prefers it over IP-based attribution, falling back to IP for non-browser callers (curl, health checks, direct API access). `disable_geoip` always stays IP-based regardless of which distinct_id is used.
- **Gotcha**: editing `POSTHOG_API_KEY` / `NUXT_PUBLIC_POSTHOG_KEY` in `.env.dev` does not affect an already-running container — env vars are baked in at container creation, not read live. Use `docker compose up -d --force-recreate front api` (a plain `restart` reuses the old environment). The Vercel-deployed frontend has the equivalent gotcha: env var changes need a new deployment to take effect, and the repo's `vercel.json` sets `git.deploymentEnabled: false`, so git pushes don't auto-deploy — trigger a manual deploy after changing env vars there too.

PostHog is now the **only** usage-analytics tool — a pre-existing self-hosted Umami setup (script tag in `nuxt.config.ts`'s `<head>`, plus a `umami`/`umami-db` service pair in `docker-compose.prod.frontend.yml`) was removed once PostHog covered pageviews and more. If `analytics.oa.cioospacificlabs.ca` still resolves anywhere (external DNS/reverse proxy, outside this repo), that's now a dangling route to clean up separately.

## Python Environment

`process/` and `scripts/` use **uv** (see `pyproject.toml` + `uv.lock` in each). `api/` uses pip with `requirements.txt`. Each subproject has its own `.venv`.

## Version Logging
When asked to log a new version or update changes:
- Update `CHANGELOG.md` using Keep a Changelog format (Added, Changed, Fixed).
- Bump the version in `package.json` according to SemVer.
- Summarize key user-facing updates in 3 bullet points or less.