# Changelog

All notable changes to OceanECO are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Releases have historically been tracked as long-lived `vX.Y` branches rather than
git tags; the dates below are the last commit on each version branch.

## [Unreleased] — v0.4.1

### Added
- **Share links** — the entire view (tab, coordinate, variable, depth, instant, drawn
  cross-section line, per-tab control state) encodes into a `#s=` URL fragment via
  `composables/useShareState.ts`, compressed with the platform's own `CompressionStream`.
- **`useViewState` / `useChartZoom`** — namespaced per-view state bag in the Pinia store so
  per-tab controls and chart zoom extents become shareable without schema changes.
- **DFO MEDS buoy catalog** (`sensors/catalog_dfo_meds.yml`) and extra ERDDAP fetch
  constraints for more reliable sensor ingestion.
- Disabled states and explanatory tooltips across controls that require a prior selection.

### Changed
- **Variable-aware value rounding** — `formatDisplayValue()` is now the single rounding
  answer for every tooltip, axis label and readout, derived from `variable_config.yml`
  precision in the currently displayed unit.
- **Machine-readable CSV preamble** — provenance lines are emitted as `# label,value`
  two-column CSV, with `latitude`/`longitude` on their own rows and units moved to a
  second header row instead of being baked into column names.
- Legend swatches pinned to one style app-wide via the shared ECharts dark theme.

## [0.4.1-nuxtui] — 2026-08-28

### Added
- **CSV export across the app** — `composables/useCsvExport.ts` + `DownloadButton.vue`,
  replacing ECharts' read-only `dataView` toolbox. One control per host view, datasets
  built from source data so files keep full precision.

### Changed
- **Migrated the frontend from Vuetify to Nuxt UI v4** (Tailwind v4 + Reka UI), including
  a full overlays/button refactor and `UAlert`s given proper `description` messaging.
- Reworked climate data extraction and its error handling.

## [0.4.0] — 2026-08-19

### Added
- **Cross-Section pane** — new `POST /crossSection` endpoint plus `CrossSectionPanel.vue`,
  rendering the model's depth-vs-distance grid along a polyline drawn on the map, with
  numbered vertex markers and KD-tree nearest-cell snapping.
- **Time-Depth (Hovmöller) heatmap** — `depth/TimeDepthHeatmap.vue` as an ECharts `custom`
  series, with a bin-mode-aware vertical-profile drawer and a crosshair readout.
- **`ChartContextBar`** — per-source context items (Field/Model/Sensor/Range/Point) so a
  depth shown on a chart is never unattributed.
- **2021 heat dome case study** — first long-form narrative page under `pages/caseStudy/`.
- **Display-unit toggling** for variables, and `bin_mode` support on `/getMinMax` and
  `/extract_climateTimeseries`.
- In-process response caching for the extract endpoints; client-side request caching.
- Optional map place labels (off by default).

### Changed
- Unified datetime handling across components, aligning chart timestamps with the raster
  layer's instant and removing redundant duplicated state.

## [0.3.1] — 2026-08-05

### Added
- **Variable display registry** — `variable_config.yml`'s `name:` becomes the sole label
  source across every component.
- `MobileBlocker` component and a user-resizable footer with a drag handle.

### Changed
- Refactored control-panel layout.
- Improved buoy click resolution so adjacent stations no longer disambiguate incorrectly.

### Removed
- **Large dead-code cleanup campaign**: the orphaned model-evaluation (`getEval`) subsystem,
  dead API extract-handler boilerplate, superseded standalone process scripts, dead
  frontend composables/icons/plugins/prototype pages, stale root scripts and docs.

## [0.3.0] — 2026-07-28

The largest release: the database was replaced, the process pipeline rewritten, and
sensors became a first-class subsystem.

### Added
- **ClickHouse as the sole database** — `grid_SSC`, `SalishSeaCast_hourly`/`_daily`,
  `SalishSeaCast_status`, `sensors`/`sensor_timeseries`.
- **SalishSeaCast process pipeline** (`process/SSC/`) with a `check → download → compute →
  image → ingest → sync` state machine, remote-processing mode, and rsync-over-
  `cloudflared access ssh` transfer to the API machine (`POST /admin/syncHourly`).
- **Analysis Builder + advanced analysis modes** — trend significance, extreme events,
  compound stress, climatology anomalies, correlation; point and area (polygon) query modes.
- **Sensor subsystem** (`sensors/`) — ONC/ERDDAP ingestion, YAML catalogs, UUID sensor ids,
  variable-depth (profiler) support, map spiderfying, filtering, and a Sensor Analysis tab.
- **Depth Profile** component and `/getProfile` migrated to ClickHouse.
- **PostHog analytics** on both API and frontend, with `X-PostHog-Distinct-Id` correlation.
- Bathymetry vector tiles, palette picker, cursor coordinates, chart zoom persistence,
  historical range statistics, sticky legend highlighting, and the OceanECO logo/branding.

### Changed
- `/variables` and `/colormaps` migrated to ClickHouse; depth config simplified to plain
  numbers with an enforced tolerance instead of silent fallback.
- Area-mode support added to `/extractTimeseries`.

### Removed
- **Postgres and PostGIS entirely**, along with the Umami analytics stack and the
  deprecated `nc_jobs` migration scripts.

## [0.2.1] — 2026-06-23

### Changed
- **Renamed the app and all components from OAH to OceanECO.**
- Monthly climatology restored; timeseries chart info button added; map zoom-out extended.

## [0.2.0] — 2026-05-06

### Added
- **Control panel architecture** — variable selection, colorbar settings (nouislider),
  sensor list, and a toggleable panel.
- `TimeseriesChart.vue` extracted from the landing page; `TimeControls` with a date picker
  and 24-hour stepping.
- Sensor management: ONC/ERDDAP fetch scripts, variable mapping/conversion, netCDF4 storage.
- Cloudflare Pages deployment via GitHub Actions.
- CIOOS/MEOPAR branding, buoy map icons, animated map-click marker, redesigned About page.

### Changed
- Upgraded to Vuetify 4; `moment` → `moment-timezone`.
- Resolved the long-standing NetCDF read/lock contention issue.

## [0.1.0] — 2026-04-10

Initial working dashboard.

### Added
- **API** (FastAPI) — `/extractTimeseries` with depth and date-range support,
  `/extract_climateTimeseries`, `/getProfile`, `/getMinMax`, `/sensorTimeseries`, and a
  model-evaluation endpoint, all behind a `ProcessPoolExecutor` with concurrency limits.
- **Frontend** (Nuxt + Vuetify + MapboxGL) — map, depth slider, colorbar select, vertical
  profile drawer, model-evaluation and About pages, dark theme.
- **Process pipeline** — ERDDAP download, PyCO2SYS carbonate-system computation (pH,
  Ω aragonite), `nc_jobs` state tracking in Postgres, bottom-layer extraction.
- **Tile rendering** — `nc2tile.py` curvilinear → Web-Mercator reprojection, PNG → WebP.
- Bathymetry contours and raster tiles; Mapbox integration.
- Postgres-backed storage with backup/restore/migrate helpers, and Docker Compose dev +
  production configurations.
