# OA API — Endpoint Reference

FastAPI app defined in [`api/SERVER.py`](api/SERVER.py). This document is a hand-maintained
overview; the **authoritative, always-current schema is the auto-generated OpenAPI docs at
`/docs`** (Swagger UI) or `/openapi.json`. When in doubt, trust those over this file.

**Total routes: 18** (7 GET, 11 POST). One route (`/metadata/{var}`) is currently commented out
in `SERVER.py` and is *not* served — it is omitted here.

## Cross-cutting behavior

- **Concurrency**: Most extraction endpoints acquire `_extract_semaphore`
  (`MAX_CONCURRENT_EXTRACTS`, default 4). If a slot isn't free within 10s the request returns
  **429**. Blocking work runs in a `ProcessPoolExecutor`/threadpool with a hard `THREADPOOL_TIMEOUT`
  (default 120s).
- **Out-of-domain / no-data errors** (e.g. "N km from the nearest grid point", "Grid table is
  empty", "No data available…") are returned as **400**, not 500.
- **Usage analytics**: Every endpoint except tile-serving (`/png`, `/vector`, `/raster_tiles`) and
  the `/admin/*` sync routes fires a PostHog `capture_event` on success (see
  `api/modules/posthog_helpers.py`).
- **Data source**: SalishSeaCast (SSC) is the only supported `source`; requests with any other
  source return **400**. LiveOcean support was removed.
- **Storage**: model timeseries/grid/sensors live in **ClickHouse**; variable + colormap metadata
  are static files (`shared/variable_config.yml`, `shared/colormaps.json`); rendered tiles are
  files on disk. PostgreSQL/PostGIS has been fully removed.

---

## GET endpoints

### `GET /`
Health check. Returns `{"message": "Hello from OAH API!"}`.

### `GET /variables`
List of variables with per-variable metadata (colormap bounds, precision, available depths,
available datetimes, geographic bounds, source). Backed by `modules/variables.py` (reads
`shared/variable_config.yml` + ClickHouse availability). No parameters.

### `GET /sensors`
Active sensor stations from ClickHouse (`sensors FINAL` joined to `sensor_timeseries` aggregates).
Each item:
```json
{
  "id": "uuid-string",
  "name": "…", "latitude": 0.0, "longitude": 0.0, "depth": 0.0,
  "device_config": {}, "variables": {}, "active": true,
  "first_data_at": "ISO|null", "latest_data_at": "ISO|null",
  "depth_min": 0.0, "depth_max": 0.0,
  "source": {}, "organization": "…"
}
```
Note: `id` is a **string UUID**; only `active = 1` sensors are returned.

### `GET /colormaps`
All colormaps from `shared/colormaps.json`, sorted by `name`.

### `GET /png/{source}/{var}/{dt}/{depth}`
Serves a preprocessed WebP/PNG tile. `source` must be `SalishSeaCast` (else 400). Searches
`SSC_IMAGE_DIR` (+ optional `SSC_IMAGE_DIR_ARCHIVE`), tries `.webp` then `.png`. **Never generates
on demand** — 404 if the file doesn't exist. Immutable cache headers.

### `GET /vector/{z}/{x}/{y}.pbf`
Bathymetry vector tile (`VECTOR_ROOT`, default `/opt/data/bathymetry/NONNA/tiles`). 404 if missing.
Immutable cache headers.

### `GET /raster_tiles/{z}/{x}/{y}.webp`
Bathymetry raster tile (`RASTER_TILES_ROOT`, default `/opt/data/bathymetry/NONNA/raster_tiles`).
404 if missing. Immutable cache headers.

---

## POST endpoints

### `POST /sensorTimeseries`
Sensor telemetry from ClickHouse `sensor_timeseries` over a date range.
```json
// Request
{ "sensorId": "uuid", "modelVariable": "dissolved_oxygen",
  "fromDate": "ISO", "toDate": "ISO", "depth": 0.0, "source": "SalishSeaCast" }
// Response
{ "time": ["ISO", …], "value": [12.5, null, …] }
// or, with a depth axis:
{ "time": […], "depth": […], "value": […] }
```
`depth` and `source` are optional (only needed to resolve depth for variable-depth sensors).
Module: `modules/extractSensorTimeseries.py`.

### `POST /depthProfile`
Bins a variable-depth sensor's raw casts onto the model's depth levels + time buckets — the
Comparison tab's Depth Profile (Hovmöller) view.
```json
// Request
{ "source": "SalishSeaCast", "var": "…", "sensorId": "uuid",
  "lat": 0.0, "lon": 0.0, "fromDate": "ISO", "toDate": "ISO", "binHours": 1 }
// Response  (both grids are depths × time)
{ "time": ["ISO", …], "depths": [0.5, …],
  "model": [[…], …], "sensor": [[… | null], …] }
```
`binHours` is `1` or `24`. Module: `modules/extract_depth_profile.py`.

### `POST /extractTimeseries`
Model timeseries at a point **or** averaged over a polygon.
```json
// Request — provide EITHER lat+lon (point) OR polygon (area, ≥3 pts)
{ "source": "SalishSeaCast", "var": "…",
  "lat": 0.0, "lon": 0.0,
  "polygon": [[lon, lat], …],
  "depth": 0.0, "fromDate": "ISO", "toDate": "ISO" }
// Response
{ "time": [… | null], "value": [… | null] }
```
`depth` omitted/null means all depths. Missing both point and polygon → 400. Module:
`modules/extractTimeseries.py` (via `ProcessPoolExecutor`).

### `POST /extract_climateTimeseries`
Day-of-year climatology (mean/min/max per calendar day) across the requested date range, for the
nearest grid cell.
```json
// Request
{ "var": "…", "lat": 0.0, "lon": 0.0, "depth": 0.0, "fromDate": "ISO", "toDate": "ISO" }
// Response
[ { "requested_date": "YYYY-MM-DDT12:00:00", "mean": 0.0, "min": 0.0, "max": 0.0 }, … ]
```
Module: `modules/extract_climate_timeseries.py`. (Reads pre-aggregated daily climatology; only days
with data are returned.)

### `POST /getMinMax`
Min/max of a variable at a datetime/depth, optionally within a bounding box.
```json
// Request
{ "source": "SalishSeaCast", "var": "…", "dt": "ISO", "depth": 0.0,
  "north": 0.0, "south": 0.0, "east": 0.0, "west": 0.0 }
// Response
{ "min": 0.0, "max": 0.0 }
```
Bounding-box fields optional. Module: `modules/extractMinMax.py` (via `ProcessPoolExecutor`).

### `POST /getProfile`
Vertical profile (all depths) at a coordinate + datetime.
```json
// Request  — note lng (not lon); var optional, defaults to "temperature"
{ "source": "SalishSeaCast", "var": "…", "dt": "ISO", "lat": 0.0, "lng": 0.0 }
```
Module: `modules/extract_profile.py` (via `ProcessPoolExecutor`).

### `POST /getEval`
Sensor-vs-model comparison series from an evaluation NetCDF file.
```json
// Request
{ "sensor": "Baynes_5m", "variable": "temperature", "model": "SSC" }
// Response
{ "time": ["ISO", …], "sensor": [… | null], "model": [… | null] }
```
`model` must be `"SSC"` (else 400); file `{EVAL_DATA_DIR}/{sensor}.nc` (default `/opt/data/eval`),
404 if missing. Module: `modules/eval_extractor.py`.

### `POST /analysis/timeseries`
Flat daily series for the "Model Analysis" tab + Advanced Analysis dialog. Point or polygon; the
frontend derives climatology/trend/threshold/extreme stats client-side from this series.
```json
// Request — provide EITHER lat+lon OR polygon (≥3 pts)
{ "polygon": [[lon, lat], …], "lat": 0.0, "lon": 0.0, "depth": 0.0,
  "primaryMetric": { "variable": "…", "stat": "min|mean|max" },
  "temporal": { "yearRange": [2007, 2026] } }
// Response
{ "data": [ { "time": "YYYY-MM-DD", "value": 0.0 }, … ] }
```
Reads `{variable}_{stat}` from `SalishSeaCast_daily`. Module: `modules/ocean_analysis.py`.

### `POST /admin/syncHourly`
Imports a remote pipeline server's exported `SalishSeaCast_hourly` rows. **Bearer-token auth**
(`Authorization: Bearer <SYNC_API_TOKEN>`, else 401). The Native-format file path is derived
server-side from `SSC_SYNC_STAGING_DIR` — never taken from the request.
```json
// Request
{ "date": "YYYY-MM-DD", "expected_rows": 0 }
```
Returns the import result. **409** on `SyncConflict`, **400** on `SyncError`. Called by
`process/SSC/sync.py`. Not analytics-tracked. Module: `modules/sync_hourly.py`.

### `POST /admin/syncDaily`
Counterpart to `/admin/syncHourly` for the `SalishSeaCast_daily` (mean/min/max) table and its own
sync log. Same request shape, same bearer-token auth, same server-derived staging path, same
409/400 semantics. Module: `modules/sync_hourly.py` (`import_daily_native_file`).

---

## Error codes

| Status | Meaning |
|--------|---------|
| 200 | Success |
| 400 | Bad request — invalid params, unsupported source, or out-of-domain / no-data condition |
| 401 | Unauthorized — `/admin/*` without a valid bearer token |
| 404 | Not found — missing tile, eval file, or sensor data |
| 409 | Conflict — `/admin/*` sync conflict (data already imported / mismatch) |
| 429 | Too many concurrent extract requests (semaphore timeout) |
| 500 | Unexpected server error |

## Related modules

| Module | Purpose |
|--------|---------|
| `modules/extractTimeseries.py` | Point/area model timeseries |
| `modules/extract_profile.py` | Vertical profile extraction |
| `modules/extract_depth_profile.py` | Sensor-vs-model depth/time Hovmöller binning |
| `modules/extract_climate_timeseries.py` | Day-of-year climatology |
| `modules/extractMinMax.py` | Min/max (optionally bbox-scoped) |
| `modules/extractSensorTimeseries.py` | Sensor telemetry from ClickHouse |
| `modules/eval_extractor.py` | Sensor-vs-model evaluation series |
| `modules/ocean_analysis.py` | Analysis Builder daily series (point/polygon) |
| `modules/variables.py` | Variable metadata |
| `modules/sync_hourly.py` | `/admin/sync{Hourly,Daily}` importers |
| `modules/clickhouse_helpers.py` | ClickHouse client (local/remote selection) |
| `modules/posthog_helpers.py` | Usage-analytics event capture |
