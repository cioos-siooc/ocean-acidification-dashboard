# Sensors service

Fetches time-series data from ONC and ERDDAP sources and stores it in ClickHouse.  Runs on the **API machine** alongside `db-ch` — no sync stage needed.

## Architecture

```
ONC API / ERDDAP  →  sensors container  →  db-ch (ClickHouse)  →  /sensorTimeseries API
```

Sensor metadata (`sensors` table) and time-series observations (`sensor_timeseries` table) both live in ClickHouse.  PostgreSQL is not used.

---

## First-time setup (new API machine)

### 1. Build the image

```bash
docker compose -f docker-compose.prod.api.yml build sensors
```

### 2. Create ClickHouse tables

Safe to re-run on any machine — uses `IF NOT EXISTS`. It warns when an existing `sensor_timeseries` predates the per-instrument key; rebuild that one once, after `import --overwrite` and with `sensors-scheduler` stopped:

```bash
docker compose -f docker-compose.prod.api.yml run --rm sensors \
  uv run python manage_sensors.py migrate-source-key
```

It copies every row into a table keyed by `source`, tagging old rows with their variable's source when it has only one. Old rows of a multi-source variable can't be attributed, so they are dropped and the next harvest re-fetches each source. The previous table is kept as `sensor_timeseries_old` until you drop it.

```bash
docker compose -f docker-compose.prod.api.yml run --rm sensors \
  uv run python manage_sensors.py setup
```

---

## Discovering sensors

Before adding a sensor, use these helper scripts to look up what's available and generate a catalog entry to paste into the matching `catalog_*.yml` file (run locally with `uv run`, not inside the container).

### ONC locations: `discover_location.py`

Looks up an ONC `locationCode` and its child locations (e.g. `CRIP` → `CRIP.C1`, `CRIP.C2`, `CRIP.O1`) and prints the variables available there, mapped to canonical names via the `CANONICAL` dict in the script. Each variable lists every instrument that reports it as `sources`, most recently deployed first; review that order before pasting.

```bash
cd sensors
uv run python discover_location.py SCVIP
uv run python discover_location.py SCVIP SEVIP FGPD   # multiple at once

# Also show unmapped sensor codes as commented-out lines (useful for extending CANONICAL)
uv run python discover_location.py SCVIP --all
```

When given multiple location codes, a failure on one (e.g. an invalid code) doesn't abort the others — failed codes are collected and reported at the end.

### ERDDAP servers: `discover_erddap.py`

Queries an ERDDAP server's tabledap datasets and generates catalog entries.

```bash
# List all tabledap datasets on a server
uv run python discover_erddap.py https://catalogue.hakai.org/erddap

# One specific dataset
uv run python discover_erddap.py https://catalogue.hakai.org/erddap --dataset HakaiBIOOSBuoy1hour

# Filter by keyword in title or dataset ID
uv run python discover_erddap.py https://catalogue.hakai.org/erddap --filter buoy

# Also show unmapped variables as commented-out lines
uv run python discover_erddap.py https://catalogue.hakai.org/erddap --filter buoy --all
```

Progress messages go to stderr and the catalog YAML goes to stdout, so redirect stdout to save the output:

```bash
uv run python discover_erddap.py https://catalogue.hakai.org/erddap --filter buoy > candidates.yaml
```

Review the generated YAML (remove variables you don't want, double-check dissolved-oxygen unit conversion factors) before pasting into a `sensors/catalog_*.yml` file.

---

## Adding sensors

### Preferred: catalog files (batch)

Every sensor lives in exactly one catalog file, one per harvest source:

| File | Harvested by |
|---|---|
| `catalog_ONC.yml` | `onc_to_ch.py` |
| `catalog_hakai.yml`, `catalog_uw.yml`, `catalog_dfo_meds.yml` | `erddap_to_ch.py` |

A new ERDDAP provider gets its own `catalog_<provider>.yml`. `manage_sensors.py import` reads every `catalog_*.yml` (a sensor name may appear only once across them), and the harvesters read what it wrote to the `sensors` table.

#### Catalog format

```yaml
sensors:
  - name: "Campbell River Underwater Network"
    lat: 50.020757
    lon: -125.23537
    depth: 8.248571
    api: ONC
    organization: ONC
    location_code: CRIP              # the site; its Data Search page and profiler depth source
    source_link: "https://data.oceannetworks.ca/DataSearch?location=CRIP"
    variables:
      temperature:
        unit: C
        factor: 1.0
        sources:                     # LOCATION/DEVICE_CATEGORY/sensor_code, priority order
        - CRIP.C1/CTD/temperature
        - CRIP/WETLABS_WQM/temperature
        - CRIP.C2/CTD/temperature
      chlorophyll:
        unit: µg/L
        factor: 1.0
        sources: [CRIP/FLUOROMETER/chlorophyll]

  - name: "Hakai KC Buoy"
    api: ERDDAP
    source_link: "https://catalogue.hakai.org/erddap/tabledap/HakaiKCBuoy1hour"
    # …lat, lon, depth, organization
    variables:
      temperature:
        code: WaterTemp              # the ERDDAP column
        unit: C
        factor: 1.0
```

| Field | Description |
|---|---|
| variable key | Canonical name used throughout the system (e.g. `dissolved_oxygen`) |
| `unit` | Display unit string (e.g. `ml/L`) |
| `factor` | Raw value is multiplied by this before storage — `1.0` if no conversion is needed |
| `code` | ERDDAP only: the dataset's column. `time`/`depth` entries rename the axis columns; a `depth` with `unit: dbar` is pressure, converted at ingestion |
| `sources` | ONC only: `LOCATION/DEVICE_CATEGORY/sensor_code` for each instrument that reports the variable, highest priority first. The code can differ per instrument (e.g. `oxygen` on a WQM, `oxygen_corrected` on an optode) |

**Several sources.** ONC sites often have instruments on child locations (`CRIP.C1`, `CRIP.C2`) that come and go between deployments. `onc_to_ch.py` stores every source as its own series, tagged in `sensor_timeseries.source` (`CRIP.C1/CTD`; part of the table key) and resumed from its own last timestamp. A source with no deployment in the requested window is skipped silently. Readers merge them: `/sensorTimeseries` and `/depthProfile` give each timestamp the value of the first source in the list that has one, and `/sensorTimeseries` with `instrument` returns one source's series, which Explore's "Compare instruments" toggle plots side by side. Profilers (`depth: -1`) harvest only each variable's first source, which must be at `location_code`.

YAML anchors (`&name` / `*name`) work for variable sets shared by several sensors.

Import all catalogs in one command (the image carries the catalogs it was built with; mount an edited file over its copy to use it without a rebuild):

```bash
docker compose -f docker-compose.prod.api.yml run --rm \
  -v $(pwd)/sensors/catalog_ONC.yml:/app/catalog_ONC.yml \
  sensors uv run python manage_sensors.py import

# --overwrite updates sensors that already exist by name; --file limits it to one catalog
docker compose -f docker-compose.prod.api.yml run --rm sensors \
  uv run python manage_sensors.py import --overwrite --file catalog_ONC.yml
```

The import is idempotent by name — sensors already in CH are skipped unless `--overwrite` is passed. Edit a sensor in its catalog and re-import with `--overwrite`, rather than with `update`, so the catalog stays the record. After importing, backfill all new sensors at once:

```bash
docker compose -f docker-compose.prod.api.yml run --rm sensors \
  uv run python onc_to_ch.py
```

Full ONC location list: https://wiki.oceannetworks.ca/spaces/O2A/pages/49447553/Available+Locations

---

### One-off: single sensor via CLI

### ONC sensor

The `--variable` format for ONC is `canonical:onc_code:unit:conversion_factor:device_category`.  The 5th field is the ONC `deviceCategoryCode` at `--location-code` — a single source. For several, or a child location, use the catalog.

```bash
docker compose -f docker-compose.prod.api.yml run --rm sensors \
  uv run python manage_sensors.py add \
  --name "Central Strait of Georgia VENUS platform" \
  --lat 49.0405 --lon -123.4247 --depth 298.93 \
  --source-type ONC --location-code SCVIP \
  --variable "dissolved_oxygen:oxygen_corrected:ml/L:44.66:OXYSENSOR" \
  --variable "temperature:temperature:C:1.0:CTD" \
  --variable "salinity:salinity:PSU:1.0:CTD"
```

### ONC profiler (variable depth)

Use `depth: -1` (catalog) or `--depth -1` (CLI), e.g. `YPVPP`. `onc_to_ch.py` then fetches 10 s ONC averages from first deployment onward (in 7-day windows), pairs every variable with the location's `CTD` `depth` reading at the same timestamp, and stores (hour, 1 m) cell means instead of one hourly value at depth 0. The location must have a `CTD` device; its first backfill is long, so run it by hand rather than leaving it to the scheduler.

### ERDDAP tabledap sensor (fixed depth)

```bash
docker compose -f docker-compose.prod.api.yml run --rm sensors \
  uv run python manage_sensors.py add \
  --name "Hakai Wirewalker" \
  --lat 50.112 --lon -125.093 --depth 20.0 \
  --source-type ERDDAP \
  --source-link "https://catalogue.hakai.org/erddap/tabledap/HakaiWirewalker" \
  --variable "temperature:temperature:C:1.0" \
  --variable "salinity:salinity:PSU:1.0"
```

### ERDDAP griddap sensor (variable depth)

Use `--depth -1` for sensors that return data at multiple depths.

```bash
docker compose -f docker-compose.prod.api.yml run --rm sensors \
  uv run python manage_sensors.py add \
  --name "ORCA Buoy" \
  --lat 47.35 --lon -122.65 --depth -1 \
  --source-type ERDDAP \
  --source-link "https://nwem.apl.uw.edu/erddap/griddap/orca1_L3" \
  --variable "temperature:temperature:C:1.0" \
  --variable "salinity:salinity:PSU:1.0"
```

The `add` command prints the generated UUID and the exact backfill command to run next.

---

## Backfilling a sensor

Run after `add`.  With no prior data in ClickHouse the script fetches the full history from ONC/ERDDAP automatically.

```bash
# ONC
docker compose -f docker-compose.prod.api.yml run --rm sensors \
  uv run python onc_to_ch.py --sensor-id <uuid>

# ERDDAP
docker compose -f docker-compose.prod.api.yml run --rm sensors \
  uv run python erddap_to_ch.py --sensor-id <uuid>
```

---

## Ongoing updates (Prefect)

The `sensors-scheduler` service (`docker-compose.prod.api.yml`) serves `flows.py` to the shared Prefect server at https://pipelines.cioospacific.ca: deployments `OceanECO-sensors-ERDDAP` (hourly, on the hour) and `OceanECO-sensors-ONC` (hourly, at half past), one task run per sensor. A sensor whose source fails shows up as a failed task, and the run is marked Failed; the other sensors still run. A run that comes due while the previous one is still going is cancelled rather than queued (what `flock -n` did for the old cron).

Add to the API machine's env file:

```bash
PREFECT_API_URL=https://pipelines.cioospacific.ca/api
PREFECT_AUTH_STRING=user:password
```

then start it:

```bash
docker compose -f docker-compose.prod.api.yml --env-file <env file> up -d --build sensors-scheduler
```

`PREFECT_API_URL` is required (`:?`) by every command against this compose file, `api` included. Optional: `ERDDAP_CRON`, `ONC_CRON` (defaults `0 * * * *`, `30 * * * *`), `SENSORS_SCHEDULE_PAUSED` (default `false`, re-applied on every start). Ad-hoc runs (e.g. one `sensor_id`) go through the UI's "Run → custom". It replaces the host crontab entries for `erddap_to_ch.py`/`onc_to_ch.py` (and `updateSensors.sh`): remove those, or the runs overlap. Rebuild/recreate `sensors-scheduler` after a code update.

---

## Sensor management reference

```bash
# List all sensors
uv run python manage_sensors.py list

# Deactivate / reactivate (use UUID from list)
uv run python manage_sensors.py deactivate --id <uuid>
uv run python manage_sensors.py activate   --id <uuid>

# Update fields on an existing sensor
uv run python manage_sensors.py update --id <uuid> --name "New name"
uv run python manage_sensors.py update --id <uuid> \
  --variable "temperature:temperature:C:1.0:CTD" \
  --variable "salinity:salinity:PSU:1.0:CTD" \
  --location-code SCVIP
```

---

## Maintenance utilities

### Backfill ONC source metadata: `backfill_onc_source.py`

A re-runnable maintenance script (separate from data ingestion) that populates
`source.description` and `source.link` on existing **ONC** sensors in the CH
`sensors` table. It looks up each sensor's `device_config.locationCode` against
the ONC `/locations` API. ONC's `description` is often blank for generic
platforms/buoys, so the location's `dataSearchURL` is always used as the link.

```bash
docker compose -f docker-compose.prod.api.yml run --rm sensors \
  uv run python backfill_onc_source.py             # apply
docker compose -f docker-compose.prod.api.yml run --rm sensors \
  uv run python backfill_onc_source.py --dry-run   # preview only
docker compose -f docker-compose.prod.api.yml run --rm sensors \
  uv run python backfill_onc_source.py --force     # overwrite existing description/link too
```

---

## Known ONC sensors

| Name | Location code | Depth |
|---|---|---|
| Central Strait of Georgia VENUS platform | SCVIP | 299 m |
| Strait of Georgia East | SEVIP | 167 m |
| Folger Deep | FGPD | 96 m |
| Folger Pinnacle | FGPPN | 25 m |
