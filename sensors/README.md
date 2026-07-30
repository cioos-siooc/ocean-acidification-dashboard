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

Safe to re-run on any machine — uses `IF NOT EXISTS`.

```bash
docker compose -f docker-compose.prod.api.yml run --rm sensors \
  uv run python manage_sensors.py setup
```

---

## Discovering sensors

Before adding a sensor, use these helper scripts to look up what's available and generate a ready-to-paste `catalog.yaml` entry (run locally with `uv run`, not inside the container).

### ONC locations: `discover_location.py`

Looks up an ONC `locationCode` and prints the sensor variables available there, mapped to canonical names via the `CANONICAL` dict in the script.

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

Review the generated YAML (remove variables you don't want, double-check dissolved-oxygen unit conversion factors) before pasting into `sensors/catalog.yaml`.

---

## Adding sensors

### Preferred: catalog file (batch)

Edit `sensors/catalog.yaml` and add entries for every sensor you want to track.  YAML anchors (`&name` / `*name`) let you define a shared variable set once and reuse it across all sensors with the same instruments — most standard VENUS/NEPTUNE stations share the same CTD + OXYSENSOR variables.

```yaml
_venus_standard: &venus_standard
  - "dissolved_oxygen:oxygen_corrected:ml/L:44.66:OXYSENSOR"
  - "temperature:temperature:C:1.0:CTD"
  - "salinity:salinity:PSU:1.0:CTD"

sensors:
  - name: "Strait of Georgia East"
    lat: 49.042635
    lon: -123.317605
    depth: 167.14
    source_type: ONC
    location_code: SEVIP
    variables: *venus_standard
```

Then import the whole file in one command:

```bash
docker compose -f docker-compose.prod.api.yml run --rm \
  -v $(pwd)/sensors/catalog.yaml:/app/catalog.yaml \
  sensors uv run python manage_sensors.py import

# --overwrite updates sensors that already exist by name
docker compose -f docker-compose.prod.api.yml run --rm \
  -v $(pwd)/sensors/catalog.yaml:/app/catalog.yaml \
  sensors uv run python manage_sensors.py import --overwrite
```

The import is idempotent by name — sensors already in CH are skipped unless `--overwrite` is passed.  After importing, backfill all new sensors at once:

```bash
docker compose -f docker-compose.prod.api.yml run --rm sensors \
  uv run python onc_to_ch.py
```

Full ONC location list: https://wiki.oceannetworks.ca/spaces/O2A/pages/49447553/Available+Locations

---

### One-off: single sensor via CLI

### ONC sensor

The `--variable` format for ONC is `canonical:onc_code:unit:conversion_factor:device_category`.  The 5th field ties the variable to the ONC `deviceCategoryCode` used to fetch it — no separate device flag needed.

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

## Ongoing updates (cron)

`updateSensors.sh` (project root) runs both ingestion scripts for all active sensors.  Add it to crontab on the API machine:

```bash
# Run daily at 06:00
0 6 * * * cd /home/cioos/ocean-acidification-dashboard && ./updateSensors.sh >> logs/sensors.log 2>&1
```

The script uses a `.updating_sensors` lockfile to prevent overlapping runs.

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

## Variable format

| Field | Description |
|---|---|
| `canonical` | Name used throughout the system (e.g. `dissolved_oxygen`) |
| `source_name` | Column / sensor-category code in ERDDAP or ONC (e.g. `oxygen_corrected`) |
| `unit` | Display unit string (e.g. `ml/L`) |
| `conversion_factor` | Raw value is multiplied by this before storage — use `1.0` if no conversion needed |
| `device_category` | ONC only: `deviceCategoryCode` that supplies this variable (e.g. `OXYSENSOR`, `CTD`) |

---

## Known ONC sensors

| Name | Location code | Depth |
|---|---|---|
| Central Strait of Georgia VENUS platform | SCVIP | 299 m |
| Strait of Georgia East | SEVIP | 167 m |
| Folger Deep | FGPD | 96 m |
| Folger Pinnacle | FGPPN | 25 m |
