# clickhouse_test

ClickHouse instance for SalishSeaCast analytics, containerized via Docker Compose.

## Setup

```bash
docker compose up -d
```

ClickHouse HTTP interface is exposed on port `8123`, native protocol on `9000`.
Data is persisted in `./clickhouse_data/`, logs in `./clickhouse_logs/`.

Set `CLICKHOUSE_PASSWORD` in the environment or a `.env` file if you want a non-empty default user password.

## Querying

```bash
# Always use clickhouse-client (not `clickhouse --query`)
docker compose exec db clickhouse-client "SHOW TABLES"
docker compose exec db clickhouse-client "SELECT count() FROM SalishSeaCast_daily"
docker compose exec db clickhouse-client "SELECT DISTINCT toYear(time) AS year FROM SalishSeaCast_daily ORDER BY year"
```

## Adding Data (SSC.py)

`scripts/SSC.py` reads per-variable NetCDF files and inserts them into the `SalishSeaCast_daily` table (created automatically on first run).

**Expected file layout** (mounted at `/app/data/` inside the container):
```
/app/data/
  temperature/temperature_{suffix}.nc
  salinity/salinity_{suffix}.nc
  total_alkalinity/total_alkalinity_{suffix}.nc
  omega_arag/omega_arag_{suffix}.nc
  omega_cal/omega_cal_{suffix}.nc
  ph_total/ph_total_{suffix}.nc
  dissolved_oxygen/dissolved_oxygen_{suffix}.nc
  dissolved_inorganic_carbon/dissolved_inorganic_carbon_{suffix}.nc
```

The suffix is typically a year (e.g., `2024`) or year-month (e.g., `202601`). All 8 files must be present for a suffix — any missing file causes that suffix to be skipped entirely.

**Load one year:**
```bash
docker compose exec db python3 /app/scripts/SSC.py 2024
```

**Load multiple years in one run:**
```bash
docker compose exec db python3 /app/scripts/SSC.py 2022 2023 2024
```

The script uses 2 CPU cores by default (`CORES_TO_USE` at the top of `SSC.py`) and inserts in batches of 500 000 rows. Re-running for an already-loaded suffix will insert duplicate rows — ClickHouse does not deduplicate automatically with MergeTree, so avoid re-running for the same suffix unless you first delete the partition:

```bash
docker compose exec db clickhouse-client --query "ALTER TABLE SalishSeaCast_daily DROP PARTITION '202401'"
```
