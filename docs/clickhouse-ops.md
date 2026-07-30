# ClickHouse Operations Guide

Runbook for diagnosing and fixing data issues in the SalishSeaCast ClickHouse tables on the API machine.

---

## Duplicate rows in SalishSeaCast_hourly

**Symptom:** A date's row count is a multiple of the expected 51,687,264 (e.g. 103,374,528 = 2×).

**Cause:** `SalishSeaCast_hourly` is a plain `MergeTree` — it keeps every insert. A concurrent race in the sync stage's `_claim_and_import` (two HTTP requests for the same date arriving before either writes `syncing` to the log) can trigger two back-to-back inserts of identical data.

### 1. Find affected dates and partitions

```sql
SELECT toYYYYMM(time) AS partition, toDate(time) AS date, count() AS rows
FROM SalishSeaCast_hourly
GROUP BY partition, date
HAVING rows != 51687264
ORDER BY date;
```

### 2. Deduplicate each affected partition

Run one partition at a time. `FINAL` merges all parts in the partition into one, guaranteeing complete deduplication. Rows are byte-identical (same Native-format source file), so `DEDUPLICATE` removes exact duplicates safely.

Use `--receive_timeout` to avoid the client's default 300-second timeout disconnecting you mid-wait. The operation continues server-side either way, but the disconnect makes it look like a failure when it isn't.

```bash
# Replace 202603 with the YYYYMM value from step 1
docker compose -f docker-compose.prod.api.yml exec db-ch \
  clickhouse-client --receive_timeout=3600 \
  --query "OPTIMIZE TABLE SalishSeaCast_hourly PARTITION '202603' FINAL DEDUPLICATE"
```

Expect 15–20 minutes per partition for a full month of data. If the client times out anyway (you'll see `Timeout exceeded while receiving data from server`), the server-side operation is still running — verify completion with step 3 before assuming failure.

### 3. Verify

```sql
SELECT toDate(time) AS date, count() AS rows
FROM SalishSeaCast_hourly
WHERE toYYYYMM(time) = '202603'
GROUP BY date
ORDER BY date;
```

All dates should show exactly 51,687,264 rows.

### 4. Fix the sync log for affected dates

Check the log — a second import attempt that saw 2× rows would have set status to `failed`:

```sql
SELECT * FROM SalishSeaCast_sync_log FINAL
WHERE date IN ('2026-03-06')   -- substitute affected dates
ORDER BY date;
```

If any show `failed`, mark them `success` so the process machine doesn't re-import them:

```sql
INSERT INTO SalishSeaCast_sync_log (date, status, updated_at)
VALUES ('2026-03-06', 'success', now());

OPTIMIZE TABLE SalishSeaCast_sync_log FINAL;
```

Repeat for each affected date.

---

## Checking sync status

### Row counts by date (all time)

```sql
SELECT toDate(time) AS date, count() AS rows
FROM SalishSeaCast_hourly
GROUP BY date
ORDER BY date DESC
LIMIT 30;
```

### Sync log — recent activity

```sql
SELECT *
FROM SalishSeaCast_sync_log FINAL
ORDER BY date DESC
LIMIT 30;
```

### Daily sync log

```sql
SELECT *
FROM SalishSeaCast_daily_sync_log FINAL
ORDER BY date DESC
LIMIT 30;
```

### Dates with no hourly data

Useful after a migration or suspected missed sync:

```sql
SELECT DISTINCT toDate(time) AS date
FROM SalishSeaCast_daily            -- daily has one row per date, cheap to scan
WHERE date NOT IN (
    SELECT DISTINCT toDate(time) FROM SalishSeaCast_hourly
)
ORDER BY date;
```

---

## How sync works (quick reference)

The process machine (`process/SSC/sync.py`) does the following for each date:

1. Exports `SalishSeaCast_hourly` rows to a Native-format file and records the row count.
2. rsyncs the file (and WebP image directories) to the API machine over SSH via Cloudflare Access.
3. POSTs to `POST /admin/syncHourly` with `{ date, expected_rows }`.
4. The API machine (`api/modules/sync_hourly.py`) claims the date in `SalishSeaCast_sync_log`, imports the file, verifies the row count matches `expected_rows`, then marks the log `success`.
5. Only after a `200` response does the process machine delete its local copy of the rows.
6. Steps 1–5 repeat for `SalishSeaCast_daily` via `POST /admin/syncDaily`.

A `409` response means the API already has the date (idempotent — treated as success). Any other non-200 is a failure; the process machine retries on the next sync run.

**The process machine never queries the API's ClickHouse directly** — it trusts the HTTP response code from `/admin/syncHourly`.

---

## Re-syncing a specific date

If you need to force a full re-sync of a date from the process machine (e.g. after manually clearing bad data):

```bash
# On the process machine
uv run python -m SSC.cli sync --date YYYY-MM-DD --force
```

Before doing this, clear the API's sync log entry for that date so the import isn't blocked by a `success` status:

```sql
-- On the API machine's ClickHouse
INSERT INTO SalishSeaCast_sync_log (date, status, updated_at)
VALUES ('YYYY-MM-DD', 'failed', now());

OPTIMIZE TABLE SalishSeaCast_sync_log FINAL;
```

And delete any existing rows for that date to avoid the partial-import path leaving stale data:

```sql
ALTER TABLE SalishSeaCast_hourly
DELETE WHERE time >= 'YYYY-MM-DD 00:00:00' AND time < 'YYYY-MM-DD 00:00:00' + INTERVAL 1 DAY;

-- Wait for mutation to finish, then verify:
SELECT count() FROM SalishSeaCast_hourly WHERE toDate(time) = 'YYYY-MM-DD';
```

---

## Connecting to ClickHouse on the API machine

```bash
# One-shot query
docker compose -f docker-compose.prod.api.yml exec db-ch \
  clickhouse-client --query "SELECT count() FROM SalishSeaCast_hourly"

# Interactive session (raise receive_timeout for long-running OPTIMIZE commands)
docker compose -f docker-compose.prod.api.yml exec db-ch \
  clickhouse-client --receive_timeout=3600
```

If the API is configured with `CH_USE_REMOTE=true`, the data lives in the remote ClickHouse pointed to by `CH_REMOTE_URL` — the local `db-ch` container will be empty. Check:

```bash
docker compose -f docker-compose.prod.api.yml exec api env | grep CH_
```
