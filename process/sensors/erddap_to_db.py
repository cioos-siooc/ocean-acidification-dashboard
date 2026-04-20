"""
Fetch sensor data from ERDDAP tabledap sources and store in sensors_data table.

Reads active sensors with source.type == "ERDDAP" from the sensors table,
queries each dataset from the last stored time onward, and upserts into sensors_data.
"""

import os
import io
import csv
import math
import requests
import psycopg2
from psycopg2.extras import Json, execute_values
from datetime import datetime, timedelta, timezone

# DB connection from environment (same as onc_to_db.py)
DB_HOST = os.getenv("PGHOST", "db")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "oa")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "postgres")

# ERDDAP time variable name
ERDDAP_TIME_VAR = "time"

# ISO 8601 UTC format expected by ERDDAP
ERDDAP_TIME_FMT = "%Y-%m-%dT%H:%M:%SZ"


def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )


def fetch_erddap_csv(base_url: str, variables: list[str], date_from: datetime | None) -> list[dict]:
    """
    Fetch data from an ERDDAP tabledap dataset as CSV.

    Args:
        base_url: Full tabledap dataset URL (no extension), e.g.
                  https://catalogue.hakai.org/erddap/tabledap/HakaiWirewalkerProvisional
        variables: List of variable names to request (must match ERDDAP names exactly).
        date_from: Lower time bound (UTC). If None, fetches all available data.

    Returns:
        List of dicts: {var_name: value, ...} per row, including 'time' as a datetime.
    """
    fields = [ERDDAP_TIME_VAR] + variables
    query = ",".join(fields)

    constraints = ""
    if date_from is not None:
        constraints = f"&{ERDDAP_TIME_VAR}>={date_from.strftime(ERDDAP_TIME_FMT)}"

    url = f"{base_url}.csv?{query}{constraints}"
    print(f"  GET {url}")

    resp = requests.get(url, timeout=120)

    if resp.status_code == 404:
        raise RuntimeError(f"Dataset not found (404): {base_url}")
    if resp.status_code != 200:
        raise RuntimeError(f"ERDDAP returned HTTP {resp.status_code}: {resp.text[:300]}")

    # ERDDAP CSV format: first row = column headers, second row = units (skip), then data
    reader = csv.reader(io.StringIO(resp.text))
    rows = list(reader)

    if len(rows) < 2:
        return []

    headers = rows[0]      # variable names
    # rows[1] is the units row — skip it
    data_rows = rows[2:]

    records = []
    for row in data_rows:
        if len(row) != len(headers):
            continue
        record = {}
        for header, value in zip(headers, row):
            if value == "" or value == "NaN":
                continue
            if header == ERDDAP_TIME_VAR:
                try:
                    # ERDDAP time is ISO 8601 UTC e.g. "2026-01-01T00:00:00Z"
                    record[ERDDAP_TIME_VAR] = datetime.strptime(value, ERDDAP_TIME_FMT).replace(tzinfo=timezone.utc)
                except ValueError:
                    record[ERDDAP_TIME_VAR] = None
            else:
                try:
                    v = float(value)
                    if not math.isnan(v):
                        record[header] = v
                except ValueError:
                    pass  # skip non-numeric values
        if record.get(ERDDAP_TIME_VAR) is not None:
            records.append(record)

    return records


def bin_to_hourly(records: list[dict]) -> list[dict]:
    """
    Bin records into hourly slots centred at HH:30:00 UTC.

    Each slot covers [HH:00:00, HH+1:00:00). Records that fall within a slot
    are averaged. Slots with no data are skipped.
    """
    if not records:
        return []

    # Group records by hour bin key (year, month, day, hour)
    bins: dict[tuple, list[dict]] = {}
    for rec in records:
        t = rec[ERDDAP_TIME_VAR]
        bin_key = (t.year, t.month, t.day, t.hour)
        bins.setdefault(bin_key, []).append(rec)

    result = []
    for (year, month, day, hour), bin_records in sorted(bins.items()):
        bin_time = datetime(year, month, day, hour, 30, 0, tzinfo=timezone.utc)

        var_values: dict[str, list[float]] = {}
        for rec in bin_records:
            for k, v in rec.items():
                if k == ERDDAP_TIME_VAR:
                    continue
                var_values.setdefault(k, []).append(v)

        averaged = {ERDDAP_TIME_VAR: bin_time}
        for var, vals in var_values.items():
            averaged[var] = sum(vals) / len(vals)

        result.append(averaged)

    print(f"  Binned into {len(result)} hourly slot(s) at HH:30.")
    return result


def get_last_stored_time(conn, sensor_id: int) -> datetime | None:
    """Return the latest time stored in sensors_data for this sensor."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(time) FROM sensors_data WHERE sensor_id = %s",
            (sensor_id,),
        )
        result = cur.fetchone()
        return result[0] if result and result[0] else None


def upsert_records(conn, sensor_id: int, records: list[dict]):
    """Upsert a list of {time, ...vars} dicts into sensors_data."""
    rows = []
    for rec in records:
        t = rec.pop(ERDDAP_TIME_VAR)
        if not rec:
            continue
        rows.append((t, sensor_id, Json(rec)))

    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO sensors_data (time, sensor_id, measurements)
            VALUES %s
            ON CONFLICT (sensor_id, time) DO UPDATE SET
                measurements = sensors_data.measurements || EXCLUDED.measurements
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def fetch_and_store():
    conn = get_db_conn()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, name, variables, source->>'link'
            FROM sensors
            WHERE active = true
              AND source->>'type' = 'ERDDAP'
              AND source->>'link' IS NOT NULL
              AND variables IS NOT NULL
              AND array_length(variables, 1) > 0
            """
        )
        sensors = cur.fetchall()

    if not sensors:
        print("No active ERDDAP sensors found.")
        conn.close()
        return

    print(f"Found {len(sensors)} active ERDDAP sensor(s).")

    for sensor_id, name, variables, erddap_link in sensors:
        print(f"\nSensor: {name} (ID: {sensor_id})")
        print(f"  Dataset: {erddap_link}")
        print(f"  Variables: {variables}")

        last_time = get_last_stored_time(conn, sensor_id)
        if last_time:
            print(f"  Last stored time: {last_time.isoformat()}")
        else:
            print("  No prior data — fetching all available.")

        try:
            records = fetch_erddap_csv(erddap_link, list(variables), last_time)
            if not records:
                print("  No new data returned.")
                continue

            print(f"  Fetched {len(records)} raw record(s).")
            hourly = bin_to_hourly(records)
            if not hourly:
                print("  No hourly bins produced — skipping.")
                continue

            count = upsert_records(conn, sensor_id, hourly)
            print(f"  Stored {count} hourly record(s).")

        except Exception as e:
            print(f"  ERROR: {e}")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    fetch_and_store()
