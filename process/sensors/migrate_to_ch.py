"""
migrate_to_ch.py

One-off migration: copy sensor metadata from PostgreSQL and time-series data
from NC files into ClickHouse.  Idempotent — re-running is safe because both
target tables use ReplacingMergeTree.

Steps
-----
1. Read all rows from PostgreSQL `sensors` → insert into CH `sensors`.
2. For each sensor, scan /opt/data/sensors/{id}/*.nc, resolve the NC file stem
   (which is a sensor-specific code like "DOXY") back to a canonical variable
   name via sensors.variables JSON, then batch-insert all rows into CH
   `sensor_timeseries` with (sensor_id, time, depth, variable=canonical, value).

Usage
-----
    # From inside the process container:
    uv run python sensors/migrate_to_ch.py

    # With a non-default sensors root:
    uv run python sensors/migrate_to_ch.py --sensors-root /opt/data/sensors

    # Migrate only specific sensors:
    uv run python sensors/migrate_to_ch.py --sensor-id 3 --sensor-id 7
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import netCDF4
import numpy as np
import psycopg2
import psycopg2.extras

from ch_helpers import get_ch_client

SENSORS_ROOT = os.getenv("SENSORS_ROOT", "/opt/data/sensors")
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
BATCH_SIZE = 50_000


# ── PostgreSQL ────────────────────────────────────────────────────────────────

def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "db"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "oa"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"),
    )


# ── Metadata migration ────────────────────────────────────────────────────────

def migrate_sensors_metadata(pg_conn, ch_client, sensor_ids: list[int] | None):
    print("\n── Migrating sensor metadata ──────────────────────────────────────")
    with pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        if sensor_ids:
            cur.execute(
                "SELECT id, name, latitude, longitude, depth, variables, "
                "device_config, source, active FROM sensors WHERE id = ANY(%s)",
                (sensor_ids,),
            )
        else:
            cur.execute(
                "SELECT id, name, latitude, longitude, depth, variables, "
                "device_config, source, active FROM sensors"
            )
        rows = cur.fetchall()

    if not rows:
        print("  No sensors found in PostgreSQL.")
        return []

    data = []
    for row in rows:
        data.append([
            int(row["id"]),
            row["name"] or "",
            float(row["latitude"] or 0),
            float(row["longitude"] or 0),
            float((row["depth"][0] if isinstance(row["depth"], list) else row["depth"]) if row["depth"] is not None else -1),
            json.dumps(row["variables"]) if row["variables"] else "{}",
            json.dumps(row["device_config"]) if row["device_config"] else "{}",
            json.dumps(row["source"]) if row["source"] else "{}",
            1 if row["active"] else 0,
            datetime.now(tz=timezone.utc).replace(tzinfo=None),
        ])

    ch_client.insert(
        "sensors",
        data,
        column_names=["id", "name", "latitude", "longitude", "depth",
                       "variables", "device_config", "source", "active", "updated_at"],
    )
    print(f"  Inserted {len(data)} sensor(s) into CH.")
    return [row["id"] for row in rows]


# ── NC file helpers ───────────────────────────────────────────────────────────

def _epoch_to_dt(seconds: float) -> datetime:
    return EPOCH + timedelta(seconds=float(seconds))


def _build_code_to_canonical(variables_json: str | dict | None) -> dict[str, str]:
    """Build {sensor_code: canonical_name} reverse map from variables JSON."""
    if not variables_json:
        return {}
    if isinstance(variables_json, str):
        try:
            mapping = json.loads(variables_json)
        except json.JSONDecodeError:
            return {}
    else:
        mapping = variables_json
    result = {}
    for canonical, info in mapping.items():
        if canonical in ("time", "depth"):
            continue
        if isinstance(info, dict) and info.get("name"):
            result[info["name"]] = canonical
    return result


def _migrate_nc_file(
    nc_path: Path,
    sensor_id: int,
    canonical: str,
    sensor_depth: float,
    ch_client,
) -> int:
    """Read one NC file and batch-insert into sensor_timeseries. Returns row count."""
    if not nc_path.exists():
        return 0

    with netCDF4.Dataset(nc_path, "r") as ds:
        if "time" not in ds.variables:
            print(f"    SKIP {nc_path.name}: no time variable")
            return 0

        var_name = nc_path.stem  # e.g. "DOXY"
        if var_name not in ds.variables:
            # Try the canonical name (in case file was already remapped)
            if canonical in ds.variables:
                var_name = canonical
            else:
                print(f"    SKIP {nc_path.name}: variable '{var_name}' not found in file")
                return 0

        times_raw = np.array(ds.variables["time"][:], dtype=np.float64)
        dims = ds.variables[var_name].dimensions
        has_depth_dim = "depth" in dims and "depth" in ds.variables
        has_obs_depth = "obs" in dims and "depth" in ds.variables

        if has_depth_dim:
            depths_raw = np.array(ds.variables["depth"][:], dtype=np.float32)
            values_raw = np.array(ds.variables[var_name][:], dtype=np.float32)
            # values_raw shape: (n_time, n_depth) — flatten
            rows = []
            for ti, t_epoch in enumerate(times_raw):
                if np.isnan(t_epoch):
                    continue
                t_dt = _epoch_to_dt(t_epoch).replace(tzinfo=None)
                for di, d in enumerate(depths_raw):
                    v = float(values_raw[ti, di])
                    if np.isnan(v):
                        continue
                    rows.append([sensor_id, t_dt, float(d), canonical, v])
        elif has_obs_depth:
            depths_raw = np.array(ds.variables["depth"][:], dtype=np.float32)
            values_raw = np.array(ds.variables[var_name][:], dtype=np.float32)
            rows = []
            for i, t_epoch in enumerate(times_raw):
                if np.isnan(t_epoch):
                    continue
                v = float(values_raw[i])
                if np.isnan(v):
                    continue
                t_dt = _epoch_to_dt(t_epoch).replace(tzinfo=None)
                rows.append([sensor_id, t_dt, float(depths_raw[i]), canonical, v])
        else:
            # 1D (time,) — fixed depth from sensors table
            depth_val = sensor_depth if sensor_depth >= 0 else 0.0
            values_raw = np.array(ds.variables[var_name][:], dtype=np.float32)
            rows = []
            for i, t_epoch in enumerate(times_raw):
                if np.isnan(t_epoch):
                    continue
                v = float(values_raw[i])
                if np.isnan(v):
                    continue
                t_dt = _epoch_to_dt(t_epoch).replace(tzinfo=None)
                rows.append([sensor_id, t_dt, depth_val, canonical, v])

    total = 0
    for start in range(0, len(rows), BATCH_SIZE):
        batch = rows[start:start + BATCH_SIZE]
        ch_client.insert(
            "sensor_timeseries",
            batch,
            column_names=["sensor_id", "time", "depth", "variable", "value"],
        )
        total += len(batch)

    return total


# ── Time-series migration ─────────────────────────────────────────────────────

def migrate_timeseries(pg_conn, ch_client, sensor_ids: list[int], sensors_root: Path):
    print("\n── Migrating time-series from NC files ────────────────────────────")

    with pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            "SELECT id, depth, variables FROM sensors WHERE id = ANY(%s)",
            (sensor_ids,),
        )
        sensors = cur.fetchall()

    for row in sensors:
        sensor_id = int(row["id"])
        sensor_depth = float(row["depth"] if row["depth"] is not None else -1)
        code_to_canonical = _build_code_to_canonical(row["variables"])

        sensor_dir = sensors_root / str(sensor_id)
        if not sensor_dir.is_dir():
            print(f"  Sensor {sensor_id}: no NC directory at {sensor_dir}, skipping.")
            continue

        nc_files = sorted(sensor_dir.glob("*.nc"))
        if not nc_files:
            print(f"  Sensor {sensor_id}: no NC files found.")
            continue

        print(f"\n  Sensor {sensor_id} ({len(nc_files)} NC file(s)):")
        for nc_path in nc_files:
            stem = nc_path.stem
            canonical = code_to_canonical.get(stem)
            if canonical is None:
                print(f"    SKIP {nc_path.name}: code '{stem}' not in variables mapping")
                continue
            n = _migrate_nc_file(nc_path, sensor_id, canonical, sensor_depth, ch_client)
            print(f"    {nc_path.name} → '{canonical}': {n} row(s) inserted")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Migrate sensor data from PG + NC files to ClickHouse.")
    parser.add_argument("--sensor-id", type=int, action="append", dest="sensor_ids",
                        metavar="ID", help="Migrate only this sensor (repeatable)")
    parser.add_argument("--sensors-root", default=SENSORS_ROOT,
                        help=f"Path to sensor NC files (default: {SENSORS_ROOT})")
    parser.add_argument("--skip-timeseries", action="store_true",
                        help="Only migrate metadata, skip NC file ingestion")
    args = parser.parse_args()

    sensors_root = Path(args.sensors_root)
    sensor_ids = args.sensor_ids or None

    print("Connecting to PostgreSQL and ClickHouse...")
    pg_conn = get_pg_conn()
    ch_client = get_ch_client()

    migrated_ids = migrate_sensors_metadata(pg_conn, ch_client, sensor_ids)

    if not args.skip_timeseries:
        ids_to_migrate = sensor_ids if sensor_ids else migrated_ids
        if ids_to_migrate:
            migrate_timeseries(pg_conn, ch_client, ids_to_migrate, sensors_root)
        else:
            print("\nNo sensors to migrate time-series for.")

    pg_conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
