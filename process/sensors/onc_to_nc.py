"""
onc_to_nc.py

Parallel to onc_to_db.py — fetches ONC scalar data and stores it as
compressed netCDF4 files instead of a PostgreSQL database.

One file per sensor per variable:
    {storage_dir}/{sensor_name}/{variable}.nc

Data is appended only if the timestamp doesn't already exist in the file.
The time dimension is unlimited so files grow incrementally.

File layout (dimensions):
    time (unlimited)   — CF-convention seconds since epoch, sorted ascending

Variables:
    time               — float64, units="seconds since 1970-01-01 00:00:00 UTC"
    <sensorCategoryCode> — float32, one variable per ONC sensor category code
"""

import json
import os
import psycopg2
import netCDF4
import numpy as np
from onc import ONC
from datetime import datetime, timedelta
from pathlib import Path

# ── ONC credentials ──────────────────────────────────────────────────────────
TOKEN = "7d291a6a-b57e-49cd-acb1-83f59010d32b"
onc = ONC(TOKEN)

# ── DB connection (read-only: sensors metadata) ──────────────────────────────
DB_HOST = os.getenv("PGHOST", "db")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "oa")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "postgres")

EPOCH = datetime(1970, 1, 1)


def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )


def get_sensor_var_mapping(conn, sensor_id: int) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT variables FROM sensors WHERE id = %s", (sensor_id,))
        result = cur.fetchone()
        if not result or not result[0]:
            return {}
        variables_data = result[0]
        if isinstance(variables_data, str):
            return json.loads(variables_data)
        return variables_data or {}


def apply_conversion(value: float, canonical_var: str, sensor_var_mapping: dict) -> float:
    if value is None or not isinstance(value, (int, float)):
        return value
    if canonical_var not in sensor_var_mapping:
        return value
    var_info = sensor_var_mapping[canonical_var]
    if isinstance(var_info, dict):
        return value * var_info.get("conversion_factor", 1.0)
    return value


def dt_to_epoch(dt_str: str) -> float:
    """Convert an ISO-8601 string (with or without trailing Z) to seconds since epoch."""
    dt_str = dt_str.rstrip("Z").replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return (datetime.strptime(dt_str, fmt) - EPOCH).total_seconds()
        except ValueError:
            continue
    raise ValueError(f"Cannot parse datetime: {dt_str!r}")


# ── NC file helpers ──────────────────────────────────────────────────────────

def _open_or_create(nc_path: Path, var_code: str) -> netCDF4.Dataset:
    """Open an existing NC file or create a new one with standard layout."""
    if nc_path.exists():
        return netCDF4.Dataset(nc_path, "a")

    nc_path.parent.mkdir(parents=True, exist_ok=True)
    ds = netCDF4.Dataset(nc_path, "w", format="NETCDF4")
    ds.createDimension("time", None)  # unlimited

    t_var = ds.createVariable("time", "f8", ("time",), zlib=True, complevel=4)
    t_var.units = "seconds since 1970-01-01 00:00:00 UTC"
    t_var.calendar = "gregorian"
    t_var.long_name = "time"

    v_var = ds.createVariable(
        var_code, "f4", ("time",),
        zlib=True, complevel=4,
        fill_value=np.float32(np.nan),
    )
    v_var.long_name = var_code
    return ds


def append_to_nc(nc_path: Path, var_code: str, times_epoch: list[float], values: list[float]) -> int:
    """
    Append (time, value) pairs to the NC file, skipping timestamps that already exist.
    Returns the number of new records written.
    """
    if not times_epoch:
        return 0

    ds = _open_or_create(nc_path, var_code)
    try:
        existing_times: set[float] = set()
        if "time" in ds.variables and len(ds.variables["time"]) > 0:
            existing_times = set(ds.variables["time"][:].tolist())

        new_pairs = [
            (t, v) for t, v in zip(times_epoch, values)
            if t not in existing_times and v is not None and not np.isnan(v)
        ]

        if not new_pairs:
            return 0

        # Sort by time so the file stays ordered
        new_pairs.sort(key=lambda x: x[0])

        if "time" not in ds.variables:
            # Shouldn't happen after _open_or_create, but be safe
            t_var = ds.createVariable("time", "f8", ("time",), zlib=True, complevel=4)
            t_var.units = "seconds since 1970-01-01 00:00:00 UTC"
        if var_code not in ds.variables:
            v_var = ds.createVariable(
                var_code, "f4", ("time",),
                zlib=True, complevel=4,
                fill_value=np.float32(np.nan),
            )
            v_var.long_name = var_code

        n = len(ds.variables["time"])
        new_t, new_v = zip(*new_pairs)
        ds.variables["time"][n:] = list(new_t)
        ds.variables[var_code][n:] = list(new_v)

        return len(new_pairs)
    finally:
        ds.close()


# ── Main fetch loop ──────────────────────────────────────────────────────────

def fetch_and_store(sensor_id_filter: int | None = None):
    conn = get_db_conn()

    with conn.cursor() as cur:
        if sensor_id_filter is not None:
            cur.execute(
                "SELECT id, name, device_config FROM sensors WHERE source->>'type' = 'ONC' AND id = %s",
                (sensor_id_filter,),
            )
        else:
            cur.execute("SELECT id, name, device_config FROM sensors WHERE source->>'type' = 'ONC'")
        sensors = cur.fetchall()

    if not sensors:
        print("No sensors found in database.")
        conn.close()
        return

    dateTo = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")

    for sensor_id, sensor_name, device_config in sensors:
        print(f"Processing Sensor: {sensor_name} (ID: {sensor_id})")

        storage_dir = Path("/opt/data/sensors") / str(sensor_id)
        if not (device_config and isinstance(device_config, dict) and device_config):
            print(f"  No valid device_config for sensor {sensor_name}, skipping.")
            continue

        locationCode = device_config.get("locationCode")
        codeRows = device_config.get("codes", [])
        sensor_var_mapping = get_sensor_var_mapping(conn, sensor_id)
        col_to_canonical = {
            v_info.get("name"): canonical
            for canonical, v_info in sensor_var_mapping.items()
            if isinstance(v_info, dict) and v_info.get("name")
        }

        for codeRow in codeRows:
            deviceCategoryCode = codeRow.get("deviceCategoryCode")
            sensorCategoryCodes = codeRow.get("sensorCategoryCodes")

            # Determine dateFrom: latest timestamp already stored in the NC file
            nc_path = storage_dir / f"{sensorCategoryCodes}.nc"
            dateFrom = None
            if nc_path.exists():
                try:
                    with netCDF4.Dataset(nc_path, "r") as ds:
                        if "time" in ds.variables and len(ds.variables["time"]) > 0:
                            latest_epoch = float(ds.variables["time"][:].max())
                            latest_dt = EPOCH + timedelta(seconds=latest_epoch - 86400)  # -1 day overlap
                            dateFrom = latest_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                except Exception as e:
                    print(f"    Warning: could not read existing NC file {nc_path}: {e}")

            print(f"  Fetching locationCode={locationCode}, deviceCategoryCode={deviceCategoryCode}, sensorCategoryCodes={sensorCategoryCodes}")
            try:
                data = onc.getScalardataByLocation(
                    {
                        "locationCode": locationCode,
                        "deviceCategoryCode": deviceCategoryCode,
                        "getLatest": True,
                        "resamplePeriod": 3600,
                        "resampleType": "avg",
                        "qualityControl": "clean",
                        "sensorCategoryCodes": sensorCategoryCodes,
                        "dateFrom": dateFrom,
                        "dateTo": dateTo,
                    }
                )

                if "sensorData" not in data:
                    print(f"    No sensorData found for {deviceCategoryCode}")
                    continue

                for s in data["sensorData"]:
                    s_code = s.get("sensorCategoryCode")
                    vals = s.get("data", {}).get("values", [])
                    raw_times = s.get("data", {}).get("sampleTimes", [])

                    times_epoch: list[float] = []
                    values_out: list[float] = []

                    for t_str, v in zip(raw_times, vals):
                        if v is None or (isinstance(v, float) and np.isnan(v)):
                            continue
                        try:
                            t_epoch = dt_to_epoch(t_str)
                        except ValueError:
                            continue
                        canonical = col_to_canonical.get(s_code, s_code)
                        converted = apply_conversion(float(v), canonical, sensor_var_mapping)
                        times_epoch.append(t_epoch)
                        values_out.append(converted)

                    out_path = storage_dir / f"{s_code}.nc"
                    n_written = append_to_nc(out_path, s_code, times_epoch, values_out)
                    print(f"    {s_code}: wrote {n_written} new points → {out_path}")

            except Exception as e:
                print(f"    Error: {e}")

    conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch ONC sensor data and store as netCDF4 files.")
    parser.add_argument("--sensor-id", type=int, default=None, help="Process only this sensor ID (default: all ONC sensors)")
    args = parser.parse_args()
    fetch_and_store(sensor_id_filter=args.sensor_id)
