"""
erddap_to_nc.py

Fetch sensor data from ERDDAP tabledap sources and store as compressed
netCDF4 files — one file per sensor per variable.

File layout
-----------
Fixed-depth sensors (sensors.depth != -1):
    {SENSORS_ROOT}/{sensor_id}/{erddap_column_name}.nc
    Dimensions:  time (unlimited)
    Variables:   time(time), <varname>(time)
    Data is hourly-binned before storing.

Variable-depth sensors (sensors.depth == -1):
    {SENSORS_ROOT}/{sensor_id}/{erddap_column_name}.nc
    Dimensions:  obs (unlimited)
    Variables:   time(obs), depth(obs), <varname>(obs)
    Raw (un-binned) records are stored to preserve per-observation depth.
    Deduplication is on (time, depth) pairs.
"""

import io
import csv
import json
import math
import os
import numpy as np
import netCDF4
import psycopg2
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Storage root ─────────────────────────────────────────────────────────────
SENSORS_ROOT = os.getenv("SENSORS_ROOT", "/opt/data/sensors")

# ── DB ────────────────────────────────────────────────────────────────────────
DB_HOST = os.getenv("PGHOST", "db")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "oa")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "postgres")

# ── ERDDAP constants ──────────────────────────────────────────────────────────
ERDDAP_TIME_VAR  = "time"
ERDDAP_DEPTH_VAR = "depth"
ERDDAP_TIME_FMT  = "%Y-%m-%dT%H:%M:%SZ"

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )


def dt_to_epoch(dt: datetime) -> float:
    return (dt - EPOCH).total_seconds()


# ── ERDDAP fetch ──────────────────────────────────────────────────────────────

def fetch_erddap_csv(
    base_url: str,
    variables: list[str],
    date_from: datetime | None,
    include_depth: bool = False,
    time_col: str = "time",
    depth_col: str = "depth",
) -> list[dict]:
    """
    Download a tabledap dataset as CSV.

    Args:
        base_url:      Full tabledap URL without extension, e.g.
                       https://catalogue.hakai.org/erddap/tabledap/HakaiWirewalker
        variables:     ERDDAP column names to fetch (must match the dataset exactly).
        date_from:     Lower time bound (UTC); None = fetch all available.
        include_depth: If True, also request the 'depth' column from ERDDAP.

    Returns:
        List of dicts: {var_name: value, ...} per row.
        'time' values are datetime objects (UTC-aware).
        'depth' values are float (metres, positive down) when include_depth=True.
    """
    extra_fields = [depth_col] if include_depth else []
    fields = [time_col] + extra_fields + variables
    query = ",".join(fields)

    constraints = ""
    if date_from is not None:
        constraints = f"&{time_col}>={date_from.strftime(ERDDAP_TIME_FMT)}"

    url = f"{base_url}.csv?{query}{constraints}"
    print(f"  GET {url}")

    resp = requests.get(url, timeout=120)
    if resp.status_code == 404:
        raise RuntimeError(f"Dataset not found (404): {base_url}")
    if resp.status_code != 200:
        raise RuntimeError(f"ERDDAP returned HTTP {resp.status_code}: {resp.text[:300]}")

    # ERDDAP CSV: row 0 = headers, row 1 = units (skip), rows 2+ = data
    reader = csv.reader(io.StringIO(resp.text))
    rows = list(reader)
    if len(rows) < 2:
        return []

    headers = rows[0]
    data_rows = rows[2:]

    records = []
    for row in data_rows:
        if len(row) != len(headers):
            continue
        record: dict = {}
        valid = True
        for header, value in zip(headers, row):
            if value in ("", "NaN"):
                if header == time_col:
                    valid = False
                continue
            if header == time_col:
                try:
                    record[ERDDAP_TIME_VAR] = datetime.strptime(value, ERDDAP_TIME_FMT).replace(
                        tzinfo=timezone.utc
                    )
                except ValueError:
                    valid = False
            elif header == depth_col and include_depth:
                try:
                    v = float(value)
                    if not math.isnan(v):
                        record[ERDDAP_DEPTH_VAR] = v  # normalize to canonical "depth" key
                except ValueError:
                    pass
            else:
                try:
                    v = float(value)
                    if not math.isnan(v):
                        record[header] = v
                except ValueError:
                    pass
        if valid and ERDDAP_TIME_VAR in record:
            records.append(record)

    return records


# ── Hourly binning (fixed-depth only) ────────────────────────────────────────

def bin_to_hourly(records: list[dict]) -> list[dict]:
    """
    Bin records into hourly slots centred at HH:30:00 UTC.
    Slots with no data are skipped.  Does NOT touch the 'depth' field.
    """
    if not records:
        return []

    bins: dict[tuple, list[dict]] = {}
    for rec in records:
        t = rec[ERDDAP_TIME_VAR]
        key = (t.year, t.month, t.day, t.hour)
        bins.setdefault(key, []).append(rec)

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


# ── Conversion ────────────────────────────────────────────────────────────────

def apply_conversion(value: float, canonical_var: str, sensor_var_mapping: dict) -> float:
    if value is None or not isinstance(value, (int, float)):
        return value
    var_info = sensor_var_mapping.get(canonical_var)
    if isinstance(var_info, dict):
        return value * var_info.get("conversion_factor", 1.0)
    return value


# ── NC helpers — fixed depth (1D time) ───────────────────────────────────────

def _open_or_create_1d(nc_path: Path, var_code: str) -> netCDF4.Dataset:
    if nc_path.exists():
        return netCDF4.Dataset(nc_path, "a")
    nc_path.parent.mkdir(parents=True, exist_ok=True)
    ds = netCDF4.Dataset(nc_path, "w", format="NETCDF4")
    ds.createDimension("time", None)
    t = ds.createVariable("time", "f8", ("time",), zlib=True, complevel=4)
    t.units    = "seconds since 1970-01-01 00:00:00 UTC"
    t.calendar = "gregorian"
    t.long_name = "time"
    v = ds.createVariable(var_code, "f4", ("time",), zlib=True, complevel=4,
                          fill_value=np.float32(np.nan))
    v.long_name = var_code
    return ds


def append_to_nc_1d(
    nc_path: Path,
    var_code: str,
    times_epoch: list[float],
    values: list[float],
) -> int:
    """Append fixed-depth (time,) records; deduplicate on timestamp."""
    if not times_epoch:
        return 0
    ds = _open_or_create_1d(nc_path, var_code)
    try:
        existing: set[float] = set()
        if "time" in ds.variables and len(ds.variables["time"]) > 0:
            existing = set(ds.variables["time"][:].tolist())

        new_pairs = [
            (t, v) for t, v in zip(times_epoch, values)
            if t not in existing and v is not None and not np.isnan(v)
        ]
        if not new_pairs:
            return 0
        new_pairs.sort(key=lambda x: x[0])

        n = len(ds.variables["time"])
        t_arr, v_arr = zip(*new_pairs)
        ds.variables["time"][n:] = list(t_arr)
        ds.variables[var_code][n:] = list(v_arr)
        return len(new_pairs)
    finally:
        ds.close()


# ── NC helpers — variable depth (point cloud, obs dimension) ─────────────────

def _open_or_create_pointcloud(nc_path: Path, var_code: str) -> netCDF4.Dataset:
    if nc_path.exists():
        return netCDF4.Dataset(nc_path, "a")
    nc_path.parent.mkdir(parents=True, exist_ok=True)
    ds = netCDF4.Dataset(nc_path, "w", format="NETCDF4")
    ds.createDimension("obs", None)      # unlimited — one slot per observation
    t = ds.createVariable("time", "f8", ("obs",), zlib=True, complevel=4)
    t.units     = "seconds since 1970-01-01 00:00:00 UTC"
    t.calendar  = "gregorian"
    t.long_name = "time"
    d = ds.createVariable("depth", "f4", ("obs",), zlib=True, complevel=4)
    d.units     = "m"
    d.positive  = "down"
    d.long_name = "depth"
    v = ds.createVariable(var_code, "f4", ("obs",), zlib=True, complevel=4,
                          fill_value=np.float32(np.nan))
    v.long_name = var_code
    return ds


def append_to_nc_pointcloud(
    nc_path: Path,
    var_code: str,
    times_epoch: list[float],
    depths: list[float],
    values: list[float],
) -> int:
    """Append variable-depth (obs,) records; deduplicate on (time, depth) pairs."""
    if not times_epoch:
        return 0
    ds = _open_or_create_pointcloud(nc_path, var_code)
    try:
        existing: set[tuple] = set()
        if "time" in ds.variables and len(ds.variables["time"]) > 0:
            t_arr = ds.variables["time"][:].tolist()
            d_arr = ds.variables["depth"][:].tolist()
            existing = set(zip(t_arr, d_arr))

        new_triples = [
            (t, d, v)
            for t, d, v in zip(times_epoch, depths, values)
            if (t, d) not in existing and v is not None and not np.isnan(v)
        ]
        if not new_triples:
            return 0
        new_triples.sort(key=lambda x: (x[0], x[1]))

        n = len(ds.variables["time"])
        t_arr, d_arr, v_arr = zip(*new_triples)
        ds.variables["time"][n:]    = list(t_arr)
        ds.variables["depth"][n:]   = list(d_arr)
        ds.variables[var_code][n:]  = list(v_arr)
        return len(new_triples)
    finally:
        ds.close()


# ── Last-stored-time helper ───────────────────────────────────────────────────

def get_last_stored_time_nc(nc_path: Path) -> datetime | None:
    """Return the most recent timestamp in an existing NC file (any dimension layout)."""
    if not nc_path.exists():
        return None
    try:
        with netCDF4.Dataset(nc_path, "r") as ds:
            if "time" not in ds.variables or len(ds.variables["time"]) == 0:
                return None
            latest_epoch = float(ds.variables["time"][:].max())
            return EPOCH + timedelta(seconds=latest_epoch)
    except Exception as e:
        print(f"    Warning: could not read existing NC file {nc_path}: {e}")
        return None


# ── Main loop ─────────────────────────────────────────────────────────────────

def fetch_and_store(sensor_id_filter: int | None = None):
    conn = get_db_conn()

    with conn.cursor() as cur:
        query = """
            SELECT id, name, depth, variables, source->>'link'
            FROM sensors
            WHERE active = true
              AND source->>'type' = 'ERDDAP'
              AND source->>'link' IS NOT NULL
              AND variables IS NOT NULL
        """
        if sensor_id_filter is not None:
            query += " AND id = %s"
            cur.execute(query, (sensor_id_filter,))
        else:
            cur.execute(query)
        sensors = cur.fetchall()

    conn.close()

    if not sensors:
        print("No active ERDDAP sensors found.")
        return

    print(f"Found {len(sensors)} active ERDDAP sensor(s).")

    for sensor_id, name, sensor_depth, variables_json, erddap_link in sensors:
        variable_depth = (sensor_depth is not None and sensor_depth == -1)
        print(f"\nSensor: {name} (ID: {sensor_id})"
              f"  — {'variable' if variable_depth else 'fixed'} depth")
        print(f"  Dataset: {erddap_link}")

        if "/tabledap/" not in erddap_link:
            print(f"  SKIPPED — link does not point to a tabledap endpoint (/tabledap/ not found in URL).")
            continue

        try:
            variables_mapping = (
                json.loads(variables_json)
                if isinstance(variables_json, str)
                else variables_json or {}
            )
            if not variables_mapping:
                print("  No variables mapping defined, skipping.")
                continue

            # Resolve axis column names (default to standard names if not mapped)
            time_col  = (variables_mapping.get("time",  {}) or {}).get("name", "time")
            depth_col = (variables_mapping.get("depth", {}) or {}).get("name", "depth")

            # {erddap_col_name: canonical_name}  for reverse lookup (exclude axis entries)
            erddap_to_canonical = {
                info["name"]: canonical
                for canonical, info in variables_mapping.items()
                if canonical not in ("time", "depth")
                and isinstance(info, dict) and info.get("name")
            }
            erddap_vars = list(erddap_to_canonical.keys())
            print(f"  time axis: {time_col}, depth axis: {depth_col}")
            print(f"  ERDDAP columns: {erddap_vars}")

        except Exception as e:
            print(f"  ERROR parsing variables mapping: {e}")
            continue

        storage_dir = Path(SENSORS_ROOT) / str(sensor_id)

        # Determine dateFrom: use the earliest "last stored time" across all variable files
        # so we don't miss data for any variable.
        date_froms = []
        for erddap_col in erddap_vars:
            nc_path = storage_dir / f"{erddap_col}.nc"
            t = get_last_stored_time_nc(nc_path)
            if t:
                date_froms.append(t - timedelta(days=1))  # 1-day overlap

        date_from = min(date_froms) if date_froms else None
        if date_from:
            print(f"  Fetching from: {date_from.strftime(ERDDAP_TIME_FMT)}")
        else:
            print("  No prior data — fetching all available.")

        try:
            records = fetch_erddap_csv(
                erddap_link,
                erddap_vars,
                date_from,
                include_depth=variable_depth,
                time_col=time_col,
                depth_col=depth_col,
            )
            if not records:
                print("  No new data returned.")
                continue
            print(f"  Fetched {len(records)} raw record(s).")
        except Exception as e:
            print(f"  ERROR fetching: {e}")
            continue

        if not variable_depth:
            # Hourly binning for fixed-depth sensors
            records = bin_to_hourly(records)
            if not records:
                print("  No hourly bins produced — skipping.")
                continue

        # Write one NC file per variable
        for erddap_col in erddap_vars:
            canonical = erddap_to_canonical[erddap_col]
            nc_path = storage_dir / f"{erddap_col}.nc"

            times_epoch: list[float] = []
            depths_list: list[float] = []
            values_list: list[float] = []

            for rec in records:
                t = rec.get(ERDDAP_TIME_VAR)
                v = rec.get(erddap_col)
                if t is None or v is None:
                    continue
                converted = apply_conversion(v, canonical, variables_mapping)
                t_epoch = dt_to_epoch(t)
                times_epoch.append(t_epoch)
                values_list.append(converted)
                if variable_depth:
                    depths_list.append(float(rec.get(ERDDAP_DEPTH_VAR, 0.0)))

            if not times_epoch:
                print(f"  {erddap_col}: no data — skipping.")
                continue

            if variable_depth:
                n_written = append_to_nc_pointcloud(
                    nc_path, erddap_col, times_epoch, depths_list, values_list
                )
            else:
                n_written = append_to_nc_1d(nc_path, erddap_col, times_epoch, values_list)

            print(f"  {erddap_col}: wrote {n_written} new record(s) → {nc_path}")

    print("\nDone.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Fetch ERDDAP sensor data and store as netCDF4 files."
    )
    parser.add_argument(
        "--sensor-id",
        type=int,
        default=None,
        help="Process only this sensor ID (default: all active ERDDAP sensors)",
    )
    args = parser.parse_args()
    fetch_and_store(sensor_id_filter=args.sensor_id)
