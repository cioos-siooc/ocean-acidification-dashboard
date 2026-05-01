"""
erddapGrid_to_nc.py

Fetch sensor data from ERDDAP **griddap** endpoints (sensors.depth == -1)
and store as 2D compressed netCDF4 files.

This script only handles griddap sources (base URL must contain '/griddap/').
For tabledap fixed-depth sensors use erddap_to_nc.py instead.

File layout
-----------
    {SENSORS_ROOT}/{sensor_id}/{erddap_column_name}.nc
    Dimensions:  time (unlimited), depth (fixed — set on first write)
    Variables:   time(time), depth(depth), <varname>(time, depth)

Download format
---------------
Data is fetched as netCDF using griddap bracket notation:
    var[(t_start):1:(last)][0:1:last]

The griddap response is already a proper 2D grid — no pivoting needed.
time and depth come back as 1D coordinate variables; data variables are 2D.

Deduplication
-------------
On subsequent runs the script reads the latest timestamp already stored and
fetches from 1 day before that.  Timestamps already in the file are skipped.
"""

import json
import os
import numpy as np
import netCDF4
import psycopg2
import requests
import urllib3
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Suppress InsecureRequestWarning for servers with self-signed / incomplete cert chains
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Storage root ──────────────────────────────────────────────────────────────
SENSORS_ROOT = os.getenv("SENSORS_ROOT", "/opt/data/sensors")

# ── DB ─────────────────────────────────────────────────────────────────────────
DB_HOST = os.getenv("PGHOST", "db")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "oa")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "postgres")

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


# ── ERDDAP griddap fetch ──────────────────────────────────────────────────────

def fetch_erddap_griddap_nc(
    base_url: str,
    variables: list[str],
    date_from: datetime | None,
    time_col: str = ERDDAP_TIME_VAR,
    depth_col: str = ERDDAP_DEPTH_VAR,
) -> tuple[np.ndarray, np.ndarray, dict[str, np.ndarray]]:
    """
    Download from an ERDDAP griddap endpoint using bracket-notation queries.

    Requests all depth levels and time from date_from to the latest available.
    The response is already a proper 2D NC file — no pivoting required.

    Args:
        base_url:  Full griddap URL without extension, e.g.
                   https://nwem.apl.uw.edu/erddap/griddap/orca1_L3_depthgridded_025
        variables: ERDDAP variable names to fetch.
        date_from: Lower time bound (UTC); None = fetch all available data.
        time_col:  Name of the time axis variable in this ERDDAP dataset.
        depth_col: Name of the depth axis variable in this ERDDAP dataset.

    Returns:
        times_epoch  — 1D float64 array, epoch seconds, ascending
        depth_levels — 1D float32 array, metres
        grids        — {erddap_var_name: 2D float32 array shape (n_time, n_depth)}
    """
    # Build time subscript: parenthesised date value or full index range
    if date_from:
        t_sub = f"({date_from.strftime(ERDDAP_TIME_FMT)}):1:(last)"
    else:
        t_sub = "0:1:last"

    # Explicitly include axis variables so they are always present in the response
    queries = (
        [f"{var}[{t_sub}][0:1:last]" for var in variables]
    )
    url = f"{base_url}.nc?{','.join(queries)}"
    print(f"  GET {url}")

    resp = requests.get(url, timeout=300, verify=False)
    if resp.status_code == 404:
        raise RuntimeError(f"Dataset not found (404): {base_url}")
    if resp.status_code != 200:
        raise RuntimeError(f"ERDDAP returned HTTP {resp.status_code}: {resp.text[:300]}")

    ds = netCDF4.Dataset("in_memory.nc", mode="r", memory=resp.content)
    try:
        if time_col not in ds.variables:
            raise RuntimeError(f"Griddap response has no '{time_col}' variable")
        if depth_col not in ds.variables:
            raise RuntimeError(f"Griddap response has no '{depth_col}' variable")

        # Decode time axis (CF convention)
        t_var = ds.variables[time_col]
        units = getattr(t_var, "units", "")
        if "since" in units:
            times_dt = netCDF4.num2date(
                t_var[:], units=units,
                calendar=getattr(t_var, "calendar", "standard"),
            )
            times_epoch = np.array([
                dt_to_epoch(datetime(d.year, d.month, d.day, d.hour, d.minute, d.second,
                                     tzinfo=timezone.utc))
                for d in times_dt
            ], dtype=np.float64)
        else:
            times_epoch = np.array(t_var[:], dtype=np.float64)

        depth_levels = np.array(ds.variables[depth_col][:], dtype=np.float32)

        grids: dict[str, np.ndarray] = {}
        for var in variables:
            if var not in ds.variables:
                print(f"  Warning: '{var}' not in griddap response, skipping.")
                continue
            arr = np.array(ds.variables[var][:], dtype=np.float32)
            fillValue_vocab = ["_FillValue", "missing_value", "fill_value"]
            for fill_attr in fillValue_vocab:
                if hasattr(ds.variables[var], fill_attr):
                    fill = float(getattr(ds.variables[var], fill_attr))
                    arr[arr == fill] = np.nan
            # Ensure 2D shape (n_time, n_depth)
            if arr.ndim == 1:
                arr = arr.reshape(len(times_epoch), len(depth_levels))
            grids[var] = arr

        print(f"  Downloaded {len(times_epoch)} time step(s) x {len(depth_levels)} depth level(s).")
        return times_epoch, depth_levels, grids

    finally:
        ds.close()



# ── NC helpers — 2D (time, depth) ─────────────────────────────────────────────

def _open_or_create_2d(
    nc_path: Path,
    var_code: str,
    depth_levels: np.ndarray,
) -> netCDF4.Dataset:
    """Open an existing 2D NC file or create one with the given depth axis."""
    if nc_path.exists():
        ds = netCDF4.Dataset(nc_path, "a")
        # Verify depth axis matches
        existing_depths = ds.variables["depth"][:].tolist()
        new_depths = depth_levels.tolist()
        if existing_depths != new_depths:
            ds.close()
            raise ValueError(
                f"Depth axis mismatch for {nc_path.name}: "
                f"existing {len(existing_depths)} levels vs new {len(new_depths)} levels. "
                f"Delete the file and re-run to rebuild with the updated grid."
            )
        return ds

    nc_path.parent.mkdir(parents=True, exist_ok=True)
    ds = netCDF4.Dataset(nc_path, "w", format="NETCDF4")

    ds.createDimension("time",  None)               # unlimited
    ds.createDimension("depth", len(depth_levels))  # fixed

    t_var = ds.createVariable("time", "f8", ("time",), zlib=True, complevel=4)
    t_var.units     = "seconds since 1970-01-01 00:00:00 UTC"
    t_var.calendar  = "gregorian"
    t_var.long_name = "time"

    d_var = ds.createVariable("depth", "f4", ("depth",), zlib=True, complevel=4)
    d_var.units     = "m"
    d_var.positive  = "down"
    d_var.long_name = "depth"
    d_var[:] = depth_levels

    v_var = ds.createVariable(
        var_code, "f4", ("time", "depth"),
        zlib=True, complevel=4,
        chunksizes=(1, len(depth_levels)),
        fill_value=np.float32(np.nan),
    )
    v_var.long_name = var_code

    return ds


def append_to_nc_2d(
    nc_path: Path,
    var_code: str,
    times_epoch: np.ndarray,
    depth_levels: np.ndarray,
    data_2d: np.ndarray,
) -> int:
    """
    Append new time slices to a 2D (time, depth) NC file.

    Args:
        times_epoch: 1D float64 array — epoch seconds, sorted ascending
        depth_levels: 1D float32 array — depth axis (used for creation)
        data_2d: 2D float32 array shape (len(times_epoch), len(depth_levels))

    Returns:
        Number of new time slices written.
    """
    if len(times_epoch) == 0:
        return 0

    ds = _open_or_create_2d(nc_path, var_code, depth_levels)
    try:
        existing: set[float] = set()
        if "time" in ds.variables and len(ds.variables["time"]) > 0:
            existing = set(ds.variables["time"][:].tolist())

        new_indices = [
            i for i, t in enumerate(times_epoch.tolist())
            if t not in existing
        ]
        if not new_indices:
            return 0

        n = len(ds.variables["time"])
        for offset, i in enumerate(new_indices):
            ds.variables["time"][n + offset]        = times_epoch[i]
            ds.variables[var_code][n + offset, :]   = data_2d[i, :]

        return len(new_indices)
    finally:
        ds.close()


# ── Last-stored-time helper ────────────────────────────────────────────────────

def get_last_stored_time_nc(nc_path: Path) -> datetime | None:
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


# ── Conversion ─────────────────────────────────────────────────────────────────

def apply_conversion(value: float, canonical_var: str, sensor_var_mapping: dict) -> float:
    if value is None or not isinstance(value, (int, float)):
        return value
    var_info = sensor_var_mapping.get(canonical_var)
    if isinstance(var_info, dict):
        return value * var_info.get("conversion_factor", 1.0)
    return value


# ── Main loop ──────────────────────────────────────────────────────────────────

def fetch_and_store(sensor_id_filter: int | None = None):
    conn = get_db_conn()

    with conn.cursor() as cur:
        query = """
            SELECT id, name, variables, source->>'link'
            FROM sensors
            WHERE active = true
              AND source->>'type' = 'ERDDAP'
              AND cardinality(depth) > 1
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
        print("No active variable-depth ERDDAP sensors found (depth = -1).")
        return

    print(f"Found {len(sensors)} variable-depth ERDDAP sensor(s).")

    for sensor_id, name, variables_json, erddap_link in sensors:
        print(f"\nSensor: {name} (ID: {sensor_id})")
        print(f"  Dataset: {erddap_link}")

        if "/griddap/" not in erddap_link:
            print(f"  SKIPPED — link does not point to a griddap endpoint (/griddap/ not found in URL).")
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

            # Exclude any axis entries ('time', 'depth') from data variables
            erddap_to_canonical = {
                info["name"]: canonical
                for canonical, info in variables_mapping.items()
                if canonical not in ("time", "depth")
                and isinstance(info, dict) and info.get("name")
            }
            erddap_vars = list(erddap_to_canonical.keys())
            print(f"  ERDDAP columns: {erddap_vars}")

            time_col  = variables_mapping.get("time",  {}).get("name", ERDDAP_TIME_VAR)
            depth_col = variables_mapping.get("depth", {}).get("name", ERDDAP_DEPTH_VAR)
            if time_col != ERDDAP_TIME_VAR:
                print(f"  Time axis:  '{time_col}'")
            if depth_col != ERDDAP_DEPTH_VAR:
                print(f"  Depth axis: '{depth_col}'")

        except Exception as e:
            print(f"  ERROR parsing variables mapping: {e}")
            continue

        storage_dir = Path(SENSORS_ROOT) / str(sensor_id)

        # dateFrom: earliest last-stored time across all variable files minus 1 day
        date_froms = []
        for erddap_col in erddap_vars:
            t = get_last_stored_time_nc(storage_dir / f"{erddap_col}.nc")
            if t:
                date_froms.append(t - timedelta(days=1))
        date_from = min(date_froms) if date_froms else None

        if date_from:
            print(f"  Fetching from: {date_from.strftime(ERDDAP_TIME_FMT)}")
        else:
            print("  No prior data — fetching all available.")

        try:
            times_epoch, depth_levels, grids = fetch_erddap_griddap_nc(
                erddap_link, erddap_vars, date_from,
                time_col=time_col, depth_col=depth_col,
            )
        except Exception as e:
            print(f"  ERROR fetching: {e}")
            continue

        if len(times_epoch) == 0:
            print("  No new data returned.")
            continue

        print(f"  Grid: {len(times_epoch)} time step(s) × {len(depth_levels)} depth level(s)")

        # Apply conversion and write one NC file per variable
        for erddap_col, grid in grids.items():
            canonical = erddap_to_canonical[erddap_col]
            nc_path = storage_dir / f"{erddap_col}.nc"

            # Apply conversion factor (scalar multiply across the grid)
            var_info = variables_mapping.get(canonical)
            if isinstance(var_info, dict):
                factor = var_info.get("conversion_factor", 1.0)
                if factor != 1.0:
                    grid = grid * np.float32(factor)

            try:
                n_written = append_to_nc_2d(
                    nc_path, erddap_col, times_epoch, depth_levels, grid
                )
                print(f"  {erddap_col}: wrote {n_written} new time slice(s) → {nc_path}")
            except ValueError as e:
                print(f"  {erddap_col}: SKIPPED — {e}")
            except Exception as e:
                print(f"  {erddap_col}: ERROR writing NC: {e}")

    print("\nDone.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Fetch variable-depth gridded ERDDAP data and store as 2D netCDF4 files."
    )
    parser.add_argument(
        "--sensor-id",
        type=int,
        default=None,
        help="Process only this sensor ID (default: all variable-depth ERDDAP sensors)",
    )
    args = parser.parse_args()
    fetch_and_store(sensor_id_filter=args.sensor_id)
