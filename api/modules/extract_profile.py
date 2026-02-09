#!/usr/bin/env python3
"""extract_profile.py

Extract a vertical profile (depth vs. value) for a NetCDF variable at a coordinate and datetime.

This module reads NetCDF files to extract all depth levels at a specific lat/lon/time.
It follows the same pattern as extractTimeseries but returns depth-profile data instead
of time-series data.

Returns:
  List of dicts with 'depth' and 'value' keys, or empty list if no data found.
"""

import os
import re
from glob import glob
from typing import Optional, Tuple, List
import numpy as np
import xarray as xr
import pandas as pd
import threading
from lock_manager import io_lock as _io_lock

try:
    import psycopg2
    import psycopg2.extras
    psycopg2_import_error = False
except Exception:
    psycopg2 = None
    psycopg2_import_error = True

TIME_CANDIDATES = ("time",)
DEPTH_CANDIDATES = ("depth", "lev", "level", "z", "deptht", "depthu", "depths")


def find_variable(ds: xr.Dataset, name: str) -> xr.DataArray:
    """Find variable by name (case-insensitive)."""
    if name in ds:
        return ds[name]
    low = name.lower()
    for k in ds.data_vars:
        if k.lower() == low:
            return ds[k]
    raise KeyError(f"Variable '{name}' not found in dataset")


def connect_db(dsn: Optional[str], host: str, port: int, user: str, password: str, dbname: str):
    """Connect to PostGIS database."""
    if psycopg2 is None:
        raise RuntimeError(
            "psycopg2 is not available. For quick development install the binary wheel:\n"
            "  pip install psycopg2-binary\n"
            "or in uv: uv add --active psycopg2-binary\n"
        )
    if dsn:
        return psycopg2.connect(dsn)
    return psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)


def query_nearest_rowcol(conn, table: str, lat: float, lon: float) -> Tuple[int, int, float, float]:
    """Query the nearest grid cell from the database."""
    sql = f"SELECT row_idx, col_idx, lat, lon FROM {table} ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s,%s),4326) LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(sql, (lon, lat))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("Grid table is empty or not found")
        return int(row[0]), int(row[1]), float(row[2]), float(row[3])


def get_grid_shape_from_db(conn, table: str) -> Tuple[int, int]:
    """Get grid dimensions from database."""
    sql = f"SELECT COALESCE(MAX(row_idx),0), COALESCE(MAX(col_idx),0) FROM {table}"
    with conn.cursor() as cur:
        cur.execute(sql)
        r, c = cur.fetchone()
        return int(r) + 1, int(c) + 1


def find_horiz_dims_by_shape(var: xr.DataArray, nrows: int, ncols: int) -> Tuple[str, str]:
    """Identify horizontal dimension names."""
    y_dim = None
    x_dim = None
    for d in var.dims:
        if var.sizes[d] == nrows and y_dim is None:
            y_dim = d
        elif var.sizes[d] == ncols and x_dim is None:
            x_dim = d
    if y_dim is None or x_dim is None:
        for d in var.dims:
            if d.lower() in ("y", "j", "ygrid", "eta") and y_dim is None:
                y_dim = d
            if d.lower() in ("x", "i", "xgrid", "xi") and x_dim is None:
                x_dim = d
    if y_dim is None or x_dim is None:
        if len(var.dims) >= 2:
            return var.dims[-2], var.dims[-1]
        raise RuntimeError("Could not determine horizontal dims of variable")
    return y_dim, x_dim


def find_depth_dim(var: xr.DataArray, forced: Optional[str] = None) -> Optional[str]:
    """Identify depth dimension name."""
    if forced:
        return forced
    for d in var.dims:
        if d.lower() in DEPTH_CANDIDATES:
            return d
    for d in var.dims:
        if d.lower() not in TIME_CANDIDATES and var[d].ndim == 1 and var.sizes[d] > 1:
            return d
    return None


def find_time_dim(var: xr.DataArray) -> Optional[str]:
    """Identify time dimension name."""
    for d in var.dims:
        if d.lower() in TIME_CANDIDATES:
            return d
    return None


def extract_profile(
    *,
    var: str,
    lat: float,
    lon: float,
    dt: str,
    data_dir: str = "/opt/data/nc",
    db_dsn: Optional[str] = None,
    db_host: Optional[str] = "db",
    db_port: int = 5432,
    db_user: str = "postgres",
    db_password: str = "postgres",
    db_name: str = "oa",
    db_table: str = "grid",
    verbose: bool = False,
) -> List[dict]:
    """
    Extract a vertical profile at a given coordinate and datetime.
    
    Args:
        var: Variable name
        lat: Latitude
        lon: Longitude
        dt: Datetime as ISO-8601 string (e.g., "2023-01-15T12:00:00")
        data_dir: Root directory containing .nc files organized by variable
        db_dsn: Database connection string (optional)
        db_host: Database host (default: "db")
        db_port: Database port (default: 5432)
        db_user: Database user (default: "postgres")
        db_password: Database password (default: "postgres")
        db_name: Database name (default: "oa")
        db_table: Grid table name (default: "grid")
        verbose: Print debug messages
    
    Returns:
        List of dicts with 'depth' and 'value' keys, sorted by depth (ascending)
    """
    if db_dsn is None and not db_host:
        raise RuntimeError("No database host or DSN provided. Specify db_dsn or db_host")

    # Parse target datetime
    try:
        target_dt = pd.to_datetime(dt)
    except Exception as exc:
        raise ValueError(f"Could not parse datetime '{dt}': {exc}")

    try:
        conn = connect_db(db_dsn, db_host, db_port, db_user, db_password, db_name)
    except Exception as exc:
        raise RuntimeError(f"Could not connect to PostGIS DB: {exc}")

    # Determine row/col by nearest grid point in DB
    yi, xi, lat_pt, lon_pt = query_nearest_rowcol(conn, db_table, lat, lon)
    if verbose:
        print(f"Nearest grid point in DB: row={yi} col={xi} lat={lat_pt} lon={lon_pt}")

    try:
        nrows, ncols = get_grid_shape_from_db(conn, db_table)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # Find candidate files
    if not os.path.isdir(data_dir):
        raise RuntimeError(f"Data directory not found: {data_dir}")

    files = sorted(glob(os.path.join(data_dir, var, "*.nc")))
    if verbose:
        print(f"Found {len(files)} candidate files for variable '{var}'")

    if not files:
        raise RuntimeError(f"No NetCDF files found for variable '{var}' in '{data_dir}'")

    # Find a sample file to inspect variable structure
    sample_ds = None
    var_sample = None
    depth_dim = None
    time_dim = None

    for fp in files:
        try:
            with _io_lock:
                sample_ds = xr.open_dataset(fp)
                var_sample = find_variable(sample_ds, var)
                depth_dim = find_depth_dim(var_sample)
                time_dim = find_time_dim(var_sample)
            break
        except Exception as e:
            if verbose:
                print(f"Skipping sample file {fp}: {e}")
            try:
                if sample_ds is not None:
                    sample_ds.close()
            except Exception:
                pass
            sample_ds = None
            var_sample = None
            continue

    if var_sample is None:
        raise RuntimeError(
            f"Could not open any NetCDF files to inspect variable '{var}'; "
            f"files_found={len(files)}"
        )

    # Get horizontal dimensions
    y_dim, x_dim = find_horiz_dims_by_shape(var_sample, nrows, ncols)
    try:
        sample_ds.close()
    except Exception:
        pass

    # If no depth dimension, we can't extract a profile
    if depth_dim is None:
        raise RuntimeError(f"Variable '{var}' has no depth dimension; cannot extract profile")

    # Iterate files to find the one containing the target datetime
    profile_data = []

    for fp in files:
        try:
            with _io_lock:
                ds = xr.open_dataset(fp)
            
            try:
                # Check if this file contains the target time
                if time_dim is None:
                    # File has only one time; assume it matches
                    file_contains_time = True
                    time_idx = None
                else:
                    times = ds[time_dim].values
                    times_pd = pd.to_datetime(times)
                    # Find exact match or closest match
                    diffs = np.abs((times_pd - target_dt).total_seconds())
                    if diffs.min() < 3600:  # Within 1 hour
                        time_idx = int(np.argmin(diffs))
                        file_contains_time = True
                    else:
                        file_contains_time = False

                if not file_contains_time:
                    continue

                # Extract the profile for this coordinate at the target time
                if time_dim is not None:
                    data_at_time = ds[var].isel({time_dim: time_idx})
                else:
                    data_at_time = ds[var]

                # Select horizontal location
                data_at_loc = data_at_time.isel({y_dim: yi, x_dim: xi})

                # Extract all depth levels
                depths = ds[depth_dim].values
                if data_at_loc.ndim == 1:
                    # data_at_loc is 1D along depth
                    values = data_at_loc.values
                else:
                    # Shouldn't happen if we selected correctly, but handle it
                    values = data_at_loc.values.flatten()

                # Build profile list (depth, value)
                for d_idx, depth_val in enumerate(depths):
                    if d_idx < len(values):
                        val = values[d_idx]
                        # Skip NaN/masked values
                        if not np.isnan(val) and val is not None and val != 0:
                            profile_data.append({
                                "depth": float(depth_val),
                                "value": float(val)
                            })

                # Found the data; stop searching files
                break

            finally:
                try:
                    ds.close()
                except Exception:
                    pass

        except Exception as e:
            if verbose:
                print(f"Error processing file {fp}: {e}")
            continue

    # Sort by depth ascending
    profile_data.sort(key=lambda x: x["depth"])
    return profile_data
