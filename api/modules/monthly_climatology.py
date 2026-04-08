"""Utilities for extracting monthly climatology and monthly time series at a coordinate.

Provides `get_monthly_climatology_at_coord(lat, lon, depth, var, ...)` which returns:
{
  "timeseries": {"time": [iso...], "value": [float...]},
  "climatology": {"month": [1..12], "virtual_time": [iso...], "mean": [...], "q1": [...], "q3": [...], "min": [...], "max": [...]} 
}

This module reuses helpers from `api/extractTimeseries.py` for DB queries and indexing.
"""
from __future__ import annotations

import os
from typing import Optional, Dict, Any

import numpy as np
import xarray as xr
import pandas as pd
from nc_reader import open_nc_uncached, close_nc

# Import helpers from extractTimeseries
from extractTimeseries import (
    connect_db,
    query_nearest_rowcol,
    get_grid_shape_from_db,
    find_horiz_dims_by_shape,
    find_depth_dim,
    extract_timeseries,
)


def _to_iso_list(dtarr) -> list[str]:
    # dtarr is numpy datetime64 or pandas.DatetimeIndex
    try:
        return [pd.Timestamp(t).isoformat() for t in dtarr]
    except Exception:
        return [str(t) for t in dtarr]


def get_monthly_climatology_at_coord(
    *,
    lat: float,
    lon: float,
    depth: float,
    variable: str,
    data_root: str = "/opt/data/SalishSeaCast",
    db_dsn: Optional[str] = None,
    db_host: str = "db",
    db_port: int = 5432,
    db_user: str = "postgres",
    db_password: str = "postgres",
    db_name: str = "oa",
    db_table: str = "grid",
    verbose: bool = False,
) -> Dict[str, Any]:
    """Extract monthly time series for `variable` at (lat,lon,depth) from per-year monthly files,
    and read monthly climatology stats from monthly_stats file.

    Returns a dictionary suitable for JSON encoding.
    """
    # Connect to DB and find nearest grid point
    conn = None
    try:
        conn = connect_db(db_dsn, db_host, db_port, db_user, db_password, db_name)
    except Exception as exc:
        raise RuntimeError(f"Could not connect to PostGIS DB: {exc}")

    try:
        yi, xi, lat_pt, lon_pt = query_nearest_rowcol(conn, db_table, lat, lon)
        if verbose:
            print(f"Nearest grid point: row={yi} col={xi} lat={lat_pt} lon={lon_pt}")

        nrows, ncols = get_grid_shape_from_db(conn, db_table)
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
        conn = None

    # 1) Extract timeseries from monthly per-year files using existing helper
    monthly_dir = os.path.join(data_root, "erddap_monthly")

    # Build a per-year timeseries mapping: { '2007': {time: [...], value: [...]}, ... }
    timeseries_by_year = {}
    try:
        times_pd, vals = extract_timeseries(
            var=variable,
            lat=lat,
            lon=lon,
            depth=depth,
            data_dir=monthly_dir,
            db_dsn=db_dsn,
            db_host=db_host,
            db_port=db_port,
            db_user=db_user,
            db_password=db_password,
            db_name=db_name,
            db_table=db_table,
            verbose=verbose,
            recent_days=None,  # include all monthly/yearly files for climatology
        )
        times_index = pd.to_datetime(times_pd)
        # Ensure we have a DatetimeIndex so `.year` attribute works even when a Series is returned
        times_index = pd.DatetimeIndex(times_index)
        if len(times_index) > 0:
            years = times_index.year
            unique_years = sorted({int(y) for y in years})
            for y in unique_years:
                sel = (years == y)
                sel_times = times_index[sel]
                # `vals` may be a numpy array or pandas Series; boolean mask should work for either
                sel_vals = vals[sel]
                times_arr = [t.isoformat() for t in sel_times]
                values_arr = [None if np.isnan(v) else float(v) for v in sel_vals]
                timeseries_by_year[str(int(y))] = {"time": times_arr, "value": values_arr}
    except Exception as exc:
        # If extraction fails, continue; climatology will still be returned
        timeseries_by_year = {}
        print(f"extract_timeseries failed: {exc}")

    # 2) Read monthly_stats climatology file
    from nc_finder import find_file
    stats_path = find_file(data_root, f"monthly_stats/{variable}_monthly_climatology.nc")
    if stats_path is None:
        raise FileNotFoundError(f"Monthly climatology file not found for: {variable}")

    ds = open_nc_uncached(stats_path)
    if ds is None:
        raise FileNotFoundError(f"Could not open monthly climatology file: {stats_path}")

    try:
        # Stats dataset expected to have variables: mean, q1, q3, min, max and coordinate 'month'
        required = ["mean", "q1", "q3", "min", "max"]
        for r in required:
            if r not in ds.data_vars:
                raise RuntimeError(f"Monthly stats file missing required variable: {r}")

        # find horizontal dims matching grid shape
        sample_var = ds[required[0]]
        try:
            y_dim, x_dim = find_horiz_dims_by_shape(sample_var, nrows, ncols)
        except Exception:
            # fallback to common names
            y_dim, x_dim = ("gridY", "gridX") if "gridY" in sample_var.dims and "gridX" in sample_var.dims else (sample_var.dims[-2], sample_var.dims[-1])

        depth_dim = find_depth_dim(sample_var)
        if depth_dim is not None:
            # pick nearest depth index
            try:
                depths = ds[depth_dim].values
                depth_idx = int(np.argmin(np.abs(depths - depth)))
            except Exception:
                depth_idx = 0
        else:
            depth_idx = None

        # select at row/col and optional depth
        sel = {y_dim: int(yi), x_dim: int(xi)}
        if depth_idx is not None:
            sel[depth_dim] = depth_idx

        # For each stat, select over sel and return array along month
        months = ds['month'].values if 'month' in ds.coords else np.arange(1, 13)
        virtual_time = ds['virtual_time'].values if 'virtual_time' in ds.coords else None

        def _sel_monthly(varname: str):
            da = ds[varname]
            # da dims expected to include 'month' as first dim
            try:
                sel_da = da.isel(sel)
            except Exception:
                # try sel with dict mapping names to ints
                sel2 = {k: int(v) for k, v in sel.items()}
                sel_da = da.isel(sel2)
            # now sel_da has 'month' dim
            if 'month' in sel_da.dims:
                arr = sel_da.values
            else:
                # if month is not a dim (unexpected), try to squeeze
                arr = np.asanyarray(sel_da.values)
            # ensure length 12 array
            return np.asarray(arr).astype(np.float64)

        mean_arr = _sel_monthly('mean')
        q1_arr = _sel_monthly('q1')
        q3_arr = _sel_monthly('q3')
        min_arr = _sel_monthly('min')
        max_arr = _sel_monthly('max')

        # Convert virtual_time to ISO strings if present
        vt_list = _to_iso_list(virtual_time) if virtual_time is not None else None
    finally:
        close_nc(ds)

    climatology = {
        'month': months.tolist() if hasattr(months, 'tolist') else list(months),
        'virtual_time': vt_list,
        'mean': [None if np.isnan(x) else float(x) for x in mean_arr.tolist()],
        'q1': [None if np.isnan(x) else float(x) for x in q1_arr.tolist()],
        'q3': [None if np.isnan(x) else float(x) for x in q3_arr.tolist()],
        'min': [None if np.isnan(x) else float(x) for x in min_arr.tolist()],
        'max': [None if np.isnan(x) else float(x) for x in max_arr.tolist()],
    }

    return {
        'timeseries': {'by_year': timeseries_by_year, 'years': [int(y) for y in sorted(timeseries_by_year.keys())]},
        'climatology': climatology,
        'nearest_grid_point': {'row': int(yi), 'col': int(xi), 'lat': lat_pt, 'lon': lon_pt},
    }
