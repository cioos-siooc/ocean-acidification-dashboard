#!/usr/bin/env python3
"""extractTimeseries.py

Extract a time series for a NetCDF variable at a coordinate stored in PostGIS.

This script is DB-only: it requires a PostGIS table containing the grid coordinates
`row_idx`, `col_idx`, `lat`, `lon`, and `geom` (SRID=4326). Use `--lat/--lon` to
find the nearest grid cell or provide `--row/--col` to select a specific cell.

Examples:
  python process/extractTimeseries.py --data-dir ./data/nc --var temp \
      --from 2023-01-01T00:00:00 --to 2023-01-31T23:59:59 --lat 49.2 --lon -123.5 --db-host db
  python process/extractTimeseries.py --data-dir ./data/nc --var temp \
      --from 2023-01-01T00:00:00 --to 2023-01-01T23:59:59 --lat 49.2 --lon -123.5 --db-dsn "dbname=oa user=postgres"

Options:
  --from : inclusive start datetime (ISO-8601)
  --to   : inclusive end datetime (ISO-8601)
  --depth-index : optional integer to pick a vertical level when var has depth dim
  --output : CSV file path (time,value)

"""
from __future__ import annotations
import argparse
import os
from glob import glob
from typing import Optional, Sequence, Tuple, List, Union
from modules.nc_finder import list_nc_files

import re
import numpy as np
import xarray as xr
import pandas as pd
from nc_reader import open_nc_uncached, close_nc, _nc_lock as _io_lock
from modules.postgis_helpers import connect_db, query_nearest_rowcol, get_grid_shape_from_db

# Reuse same DB helper patterns as extractProfile
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
    if name in ds:
        return ds[name]
    low = name.lower()
    for k in ds.data_vars:
        if k.lower() == low:
            return ds[k]
    raise KeyError(f"Variable '{name}' not found in dataset")


def find_horiz_dims_by_shape(var: xr.DataArray, nrows: int, ncols: int) -> Tuple[str, str]:
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
    if forced:
        return forced
    for d in var.dims:
        if d.lower() in DEPTH_CANDIDATES:
            return d
    for d in var.dims:
        if d.lower() not in TIME_CANDIDATES and var[d].ndim == 1 and var.sizes[d] > 1:
            return d
    return None


def pick_time_slice(ds: xr.Dataset, time_dim: str, from_arg: Optional[str], to_arg: Optional[str]):
    times = ds[time_dim].values
    times_pd = pd.to_datetime(times)
    if from_arg is None:
        start = times_pd.min()
    else:
        start = pd.to_datetime(from_arg)
    if to_arg is None:
        end = times_pd.max()
    else:
        end = pd.to_datetime(to_arg)
    mask = (times_pd >= start) & (times_pd <= end)
    if not mask.any():
        raise RuntimeError(f"No timestamps found in [{start} - {end}]")
    idxs = np.nonzero(mask)[0]
    return idxs, times_pd[idxs]


def extract_timeseries(
    *,
    var: str,
    lat: float,
    lon: float,
    depth: Optional[float] = None,
    data_dir: str = "/opt/data/nc",
    db_dsn: Optional[str] = None,
    db_host: Optional[str] = "db",
    db_port: int = 5432,
    db_user: str = "postgres",
    db_password: str = "postgres",
    db_name: str = "oa",
    db_table: str = "grid",
    verbose: bool = False,
    from_date: str,
    to_date: str,
    allowed_dates: Optional[Sequence] = None,
) -> Union[Tuple[pd.Series, pd.Series], pd.DataFrame]:
    """Extract a time series across available files.

    When *depth* is provided, returns ``(times, values)`` as a tuple of pd.Series.
    When *depth* is ``None``, returns a ``pd.DataFrame`` with columns
    ``time``, ``depth``, ``value`` covering every depth level in each file.

    Files are filtered by date range extracted from their filenames (YYYYMMDD format).
    Only files with dates between from_date and to_date (inclusive) are considered.
    from_date and to_date must be ISO-8601 date strings (YYYY-MM-DD) or datetime strings.

    Exceptions are raised on errors; caller should catch and handle them.
    """
    
    if db_dsn is None and not db_host:
        raise RuntimeError("No database host or DSN provided. Specify db_dsn or db_host")

    try:
        conn = connect_db(db_dsn, db_host, db_port, db_user, db_password, db_name)
    except Exception as exc:
        raise RuntimeError(f"Could not connect to PostGIS DB: {exc}")

    # determine row/col by nearest grid point in DB (lat/lon are required)
    yi, xi, lat_pt, lon_pt = query_nearest_rowcol(conn, db_table, lat, lon)
    print(f"Nearest grid point in DB: row={yi} col={xi} lat={lat_pt} lon={lon_pt}")

    # Get grid shape and then close the DB connection promptly so heavy file work
    # that follows doesn't hold DB resources or transactions open while clients
    # may disconnect.
    try:
        nrows, ncols = get_grid_shape_from_db(conn, db_table)
    finally:
        try:
            conn.close()
        except Exception:
            pass
        conn = None

    # find candidate files
    # For depth=-1 (bottom layer), use the pre-extracted bottom NC files exclusively.
    # Those files are named {var}_bottom_{YYYYMMDD}.nc and have a single depth level at -1.0.
    use_bottom = (depth is not None and float(depth) == -1.0)
    files = list_nc_files(data_dir, var)
    if use_bottom:
        files = [f for f in files if os.path.basename(f).startswith(f"{var}_") and os.path.basename(f).endswith("_bottom.nc")]
    else:
        files = [f for f in files if not os.path.basename(f).endswith("_bottom.nc")]
    if verbose:
        print(f"DEBUG: Found {len(files)} candidate files for variable '{var}'" + (" (bottom)" if use_bottom else ""))
        if files:
            print("DEBUG: Sample files:", files[:5])

    # Filter files by date range. Extract dates from filenames.
    # Handles daily files:  {var}_{YYYYMMDD}.nc, {var}_{YYYYMMDD}T{HHMM}.nc, {var}_{YYYYMMDD}_bottom.nc
    # Handles yearly files: {var}_{YYYY}.nc  (4-digit year — always included if year overlaps range)
    date_pattern = re.compile(r"(\d{8})(?:T\d{4,6})?(?:_\w+)?\.nc$")
    year_pattern = re.compile(r"_(\d{4})\.nc$")

    # Parse from_date and to_date (mandatory parameters)
    try:
        start_date = pd.to_datetime(from_date).date()
    except Exception as e:
        raise ValueError(f"Invalid from_date format: {from_date}. Use ISO-8601 date format (YYYY-MM-DD)") from e
    try:
        end_date = pd.to_datetime(to_date).date()
    except Exception as e:
        raise ValueError(f"Invalid to_date format: {to_date}. Use ISO-8601 date format (YYYY-MM-DD)") from e

    if start_date > end_date:
        raise ValueError(f"from_date ({start_date}) cannot be after to_date ({end_date})")

    # Normalise allowed_dates to a set of date objects for O(1) lookup
    allowed_date_set = None
    if allowed_dates is not None:
        allowed_date_set = set()
        for d in allowed_dates:
            try:
                allowed_date_set.add(pd.to_datetime(d).date())
            except Exception:
                pass

    filtered_files = []
    for fp in files:
        # Try daily filename first (YYYYMMDD)
        m = date_pattern.search(fp)
        if m:
            datestr = m.group(1)
            try:
                file_dt = pd.to_datetime(datestr, format="%Y%m%d").date()
            except Exception:
                if verbose:
                    print(f"Skipping file with invalid date in name: {fp}")
                continue
            if file_dt < start_date or file_dt > end_date:
                continue
            # allowed_dates whitelist applies only to daily files
            if allowed_date_set is not None and file_dt not in allowed_date_set:
                if verbose:
                    print(f"Skipping file {fp}: date {file_dt} not in allowed_dates")
                continue
            filtered_files.append(fp)
            continue

        # Try yearly filename (YYYY) — include if the year overlaps the requested range
        ym = year_pattern.search(fp)
        if ym:
            year = int(ym.group(1))
            file_year_start = pd.Timestamp(year=year, month=1, day=1).date()
            file_year_end   = pd.Timestamp(year=year, month=12, day=31).date()
            if file_year_end < start_date or file_year_start > end_date:
                continue
            filtered_files.append(fp)
            continue

        if verbose:
            print(f"Skipping file with unrecognized name format: {fp}")
        continue

    files = filtered_files
    if verbose:
        print(f"Found {len(files)} files for variable '{var}' in data directory '{data_dir}' between {start_date} and {end_date}")


    # iterate files and extract
    times_list:  List[pd.DatetimeIndex] = []
    values_list: List[np.ndarray] = []
    depths_list: List[np.ndarray] = []  # populated only in all-depths mode

    # Find a sample dataset that can be opened to inspect variable and depth axis
    sample_ds = None
    var_sample = None
    depth_dim = None
    depths: Optional[np.ndarray] = None
    for fp in files:
        try:
            sample_ds = open_nc_uncached(fp)
            if sample_ds is None:
                raise RuntimeError(f"open_nc_uncached returned None for {fp}")
            var_sample = find_variable(sample_ds, var)
            depth_dim = find_depth_dim(var_sample)
            # Extract values and close immediately after inspection
            if depth_dim is not None:
                depths_values = sample_ds[depth_dim].values
            else:
                depths_values = None
            # Use extracted values outside the lock
            depths = depths_values
            # Close the sample file after we're done inspecting it
            close_nc(sample_ds)
            break
        except Exception as e:
            if verbose:
                print(f"Skipping sample file {fp} due to open/inspection error: {e}")
            close_nc(sample_ds)
            sample_ds = None
            var_sample = None
            depth_dim = None
            depths = None
            continue

    if var_sample is None:
        # Ensure DB connection is closed if still open
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
        conn = None
        # Provide more diagnostics in error so logs show what was attempted
        sample_paths = files[:5] if files else []
        if use_bottom:
            raise RuntimeError(
                f"No bottom-layer NC files found for variable '{var}' between {from_date} and {to_date}. "
                f"Bottom files (named {{var}}_YYYYMMDD_bottom.nc) are produced by the bottom_layer pipeline step. "
                f"Ensure the pipeline has run the bottom_layer stage for this variable and date range."
            )
        raise RuntimeError(
            f"Could not open any NetCDF files to inspect variable and depth dimension; "
            f"files_found={len(files)}, sample_paths={sample_paths}"
        )

    # Determine which depth index to select based on the requested depth value
    if depth is None:
        # All-depths mode — do not slice the depth axis
        depth_sel = None
        if verbose:
            n_d = len(depths) if (depths is not None) else "unknown"  # type: ignore[arg-type]
            print(f"No depth specified — extracting all {n_d} depth levels")
    elif depth_dim is not None:
        try:
            # support depth arrays that may be increasing or decreasing
            assert depths is not None
            depth_sel = int(np.argmin(np.abs(depths - depth)))
            if verbose:
                print(f"Selecting depth index {depth_sel} nearest to requested depth {depth}")
        except Exception:
            depth_sel = 0
            if verbose:
                print("Failed to map requested depth to an index; defaulting to 0")
    else:
        depth_sel = None

    y_dim, x_dim = find_horiz_dims_by_shape(var_sample, nrows, ncols)

    # Keep iteration robust: ensure file resources are closed on error and skip files that fail
    # to open or select. This prevents a single corrupted .nc from bringing down the whole
    # API and avoids leaving backend resources in an inconsistent/locked state.

    for fp in files:
        dsf = None
        try:
            dsf = open_nc_uncached(fp)
            if dsf is None:
                if verbose:
                    print(f"Skipping {fp}: could not open")
                continue

            try:
                varf = find_variable(dsf, var)
            except KeyError:
                if verbose:
                    print(f"Skipping {fp}: variable {var} not present")
                continue

            tdim = None
            for d in varf.dims:
                if d.lower() in TIME_CANDIDATES:
                    tdim = d
                    break
            if tdim is None:
                if verbose:
                    print(f"Skipping {fp}: no time dimension found")
                continue

            try:
                idxs_local, times_local = pick_time_slice(dsf, tdim, from_date, to_date)
            except Exception as exc:
                if verbose:
                    print(f"Skipping {fp}: pick_time_slice failed: {exc}")
                continue

            if len(idxs_local) == 0:
                if verbose:
                    print(f"Skipping {fp}: no timestamps in requested range")
                continue

            sel = {tdim: idxs_local, y_dim: yi, x_dim: xi}
            if depth_dim is not None and depth_sel is not None:
                sel[depth_dim] = depth_sel

            try:
                sub = varf.isel(sel)
            except Exception as exc:
                if verbose:
                    print(f"Skipping {fp}: failed to index variable with selection {sel}: {exc}")
                continue

            if tdim not in sub.dims and sub.ndim != 1:
                if verbose:
                    print(f"Skipping {fp}: unexpected selection result; expected 1D time series")
                continue

            vals = np.asarray(sub.values, dtype=float)

            if depth_sel is None and depth_dim is not None and depth_dim in sub.dims:
                # All-depths: vals is 2-D (time, depth) — flatten to parallel flat arrays
                dims = list(sub.dims)
                t_ax = dims.index(tdim) if tdim in dims else 0
                d_ax = dims.index(depth_dim)
                if t_ax != 0:
                    vals = np.moveaxis(vals, t_ax, 0)  # ensure (n_times, n_depths)
                n_t, n_d = vals.shape
                times_list.append(pd.DatetimeIndex(np.repeat(times_local, n_d)))
                values_list.append(vals.flatten())
                assert depths is not None
                depths_list.append(np.tile(depths, n_t))
            else:
                times_list.append(pd.to_datetime(times_local).copy())
                values_list.append(vals)
        except Exception as e:
            if verbose:
                print(f"Skipping {fp}: failed to open/process file: {e}")
        finally:
            close_nc(dsf)

    if not times_list:
        # Ensure DB connection is closed if still open
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
        conn = None
        raise RuntimeError("No data found in files for the requested time range")

    times_concat  = pd.DatetimeIndex([]).append(times_list)
    values_concat = np.concatenate(values_list)

    # Ensure DB connection is closed if still open
    try:
        if conn is not None:
            conn.close()
    except Exception:
        pass
    conn = None

    if depths_list:
        # All-depths mode: return a DataFrame with time, depth, value columns
        depths_concat = np.concatenate(depths_list)
        df = pd.DataFrame({"time": times_concat, "depth": depths_concat, "value": values_concat})
        df = (
            df.drop_duplicates(subset=["time", "depth"])
            .sort_values(by=["time", "depth"])
            .reset_index(drop=True)
        )
        return df

    df = pd.DataFrame({"time": times_concat, "value": values_concat})
    df = df.drop_duplicates(subset="time").sort_values(by="time").reset_index(drop=True)
    return df.time, df.value



def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Extract a time series from a NetCDF variable at a grid point stored in PostGIS")
    p.add_argument("--var", "-v", required=True, help="Variable name to extract")
    # start/end selection removed — function now returns all available times
    # p.add_argument("--from", dest="from_dt", required=True, help="Start datetime (inclusive), ISO-8601")
    # p.add_argument("--to", dest="to_dt", required=True, help="End datetime (inclusive), ISO-8601")
    p.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "/opt/data/nc"), help="Directory containing daily NetCDF files (default: /opt/data/nc)")
    p.add_argument("--lat", "-a", type=float, required=True, help="Latitude (required)")
    p.add_argument("--lon", "-o", type=float, required=True, help="Longitude (required)")
    p.add_argument("--depth", type=float, default=None,
                   help="Depth value to select; omit to extract all depth levels")
    p.add_argument("--from-date", required=True, help="Start date (ISO-8601 format, e.g., 2023-01-01)")
    p.add_argument("--to-date", required=True, help="End date (ISO-8601 format, e.g., 2023-12-31)")
    p.add_argument("--output", "-O", default=None, help="CSV output file (time,value)")
    p.add_argument("--verbose", "-V", action="store_true", help="Verbose output")

    # DB options (required)
    p.add_argument("--db-dsn", default=None, help="Optional libpq DSN to PostGIS (overrides host/user/password/dbname)")
    p.add_argument("--db-host", default="db", help="PostGIS host")
    p.add_argument("--db-port", type=int, default=int(os.environ.get("DB_PORT", 5432)), help="PostGIS port")
    p.add_argument("--db-user", default=os.environ.get("DB_USER", "postgres"), help="PostGIS user")
    p.add_argument("--db-password", default=os.environ.get("DB_PASSWORD", "postgres"), help="PostGIS password")
    p.add_argument("--db-name", default=os.environ.get("DB_NAME", "oa"), help="PostGIS database name")
    p.add_argument("--db-table", default="grid", help="PostGIS table that contains row_idx,col_idx,lat,lon,geom")

    args = p.parse_args(argv)

    # Use the high-level callable to perform extraction
    result = extract_timeseries(
        var=args.var,
        lat=args.lat,
        lon=args.lon,
        depth=args.depth,
        data_dir=args.data_dir,
        db_dsn=args.db_dsn,
        db_host=args.db_host,
        db_port=args.db_port,
        db_user=args.db_user,
        db_password=args.db_password,
        db_name=args.db_name,
        db_table=args.db_table,
        from_date=args.from_date,
        to_date=args.to_date,
        verbose=args.verbose,
    )

    if isinstance(result, pd.DataFrame):
        # All-depths result
        if args.output:
            result.to_csv(args.output, index=False)
            if args.verbose:
                print(f"Wrote {len(result)} rows to {args.output}")
        else:
            print("time,depth,value")
            for _, row in result.iterrows():
                v = float(row["value"])
                t = pd.Timestamp(row["time"])
                print(f"{t.isoformat()},{row['depth']},{'' if np.isnan(v) else v}")
    else:
        times, values = result
        if args.output:
            pd.DataFrame({"time": times, "value": values}).to_csv(args.output, index=False)
            if args.verbose:
                print(f"Wrote {len(times)} rows to {args.output}")
        else:
            print("time,value")
            for t, v in zip(times, values):
                print(f"{t.isoformat()},{'' if np.isnan(v) else v}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
