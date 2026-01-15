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
from typing import Optional, Sequence, Tuple, List

import re
import numpy as np
import xarray as xr
import pandas as pd

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


# DB helpers (same semantics as extractProfile)

def connect_db(dsn: Optional[str], host: str, port: int, user: str, password: str, dbname: str):
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
    sql = f"SELECT row_idx, col_idx, lat, lon FROM {table} ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s,%s),4326) LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(sql, (lon, lat))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("Grid table is empty or not found")
        return int(row[0]), int(row[1]), float(row[2]), float(row[3])


def get_grid_shape_from_db(conn, table: str) -> Tuple[int, int]:
    sql = f"SELECT COALESCE(MAX(row_idx),0), COALESCE(MAX(col_idx),0) FROM {table}"
    with conn.cursor() as cur:
        cur.execute(sql)
        r, c = cur.fetchone()
        return int(r) + 1, int(c) + 1


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
    depth: float,
    data_dir: str = "/opt/data/nc",
    db_dsn: Optional[str] = None,
    db_host: Optional[str] = "db",
    db_port: int = 5432,
    db_user: str = "postgres",
    db_password: str = "postgres",
    db_name: str = "oa",
    db_table: str = "grid",
    verbose: bool = False,
) -> Tuple[pd.Series, pd.Series]:
    """Extract a time series across all available files and return pandas Series (time, value).

    Note: There is no longer a `from_dt`/`to_dt` filter — the function will
    scan all NetCDF files matching the naming pattern for the requested
    variable and return the concatenated, deduplicated, sorted timeseries.
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
    if verbose:
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
    if not os.path.isdir(data_dir):
        raise RuntimeError(f"Data directory not found: {data_dir}")

    files = sorted(glob(os.path.join(data_dir, var, "*.nc")))

    # iterate files and extract
    times_list: List[pd.DatetimeIndex] = []
    values_list: List[np.ndarray] = []

    # Find a sample dataset that can be opened to inspect variable and depth axis
    sample_ds = None
    var_sample = None
    depth_dim = None
    for fp in files:
        try:
            sample_ds = xr.open_dataset(fp)
            var_sample = find_variable(sample_ds, var)
            depth_dim = find_depth_dim(var_sample)
            break
        except Exception as e:
            if verbose:
                print(f"Skipping sample file {fp} due to open/inspection error: {e}")
            try:
                if sample_ds is not None:
                    sample_ds.close()
            except Exception:
                pass
            sample_ds = None
            var_sample = None
            depth_dim = None
            continue

    if var_sample is None:
        # Ensure DB connection is closed if still open
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
        conn = None
        raise RuntimeError("Could not open any NetCDF files to inspect variable and depth dimension")

    # Determine which depth index to select based on the requested depth value
    if depth_dim is not None:
        try:
            depths = sample_ds[depth_dim].values
            # support depth arrays that may be increasing or decreasing
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
    try:
        sample_ds.close()
    except Exception:
        pass

    # Keep iteration robust: ensure file resources are closed on error and skip files that fail
    # to open or select. This prevents a single corrupted .nc from bringing down the whole
    # API and avoids leaving backend resources in an inconsistent/locked state.

    for fp in files:
        try:
            with xr.open_dataset(fp) as dsf:
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
                    idxs_local, times_local = pick_time_slice(dsf, tdim, None, None)
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
                times_list.append(pd.to_datetime(times_local))
                values_list.append(vals)
        except Exception as e:
            if verbose:
                print(f"Skipping {fp}: failed to open/process file: {e}")
            continue

    if not times_list:
        # Ensure DB connection is closed if still open
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
        conn = None
        raise RuntimeError("No data found in files for the requested time range")

    times_concat = pd.DatetimeIndex([]).append(times_list)
    values_concat = np.concatenate(values_list)

    df = pd.DataFrame({"time": times_concat, "value": values_concat})
    df = df.drop_duplicates(subset="time").sort_values(by="time").reset_index(drop=True)

    # Ensure DB connection is closed if still open
    try:
        if conn is not None:
            conn.close()
    except Exception:
        pass
    conn = None
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
    p.add_argument("--depth", type=float, required=True, help="Depth value to select (required)")
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
    df = extract_timeseries(
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
        verbose=args.verbose,
    )

    if args.output:
        df.to_csv(args.output, index=False)
        if args.verbose:
            print(f"Wrote {len(df)} rows to {args.output}")
    else:
        print("time,value")
        for t, v in zip(df["time"], df["value"]):
            print(f"{t.isoformat()},{'' if np.isnan(v) else v}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
