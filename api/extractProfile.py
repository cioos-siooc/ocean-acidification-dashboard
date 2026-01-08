#!/usr/bin/env python3
"""extractProfile.py

Extract a vertical profile from a NetCDF variable at a given lat/lon.

This script is DB-only: it requires a PostGIS table containing the grid coordinates
`row_idx`, `col_idx`, `lat`, `lon`, and `geom` (SRID=4326). The script will connect
to the configured PostGIS database and use that grid. If the DB connection cannot be
established or the table does not exist/contains no rows, the script will exit with
an error.

Usage examples:
  python process/extractProfile.py --data data/out.nc --db-host localhost --db-name oa --db-user postgres --var temp --lat 12.3 --lon 45.6
  python process/extractProfile.py --data data/out.nc --db-dsn "dbname=oa user=postgres" --var temp --row 120 --col 45 --output profile.csv

Options:
  --method {nearest,linear} : interpolation method in the horizontal (default: nearest)
  --time TIME               : time index (int) or ISO-8601 string (nearest match). Default: first time if present
  --depth-name NAME         : force name of vertical coordinate if auto-detection fails
  --output FILE             : write CSV (depth,value)
  --verbose                 : print extra info
  --db-dsn/--db-host/...    : PostGIS connection options (required)
  --row/--col               : optional grid indices to extract directly (bypass lat/lon lookup) 

"""
from __future__ import annotations
import argparse
import os
from typing import Tuple, Optional, Sequence

import numpy as np
import xarray as xr
import pandas as pd

try:
    from scipy.interpolate import griddata
except Exception:
    griddata = None


LAT_CANDIDATES = ("lat", "latitude", "nav_lat", "y")
LON_CANDIDATES = ("lon", "longitude", "nav_lon", "x")
DEPTH_CANDIDATES = ("depth", "lev", "level", "z", "deptht", "depthu", "depths")
TIME_CANDIDATES = ("time",)


def find_variable(ds: xr.Dataset, name: str) -> xr.DataArray:
    if name in ds:
        return ds[name]
    # try case-insensitive match
    low = name.lower()
    for k in ds.data_vars:
        if k.lower() == low:
            return ds[k]
    raise KeyError(f"Variable '{name}' not found in dataset")


# Grid file support removed: lat/lon grids are expected to be available in PostGIS only.


def pick_time_index(ds: xr.Dataset, var: xr.DataArray, time_arg: Optional[str]) -> Tuple[Optional[str], Optional[int]]:
    """Return (time_dim_name, index) or (None, None) if no time dim."""
    time_dim = None
    for d in var.dims:
        if d.lower() in TIME_CANDIDATES:
            time_dim = d
            break
    if time_dim is None:
        return None, None

    if time_arg is None:
        return time_dim, 0

    # try integer
    try:
        ti = int(time_arg)
        return time_dim, ti
    except Exception:
        pass

    # parse datetime and find nearest
    times = ds[time_dim].values
    tgt = pd.to_datetime(time_arg)
    # convert dataset times to pandas datetimes
    times_pd = pd.to_datetime(times)
    idx = int((np.abs(times_pd - tgt)).argmin())
    return time_dim, idx


def normalize_lon_diff(lon_grid: np.ndarray, lon_pt: float) -> np.ndarray:
    """Compute wrapped absolute lon difference (degrees) accounting for 360 wrap."""
    d = np.abs(lon_grid - lon_pt)
    d = np.where(d > 180, 360 - d, d)
    return d


# --- Optional PostGIS support ---
try:
    import psycopg2
    import psycopg2.extras
    psycopg2_import_error = False
except Exception:
    psycopg2 = None
    psycopg2_import_error = True


def connect_db(dsn: Optional[str], host: str, port: int, user: str, password: str, dbname: str):
    if psycopg2 is None:
        raise RuntimeError(
            "psycopg2 is not available. For quick development install the binary wheel:\n"
            "  pip install psycopg2-binary\n"
            "or with uv in the project's active environment:\n"
            "  uv add --active psycopg2-binary\n\n"
            "If you prefer building from source (recommended for production), install system deps first, e.g. on Debian/Ubuntu:\n"
            "  apt-get update && apt-get install -y libpq-dev build-essential python3-dev && pip install psycopg2\n"
        )
    if dsn:
        return psycopg2.connect(dsn)
    return psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)


def query_nearest_rowcol(conn, table: str, lat: float, lon: float):
    """Return nearest (row_idx, col_idx, lat, lon) from PostGIS table using KNN (<->)"""
    sql = f"SELECT row_idx, col_idx, lat, lon FROM {table} ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s,%s),4326) LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(sql, (lon, lat))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("Grid table is empty or not found")
        return int(row[0]), int(row[1]), float(row[2]), float(row[3])


def query_neighbors(conn, table: str, row: int, col: int, radius: int = 1):
    """Return list of neighbor records with (row_idx, col_idx, lat, lon) within radius (inclusive)"""
    r0 = row - radius
    r1 = row + radius
    c0 = col - radius
    c1 = col + radius
    sql = f"SELECT row_idx, col_idx, lat, lon FROM {table} WHERE row_idx BETWEEN %s AND %s AND col_idx BETWEEN %s AND %s"
    with conn.cursor() as cur:
        cur.execute(sql, (r0, r1, c0, c1))
        return [(int(r), int(c), float(latv), float(lonv)) for r, c, latv, lonv in cur.fetchall()]


def get_grid_shape_from_db(conn, table: str) -> Tuple[int, int]:
    sql = f"SELECT COALESCE(MAX(row_idx),0), COALESCE(MAX(col_idx),0) FROM {table}"
    with conn.cursor() as cur:
        cur.execute(sql)
        r,c = cur.fetchone()
        return int(r) + 1, int(c) + 1


def find_horiz_dims_by_shape(var: xr.DataArray, nrows: int, ncols: int) -> Tuple[str, str]:
    """Try to find variable dims corresponding to grid rows and cols by matching sizes; fallback to last two dims"""
    y_dim = None
    x_dim = None
    for d in var.dims:
        if var.sizes[d] == nrows and y_dim is None:
            y_dim = d
        elif var.sizes[d] == ncols and x_dim is None:
            x_dim = d
    if y_dim is None or x_dim is None:
        # fallback heuristics: look for typical names
        for d in var.dims:
            if d.lower() in ("y","j","ygrid","eta") and y_dim is None:
                y_dim = d
            if d.lower() in ("x","i","xgrid","xi") and x_dim is None:
                x_dim = d
    if y_dim is None or x_dim is None:
        if len(var.dims) >= 2:
            return var.dims[-2], var.dims[-1]
        raise RuntimeError("Could not determine horizontal dims of variable")
    return y_dim, x_dim



# Legacy helper removed: extraction is DB-only and uses grid indices. Use --row/--col or DB lookup to identify grid cell.


def extract_profile_from_indices(var: xr.DataArray, yi: int, xi: int) -> Tuple[np.ndarray, np.ndarray]:
    """Extract vertical profile given grid indices (row=yi, col=xi)."""
    # find horizontal dims by heuristic: prefer dims with sizes >= indices
    y_dim = None
    x_dim = None
    for d in var.dims:
        if var.sizes[d] > yi and y_dim is None:
            y_dim = d
        elif var.sizes[d] > xi and x_dim is None:
            # avoid selecting same dim
            if d != y_dim:
                x_dim = d
    if y_dim is None or x_dim is None:
        # fallback to last two dims
        if len(var.dims) >= 2:
            y_dim = var.dims[-2]
            x_dim = var.dims[-1]
        else:
            raise RuntimeError("Could not determine horizontal dims of variable for indexing")

    sel = var.isel({y_dim: yi, x_dim: xi})

    # remaining dims should include depth
    depth_dim = None
    for d in sel.dims:
        if d not in (y_dim, x_dim,):
            depth_dim = d
            break
    if depth_dim is None:
        values = np.atleast_1d(sel.values)
        depths = np.arange(values.size)
        return depths, values

    depths = sel[depth_dim].values if depth_dim in sel.coords else np.arange(sel[depth_dim].size)
    values = sel.values
    return depths, values


# Legacy helper removed: linear interpolation across the whole grid is not supported in DB-only mode. Use --method linear which performs local patch interpolation using DB neighbors.


def extract_profile_linear_db(var: xr.DataArray, conn, table: str, yi: int, xi: int, radius: int = 1) -> Tuple[np.ndarray, np.ndarray]:
    """Linear interpolate using a local patch of neighbors taken from the DB (radius defines +/- neighbor window)."""
    if griddata is None:
        raise RuntimeError("Scipy not available. Install scipy to use --method linear")

    neighbors = query_neighbors(conn, table, yi, xi, radius=radius)
    if not neighbors:
        raise RuntimeError("No neighbor points found in DB around the specified index")

    points = np.array([[latv, lonv] for (_, _, latv, lonv) in neighbors])
    rows = [r for (r, _, _, _) in neighbors]
    cols = [c for (_, c, _, _) in neighbors]

    # find depth dim
    depth_dim = find_depth_dim(var)
    if depth_dim is None:
        # 2D variable
        # build values from var at single time/depth (isel will reduce appropriately)
        vals = []
        for r,c in zip(rows, cols):
            v = float(var.isel({find_horiz_dims_by_shape(var, max(rows)+1, max(cols)+1)[0]: r,
                                find_horiz_dims_by_shape(var, max(rows)+1, max(cols)+1)[1]: c}).values)
            vals.append(v)
        # do linear interpolation in 2D
        interp = griddata(points, np.array(vals), np.array([[np.mean([p[0] for p in points]), np.mean([p[1] for p in points])]]), method="linear")
        v = interp[0]
        if np.isnan(v):
            interp2 = griddata(points, np.array(vals), np.array([[np.mean([p[0] for p in points]), np.mean([p[1] for p in points])]]), method="nearest")
            v = interp2[0]
        depths = np.array([0])
        return depths, np.array([float(v)])

    n_levels = var.sizes[depth_dim]
    out_vals = np.empty(n_levels, dtype=float)
    out_vals.fill(np.nan)

    # dims for indexing
    nrows = max(rows) + 1
    ncols = max(cols) + 1
    y_dim, x_dim = find_horiz_dims_by_shape(var, nrows, ncols)

    for k in range(n_levels):
        vals = []
        for r,c in zip(rows, cols):
            v = var.isel({depth_dim: k, y_dim: r, x_dim: c}).values
            vals.append(float(v))
        interp = griddata(points, np.array(vals), np.array([[float(neighbors[0][2]), float(neighbors[0][3])]]), method="linear")
        v = interp[0]
        if np.isnan(v):
            interp2 = griddata(points, np.array(vals), np.array([[float(neighbors[0][2]), float(neighbors[0][3])]]), method="nearest")
            v = interp2[0]
        out_vals[k] = float(v)

    depths = var[depth_dim].values if depth_dim in var.coords else np.arange(n_levels)
    return depths, out_vals


def find_depth_dim(var: xr.DataArray, forced: Optional[str] = None) -> Optional[str]:
    if forced:
        return forced
    for d in var.dims:
        if d.lower() in DEPTH_CANDIDATES:
            return d
    # fallback: return first dim that is not time and whose size > 1 and not equal to any lat/lon sizes
    for d in var.dims:
        if d.lower() not in TIME_CANDIDATES and var[d].ndim == 1 and var.sizes[d] > 1:
            return d
    return None


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Extract vertical profile from a NetCDF variable at a lat/lon. Grid is read from a PostGIS table and DB connection is required.")
    p.add_argument("--data", "-d", required=True, help="Data NetCDF file containing the variable")
    p.add_argument("--var", "-v", required=True, help="Variable name to extract")
    p.add_argument("--lat", "-a", type=float, required=False, help="Latitude of point (required unless --row/--col specified)")
    p.add_argument("--lon", "-o", type=float, required=False, help="Longitude of point (required unless --row/--col specified)")
    p.add_argument("--method", "-m", choices=("nearest", "linear"), default="nearest", help="Horizontal interpolation method")
    p.add_argument("--time", "-t", default=None, help="Time index (int) or ISO timestamp string to pick nearest time")
    p.add_argument("--depth-name", default=None, help="Force depth coordinate name")
    p.add_argument("--output", "-O", default=None, help="CSV output file (depth,value)")
    p.add_argument("--verbose", "-V", action="store_true", help="Verbose output")

    # DB options (required)
    p.add_argument("--db-dsn", default=None, help="Optional libpq DSN to PostGIS (overrides host/user/password/dbname)")
    p.add_argument("--db-host", default="db", help="PostGIS host")
    p.add_argument("--db-port", type=int, default=int(os.environ.get("DB_PORT", 5432)), help="PostGIS port")
    p.add_argument("--db-user", default=os.environ.get("DB_USER", "postgres"), help="PostGIS user")
    p.add_argument("--db-password", default=os.environ.get("DB_PASSWORD", "postgres"), help="PostGIS password")
    p.add_argument("--db-name", default=os.environ.get("DB_NAME", "oa"), help="PostGIS database name")
    p.add_argument("--db-table", default="grid", help="PostGIS table that contains row_idx,col_idx,lat,lon,geom")
    p.add_argument("--neighbor-radius", type=int, default=1, help="Radius of neighbor window (for linear interpolation using DB) - default 1 => 3x3 window")

    args = p.parse_args(argv)

    # Require explicit DB connection parameters to avoid falling back to a local unix socket
    if args.db_dsn is None and not args.db_host:
        raise RuntimeError(
            "No database host or DSN provided. Specify --db-dsn or --db-host.\n"
            "Example (inside docker compose): --db-host db --db-port 5432\n"
            "Example (on host, DB mapped to host port): --db-host localhost --db-port 9012\n"
        )

    # Attempt to connect to DB - fail fast and loudly if not available
    try:
        conn = connect_db(args.db_dsn, args.db_host, args.db_port, args.db_user, args.db_password, args.db_name)
    except Exception as exc:
        raise RuntimeError(f"Could not connect to PostGIS DB: {exc}")

    # determine row/col
    if args.lat is None or args.lon is None:
        raise RuntimeError("When using DB lookup you must provide --lat/--lon or --row/--col")
    try:
        yi, xi, lat_pt, lon_pt = query_nearest_rowcol(conn, args.db_table, args.lat, args.lon)
    except Exception as exc:
        raise RuntimeError(f"Failed to find nearest grid point in DB: {exc}")
    if args.verbose:
        print(f"Nearest grid point in DB: row={yi} col={xi} lat={lat_pt} lon={lon_pt}")

    # open data and select time
    ds_data = xr.open_dataset(args.data)
    if args.verbose:
        print(f"Opened data: {args.data}")

    var = find_variable(ds_data, args.var)
    time_dim, time_idx = pick_time_index(ds_data, var, args.time)
    if time_dim is not None:
        if args.verbose:
            print(f"Using time dim '{time_dim}' index {time_idx}")
        var = var.isel({time_dim: time_idx})

    # extract
    if args.method == "nearest":
        depths, values = extract_profile_from_indices(var, yi, xi)
    else:
        depths, values = extract_profile_linear_db(var, conn, args.db_table, yi, xi, radius=args.neighbor_radius)

    conn.close()

    # Print or save
    import csv

    if args.output:
        with open(args.output, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["depth", "value"])
            for d, v in zip(depths, values):
                writer.writerow([d, v])
        if args.verbose:
            print(f"Wrote profile to {args.output}")
    else:
        print("depth,value")
        for d, v in zip(depths, values):
            print(f"{d},{v}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
