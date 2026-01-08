#!/usr/bin/env python3
"""Upload 2D lat/lon grid from a NetCDF file into a PostGIS table.

By default reads: ./data/nc/bathymetry.nc
Creates a table with columns: row, col, lat, lon, geom (Point,4326)

Supports sampling via --stride to avoid inserting every cell for very large grids.
"""
from __future__ import annotations
import argparse
import os
import sys
from typing import Optional, Tuple

import numpy as np
import xarray as xr

try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None


LAT_CANDIDATES = ("lat", "latitude", "nav_lat", "y")
LON_CANDIDATES = ("lon", "longitude", "nav_lon", "x")


def find_latlon(ds: xr.Dataset) -> Tuple[np.ndarray, np.ndarray]:
    """Return 2D arrays (lat2d, lon2d) from dataset. Raises RuntimeError if not found."""
    lat = None
    lon = None
    for c in ds.coords:
        if c.lower() in LAT_CANDIDATES and lat is None:
            lat = ds[c]
        if c.lower() in LON_CANDIDATES and lon is None:
            lon = ds[c]
    # search variables if not in coords
    if lat is None:
        for v in ds.variables:
            if v.lower() in LAT_CANDIDATES:
                lat = ds[v]
                break
    if lon is None:
        for v in ds.variables:
            if v.lower() in LON_CANDIDATES:
                lon = ds[v]
                break

    if lat is None or lon is None:
        raise RuntimeError("Could not find lat/lon in dataset; check grid file and variable names")

    lat_vals = lat.values
    lon_vals = lon.values

    if lat_vals.ndim == 1 and lon_vals.ndim == 1:
        lon2d, lat2d = np.meshgrid(lon_vals, lat_vals)
    elif lat_vals.ndim == 2 and lon_vals.ndim == 2:
        lat2d = lat_vals
        lon2d = lon_vals
    else:
        raise RuntimeError(f"Unsupported lat/lon shapes: lat={lat_vals.shape} lon={lon_vals.shape}")

    return lat2d, lon2d


def connect_db(dsn: Optional[str], host: str, port: int, user: str, password: str, dbname: str):
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not installed. Install with 'pip install psycopg2-binary'")
    if dsn:
        return psycopg2.connect(dsn)
    return psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)


def ensure_postgis(cursor):
    cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis")


def create_table(cursor, table: str, overwrite: bool = False):
    if overwrite:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            row_idx integer,
            col_idx integer,
            lat double precision,
            lon double precision,
            geom geometry(Point,4326)
        )
        """
    )
    # create spatial index
    cursor.execute(f"CREATE INDEX IF NOT EXISTS {table}_geom_gist ON {table} USING GIST (geom)")


def chunked_iter(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def main(argv=None):
    p = argparse.ArgumentParser(description="Upload 2D lat/lon grid from NetCDF into PostGIS")
    p.add_argument("--file", "-f", default="/opt/data/nc/bathymetry.nc", help="Path to bathymetry NetCDF file")
    p.add_argument("--table", "-t", default="grid", help="PostGIS table name to create/append")
    p.add_argument("--host", default=os.environ.get("DB_HOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.environ.get("DB_PORT", 5432)))
    p.add_argument("--user", default=os.environ.get("DB_USER", "postgres"))
    p.add_argument("--password", default=os.environ.get("DB_PASSWORD", "postgres"))
    p.add_argument("--dbname", default=os.environ.get("DB_NAME", "oa"))
    p.add_argument("--dsn", default=None, help="Optional full DSN to connect (overrides host/user/password/dbname)")
    p.add_argument("--overwrite", "-w", action="store_true", help="Drop and recreate the table")
    p.add_argument("--stride", type=int, default=1, help="Sample every nth cell in each dimension (default 1 = all)")
    p.add_argument("--chunk", type=int, default=5000, help="Insert chunk size (rows per transaction)")
    p.add_argument("--verbose", "-v", action="store_true")
    args = p.parse_args(argv)

    if args.verbose:
        print(f"Opening NetCDF {args.file}")

    ds = xr.open_dataset(args.file)
    lat2d, lon2d = find_latlon(ds)

    if args.verbose:
        print(f"Found lat/lon arrays with shape {lat2d.shape}")

    # sample using stride
    s = args.stride
    lat_s = lat2d[::s, ::s]
    lon_s = lon2d[::s, ::s]

    nrows, ncols = lat_s.shape
    total = nrows * ncols

    if args.verbose:
        print(f"Sampling grid with stride={s}: inserting {total} rows (shape {lat_s.shape})")

    conn = connect_db(args.dsn, args.host, args.port, args.user, args.password, args.dbname)
    cur = conn.cursor()
    ensure_postgis(cur)
    create_table(cur, args.table, overwrite=args.overwrite)
    conn.commit()

    # prepare rows: for each cell, insert (row_idx, col_idx, lat, lon, geom)
    rows = []
    for i in range(nrows):
        for j in range(ncols):
            latv = float(lat_s[i, j])
            lonv = float(lon_s[i, j])
            if np.isnan(latv) or np.isnan(lonv):
                continue
            # For the ST_SetSRID(ST_MakePoint(%s,%s),4326) template we pass lon, lat again
            rows.append((i, j, latv, lonv, lonv, latv))

    if args.verbose:
        print(f"Prepared {len(rows)} valid rows to insert")

    insert_sql = f"INSERT INTO {args.table} (row_idx, col_idx, lat, lon, geom) VALUES %s"

    # execute in chunks
    for chunk in chunked_iter(rows, args.chunk):
        psycopg2.extras.execute_values(
            cur,
            insert_sql,
            chunk,
            template="(%s,%s,%s,%s,ST_SetSRID(ST_MakePoint(%s,%s),4326))",
        )
        conn.commit()
        if args.verbose:
            print(f"Inserted chunk of {len(chunk)} rows")

    # ANALYZE table
    cur.execute(f"ANALYZE {args.table}")
    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted {len(rows)} points into {args.table}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
