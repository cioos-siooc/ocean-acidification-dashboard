#!/usr/bin/env python3
"""Export grid and variable bounds from Postgres for offline use.

Run this ONCE while you have DB access (e.g. inside the container or with
port-forwarded DB), then copy the output files to the remote machine alongside
nc2tile.py.

Output files
------------
grid.npz     -- lon/lat curvilinear grid arrays (required by nc2tile.py)
fields.json  -- per-variable min/max bounds (optional; used for scaling)

Usage
-----
    python export_grid.py --out-dir .
    python export_grid.py --out-dir /some/dir --pghost localhost --pgport 5433
"""

import argparse
import json
import os
import sys

import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor


def get_conn(host, port, dbname, user, password):
    return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)


def export_grid(conn, out_path: str) -> None:
    print("Exporting grid table...")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT row_idx, col_idx, lon, lat FROM grid")
        rows = cur.fetchall()

    if not rows:
        raise ValueError("Grid table is empty or missing")

    row_idxs = sorted({r["row_idx"] for r in rows})
    col_idxs = sorted({r["col_idx"] for r in rows})
    row_pos = {v: i for i, v in enumerate(row_idxs)}
    col_pos = {v: j for j, v in enumerate(col_idxs)}

    nrows, ncols = len(row_idxs), len(col_idxs)
    lon = np.full((nrows, ncols), np.nan, dtype=float)
    lat = np.full((nrows, ncols), np.nan, dtype=float)
    for r in rows:
        i = row_pos[r["row_idx"]]
        j = col_pos[r["col_idx"]]
        lon[i, j] = float(r["lon"])
        lat[i, j] = float(r["lat"])

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    np.savez_compressed(out_path, lon=lon, lat=lat)
    print(f"  Wrote {out_path}  (shape: {lon.shape})")


def export_fields(conn, out_path: str) -> None:
    print("Exporting fields table (variable bounds)...")
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT variable, min, max FROM fields")
            rows = cur.fetchall()
    except psycopg2.Error as e:
        print(f"  Warning: could not read fields table: {e} — skipping")
        conn.rollback()
        return

    fields = {}
    for r in rows:
        if r["variable"] and r["min"] is not None and r["max"] is not None:
            fields[r["variable"]] = {"min": float(r["min"]), "max": float(r["max"])}

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w") as fh:
        json.dump(fields, fh, indent=2)
    print(f"  Wrote {out_path}  ({len(fields)} variables)")


def main():
    p = argparse.ArgumentParser(description="Export DB grid and fields for offline nc2tile use.")
    p.add_argument("--out-dir", default=".", help="Directory to write grid.npz and fields.json")
    p.add_argument("--pghost", default=os.getenv("PGHOST", "db"))
    p.add_argument("--pgport", type=int, default=int(os.getenv("PGPORT", 5432)))
    p.add_argument("--pgdatabase", default=os.getenv("PGDATABASE", "oa"))
    p.add_argument("--pguser", default=os.getenv("PGUSER", "postgres"))
    p.add_argument("--pgpassword", default=os.getenv("PGPASSWORD", "postgres"))
    args = p.parse_args()

    conn = get_conn(args.pghost, args.pgport, args.pgdatabase, args.pguser, args.pgpassword)
    try:
        export_grid(conn, os.path.join(args.out_dir, "grid.npz"))
        export_fields(conn, os.path.join(args.out_dir, "fields.json"))
    finally:
        conn.close()

    print("Done.")


if __name__ == "__main__":
    main()
