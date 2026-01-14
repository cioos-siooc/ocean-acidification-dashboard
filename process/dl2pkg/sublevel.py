"""Sublevel processing: select depth indices from downloaded NC file and write a reduced sublevel NetCDF.

API:
- process_sublevel(conn, row, depth_indices=None, dry_run=False, force=False)
- find_pending_sublevels(conn, limit=10)
- worker loop: process_pending_sublevels(conn, depth_indices=None, limit=5, dry_run=False)

Behavior:
- Loads global depth indices from `configs.json` if depth_indices not supplied.
- Reads source NetCDF from `row['file_path']`, selects depth indices via xarray.isel(depth=indices) and writes a new file under NC_ROOT/sublevels/{variable}/
- Updates the same `nc_files` row with filename_sublevel, file_path_sublevel, file_size_sublevel, checksum_sublevel, status_sublevel and attempt counters.
"""
from __future__ import annotations

import json
import os
import hashlib
import logging
from datetime import datetime
from typing import List, Optional

import xarray as xr

from .db import get_db_conn

logger = logging.getLogger("dl2.sublevel")

CONFIG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "configs.json"))


def load_configs(config_path: Optional[str] = None) -> dict:
    path = config_path or CONFIG_PATH
    try:
        with open(path, "r") as fh:
            cfg = json.load(fh)
        return cfg
    except Exception:
        logger.exception("Failed to load configs from %s", path)
        return {}


def load_depth_indices(config_path: Optional[str] = None) -> List[int]:
    cfg = load_configs(config_path)
    return list(cfg.get("depth_indices", []))


def _write_sublevel_ds(ds: xr.Dataset, out_path: str, compression: Optional[dict] = None) -> int:
    # Apply compression encoding if requested. compression is a dict like {"zlib": True, "complevel": 4, "shuffle": True}
    if compression:
        encoding = {}
        for v in ds.data_vars:
            enc = {}
            if compression.get("zlib"):
                enc["zlib"] = True
                enc["complevel"] = int(compression.get("complevel", 4))
                if compression.get("shuffle"):
                    enc["shuffle"] = True
            encoding[v] = enc
        ds.to_netcdf(out_path, encoding=encoding)
    else:
        ds.to_netcdf(out_path)
    size = os.path.getsize(out_path)
    return size


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def find_pending_sublevels(conn, limit: int = 10):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id,dataset_id,variable,start_time,end_time,filename,file_path,meta,attempts_sublevel,status_sublevel FROM nc_files WHERE status_dl='success' AND (status_sublevel IS NULL OR status_sublevel='pending' OR status_sublevel='failed') ORDER BY start_time LIMIT %s",
            (limit,),
        )
        rows = cur.fetchall()
        results = []
        for r in rows:
            results.append({
                "id": r[0],
                "dataset_id": r[1],
                "variable": r[2],
                "start_time": r[3],
                "end_time": r[4],
                "filename": r[5],
                "file_path": r[6],
                "meta": r[7],
                "attempts_sublevel": r[8],
                "status_sublevel": r[9],
            })
        return results


def process_sublevel(conn, row, depth_indices: Optional[List[int]] = None, dry_run: bool = False, force: bool = False, max_attempts: int = 1):
    nid = row["id"]
    src = row["file_path"]
    variable = row["variable"]

    if not src or not os.path.exists(src):
        with conn.cursor() as cur:
            cur.execute("UPDATE nc_files SET status_sublevel='failed', last_error_sublevel=%s, attempts_sublevel=attempts_sublevel+1, last_attempt_sublevel=NOW() WHERE id=%s", (f"source missing: {src}", nid))
        conn.commit()
        return False

    if depth_indices is None:
        depth_indices = load_depth_indices()
        cfg = load_configs()
        compression = cfg.get("compression")
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (nid,))
        locked = cur.fetchone()[0]
    if not locked:
        logger.info("Skipping id %s: lock not acquired", nid)
        return False

    try:
        # mark pending
        with conn.cursor() as cur:
            cur.execute("UPDATE nc_files SET status_sublevel='pending', last_attempt_sublevel = NOW() WHERE id=%s", (nid,))
        conn.commit()

        if dry_run:
            logger.info("dry-run: would create sublevel for %s", src)
            return True

        ds = xr.open_dataset(src)
        # pick depth dimension
        depth_dim = None
        for d in ds.dims:
            if d.lower().startswith("depth") or d.lower() in ("lev", "k"):
                depth_dim = d
                break
        if depth_dim is None:
            # try common name
            if "depth" in ds.dims:
                depth_dim = "depth"
        if depth_dim is None:
            raise RuntimeError("No depth-like dimension found in dataset")

        # ensure indices valid
        max_idx = ds.dims[depth_dim] - 1
        sel_indices = [i for i in depth_indices if i >= 0 and i <= max_idx]
        if not sel_indices:
            raise RuntimeError("No valid depth indices found for dataset")

        sub = ds.isel({depth_dim: sel_indices})

        nc_root = os.getenv("NC_ROOT", "/opt/data/nc")
        out_dir = os.path.join(nc_root, "sublevels", variable)
        os.makedirs(out_dir, exist_ok=True)
        out_fn = f"{variable}_{row['start_time'].strftime('%Y%m%dT%H%M')}_{row['end_time'].strftime('%Y%m%dT%H%M')}.sub.nc"
        out_path = os.path.join(out_dir, out_fn)

        _write_sublevel_ds(sub, out_path, compression=compression)
        checksum = _sha256_file(out_path)
        size = os.path.getsize(out_path)

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE nc_files SET status_sublevel='success', filename_sublevel=%s, file_path_sublevel=%s, file_size_sublevel=%s, checksum_sublevel=%s WHERE id=%s",
                (out_fn, out_path, size, checksum, nid),
            )
            # set status_png pending to trigger png worker
            cur.execute(
                "UPDATE nc_files SET status_png='pending' WHERE id=%s",
                (nid,),
            )
        conn.commit()
        logger.info("Created sublevel file %s", out_path)
        return True
    except Exception as e:
        logger.exception("Sublevel processing failed for %s", src)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE nc_files SET status_sublevel='failed', last_error_sublevel=%s, attempts_sublevel=attempts_sublevel+1, last_attempt_sublevel=NOW() WHERE id=%s",
                (str(e), nid),
            )
        conn.commit()
        return False
    finally:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (nid,))
        conn.commit()


def process_pending_sublevels(conn, depth_indices: Optional[List[int]] = None, limit: int = 5, dry_run: bool = False):
    pending = find_pending_sublevels(conn, limit=limit)
    if not pending:
        logger.info("No pending sublevels")
        return
    for row in pending:
        process_sublevel(conn, row, depth_indices=depth_indices, dry_run=dry_run)
