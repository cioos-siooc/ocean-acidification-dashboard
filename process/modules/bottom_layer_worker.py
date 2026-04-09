"""Bottom-layer worker: extracts the deepest non-NaN value from 4-D (time, depth, lat, lon)
NetCDF files and writes a companion bottom-layer file alongside the original.

Pipeline position:  download → **bottom_layer** → compute → image

Status transitions on the original download row:
    success_download  →  bottoming  →  success_bottom  (or failed_bottom)

The bottom NC file is a companion to the original (same nc_jobs row).  The image worker
automatically detects and processes it when it processes the main row.
"""

from __future__ import annotations

import logging
import os

import numpy as np
import xarray as xr

from .bottomLayer import extract_bottom_layer
from .db import get_db_conn

logger = logging.getLogger("dl2.bottom_layer")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_bottom_nc_path(nc_path: str) -> str:
    """Derive the bottom-layer companion NC path from a source 4-D NC path.

    Uses the directory name as the variable name so the companion sits in the
    same folder with the pattern ``{var}_{YYYYMMDD}_bottom.nc``.

    Example::

        /opt/data/nc/temperature/temperature_20260117.nc
        →  /opt/data/nc/temperature/temperature_20260117_bottom.nc
    """
    dirname = os.path.dirname(nc_path)
    basename, ext = os.path.splitext(os.path.basename(nc_path))
    variable = os.path.basename(dirname)
    if basename.startswith(variable + "_"):
        date_suffix = basename[len(variable) + 1:]
        new_basename = f"{variable}_{date_suffix}_bottom"
    else:
        new_basename = f"{basename}_bottom"
    return os.path.join(dirname, new_basename + ext)


def _write_bottom_nc(src_path: str, out_path: str, base_var: str) -> None:
    """Read the 4-D source NC, extract the bottom layer for every time step, and
    write a 4-D (time, depth=1, lat, lon) NetCDF file with depth coordinate = [-1.0].

    The data variable in the output file is stored under ``base_var`` (same name as the
    source) so nc2tile can look it up by the canonical variable name.
    """
    with xr.open_dataset(src_path) as ds:
        var_data = ds[base_var]
        dims = list(var_data.dims)

        time_dim = next(
            (d for d in dims if "time" in str(d).lower()), None
        )
        depth_dim = next(
            (d for d in dims if any(k in str(d).lower() for k in ("depth", "lev", "z", "deptht"))),
            None,
        )
        if time_dim is None:
            raise ValueError(f"No time dimension found in {src_path}")
        if depth_dim is None:
            raise ValueError(f"No depth dimension found in {src_path}")

        spatial_dims = [d for d in dims if d not in (time_dim, depth_dim)]
        n_times = ds.sizes[time_dim]

        frames = []
        for t_idx in range(n_times):
            data_3d = var_data.isel({time_dim: t_idx}).values  # (depth, lat, lon)
            bottom_2d = extract_bottom_layer(data_3d)           # (lat, lon)
            frames.append(bottom_2d)

        bottom_arr = np.stack(frames, axis=0)           # (time, lat, lon)
        bottom_arr = bottom_arr[:, np.newaxis, :, :]    # (time, 1, lat, lon)

        coords = {time_dim: ds[time_dim], depth_dim: np.array([-1.0], dtype=float)}
        for dim in spatial_dims:
            if dim in ds.coords:
                coords[dim] = ds[dim]

        bottom_var = base_var  # keep the same variable name so nc2tile can find it by the canonical name
        da = xr.DataArray(
            bottom_arr,
            dims=[time_dim, depth_dim] + spatial_dims,
            coords=coords,
            name=bottom_var,
            attrs={**var_data.attrs, "long_name": f"Bottom layer {base_var}"},
        )
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        encoding = {bottom_var: {"zlib": True, "complevel": 4}}
        da.to_dataset().to_netcdf(out_path, encoding=encoding)

    logger.info("Wrote bottom NC: %s", out_path)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _enqueue_ready_groups(conn, limit: int = 10) -> int:
    """Find complete groups and atomically mark all their rows as ``pending_bottom``.

    A group is ready when:
    - Every download-type variable is ``success_download``
    - Every compute-type variable is ``success_compute``

    Returns the number of groups enqueued.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT j.start_time, j.end_time
            FROM nc_jobs j
            WHERE j.status IN ('success_download', 'success_compute')
            AND EXISTS (
                SELECT 1 FROM nc_jobs j2
                JOIN fields v2 ON j2.variable_id = v2.id
                WHERE j2.start_time = j.start_time AND j2.end_time = j.end_time
                  AND v2.type = 'download' AND j2.status = 'success_download'
                GROUP BY j2.start_time, j2.end_time
                HAVING COUNT(DISTINCT v2.variable) = (SELECT COUNT(*) FROM fields WHERE type='download')
            )
            AND EXISTS (
                SELECT 1 FROM nc_jobs j3
                JOIN fields v3 ON j3.variable_id = v3.id
                WHERE j3.start_time = j.start_time AND j3.end_time = j.end_time
                  AND v3.type = 'compute' AND j3.status = 'success_compute'
                GROUP BY j3.start_time, j3.end_time
                HAVING COUNT(DISTINCT v3.variable) = (SELECT COUNT(*) FROM fields WHERE type='compute')
            )
            ORDER BY j.start_time
            LIMIT %s
            """,
            (limit,),
        )
        groups = cur.fetchall()

    for start_time, end_time in groups:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE nc_jobs SET status = 'pending_bottom'
                WHERE start_time = %s AND end_time = %s
                  AND status IN ('success_download', 'success_compute')
                """,
                (start_time, end_time),
            )
        conn.commit()
        logger.info("Enqueued %s→%s for bottom-layer extraction", start_time, end_time)

    return len(groups)


def find_pending_bottom(conn) -> list[dict]:
    """Return all rows currently at ``pending_bottom`` status."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT j.id, j.dataset_id, j.variable_id,
                   j.start_time, j.end_time, j.nc_path,
                   v.variable
            FROM nc_jobs j
            JOIN fields v ON j.variable_id = v.id
            WHERE j.status = 'pending_bottom'
            ORDER BY j.start_time, v.type, v.variable
            """,
        )
        rows = cur.fetchall()
    return [
        {
            "row_id": r[0],
            "ds_id": r[1],
            "variable_id": r[2],
            "start_time": r[3],
            "end_time": r[4],
            "nc_path": r[5],
            "variable": r[6],
        }
        for r in rows
    ]


def process_bottom(conn, row: dict, base_dir: str | None = None) -> bool:
    """Extract and persist the bottom layer for a single nc_jobs row."""
    row_id = row["row_id"]
    nc_path = row["nc_path"]
    variable = row["variable"]
    ds_id = row["ds_id"]
    variable_id = row["variable_id"]
    start_time = row["start_time"]
    end_time = row["end_time"]

    if not nc_path or not os.path.exists(nc_path):
        logger.error("Source NC missing for row %s: %s", row_id, nc_path)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE nc_jobs SET status='failed_bottom', attempts=attempts+1, last_attempt=NOW() WHERE id=%s",
                (row_id,),
            )
        conn.commit()
        return False

    bottom_path = _get_bottom_nc_path(nc_path)

    # Mark as in-progress
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE nc_jobs SET status='bottoming', attempts=attempts+1, last_attempt=NOW() WHERE id=%s",
            (row_id,),
        )
    conn.commit()

    try:
        if not os.path.exists(bottom_path):
            _write_bottom_nc(nc_path, bottom_path, variable)
        else:
            logger.info("Bottom NC already exists, skipping write: %s", bottom_path)

        # Mark original row as success — no separate nc_jobs row needed.
        # The image worker detects and processes the companion file automatically.
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE nc_jobs SET status='success_bottom' WHERE id=%s",
                (row_id,),
            )
        conn.commit()
        logger.info("Bottom layer success: %s %s→%s", variable, start_time, end_time)
        return True

    except Exception:
        logger.exception("Bottom layer failed for row %s (%s)", row_id, nc_path)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE nc_jobs SET status='failed_bottom', attempts=attempts+1, last_attempt=NOW() WHERE id=%s",
                (row_id,),
            )
        conn.commit()
        return False


def process_pending_bottom(conn=None, limit: int = 10, base_dir: str | None = None) -> None:
    """Process all pending bottom-layer jobs.

    Phase 1: Find complete groups and atomically set all their rows to ``pending_bottom``.
    Phase 2: Process each ``pending_bottom`` row one by one.
    """
    if conn is None:
        conn = get_db_conn()

    enqueued = _enqueue_ready_groups(conn, limit=limit)
    if enqueued:
        logger.info("Enqueued %d group(s) for bottom-layer extraction", enqueued)

    rows = find_pending_bottom(conn)
    if not rows:
        logger.info("No pending bottom layer jobs")
        return
    logger.info("Processing %d pending_bottom row(s)", len(rows))
    for row in rows:
        process_bottom(conn, row, base_dir=base_dir)
