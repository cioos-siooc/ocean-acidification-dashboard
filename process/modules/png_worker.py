"""PNG worker: runs nc2tile on NetCDF files and updates nc_jobs status."""
import logging
import os
import traceback
import time
from typing import Optional
import numpy as np
import xarray as xr
from psycopg2.errors import DeadlockDetected

from .db import get_db_conn

logger = logging.getLogger("dl2.png")


def _update_nc_job_status_with_retry(conn, row_id: int, status: str, update_last_attempt: bool = False, max_retries: int = 3):
    """Update nc_jobs status with retry logic for deadlock handling.
    
    Args:
        conn: Database connection
        row_id: nc_jobs id to update
        status: New status value
        update_last_attempt: If True, also set last_attempt to NOW()
        max_retries: Number of times to retry on deadlock
    """
    for attempt in range(max_retries):
        try:
            with conn.cursor() as cur:
                if update_last_attempt:
                    cur.execute(
                        "UPDATE nc_jobs SET status=%s, last_attempt=NOW() WHERE id=%s",
                        (status, row_id)
                    )
                else:
                    cur.execute(
                        "UPDATE nc_jobs SET status=%s WHERE id=%s",
                        (status, row_id)
                    )
            conn.commit()
            return
        except DeadlockDetected as e:
            conn.rollback()  # Rollback the failed transaction
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * 0.1  # Exponential backoff: 0.1s, 0.2s, 0.4s
                logger.warning(f"Deadlock updating nc_jobs id {row_id}, retrying in {wait_time}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to update nc_jobs id {row_id} after {max_retries} attempts: {e}")
                raise


def _update_nc_job_with_retry(conn, row_id: int, status: str, increment_attempts: bool = False, max_retries: int = 3):
    """Update nc_jobs status with optional attempt increment and last_attempt timestamp, with retry logic.
    
    Args:
        conn: Database connection
        row_id: nc_jobs id to update
        status: New status value
        increment_attempts: If True, increment attempts and set last_attempt to NOW()
        max_retries: Number of times to retry on deadlock
    """
    for attempt in range(max_retries):
        try:
            with conn.cursor() as cur:
                if increment_attempts:
                    cur.execute(
                        "UPDATE nc_jobs SET status=%s, attempts=attempts+1, last_attempt=NOW() WHERE id=%s",
                        (status, row_id)
                    )
                else:
                    cur.execute(
                        "UPDATE nc_jobs SET status=%s WHERE id=%s",
                        (status, row_id)
                    )
            conn.commit()
            return
        except DeadlockDetected as e:
            conn.rollback()  # Rollback the failed transaction
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * 0.1  # Exponential backoff: 0.1s, 0.2s, 0.4s
                logger.warning(f"Deadlock updating nc_jobs id {row_id}, retrying in {wait_time}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to update nc_jobs id {row_id} after {max_retries} attempts: {e}")
                raise


def _promote_ready_groups_to_pending_image(conn, limit: int = 5):
    """Find (dataset_id, start_time, end_time) groups where ALL rows are in success_bottom,
    and promote rows in those groups to pending_image. Limit number of groups processed per call."""
    with conn.cursor() as cur:
        # Aggregate per group and pick groups where count == count_ok
        cur.execute(
            """
            WITH groups AS (
                SELECT dataset_id, start_time, end_time, COUNT(*) AS total,
                    SUM(CASE WHEN status = 'success_bottom' THEN 1 ELSE 0 END) AS ok
                FROM nc_jobs
                GROUP BY dataset_id, start_time, end_time
                HAVING COUNT(*) > 0 AND SUM(CASE WHEN status = 'success_bottom' THEN 1 ELSE 0 END) = COUNT(*)
                ORDER BY start_time
                LIMIT %s
            )
            UPDATE nc_jobs f
            SET status = 'pending_image', last_attempt = NULL, attempts = 0
            FROM groups g
            WHERE f.dataset_id = g.dataset_id AND f.start_time = g.start_time AND f.end_time = g.end_time AND f.status = 'success_bottom'
            RETURNING f.id
            """,
            (limit,),
        )
        promoted = cur.fetchall()
        conn.commit()
        return [r[0] for r in promoted]


def check_image_ready_rows(conn):
    """Check for rows that are ready for PNG generation.

    For each (start_time, end_time), all required variables (both download and
    compute types) must be in ``success_bottom`` status.  Only then are all rows
    for that period promoted to ``pending_image``.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, type FROM fields
            WHERE type IN ('download', 'compute')
            ORDER BY id
            """
        )
        required_vars = [(row[0], row[1]) for row in cur.fetchall()]

        if not required_vars:
            logger.warning("No required variables (type='download' or type='compute') found in fields")
            return

        logger.debug(f"Checking image readiness against {len(required_vars)} required variables")

        cur.execute(
            """
            SELECT DISTINCT start_time, end_time
            FROM nc_jobs
            ORDER BY start_time DESC, end_time DESC
            """
        )
        time_periods = cur.fetchall()

        for st_dt, end_dt in time_periods:
            all_required_ready = True
            for var_id, var_type in required_vars:
                cur.execute(
                    """
                    SELECT COUNT(*) FROM nc_jobs
                    WHERE variable_id = %s AND start_time = %s AND end_time = %s
                    AND status = 'success_bottom'
                    """,
                    (var_id, st_dt, end_dt)
                )
                if cur.fetchone()[0] == 0:
                    all_required_ready = False
                    logger.debug(f"Variable_id {var_id} (type={var_type}) not at success_bottom for period {st_dt} to {end_dt}")
                    break

            if all_required_ready:
                cur.execute(
                    """
                    UPDATE nc_jobs
                    SET status = 'pending_image', last_attempt = NULL, attempts = 0
                    WHERE start_time = %s AND end_time = %s AND status = 'success_bottom'
                    """,
                    (st_dt, end_dt)
                )
                rows_updated = cur.rowcount
                if rows_updated > 0:
                    logger.info(f"Marked {rows_updated} rows as pending_image for period {st_dt} to {end_dt}")

        conn.commit()

def find_pending_image(conn):
    """Find pending image jobs."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT j.id, j.variable_id, j.start_time, j.end_time, j.nc_path, j.checksum
            FROM nc_jobs j
            JOIN fields v ON j.variable_id = v.id
            WHERE j.status = 'pending_image'
            ORDER BY j.start_time
            """,
        )
        results = cur.fetchall()
        rows = []
        for r in results:
            rows.append({
                'row_id': r[0],
                'variable_id': r[1],
                'start_time': r[2],
                'end_time': r[3],
                'nc_path': r[4],
                'checksum': r[5],
            })
        return rows
        

def get_variable_from_id(conn, variable_id: int) -> str:
    """Get variable name from fields.id."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT variable FROM fields WHERE id=%s",
            (variable_id,),
        )
        r = cur.fetchone()
        if r:
            return r[0]
    raise RuntimeError(f"Variable not found for id={variable_id}")


def get_variable_precision(conn, variable_id: int) -> float:
    """Get the precision setting for a variable from fields.precision."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT precision FROM fields WHERE id=%s",
            (variable_id,),
        )
        r = cur.fetchone()
        if r and r[0] is not None:
            try:
                prec = float(r[0])
                return prec
            except Exception:
                pass
    raise RuntimeError(f"Invalid precision for variable_id={variable_id}")

def get_variable_depths_image(conn, variable_id: int) -> Optional[list]:
    """Get the depth values that should be rendered as PNGs for a variable.

    Reads from ``datasets.depths`` (a JSONB array of ``{value, hasImage}`` objects)
    and returns only the values where ``hasImage`` is true.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT d.depths
            FROM fields f
            JOIN datasets d ON d.id = f.dataset_id
            WHERE f.id = %s
            """,
            (variable_id,),
        )
        r = cur.fetchone()
        if r and r[0] is not None:
            try:
                depths_arr = r[0]  # already a Python list of dicts via psycopg2 JSONB
                if isinstance(depths_arr, list):
                    values = [
                        float(entry.get("value") or entry.get("depth"))
                        for entry in depths_arr
                        if entry.get("hasImage", True)
                           and (entry.get("value") is not None or entry.get("depth") is not None)
                    ]
                    if values:
                        return values
            except Exception:
                pass
    raise RuntimeError(f"No depths with hasImage=true found for variable_id={variable_id}")


def get_depth_indices_from_values(nc_path: str, desired_depths: list) -> list:
    """Convert depth values to their indices in the NetCDF file's depth coordinate.
    
    Args:
        nc_path: Path to the NetCDF file
        desired_depths: List of depth values to find indices for
    
    Returns:
        List of depth indices corresponding to the desired depth values
    """
    try:
        with xr.open_dataset(nc_path) as ds:
            # Try to find depth coordinate (common names: depth, z, level, etc.)
            depth_coord = None
            for coord_name in ['depth', 'z', 'level', 'altitude']:
                if coord_name in ds.coords:
                    depth_coord = ds.coords[coord_name].values
                    break
            
            if depth_coord is None:
                raise ValueError(f"Could not find depth coordinate in {nc_path}")
            
            indices = []
            for desired_depth in desired_depths:
                # Find the index of the closest depth value
                idx = int(np.argmin(np.abs(depth_coord - desired_depth)))
                indices.append(idx)
            
            logger.info("Mapped depths %s to indices %s", desired_depths, indices)
            return indices
    except Exception as e:
        logger.error("Error converting depth values to indices: %s", e)
        raise

def process_image(conn, row, workers: int | None = None):
    row_id = row['row_id']
    # Prefer the canonical path column `nc_path` in the new table, but accept legacy keys
    src = row.get('nc_path')
    variable_id = row['variable_id']
    variable = get_variable_from_id(conn, variable_id)

    if not src or not os.path.exists(src):
        try:
            _update_nc_job_with_retry(conn, row_id, 'failed_image', increment_attempts=True)
        except Exception as e:
            logger.error(f"Failed to update status to failed_image for missing file: {e}")
        return False

    # try to acquire lock
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (row_id,))
        try:
            r = cur.fetchone()
            locked = r[0] if r else False
        except StopIteration:
            locked = False
    if not locked:
        logger.info("Skipping png id %s: lock not acquired", row_id)
        return False

    try:
        try:
            _update_nc_job_status_with_retry(conn, row_id, 'imaging', update_last_attempt=True)
        except Exception as e:
            logger.warning(f"Failed to update status to imaging: {e}")
        
        precision = get_variable_precision(conn, variable_id)
        
        # Get depths from database — depth value -1 is a sentinel meaning "bottom layer file"
        desired_depths = get_variable_depths_image(conn, variable_id)
        has_bottom = -1.0 in desired_depths
        real_depths = [d for d in desired_depths if d != -1.0]

        if real_depths:
            depth_indices = get_depth_indices_from_values(src, real_depths)
            args = ["--data", src, "--vars", variable, "--precision", str(precision), "--depth-indices", ','.join(str(i) for i in depth_indices)]
            if workers is not None:
                args.extend(["--workers", str(int(workers))])
            processed_times = None
            try:
                import nc2tile  # lazy — not available outside the process container
                processed_times = nc2tile.main(args)
            except SystemExit as e:
                if isinstance(e.code, list):
                    processed_times = e.code
                elif e.code != 0:
                    raise

        # Process companion bottom-layer file if depths include -1 sentinel or file exists alongside the main NC.
        if has_bottom:
            try:
                from .bottom_layer_worker import _get_bottom_nc_path
                bottom_src = _get_bottom_nc_path(src)
                if os.path.exists(bottom_src):
                    bottom_args = [
                        "--data", bottom_src,
                        "--vars", variable,        # same var name → output goes to png/{variable}/{t}/
                        "--precision", str(precision),
                        "--depth-indices", "0",    # depth coord in bottom file is -1.0 → writes bottom.webp
                    ]
                    if workers is not None:
                        bottom_args.extend(["--workers", str(int(workers))])
                    try:
                        import nc2tile
                        nc2tile.main(bottom_args)
                    except SystemExit as e:
                        if not isinstance(e.code, list) and e.code != 0:
                            raise
                    logger.info("nc2tile processed bottom companion: %s", bottom_src)
                else:
                    logger.warning("Bottom sentinel in depths but companion file not found: %s", bottom_src)
            except Exception:
                logger.warning("Bottom companion processing failed for %s (non-fatal)", src, exc_info=True)

        # Use retry logic for the final status update to handle potential deadlocks
        _update_nc_job_status_with_retry(conn, row_id, 'success_image')

        logger.info("nc2tile processed %s", src)
        return True
    except Exception as e:
        logger.exception("PNG generation failed for %s", src)
        try:
            _update_nc_job_with_retry(conn, row_id, 'failed_image', increment_attempts=True)
        except Exception as retry_error:
            logger.error(f"Failed to update status to failed_image: {retry_error}")
        return False
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (row_id,))
            conn.commit()
        except Exception as e:
            logger.warning(f"Failed to release advisory lock for row {row_id}: {e}")
            conn.rollback()


def process_pending_png(conn, limit: int = 5, workers: int | None = None):
    pendings = find_pending_image(conn)
    if not pendings:
        logger.info("No pending image jobs")
        return
    for row in pendings:
        process_image(conn, row, workers=workers)