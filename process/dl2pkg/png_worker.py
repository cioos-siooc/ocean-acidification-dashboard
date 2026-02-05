"""PNG worker: runs nc2tile on NetCDF files and updates nc_jobs status."""
import logging
import os
import traceback
from typing import Optional

from .db import get_db_conn
import nc2tile

logger = logging.getLogger("dl2.png")


def _promote_ready_groups_to_pending_image(conn, limit: int = 5):
    """Find (dataset_id, start_time, end_time) groups where ALL rows are in success_download or success_compute,
    and promote rows in those groups to pending_image. Limit number of groups processed per call."""
    with conn.cursor() as cur:
        # Aggregate per group and pick groups where count == count_ok
        cur.execute(
            """
            WITH groups AS (
                SELECT dataset_id, start_time, end_time, COUNT(*) AS total,
                    SUM(CASE WHEN status IN ('success_download','success_compute') THEN 1 ELSE 0 END) AS ok
                FROM nc_jobs
                GROUP BY dataset_id, start_time, end_time
                HAVING COUNT(*) > 0 AND SUM(CASE WHEN status IN ('success_download','success_compute') THEN 1 ELSE 0 END) = COUNT(*)
                ORDER BY start_time
                LIMIT %s
            )
            UPDATE nc_jobs f
            SET status = 'pending_image', last_attempt = NULL, attempts = 0
            FROM groups g
            WHERE f.dataset_id = g.dataset_id AND f.start_time = g.start_time AND f.end_time = g.end_time AND f.status IN ('success_download','success_compute')
            RETURNING f.id
            """,
            (limit,),
        )
        promoted = cur.fetchall()
        conn.commit()
        return [r[0] for r in promoted]


def check_image_ready_rows(conn):
    """Check for rows that are ready for PNG generation (all downloads/computes successful)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT start_time, end_time
            FROM nc_jobs
            GROUP BY start_time, end_time
            HAVING COUNT(*) > 0 AND SUM(CASE WHEN status IN ('success_download','success_compute') THEN 1 ELSE 0 END) = COUNT(*)
            """,
        )
        rows = cur.fetchall()

        for r in rows:
            st_dt, end_dt = r
            # Update nc_jobs rows in this group to pending_image for rows where start_time/end_time match and status is success_download or success_compute
            cur.execute(
                """
                UPDATE nc_jobs
                SET status = 'pending_image', last_attempt = NULL, attempts = 0
                WHERE start_time = %s AND end_time = %s AND status IN ('success_download','success_compute')
                """,
                (st_dt, end_dt),
            )
        conn.commit()

def find_pending_image(conn):
    """Find pending image jobs."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT j.id, j.variable_id, j.start_time, j.end_time, j.nc_path, j.checksum
            FROM nc_jobs j
            JOIN erddap_variables v ON j.variable_id = v.id
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
    """Get variable name from erddap_variables.id."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT variable FROM erddap_variables WHERE id=%s",
            (variable_id,),
        )
        r = cur.fetchone()
        if r:
            return r[0]
    raise RuntimeError(f"Variable not found for id={variable_id}")


def get_variable_precision(conn, variable_id: int) -> float:
    """Get the precision setting for a variable from erddap_variables.precision."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT precision FROM erddap_variables WHERE id=%s",
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
    """Get the depths setting for PNG generation from erddap_variables.depths_image."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT depths_image FROM erddap_variables WHERE id=%s",
            (variable_id,),
        )
        r = cur.fetchone()
        if r and r[0] is not None:
            try:
                depths = r[0]
                if isinstance(depths, list):
                    return depths
                elif isinstance(depths, str):
                    # try to parse as comma-separated values
                    parts = [d.strip() for d in depths.split(',')]
                    depth_vals = []
                    for p in parts:
                        try:
                            depth_vals.append(float(p))
                        except Exception:
                            pass
                    if depth_vals:
                        return depth_vals
            except Exception:
                pass
    raise RuntimeError(f"Invalid depths_image for variable_id={variable_id}")

def process_image(conn, row, workers: int | None = None):
    row_id = row['row_id']
    # Prefer the canonical path column `nc_path` in the new table, but accept legacy keys
    src = row.get('nc_path')
    variable_id = row['variable_id']
    variable = get_variable_from_id(conn, variable_id)

    if not src or not os.path.exists(src):
        with conn.cursor() as cur:
            cur.execute("UPDATE nc_jobs SET status='failed_image', attempts=attempts+1, last_attempt=NOW() WHERE id=%s", (row_id,))
        conn.commit()
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
        with conn.cursor() as cur:
            cur.execute("UPDATE nc_jobs SET status='imaging', last_attempt=NOW() WHERE id=%s", (row_id,))
        conn.commit()
        precision = get_variable_precision(conn, variable_id)
        # depths_image = get_variable_depths_image(conn, variable_id)
        depth_indices = [0,5,20,24,26,30,34,38]

        # call nc2tile programmatically, optionally passing --workers
        args = ["--data", src, "--vars", variable, "--precision", str(precision), "--depth-indices", ','.join(str(i) for i in depth_indices)]
        if workers is not None:
            args.extend(["--workers", str(int(workers))])
        processed_times = None
        try:
            processed_times = nc2tile.main(args)
        except SystemExit as e:
            # nc2tile may call SystemExit when used as CLI; if it included a list of processed times as the exit code, use that
            if isinstance(e.code, list):
                processed_times = e.code
            elif e.code != 0:
                raise
        with conn.cursor() as cur:
            cur.execute("UPDATE nc_jobs SET status='success_image' WHERE id=%s", (row_id,))
        conn.commit()

        # If nc2tile returned processed datetimes, update erddap_variables.available_datetimes (timestamptz[] preferred)
        # try:
        #     from datetime import datetime, timezone
        #     if processed_times:
        #         def to_aware(x):
        #             # normalize strings or datetimes to timezone-aware UTC datetimes
        #             if isinstance(x, str):
        #                 s = x.replace('Z', '+00:00') if x.endswith('Z') else x
        #                 try:
        #                     d = datetime.fromisoformat(s)
        #                 except Exception:
        #                     return None
        #             elif isinstance(x, datetime):
        #                 d = x
        #             else:
        #                 return None
        #             if d.tzinfo is None:
        #                 # assume UTC for naive datetimes
        #                 d = d.replace(tzinfo=timezone.utc)
        #             else:
        #                 d = d.astimezone(timezone.utc)
        #             return d

        #         parsed = [to_aware(t) for t in processed_times]
        #         parsed = [p for p in parsed if p is not None]

        #         with conn.cursor() as cur:
        #             # Table may have per-dataset variable rows; match by dataset_id and variable
        #             cur.execute("SELECT available_datetimes FROM erddap_variables WHERE dataset_id=%s AND variable=%s FOR UPDATE", (dataset_id, variable))
        #             r = cur.fetchone()
        #             existing = r[0] if r else None
        #             if existing is None:
        #                 arr = parsed
        #             else:
        #                 try:
        #                     existing_list = list(existing)
        #                 except Exception:
        #                     existing_list = []
        #                 existing_norm = [to_aware(e) for e in existing_list]
        #                 existing_norm = [e for e in existing_norm if e is not None]
        #                 seen = {e.isoformat(): e for e in existing_norm}
        #                 for p in parsed:
        #                     if p.isoformat() not in seen:
        #                         seen[p.isoformat()] = p
        #                 # sort by absolute time (UTC)
        #                 arr = sorted(seen.values(), key=lambda d: d.timestamp())
        #             cur.execute("UPDATE erddap_variables SET available_datetimes=%s WHERE dataset_id=%s AND variable=%s", (arr, dataset_id, variable,))
        #         conn.commit()
        #     else:
        #         # fallback behaviour: append the row start_time if no list returned
        #         iso_dt = row['start_time']
        #         if iso_dt and not isinstance(iso_dt, (str, bytes)):
        #             with conn.cursor() as cur:
        #                 cur.execute("SELECT available_datetimes FROM erddap_variables WHERE variable=%s FOR UPDATE", (variable,))
        #                 r = cur.fetchone()
        #                 existing = r[0] if r else None
        #                 if existing is None:
        #                     arr = [iso_dt]
        #                 else:
        #                     try:
        #                         existing_list = list(existing)
        #                     except Exception:
        #                         existing_list = []
        #                     seen = {e.isoformat() if hasattr(e, 'isoformat') else str(e): e for e in existing_list}
        #                     if iso_dt.isoformat() not in seen:
        #                         seen[iso_dt.isoformat()] = iso_dt
        #                     arr = sorted(seen.values())
        #                 cur.execute("UPDATE erddap_variables SET available_datetimes=%s WHERE variable=%s", (arr, variable,))
        #             conn.commit()
        # except Exception:
        #     logger.exception("Failed to update erddap_variables.available_datetimes")

        logger.info("nc2tile processed %s", src)
        return True
    except Exception as e:
        logger.exception("PNG generation failed for %s", src)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE nc_jobs SET status='failed_image', attempts=attempts+1, last_attempt=NOW() WHERE id=%s",
                (str(e), row_id,),
            )
        conn.commit()
        return False
    finally:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (row_id,))
        conn.commit()


def process_pending_png(conn, limit: int = 5, workers: int | None = None):
    pendings = find_pending_image(conn)
    if not pendings:
        logger.info("No pending image jobs")
        return
    for row in pendings:
        process_image(conn, row, workers=workers)