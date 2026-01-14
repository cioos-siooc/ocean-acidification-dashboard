"""PNG worker: runs nc2tile on sublevel NetCDF files and updates nc_files.status_png"""
import logging
import os
import traceback
from typing import Optional

from .db import get_db_conn
from .sublevel import find_pending_sublevels
import nc2tile

logger = logging.getLogger("dl2.png")


def find_pending_png(conn, limit: int = 5):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, dataset_id, filename_sublevel, file_path_sublevel, variable, start_time, end_time, attempts_png, status_png FROM nc_files WHERE status_sublevel='success' AND (status_png IS NULL OR status_png='pending' OR status_png='failed') ORDER BY start_time LIMIT %s",
            (limit,),
        )
        rows = cur.fetchall()
        results = []
        for r in rows:
            results.append({
                'id': r[0], 'dataset_id': r[1], 'filename_sublevel': r[2], 'file_path_sublevel': r[3], 'variable': r[4], 'start_time': r[5], 'end_time': r[6], 'attempts_png': r[7], 'status_png': r[8]
            })
        return results


def process_png(conn, row, dry_run: bool = False, workers: int | None = None, simulate: bool = False):
    nid = row['id']
    src = row['file_path_sublevel']
    variable = row['variable']

    if not src or not os.path.exists(src):
        with conn.cursor() as cur:
            cur.execute("UPDATE nc_files SET status_png='failed', last_error_png=%s, attempts_png=attempts_png+1, last_attempt_png=NOW() WHERE id=%s", (f"sublevel missing: {src}", nid))
        conn.commit()
        return False

    # try to acquire lock
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (nid,))
        locked = cur.fetchone()[0]
    if not locked:
        logger.info("Skipping png id %s: lock not acquired", nid)
        return False

    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE nc_files SET status_png='pending', last_attempt_png=NOW() WHERE id=%s", (nid,))
        conn.commit()

        if dry_run:
            logger.info("dry-run: would call nc2tile on %s", src)
            return True

        # call nc2tile programmatically, optionally passing --workers and --simulate
        args = ["--data", src, "--vars", variable]
        if workers is not None:
            args.extend(["--workers", str(int(workers))])
        if simulate:
            args.append("--simulate")
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
            cur.execute("UPDATE nc_files SET status_png='success' WHERE id=%s", (nid,))
        conn.commit()

        # If nc2tile returned processed datetimes, update erddap_variables.available_datetimes (timestamptz[] preferred)
        try:
            from datetime import datetime, timezone
            if processed_times:
                def to_aware(x):
                    # normalize strings or datetimes to timezone-aware UTC datetimes
                    if isinstance(x, str):
                        s = x.replace('Z', '+00:00') if x.endswith('Z') else x
                        try:
                            d = datetime.fromisoformat(s)
                        except Exception:
                            return None
                    elif isinstance(x, datetime):
                        d = x
                    else:
                        return None
                    if d.tzinfo is None:
                        # assume UTC for naive datetimes
                        d = d.replace(tzinfo=timezone.utc)
                    else:
                        d = d.astimezone(timezone.utc)
                    return d

                parsed = [to_aware(t) for t in processed_times]
                parsed = [p for p in parsed if p is not None]

                with conn.cursor() as cur:
                    cur.execute("SELECT available_datetimes FROM erddap_variables WHERE dataset_id=%s AND variable=%s FOR UPDATE", (row['dataset_id'], variable))
                    r = cur.fetchone()
                    existing = r[0] if r else None
                    if existing is None:
                        arr = parsed
                    else:
                        try:
                            existing_list = list(existing)
                        except Exception:
                            existing_list = []
                        existing_norm = [to_aware(e) for e in existing_list]
                        existing_norm = [e for e in existing_norm if e is not None]
                        seen = {e.isoformat(): e for e in existing_norm}
                        for p in parsed:
                            if p.isoformat() not in seen:
                                seen[p.isoformat()] = p
                        # sort by absolute time (UTC)
                        arr = sorted(seen.values(), key=lambda d: d.timestamp())
                    cur.execute("UPDATE erddap_variables SET available_datetimes=%s WHERE dataset_id=%s AND variable=%s", (arr, row['dataset_id'], variable))
                conn.commit()
            else:
                # fallback behaviour: append the row start_time if no list returned
                iso_dt = row['start_time']
                if iso_dt and not isinstance(iso_dt, (str, bytes)):
                    with conn.cursor() as cur:
                        cur.execute("SELECT available_datetimes FROM erddap_variables WHERE dataset_id=%s AND variable=%s FOR UPDATE", (row['dataset_id'], variable))
                        r = cur.fetchone()
                        existing = r[0] if r else None
                        if existing is None:
                            arr = [iso_dt]
                        else:
                            try:
                                existing_list = list(existing)
                            except Exception:
                                existing_list = []
                            seen = {e.isoformat() if hasattr(e, 'isoformat') else str(e): e for e in existing_list}
                            if iso_dt.isoformat() not in seen:
                                seen[iso_dt.isoformat()] = iso_dt
                            arr = sorted(seen.values())
                        cur.execute("UPDATE erddap_variables SET available_datetimes=%s WHERE dataset_id=%s AND variable=%s", (arr, row['dataset_id'], variable))
                    conn.commit()
        except Exception:
            logger.exception("Failed to update erddap_variables.available_datetimes")

        logger.info("nc2tile processed %s", src)
        return True
    except Exception as e:
        logger.exception("PNG generation failed for %s", src)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE nc_files SET status_png='failed', last_error_png=%s, attempts_png=attempts_png+1, last_attempt_png=NOW() WHERE id=%s",
                (str(e), nid),
            )
        conn.commit()
        return False
    finally:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (nid,))
        conn.commit()


def process_pending_png(conn, limit: int = 5, dry_run: bool = False, workers: int | None = None, simulate: bool = False):
    pending = find_pending_png(conn, limit=limit)
    if not pending:
        logger.info("No pending png jobs")
        return
    for row in pending:
        process_png(conn, row, dry_run=dry_run, workers=workers, simulate=simulate)
