"""Compute worker: picks up nc_jobs rows with status='pending_compute', runs carbonate computation,
and updates nc_jobs rows for computed variables to status='success_compute'.

Minimal approach implemented:
- Find pending_compute rows
- For each, set status='computing'
- Invoke `process/calc_carbon_grid_shm_memmap.py --date YYYYMMDD --mode memmap --workers N --overwrite`
- Determine expected output filenames and verify they exist
- Update computed rows with file metadata and set status='success_compute'
- On failure set status='failed_compute' and log
"""

from __future__ import annotations
import logging
import os
import subprocess
import hashlib
from datetime import timezone
from typing import List
from psycopg2.extras import Json

logger = logging.getLogger("dl2.compute")

from .db import get_db_conn


def find_pending_compute(conn, limit=10):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, dataset_id, start_time, end_time FROM nc_jobs WHERE status='pending_compute' ORDER BY start_time LIMIT %s",
            (limit,),
        )
        return cur.fetchall()


def find_compute_groups(conn, limit=10):
    """Return groups (start_time, end_time, ids_array) where all 5 variables for any unique start_time/end_time are success_download."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT j.start_time, j.end_time
            FROM nc_jobs j JOIN erddap_variables v ON v.id = j.variable_id
            WHERE j.status='success_download'
            GROUP BY j.start_time, j.end_time
            HAVING COUNT(DISTINCT v.variable) = 5
            ORDER BY j.start_time
        """
        )
        return cur.fetchall()


def get_compute_variables(conn) -> List[str]:
    # Get the list of variable names and their id to compute from erddap_variables, column variable where type='compute'
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, variable FROM erddap_variables WHERE type='compute'"
        )
        return cur.fetchall()


def compute_for_group(
    conn, start_time, end_time, workers=3, base_dir=None
):
    """Run carbonate compute for a single time range and update nc_jobs rows."""


    try:
        # Mark group rows as running
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE nc_jobs SET status='computing', attempts = attempts+1, last_attempt = NOW() WHERE start_time=%s AND end_time=%s AND status='pending_compute'",
                (start_time, end_time),
            )
            conn.commit()

        # Determine date string for invocation
        ds = start_time.astimezone(timezone.utc)
        date_token = ds.strftime("%Y%m%d")

        cmd = [
            "python",
            "calc_carbon_grid_shm_memmap.py",
            "--date",
            date_token,
            "--mode",
            "memmap",
            "--workers",
            str(workers),
            "--overwrite",
        ]
        if base_dir:
            cmd.extend(["--base-dir", base_dir])

        logger.info(
            "Running compute command for %s->%s: %s",
            start_time,
            end_time,
            " ".join(cmd),
        )
        
        res = subprocess.run(cmd, check=False)
        if res.returncode != 0:
            raise RuntimeError(
                f"Compute subprocess failed with return code {res.returncode}"
            )

        # After compute finishes, find DIC filename for this dataset/time to infer computed filenames
        import os

        # with conn.cursor() as cur2:
        #     # Find the original DIC file path (nc_path) via join to erddap_variables
        #     cur2.execute(
        #         "SELECT j.nc_path FROM nc_jobs j JOIN erddap_variables v ON v.id = j.variable_id WHERE j.start_time=%s AND j.end_time=%s AND v.variable=%s LIMIT 1",
        #         (start_time, end_time, "dissolved_inorganic_carbon"),
        #     )
        #     r = cur2.fetchone()
        #     if not r or not r[0]:
        #         raise RuntimeError(
        #             "Could not find source DIC file row to infer output filenames"
        #         )
        #     dic_path = r[0]
        #     dic_filename = os.path.basename(dic_path)

        # Expected outputs: replace 'dissolved_inorganic_carbon' with target names in filename
        compute_vars = get_compute_variables(conn)

        for var_id, var_name in compute_vars:
            # Use consistent naming: {variable}_{YYYYMMDD}.nc (format required by calc_carbon regex)
            out_fname = f"{var_name}_{start_time.strftime('%Y%m%d')}.nc"
            out_dir = os.path.join(
                base_dir or os.getenv("DATA_DIR", "/opt/data/nc"), var_name
            )
            out_path = os.path.join(out_dir, out_fname)

            if not os.path.exists(out_path):
                logger.error("Expected output file not found: %s", out_path)
                raise RuntimeError("Expected output missing: %s" % out_path)

            # compute size and checksum
            # size = os.path.getsize(out_path)
            h = hashlib.sha256()
            with open(out_path, "rb") as fh:
                for chunk in iter(lambda: fh.read(8192), b""):
                    h.update(chunk)
            checksum = h.hexdigest()

            # Update the existing pending_compute row for this computed variable with metadata and mark computed
            with conn.cursor() as cur3:
                cur3.execute(
                    "UPDATE nc_jobs SET nc_path=%s, checksum=%s, status='success_compute' WHERE start_time=%s AND end_time=%s AND variable_id=%s",
                    (out_path, checksum, start_time, end_time, var_id),
                )
                conn.commit()
                logger.info(
                    "Marked %s %s->%s as success_compute", var_name, start_time, end_time
                )
        return True

    except Exception as e:
        logger.exception("Compute failed for %s->%s: %s", start_time, end_time, e)
        # mark group's computed rows as failed
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE nc_jobs SET status='failed_compute', attempts = attempts+1, last_attempt = NOW() WHERE start_time=%s AND end_time=%s AND status='computing'",
                (start_time, end_time),
            )
            conn.commit()
        return False


def compute_for_row(conn, row, workers=2, base_dir=None):
    """Compute wrapper that accepts a database row (id, dataset_id, start_time, end_time, status)
    or a dict with those fields, and triggers compute_for_group for the corresponding time range.
    """
    # allow either sequence or mapping
    if isinstance(row, dict):
        ds_id = row.get("dataset_id")
        start_time = row.get("start_time")
        end_time = row.get("end_time")
    else:
        # tuple/sequence expected as (id, dataset_id, start_time, end_time, status)
        ds_id = row[1]
        start_time = row[2]
        end_time = row[3]

    # Find group ids for the dataset/time
    with conn.cursor() as cur:
        cur.execute(
            "SELECT array_agg(j.id ORDER BY j.id) FROM nc_jobs j JOIN erddap_variables v ON v.id = j.variable_id WHERE j.dataset_id=%s AND j.start_time=%s AND j.end_time=%s AND v.variable IN ('ph_total','omega_arag','omega_cal')",
            (ds_id, start_time, end_time),
        )
        r2 = cur.fetchone()
        ids = r2[0] if r2 else None

    # If no ids, create placeholder rows and then requery
    if not ids:
        meta = Json(
            {
                "computed_from": [
                    "dissolved_inorganic_carbon",
                    "total_alkalinity",
                    "temperature/salinity",
                ],
                "method": "pyco2sys",
            }
        )
        with conn.cursor() as cur2:
            for cv in ("ph_total", "omega_arag", "omega_cal"):
                # Insert dataset-scoped variable_id if available, otherwise fall back to global variable id
                cur2.execute(
                    "INSERT INTO nc_jobs (dataset_id, variable_id, start_time, end_time, meta, status) VALUES (%s, COALESCE((SELECT id FROM erddap_variables WHERE dataset_id=%s AND variable=%s),(SELECT id FROM erddap_variables WHERE variable=%s LIMIT 1)), %s, %s, %s, 'pending_compute') ON CONFLICT DO NOTHING",
                    (ds_id, ds_id, cv, cv, start_time, end_time, meta),
                )
            conn.commit()
        with conn.cursor() as cur3:
            cur3.execute(
                "SELECT array_agg(j.id ORDER BY j.id) FROM nc_jobs j JOIN erddap_variables v ON v.id = j.variable_id WHERE j.dataset_id=%s AND j.start_time=%s AND j.end_time=%s AND v.variable IN ('ph_total','omega_arag','omega_cal')",
                (ds_id, start_time, end_time),
            )
            ids = cur3.fetchone()[0]

    if not ids:
        logger.error("Could not determine group ids for compute_for_row %s", row)
        return False

    return compute_for_group(
        conn, start_time, end_time, workers=workers, base_dir=base_dir
    )


def compute_for_id(conn, nid, workers=2, base_dir=None):
    """Compute for a single nc_files id (per-file compute).
    This will resolve the dataset_id/start_time/end_time for the provided id and run the group compute,
    which computes all three variables at once (pyco2sys returns all outputs together).
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, dataset_id, start_time, end_time, status FROM nc_jobs WHERE id=%s",
            (nid,),
        )
        r = cur.fetchone()
        if not r:
            logger.error("nc_jobs id %s not found", nid)
            return False
        if r[4] not in ("pending_compute", "failed_compute"):
            logger.info(
                "nc_jobs id %s has status %s, expected 'pending_compute' or 'failed_compute'",
                nid,
                r[4],
            )
            # proceed anyway
        ds_id = r[1]
        start_time = r[2]
        end_time = r[3]

    # Delegate to compute_for_row (tests expect compute_for_row to be invoked)
    try:
        return compute_for_row(conn, r, workers=workers, base_dir=base_dir)
    except Exception:
        logger.exception("compute_for_row failed for id %s", nid)
        return False


def process_pending_compute(
    conn=None, workers=2, limit=10, base_dir=None
):
    if conn is None:
        conn = get_db_conn()
    groups = find_compute_groups(conn, limit=limit)
    if not groups:
        logger.info(
            "No pending compute groups (need ph_total, omega_arag, omega_cal all pending)"
        )
        return
    for group in groups:
        try:
            st, en = group
            # create_compute_rows_for_group(conn, None, st, en)
            compute_for_group(
                conn,
                st,
                en,
                workers=workers,
                base_dir=base_dir
            )
        except Exception:
            logger.exception("Error while computing for %s->%s", st, en)
    return


if __name__ == "__main__":
    # CLI helper
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--base-dir", default=os.getenv("DATA_DIR", "/opt/data/nc"))
    args = parser.parse_args()

    conn = get_db_conn()
    process_pending_compute(
        conn,
        workers=args.workers,
        limit=args.limit,
        base_dir=args.base_dir,
    )
