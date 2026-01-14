"""Downloader v2 (dl2.py)

Goals:
- Parse ERDDAP .das to find dataset-level latest time (time_coverage_end or time.actual_range)
- Track datasets and per-variable last downloaded end time in Postgres (PostGIS instance)
- Create per-variable, per-day nc_files rows for missing intervals and download them

Usage examples:
  # check datasets, create pending nc_files for missing hours (dry-run)
  ./dl2.py check --dataset ubcSSg3DChemistryFields1hV21-11 --dry-run

  # run full cycle (check then download pending files)
  ./dl2.py run --dataset ubcSSg3DChemistryFields1hV21-11

Environment variables:
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
  ERDDAP_BASE (default: https://salishsea.eos.ubc.ca/erddap)
  NC_ROOT (default: /opt/data/nc)  # where downloaded NetCDF files are stored per variable

Notes:
- Default behavior: if a variable has no previous download, script will create pending entries for last N days (default 1 day). You can change with --init-days.
- This script uses psycopg2 to talk to Postgres. Install it in the environment.
"""

import argparse
import os
import re
import logging
from datetime import datetime, timedelta, timezone
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("dl2")

# DB helpers ---------------------------------------------------------------

# Use extracted DB helpers from the package
from dl2pkg.db import get_db_conn, ensure_schema, upsert_dataset, ensure_variable


# DAS parsing -------------------------------------------------------------

# Use shared DAS helpers from package
from dl2pkg.das import parse_das_for_times, fetch_das


# Compute per-day ranges -------------------------------------------------


from dl2pkg.detector import compute_daily_chunks, create_rows_for_date, create_nc_file_row, ensure_pending_nc_file


# DB: upsert dataset and variables ---------------------------------------


# upsert_dataset moved to dl2pkg.db


# ensure_variable moved to dl2pkg.db

# Insert pending nc_files for missing chunks -----------------------------


# create_nc_file_row and ensure_pending_nc_file moved to dl2pkg.detector




# Download and record ----------------------------------------------------


# download_nc moved to dl2pkg.downloader


# Main orchestration -----------------------------------------------------


def do_check(conn, erddap_base, dataset_id, init_days=1, dry_run=True):
    das_text = fetch_das(erddap_base, dataset_id)
    time_cov, actual_max = parse_das_for_times(das_text)
    remote_max = time_cov or actual_max
    if remote_max is None:
        logger.error("No remote time info found in DAS")
        return

    title = None
    m = re.search(r'NC_GLOBAL\s*\{[^}]*title\s+"([^"]+)"', das_text, flags=re.S)
    if m:
        title = m.group(1)

    ds_id = upsert_dataset(
        conn,
        dataset_id,
        title=title,
        last_remote_time=remote_max,
        meta={"das_parsed": True},
    )

    # For each variable we care about: we can detect variable list from store or use a superset. For now use the three variables we care about.
    variables = ["dissolved_oxygen", "dissolved_inorganic_carbon", "total_alkalinity"]

    for variable in variables:
        var_row_id = ensure_variable(conn, ds_id, variable)
        # get last_downloaded_at
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_downloaded_at FROM erddap_variables WHERE id=%s",
                (var_row_id,),
            )
            r = cur.fetchone()
            last_downloaded = r[0] if r else None

        if not last_downloaded:
            # initialize to remote_max - N days + 1 hour (to form at least one day)
            start_dt = remote_max - timedelta(days=init_days)
            # align to :30 by rounding to hour then adding 30 minutes if needed; but assume remote times are on :30
            # ensure start is at HH:30
            start_dt = start_dt.replace(minute=30, second=0, microsecond=0)
        else:
            # missing interval starts at last_downloaded + 1 hour
            start_dt = (last_downloaded + timedelta(hours=1)).astimezone(timezone.utc)

        end_dt = remote_max

        if start_dt > end_dt:
            logger.info(
                "Variable %s already up-to-date (%s >= %s)",
                variable,
                last_downloaded,
                remote_max,
            )
            continue

        chunks = compute_daily_chunks(start_dt, end_dt, require_full_day=True)
        if len(chunks) == 0:
            logger.info(
                "Variable %s: no full-day chunks available yet (partial data present, waiting for full day). Missing interval: %s -> %s",
                variable,
                start_dt,
                end_dt,
            )
        else:
            chunk_span_start = chunks[0][0]
            chunk_span_end = chunks[-1][1]
            logger.info(
                "Variable %s: creating %d full-day chunk(s) covering %s -> %s (per-day 00:30->23:30). Original missing interval: %s -> %s",
                variable,
                len(chunks),
                chunk_span_start,
                chunk_span_end,
                start_dt,
                end_dt,
            )
        for cs, ce in chunks:
            # sanity check: ensure chunk aligns to 00:30..23:30
            expected_day_start = datetime(cs.year, cs.month, cs.day, 0, 30, tzinfo=timezone.utc)
            expected_day_end = datetime(cs.year, cs.month, cs.day, 23, 30, tzinfo=timezone.utc)
            if cs != expected_day_start or ce != expected_day_end:
                logger.warning(
                    "Skipping non-full-day chunk for %s: %s -> %s (expected %s -> %s)",
                    variable,
                    cs,
                    ce,
                    expected_day_start,
                    expected_day_end,
                )
                continue
            created = create_nc_file_row(conn, ds_id, variable, cs, ce, meta={"created_by": "dl2"})
            if created:
                logger.info("Inserted pending nc_files for %s %s->%s", variable, cs, ce)
            else:
                logger.info("nc_files row already exists for %s %s->%s", variable, cs, ce)


# create_rows_for_date moved to dl2pkg.detector


from dl2pkg.downloader import find_pending_rows, download_nc, do_download

# CLI ---------------------------------------------------------------------


from dl2pkg.cli import main as package_main


def main(argv=None):
    package_main(argv)


if __name__ == "__main__":
    main()
