"""CLI wrapper for dl2 functionality using modular package components."""

import argparse
import os
import logging
from datetime import timedelta, timezone

from .db import get_db_conn, ensure_schema, upsert_dataset, ensure_variable
from .das import fetch_das, parse_das_for_times
from .detector import compute_daily_chunks, create_rows_for_date, create_nc_file_row, create_compute_rows_for_group
from .downloader import do_download

logger = logging.getLogger("dl2.cli")


def _normalize_base_url(base_url: str, erddap_base: str) -> str:
    if base_url.startswith("http://") or base_url.startswith("https://"):
        return base_url
    return f"{erddap_base.rstrip('/')}/griddap/{base_url}"


def do_check(conn, erddap_base, base_url, init_days=1, variables=None):
    base_url = _normalize_base_url(base_url, erddap_base)
    das_text = fetch_das(base_url)
    time_cov, actual_max = parse_das_for_times(das_text)
    remote_max = time_cov or actual_max
    if remote_max is None:
        logger.error("No remote time info found in DAS")
        return

    title = None
    # try to extract a title from DAS
    import re

    m = re.search(r'NC_GLOBAL\s*\{[^}]*title\s+"([^"]+)"', das_text, flags=re.S)
    if m:
        title = m.group(1)

    ds_id = upsert_dataset(
        conn,
        base_url,
        title=title,
        last_remote_time=remote_max,
        meta={"das_parsed": True},
    )

    # If a variables list is supplied use that, otherwise query the DB for variables
    # associated with this dataset that are marked as 'download'.
    if variables is None:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT variable FROM erddap_variables WHERE dataset_id=%s AND type='download'",
                (ds_id,),
            )
            variables = [r[0] for r in cur.fetchall()]

    for variable in variables:
        var_row_id = ensure_variable(conn, ds_id, variable)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_downloaded_at FROM erddap_variables WHERE id=%s",
                (var_row_id,),
            )
            r = cur.fetchone()
            last_downloaded = r[0] if r else None

        if not last_downloaded:
            start_dt = remote_max - timedelta(days=init_days)
            start_dt = start_dt.replace(minute=30, second=0, microsecond=0)
        else:
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
            for cs, ce in chunks:
                # sanity check
                expected_day_start = cs
                expected_day_end = ce
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
                created = create_nc_file_row(
                    conn, ds_id, variable, cs, ce, meta={"created_by": "dl2"}
                )
                if created:
                    logger.info(
                        "Inserted pending nc_jobs for %s %s->%s", variable, cs, ce
                    )
                else:
                    logger.info(
                        "nc_jobs row already exists for %s %s->%s", variable, cs, ce
                    )

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "cmd",
        choices=[
            "check_download",
            "download",
            "check_image",
            "image",
            "compute",
            "liveocean_download",
            "liveocean_process",
        ],
    )
    parser.add_argument(
        "--erddap-base",
        default=os.getenv("ERDDAP_BASE", "https://salishsea.eos.ubc.ca/erddap"),
    )
    parser.add_argument("--init-days", type=int, default=1)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument(
        "--date",
        help="YYYY-MM-DD (UTC). When provided, create/download data for that date only",
    )
    parser.add_argument(
        "--id",
        type=int,
        help="Optional: compute for a specific nc_jobs id (per-file compute)",
    )
    parser.add_argument("--variable", help="Optional: restrict to a single variable")
    parser.add_argument("--force", action="store_true")
    # optional worker args
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--liveocean-url", default=os.getenv("LIVE_OCEAN_URL", "https://s3.kopah.uw.edu/liveocean-share/f2026.02.04/layers.nc"))
    parser.add_argument("--liveocean-input", default=os.getenv("LIVE_OCEAN_INPUT", "/opt/data/nc/liveOcean/layers.nc"))
    parser.add_argument("--liveocean-out", default=os.getenv("LIVE_OCEAN_OUT", "/opt/data/nc"))
    parser.add_argument("--liveocean-date", help="UTC date for the Live Ocean run (YYYY-MM-DD)")
    # packing precision is fixed at 0.1 (no CLI flag)
    args = parser.parse_args(argv)

    # For test - simulate CLI args as a sequence of strings
    # args = parser.parse_args(
    #     [
    #         "check",
    #         "--erddap-base",
    #         "https://salishsea.eos.ubc.ca/erddap",
    #         "--date",
    #         "2026-01-16",
    #     ]
    # )

    conn = get_db_conn()
    ensure_schema(conn)

    if args.cmd == "check_download":
        # Check ERDDAP for available data from last_downloaded_at to today
        # Create download rows for any dates with available data
        # Also create pending_compute rows once all required download variables are available
        from datetime import datetime, timedelta, timezone as tz
        from .detector import create_compute_rows_for_group
        
        tracked_dates = {}  # Track (dataset_id, date_str) combos we process
        
        with conn.cursor() as cur:
            cur.execute("SELECT id, base_url FROM erddap_datasets ORDER BY id")
            datasets = cur.fetchall()
        
        for ds_id, base_url in datasets:
            # Get all variables for this dataset marked for download
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, variable FROM erddap_variables WHERE dataset_id=%s AND type='download'",
                    (ds_id,),
                )
                variables = cur.fetchall()
            
            for var_id, variable in variables:
                # Get last_downloaded date
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT last_downloaded_at FROM erddap_variables WHERE id=%s",
                        (var_id,),
                    )
                    r = cur.fetchone()
                    last_dl = r[0] if r else None
                
                # Determine date range for check
                if args.date:
                    # Check specific date only
                    date_to_check = args.date
                    if last_dl is None or last_dl.astimezone(tz.utc).date() <= datetime.strptime(args.date, '%Y-%m-%d').date():
                        create_rows_for_date(conn, ds_id, [variable], date_to_check, force=args.force)
                        tracked_dates[(ds_id, date_to_check)] = True
                        logger.info("Created rows for dataset %s variable %s on %s", ds_id, variable, date_to_check)
                else:
                    # Check from last_downloaded_at to today
                    if not last_dl:
                        start_date = datetime.now(tz.utc) - timedelta(days=args.init_days)
                    else:
                        start_date = (last_dl + timedelta(hours=1)).astimezone(tz.utc)
                    
                    end_date = datetime.now(tz.utc)
                    curr_date = start_date.date()
                    
                    while curr_date <= end_date.date():
                        date_str = curr_date.strftime('%Y-%m-%d')
                        create_rows_for_date(
                            conn, ds_id, [variable],
                            date_str,
                            force=args.force
                        )
                        tracked_dates[(ds_id, date_str)] = True
                        curr_date += timedelta(days=1)
        
        # Now that all download rows are created, create compute rows for completed date ranges
        for (ds_id, date_str) in tracked_dates.keys():
            try:
                create_compute_rows_for_group(conn, date_str)
                logger.info("Created compute rows for %s", date_str)
            except Exception:
                logger.exception("Failed to create compute rows for dataset %s date %s", ds_id, date_str)
        
        logger.info("check_download: Scheduling complete")
        return

    if args.cmd == "download":
        # Download only: process pending_download rows
        # Always requeue failed downloads first (retry stale failures)
        from .downloader import requeue_failed
        requeue_failed(conn, dataset=None, date=args.date, variable=args.variable)
        
        # Execute downloads for pending rows
        do_download(conn, args.erddap_base, limit=args.limit, variable=args.variable)
        logger.info("download: Processing complete")
        return

    if args.cmd == "check_image":
        # Check if for any unique start_date/end_date in nc_jobs, status is set to either 'success_download' or 'success_compute'
        # If so, update the status column to 'pending_png' for all variables for that date range
        from .png_worker import check_image_ready_rows
        check_image_ready_rows(conn)
        return
    
    if args.cmd == "image":
        from .png_worker import process_pending_png

        process_pending_png(
            conn,
            limit=args.limit,
            workers=args.workers,
        )
        return

    if args.cmd == "compute":
        # Run compute worker (do not auto-create compute rows here; those are created by download/run)
        from .compute import process_pending_compute

        # If --id is provided compute that single row directly
        if args.id:
            from .compute import compute_for_id

            logger.info("Running per-file compute for nc_jobs id %s", args.id)
            compute_for_id(
                conn,
                args.id,
                workers=args.workers,
                base_dir=os.getenv("DATA_DIR", "/opt/data/nc"),
            )
            return

        # Run pending compute jobs (process nc_jobs rows with status='pending_compute')
        process_pending_compute(
            conn,
            workers=args.workers,
            limit=args.limit,
            base_dir=os.getenv("DATA_DIR", "/opt/data/nc"),
        )
        return

    if args.cmd == "liveocean_download":
        from .live_ocean import download_live_ocean, today_utc_date

        run_date = args.liveocean_date or today_utc_date()
        download_live_ocean(
            conn,
            run_date=run_date,
            url=args.liveocean_url,
            input_path=args.liveocean_input,
            out_dir=args.liveocean_out,
        )
        return

    if args.cmd == "liveocean_process":
        from .live_ocean import process_pending_live_ocean

        process_pending_live_ocean(conn, limit=args.limit)
        return


if __name__ == "__main__":
    main()
