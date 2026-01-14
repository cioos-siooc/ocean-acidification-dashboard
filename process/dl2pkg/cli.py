"""CLI wrapper for dl2 functionality using modular package components."""
import argparse
import os
import logging
from datetime import timedelta, timezone

from .db import get_db_conn, ensure_schema, upsert_dataset, ensure_variable
from .das import fetch_das, parse_das_for_times
from .detector import compute_daily_chunks, create_rows_for_date, create_nc_file_row
from .downloader import do_download

logger = logging.getLogger("dl2.cli")


def do_check(conn, erddap_base, dataset_id, init_days=1, dry_run=True):
    das_text = fetch_das(erddap_base, dataset_id)
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
        dataset_id,
        title=title,
        last_remote_time=remote_max,
        meta={"das_parsed": True},
    )

    variables = ["dissolved_oxygen", "dissolved_inorganic_carbon", "total_alkalinity"]

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
                created = create_nc_file_row(conn, ds_id, variable, cs, ce, meta={"created_by": "dl2"})
                if created:
                    logger.info("Inserted pending nc_files for %s %s->%s", variable, cs, ce)
                else:
                    logger.info("nc_files row already exists for %s %s->%s", variable, cs, ce)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("cmd", choices=["check", "download", "run", "sublevel", "png"])
    parser.add_argument("--dataset", required=True)
    parser.add_argument(
        "--erddap-base",
        default=os.getenv("ERDDAP_BASE", "https://salishsea.eos.ubc.ca/erddap"),
    )
    parser.add_argument("--init-days", type=int, default=1)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--date", help="YYYY-MM-DD (UTC). When provided, create/download data for that date only")
    parser.add_argument("--variable", help='Optional: restrict to a single variable')
    parser.add_argument("--force", action='store_true')
    parser.add_argument("--requeue-failed", action='store_true', help='Reset failed download rows to pending before starting downloads')
    # optional worker args
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--simulate", action='store_true', help='Simulate PNG generation (no files written)')
    # packing precision is fixed at 0.1 (no CLI flag)
    args = parser.parse_args(argv)

    conn = get_db_conn()
    ensure_schema(conn)

    if args.cmd == "check":
        if args.date:
            ds_id = upsert_dataset(conn, args.dataset)
            vars_list = [args.variable] if args.variable else ["dissolved_oxygen", "dissolved_inorganic_carbon", "total_alkalinity"]
            create_rows_for_date(conn, ds_id, vars_list, args.date, force=args.force, dry_run=args.dry_run)
            return
        do_check(conn, args.erddap_base, args.dataset, init_days=args.init_days, dry_run=args.dry_run)
        return

    if args.cmd == "download":
        if args.date:
            ds_id = upsert_dataset(conn, args.dataset)
            vars_list = [args.variable] if args.variable else ["dissolved_oxygen", "dissolved_inorganic_carbon", "total_alkalinity"]
            create_rows_for_date(conn, ds_id, vars_list, args.date, force=args.force, dry_run=args.dry_run)
        if args.requeue_failed:
            from .downloader import requeue_failed
            requeue_failed(conn, dataset=args.dataset, date=args.date, variable=args.variable, dry_run=args.dry_run)
        do_download(conn, args.erddap_base, dry_run=args.dry_run, limit=args.limit)
        return

    if args.cmd == "run":
        if args.date:
            ds_id = upsert_dataset(conn, args.dataset)
            vars_list = [args.variable] if args.variable else ["dissolved_oxygen", "dissolved_inorganic_carbon", "total_alkalinity"]
            create_rows_for_date(conn, ds_id, vars_list, args.date, force=args.force, dry_run=args.dry_run)
            # forward --requeue-failed to download
            if args.requeue_failed:
                from .downloader import requeue_failed
                requeue_failed(conn, dataset=args.dataset, date=args.date, variable=args.variable, dry_run=args.dry_run)
            do_download(conn, args.erddap_base, dry_run=args.dry_run, limit=args.limit)
            return
        do_check(conn, args.erddap_base, args.dataset, init_days=args.init_days, dry_run=args.dry_run)
        if args.requeue_failed:
            from .downloader import requeue_failed
            requeue_failed(conn, dataset=args.dataset, date=args.date, variable=args.variable, dry_run=args.dry_run)
        do_download(conn, args.erddap_base, dry_run=args.dry_run, limit=args.limit)
        return

    if args.cmd == "sublevel":
        # run sublevel worker
        from .sublevel import process_pending_sublevels
        for _ in range(args.workers):
            process_pending_sublevels(conn, dry_run=args.dry_run, limit=args.limit)
        return

    if args.cmd == "png":
        from .png_worker import process_pending_png
        # Previously args.workers indicated how many times to run the worker loop.
        # Now it controls the internal nc2tile worker count passed to nc2tile via --workers.
        # The --simulate flag can be used to avoid writing PNG/sidecar files for fast tests.
        process_pending_png(conn, dry_run=args.dry_run, limit=args.limit, workers=args.workers, simulate=args.simulate)
        return


if __name__ == "__main__":
    main()
