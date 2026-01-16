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


def do_check(conn, erddap_base, dataset_id, init_days=1, dry_run=True, variables=None):
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

    # If a variables list is supplied use that, otherwise query the DB for variables
    # associated with this dataset that are marked as 'download'.
    if variables is None:
        with conn.cursor() as cur:
            cur.execute("SELECT variable FROM erddap_variables WHERE dataset_id=%s AND type='download'", (ds_id,))
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
                created = create_nc_file_row(conn, ds_id, variable, cs, ce, meta={"created_by": "dl2"})
                if created:
                    logger.info("Inserted pending nc_files for %s %s->%s", variable, cs, ce)
                else:
                    logger.info("nc_files row already exists for %s %s->%s", variable, cs, ce)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("cmd", choices=["check", "download", "run", "sublevel", "png", "compute"])
    parser.add_argument("--dataset", required=False, help="Optional: dataset id string; if omitted, process all datasets/variables of type 'download' from DB")
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
        if args.dataset:
            if args.date:
                ds_id = upsert_dataset(conn, args.dataset)
                vars_list = [args.variable] if args.variable else ["dissolved_oxygen", "dissolved_inorganic_carbon", "total_alkalinity"]
                create_rows_for_date(conn, ds_id, vars_list, args.date, force=args.force, dry_run=args.dry_run)
                return
            do_check(conn, args.erddap_base, args.dataset, init_days=args.init_days, dry_run=args.dry_run)
            return

        # No dataset provided: iterate datasets in DB and process variables marked as 'download'
        with conn.cursor() as cur:
            cur.execute("SELECT id, dataset_id FROM erddap_datasets ORDER BY id")
            rows = cur.fetchall()
        for ds_id, dataset_id in rows:
            # variables filtered inside do_check if not provided
            do_check(conn, args.erddap_base, dataset_id, init_days=args.init_days, dry_run=args.dry_run)
        return

    if args.cmd == "download":
        if args.dataset:
            if args.date:
                ds_id = upsert_dataset(conn, args.dataset)
                vars_list = [args.variable] if args.variable else ["dissolved_oxygen", "dissolved_inorganic_carbon", "total_alkalinity"]
                create_rows_for_date(conn, ds_id, vars_list, args.date, force=args.force, dry_run=args.dry_run)
            if args.requeue_failed:
                from .downloader import requeue_failed
                requeue_failed(conn, dataset=args.dataset, date=args.date, variable=args.variable, dry_run=args.dry_run)
            do_download(conn, args.erddap_base, dry_run=args.dry_run, limit=args.limit)
            return

        # No dataset provided: build rows for date or run downloads for all datasets
        if args.date:
            with conn.cursor() as cur:
                cur.execute("SELECT id, dataset_id FROM erddap_datasets ORDER BY id")
                rows = cur.fetchall()
            for ds_id, dataset_id in rows:
                # get variables of type 'download' for this dataset
                with conn.cursor() as cur:
                    cur.execute("SELECT variable FROM erddap_variables WHERE dataset_id=%s AND type='download'", (ds_id,))
                    vars_list = [r[0] for r in cur.fetchall()]
                if args.variable:
                    vars_list = [args.variable]
                if vars_list:
                    create_rows_for_date(conn, ds_id, vars_list, args.date, force=args.force, dry_run=args.dry_run)
        if args.requeue_failed:
            from .downloader import requeue_failed
            requeue_failed(conn, dataset=None, date=args.date, variable=args.variable, dry_run=args.dry_run)
        do_download(conn, args.erddap_base, dry_run=args.dry_run, limit=args.limit)
        return

    if args.cmd == "run":
        if args.dataset:
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

        # No dataset provided: do check and download for all datasets/variables type='download'
        with conn.cursor() as cur:
            cur.execute("SELECT id, dataset_id FROM erddap_datasets ORDER BY id")
            rows = cur.fetchall()
        for ds_id, dataset_id in rows:
            # do_check will query variables of type 'download' if variables not provided
            do_check(conn, args.erddap_base, dataset_id, init_days=args.init_days, dry_run=args.dry_run)
        if args.requeue_failed:
            from .downloader import requeue_failed
            requeue_failed(conn, dataset=None, date=args.date, variable=args.variable, dry_run=args.dry_run)
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
        process_pending_png(conn, dry_run=args.dry_run, limit=args.limit, workers=args.workers, simulate=args.simulate)
        return

    if args.cmd == "compute":
        # Compute carbonate-system fields for available datetimes. If --dataset is provided, limit to it.
        from subprocess import run
        if args.dataset:
            # compute for that dataset only: identify its numeric id
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM erddap_datasets WHERE dataset_id=%s", (args.dataset,))
                r = cur.fetchone()
            if not r:
                print(f"Dataset not found: {args.dataset}")
                return
            ds_id = r[0]
            # For each variable in this dataset marked 'download', run calc_carbon on its files
            with conn.cursor() as cur:
                cur.execute("SELECT variable FROM erddap_variables WHERE dataset_id=%s AND type='download'", (ds_id,))
                vars_list = [r[0] for r in cur.fetchall()]
            for v in vars_list:
                # run calc_carbon per dataset directory
                print(f"Computing carbonate fields for dataset {args.dataset}, variable {v}")
                cmd = ["python", "process/calc_carbon.py", "--in-dir", os.getenv('DATA_DIR','/opt/data/nc')]
                run(cmd)
        else:
            # run globally (over all datasets) - run calc script which scans DIC directory by default
            print("Computing carbonate fields for all available datetimes")
            cmd = ["python", "process/calc_carbon.py", "--in-dir", os.getenv('DATA_DIR','/opt/data/nc')]
            run(cmd)
        return


if __name__ == "__main__":
    main()
