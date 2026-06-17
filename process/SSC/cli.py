"""CLI entry point for the SalishSeaCast pipeline.

Usage (from the process/ directory):
  python -m SSC.cli check       [--date YYYY-MM-DD] [--init-days N]
  python -m SSC.cli download    [--date YYYY-MM-DD] [--variable VAR] [--limit N] [--force]
  python -m SSC.cli compute     [--date YYYY-MM-DD] [--limit N] [--workers N] [--force]
  python -m SSC.cli check_image [--limit N]
  python -m SSC.cli image       [--date YYYY-MM-DD] [--variable VAR] [--limit N] [--workers N] [--force]
  python -m SSC.cli promote     [--limit N]
  python -m SSC.cli ingest      [--date YYYY-MM-DD] [--limit N] [--force]
  python -m SSC.cli sync        [--date YYYY-MM-DD] [--limit N] [--force]
  python -m SSC.cli run         [--date YYYY-MM-DD] [--limit N] [--workers N]
  python -m SSC.cli status      [--date YYYY-MM-DD]

Commands:
  check       Query ERDDAP for new dates and create status rows.
  download    Process pending_download jobs.
  compute     Process pending_compute jobs (PyCO2SYS).
  check_image Find dates where all 8 variables are at their stage's success
              state (5x success_download + 3x success_compute) and promote
              all 8 together to pending_image. Never starts imaging on a
              partial date.
  image       Process pending_image jobs (nc2tile WebP).
  ingest      Process pending_ingest jobs (insert into SalishSeaCast_hourly).
  promote     Advance fully-imaged dates (all 8 at success_image) to
              pending_ingest, and fully-ingested dates (all 8 at
              success_ingest) to pending_sync.
  sync        Process pending_sync jobs: export the date's hourly rows to a
              Native-format file, rsync it plus the WebP images to the home
              server, then call the home API to import them. Requires
              SYNC_HOME_SSH_TARGET / SYNC_HOME_DATA_DIR / SYNC_HOME_API_BASE /
              SYNC_API_TOKEN to be configured (see config.py) — a no-op
              error on a local-only deployment that hasn't set these.
  run         Run all steps in order: download -> check_image -> compute ->
                                       check_image -> image -> promote ->
                                       ingest -> promote -> sync.
  status      Print pipeline status for a date (default: last 7 days).

Every command that accepts --date only acts on rows in the expected pending
state for that stage; rows already past the stage are skipped (logged), and
rows that previously failed are retried. Pass --force to act regardless of
current status (e.g. to force a redundant re-download or re-compute).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
logger = logging.getLogger('SalishSeaCast.cli')


def _parse_date(s: str) -> date:
    return datetime.strptime(s, '%Y-%m-%d').date()


def _get_client():
    from .db import ensure_schema, get_client
    client = get_client()
    ensure_schema(client)
    return client


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

def cmd_check(args) -> None:
    from .downloader import check_erddap
    client    = _get_client()
    new_dates = check_erddap(client, init_days=args.init_days, date_override=args.date)
    logger.info('check: %d new date(s) queued', len(new_dates))
    client.close()


def cmd_download(args) -> None:
    from .downloader import download_variable, process_pending_downloads
    from .config import DOWNLOAD_VARIABLES, PAST_DOWNLOAD_STATUSES
    from .db import get_row
    client = _get_client()

    def _should_skip(date_val, var) -> bool:
        if args.force:
            return False
        row = get_row(client, date_val, var)
        if row and row['status'] in PAST_DOWNLOAD_STATUSES:
            logger.info('Skipping %s %s (status: %s) — use --force to override', var, date_val, row['status'])
            return True
        return False

    if args.date and args.variable:
        if args.variable not in DOWNLOAD_VARIABLES:
            logger.error('Unknown download variable: %s', args.variable)
            sys.exit(1)
        date_val = _parse_date(args.date)
        if not _should_skip(date_val, args.variable):
            download_variable(client, date_val, args.variable)
    elif args.date:
        date_val = _parse_date(args.date)
        for var in DOWNLOAD_VARIABLES:
            if not _should_skip(date_val, var):
                download_variable(client, date_val, var)
    else:
        process_pending_downloads(client, limit=args.limit)
    client.close()


def cmd_check_image(args) -> None:
    from .db import check_image_ready
    client = _get_client()
    n = check_image_ready(client, limit=args.limit)
    logger.info('check_image: %d status rows promoted to pending_image', n)
    client.close()


def cmd_promote(args) -> None:
    from .db import promote_ready_dates
    client = _get_client()
    n = promote_ready_dates(client, limit=args.limit)
    logger.info('promote: %d status rows advanced to pending_ingest', n)
    client.close()


def cmd_compute(args) -> None:
    from .compute import compute_date, process_pending_compute
    from .config import (
        COMPUTE_VARIABLES, NC_BASE_DIR, PAST_COMPUTE_STATUSES,
        STATUS_COMPUTING, STATUS_FAILED_COMPUTE, STATUS_SUCCESS_COMPUTE,
    )
    from .db import downloads_complete, get_row, mark_failed, mark_running, mark_success
    client = _get_client()
    nc_dir = os.getenv('SSC_NC_DIR', NC_BASE_DIR)

    if args.date:
        date_val = _parse_date(args.date)

        if not args.force:
            past = {
                var: row['status']
                for var in COMPUTE_VARIABLES
                if (row := get_row(client, date_val, var)) and row['status'] in PAST_COMPUTE_STATUSES
            }
            if past:
                logger.info('Skipping compute for %s — already past compute stage (%s); '
                            'use --force to override', args.date, past)
                client.close()
                return

        if not downloads_complete(client, date_val):
            logger.error('Downloads not complete for %s', args.date)
            sys.exit(1)
        for var in COMPUTE_VARIABLES:
            row = get_row(client, date_val, var) or {}
            mark_running(client, date_val, var, STATUS_COMPUTING, row.get('attempts', 0))
        try:
            paths = compute_date(date_val, workers=args.workers, nc_base_dir=nc_dir)
            for var in COMPUTE_VARIABLES:
                row = get_row(client, date_val, var) or {}
                mark_success(client, date_val, var, STATUS_SUCCESS_COMPUTE,
                             nc_path=paths.get(var, ''), attempts=row.get('attempts', 0))
        except Exception:
            logger.exception('Compute failed for %s', args.date)
            for var in COMPUTE_VARIABLES:
                row = get_row(client, date_val, var) or {}
                mark_failed(client, date_val, var, STATUS_FAILED_COMPUTE,
                            'compute_date raised an exception', attempts=row.get('attempts', 0))
    else:
        process_pending_compute(client, limit=args.limit, workers=args.workers, nc_base_dir=nc_dir)
    client.close()


def cmd_image(args) -> None:
    from .imaging import image_variable, process_pending_images
    from .config import ALL_VARIABLES, IMAGE_BASE_DIR, NC_BASE_DIR, PAST_IMAGE_STATUSES
    from .db import get_row
    client     = _get_client()
    nc_dir     = os.getenv('SSC_NC_DIR', NC_BASE_DIR)
    image_dir  = os.getenv('SSC_IMAGE_DIR', IMAGE_BASE_DIR)
    workers    = args.workers or None

    def _should_skip(date_val, var) -> bool:
        if args.force:
            return False
        row = get_row(client, date_val, var)
        if row and row['status'] in PAST_IMAGE_STATUSES:
            logger.info('Skipping %s %s (status: %s) — use --force to override', var, date_val, row['status'])
            return True
        return False

    if args.date and args.variable:
        if args.variable not in ALL_VARIABLES:
            logger.error('Unknown variable: %s', args.variable)
            sys.exit(1)
        date_val = _parse_date(args.date)
        if not _should_skip(date_val, args.variable):
            image_variable(client, date_val, args.variable, workers=workers,
                           nc_base_dir=nc_dir, image_base_dir=image_dir)
    elif args.date:
        date_val = _parse_date(args.date)
        for var in ALL_VARIABLES:
            if not _should_skip(date_val, var):
                image_variable(client, date_val, var, workers=workers,
                               nc_base_dir=nc_dir, image_base_dir=image_dir)
    else:
        process_pending_images(client, limit=args.limit, workers=workers,
                               nc_base_dir=nc_dir, image_base_dir=image_dir)
    client.close()


def cmd_ingest(args) -> None:
    from .ingest import ingest_date, process_pending_ingests
    from .config import (
        ALL_VARIABLES, NC_BASE_DIR,
        STATUS_FAILED_INGEST, STATUS_INGESTING, STATUS_SUCCESS_INGEST,
    )
    from .db import (
        compute_complete, downloads_complete,
        get_row, mark_failed, mark_running, mark_success,
    )
    client = _get_client()
    nc_dir = os.getenv('SSC_NC_DIR', NC_BASE_DIR)
    if args.date:
        date_val = _parse_date(args.date)

        if not args.force:
            # SalishSeaCast_hourly is a plain MergeTree (no dedup) — re-running
            # ingest on an already-ingested date would insert duplicate rows.
            already = [
                var for var in ALL_VARIABLES
                if (row := get_row(client, date_val, var)) and row['status'] == STATUS_SUCCESS_INGEST
            ]
            if already:
                logger.info('Skipping ingest for %s — already ingested (%d/%d vars at %s); '
                            'use --force to override', args.date, len(already), len(ALL_VARIABLES),
                            STATUS_SUCCESS_INGEST)
                client.close()
                return

        if not downloads_complete(client, date_val) or not compute_complete(client, date_val):
            logger.error('Prerequisites not met for ingest on %s', args.date)
            sys.exit(1)
        for var in ALL_VARIABLES:
            row = get_row(client, date_val, var) or {}
            mark_running(client, date_val, var, STATUS_INGESTING, row.get('attempts', 0))
        try:
            n = ingest_date(client, date_val, nc_base_dir=nc_dir)
            for var in ALL_VARIABLES:
                row = get_row(client, date_val, var) or {}
                mark_success(client, date_val, var, STATUS_SUCCESS_INGEST,
                             nc_path=str(n), attempts=row.get('attempts', 0))
        except Exception:
            logger.exception('Ingest failed for %s', args.date)
            for var in ALL_VARIABLES:
                row = get_row(client, date_val, var) or {}
                mark_failed(client, date_val, var, STATUS_FAILED_INGEST,
                            'ingest_date raised an exception', attempts=row.get('attempts', 0))
    else:
        process_pending_ingests(client, limit=args.limit, nc_base_dir=nc_dir)
    client.close()


def cmd_sync(args) -> None:
    from .sync import sync_date, process_pending_syncs
    from .config import ALL_VARIABLES, IMAGE_BASE_DIR, STATUS_SUCCESS_SYNC
    from .db import get_row
    client    = _get_client()
    image_dir = os.getenv('SSC_IMAGE_DIR', IMAGE_BASE_DIR)

    if args.date:
        date_val = _parse_date(args.date)

        if not args.force:
            already = [
                var for var in ALL_VARIABLES
                if (row := get_row(client, date_val, var)) and row['status'] == STATUS_SUCCESS_SYNC
            ]
            if already:
                logger.info('Skipping sync for %s — already synced (%d/%d vars at %s); '
                            'use --force to override', args.date, len(already), len(ALL_VARIABLES),
                            STATUS_SUCCESS_SYNC)
                client.close()
                return

        sync_date(client, date_val, image_base_dir=image_dir)
    else:
        process_pending_syncs(client, limit=args.limit, image_base_dir=image_dir)
    client.close()


def cmd_run(args) -> None:
    """Run all pipeline steps in sequence for pending work."""
    from .config import IMAGE_BASE_DIR, NC_BASE_DIR, SYNC_HOME_API_BASE
    from .downloader import process_pending_downloads
    from .compute import process_pending_compute
    from .imaging import process_pending_images
    from .ingest import process_pending_ingests
    from .sync import process_pending_syncs
    from .db import check_image_ready, promote_ready_dates

    client    = _get_client()
    nc_dir    = os.getenv('SSC_NC_DIR', NC_BASE_DIR)
    image_dir = os.getenv('SSC_IMAGE_DIR', IMAGE_BASE_DIR)
    workers   = args.workers or None

    logger.info('=== download ===')
    process_pending_downloads(client, limit=args.limit)

    logger.info('=== check_image (post-download) ===')
    check_image_ready(client)

    logger.info('=== compute ===')
    process_pending_compute(client, limit=args.limit, workers=args.workers, nc_base_dir=nc_dir)

    logger.info('=== check_image (post-compute) ===')
    check_image_ready(client)

    logger.info('=== image ===')
    from .config import ALL_VARIABLES as _ALL_VARS
    process_pending_images(client, limit=args.limit * len(_ALL_VARS), workers=workers,
                           nc_base_dir=nc_dir, image_base_dir=image_dir)

    logger.info('=== promote (success_image → pending_ingest, success_ingest → pending_sync) ===')
    promote_ready_dates(client)

    logger.info('=== ingest ===')
    process_pending_ingests(client, limit=args.limit, nc_base_dir=nc_dir)

    logger.info('=== promote (success_ingest → pending_sync) ===')
    promote_ready_dates(client)

    if SYNC_HOME_API_BASE:
        logger.info('=== sync ===')
        process_pending_syncs(client, limit=args.limit, image_base_dir=image_dir)
    else:
        logger.info('=== sync skipped (SYNC_HOME_API_BASE not configured) ===')

    client.close()


def cmd_status(args) -> None:
    client = _get_client()
    if args.date:
        where = f"date = toDate('{args.date}')"
    else:
        cutoff = (datetime.utcnow().date() - timedelta(days=7)).isoformat()
        where  = f"date >= toDate('{cutoff}')"

    result = client.query(
        f"""
        SELECT date, variable, argMax(status, updated_at) AS cur_status,
               argMax(attempts, updated_at) AS cur_attempts,
               argMax(error_message, updated_at) AS err
        FROM SalishSeaCast_status
        WHERE {where}
        GROUP BY date, variable
        ORDER BY date DESC, variable
        """
    )

    if not result.result_rows:
        print('No status rows found.')
        client.close()
        return

    header = f"{'date':<12} {'variable':<30} {'status':<20} {'attempts':>8}  error"
    print(header)
    print('-' * len(header))
    for row in result.result_rows:
        d, var, status, attempts, err = row
        err_str = (err[:40] + '…') if len(err) > 40 else err
        print(f'{str(d):<12} {var:<30} {status:<20} {attempts:>8}  {err_str}')

    client.close()


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        prog='python -m SSC.cli',
        description='SalishSeaCast pipeline CLI',
    )
    sub = parser.add_subparsers(dest='cmd', required=True)

    def _add_date(p):
        p.add_argument('--date', metavar='YYYY-MM-DD', help='Target a specific date only')

    def _add_limit(p, default=10):
        p.add_argument('--limit', type=int, default=default, metavar='N',
                       help=f'Max jobs per step (default {default})')

    def _add_workers(p, default=4):
        p.add_argument('--workers', type=int, default=default, metavar='N',
                       help=f'Worker processes for CPU-bound steps (default {default})')

    def _add_force(p):
        p.add_argument('--force', action='store_true',
                       help='Act regardless of current status (default: skip rows already past this stage)')

    p = sub.add_parser('check', help='Query ERDDAP, queue new dates')
    _add_date(p)
    p.add_argument('--init-days', type=int, default=3, metavar='N',
                   help='Days back to start from when no download history exists (default 3)')
    p.set_defaults(func=cmd_check)

    p = sub.add_parser('download', help='Download NetCDF files from ERDDAP')
    _add_date(p); _add_limit(p, default=20); _add_force(p)
    p.add_argument('--variable', metavar='VAR', help='Restrict to one variable')
    p.set_defaults(func=cmd_download)

    p = sub.add_parser('compute', help='Derive ph_total, omega_arag, omega_cal via PyCO2SYS')
    _add_date(p); _add_limit(p, default=10); _add_workers(p, default=4); _add_force(p)
    p.set_defaults(func=cmd_compute)

    p = sub.add_parser('check_image',
                       help='Promote dates where all 8 variables succeeded (download+compute) to pending_image')
    _add_limit(p, default=20)
    p.set_defaults(func=cmd_check_image)

    p = sub.add_parser('image', help='Generate WebP tiles via nc2tile')
    _add_date(p); _add_limit(p, default=40); _add_workers(p, default=4); _add_force(p)
    p.add_argument('--variable', metavar='VAR', help='Restrict to one variable')
    p.set_defaults(func=cmd_image)

    p = sub.add_parser('ingest', help='Insert hourly rows into SalishSeaCast_hourly')
    _add_date(p); _add_limit(p, default=5); _add_force(p)
    p.set_defaults(func=cmd_ingest)

    p = sub.add_parser('promote',
                       help='Advance success_image(8)->pending_ingest and success_ingest(8)->pending_sync')
    _add_limit(p, default=20)
    p.set_defaults(func=cmd_promote)

    p = sub.add_parser('sync', help='Export + rsync + trigger home import for ingested dates')
    _add_date(p); _add_limit(p, default=5); _add_force(p)
    p.set_defaults(func=cmd_sync)

    p = sub.add_parser('run', help='Run all pipeline steps for pending work')
    _add_date(p); _add_limit(p, default=10); _add_workers(p, default=4)
    p.set_defaults(func=cmd_run)

    p = sub.add_parser('status', help='Print pipeline status summary')
    _add_date(p)
    p.set_defaults(func=cmd_status)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == '__main__':
    main()
