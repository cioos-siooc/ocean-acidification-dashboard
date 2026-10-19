"""CLI entry point for the LiveOcean pipeline.

Usage (from the process/ directory):
  python -m LO.cli check    [--date YYYY-MM-DD] [--init-days N] [--backfill [--from YYYY-MM-DD]]
  python -m LO.cli download [--date YYYY-MM-DD] [--limit N] [--force]
  python -m LO.cli compute  [--date YYYY-MM-DD] [--limit N] [--workers N] [--force]
  python -m LO.cli image    [--date YYYY-MM-DD] [--limit N] [--workers N] [--force]
  python -m LO.cli ingest   [--date YYYY-MM-DD] [--limit N] [--workers N] [--force]
  python -m LO.cli sync     [--date YYYY-MM-DD] [--limit N] [--force]
  python -m LO.cli run      [--date YYYY-MM-DD] [--limit N] [--workers N] [--force]
                            [--init-days N] [--backfill [--from YYYY-MM-DD]]
  python -m LO.cli status   [--date YYYY-MM-DD]

`--date` is a run date (the fYYYY.MM.DD in the archive URL). One status row
per run moves pending_download -> success_download -> success_compute ->
success_image -> success_ingest -> success_sync; each command picks up rows
at its input state (or failed at its own step, under LO_MAX_RETRY_ATTEMPTS).
With --date it acts on that run only if it's at that state, unless --force.

check: probes the archive for runs newer than the newest known one (or
--init-days back on a first run) and seeds them. --backfill probes from
--from (default: the archive start, 2025-07-15) instead. Runs followed by a
newer run keep only day 0 (6 steps); the newest keeps all 19.

`run` on a schedule: see LO/flows.py (Prefect).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta
from typing import Callable

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
logger = logging.getLogger('LiveOcean.cli')


class PipelineError(RuntimeError):
    """A step's precondition failed. Raised rather than sys.exit() so steps
    can run inside a Prefect task; main() turns it into exit status 1."""


def _parse_date(s: str) -> date:
    return datetime.strptime(s, '%Y-%m-%d').date()


def _get_client():
    from .db import ensure_schema, get_client
    client = get_client()
    ensure_schema(client)
    return client


# Stage name -> (module, function taking (client, run, **kw))
def _stage_fn(stage: str) -> Callable:
    from . import compute, downloader, imaging, ingest, sync
    return {
        'download': downloader.download,
        'compute': compute.compute,
        'image': imaging.image,
        'ingest': ingest.ingest,
        'sync': sync.sync,
    }[stage]


def _stage_kwargs(stage: str, workers: int | None) -> dict:
    if stage == 'compute':
        return {'workers': workers or 4}
    if stage == 'image':
        return {'workers': workers or None}
    return {}


def _run_stage_for_date(client, stage: str, run_date: date, workers=None, force=False) -> bool:
    from .config import STAGE_INPUTS
    from .db import get_run
    run = get_run(client, run_date)
    if run is None:
        raise PipelineError(f'Run {run_date} is not queued — run `check --date {run_date}` first')
    ok, failed = STAGE_INPUTS[stage]
    if not force and run['status'] not in (ok, failed):
        logger.info('Skipping %s for run %s (status: %s) — use --force to override',
                    stage, run_date, run['status'])
        return False
    if stage != 'download' and not (run['nc_path'] and os.path.exists(run['nc_path'])):
        raise PipelineError(f'Run {run_date} has no local NetCDF ({run["nc_path"]!r}); re-download it')
    return _stage_fn(stage)(client, run, **_stage_kwargs(stage, workers))


def _sweep_stage(client, stage: str, limit: int, workers=None) -> None:
    from .db import pending_runs
    runs = pending_runs(client, stage, limit)
    if stage == 'sync':
        runs = sorted(runs, key=lambda r: r['run_date'])  # newer imports land last
    if not runs:
        logger.info('No LiveOcean runs pending %s', stage)
    if stage == 'ingest':
        from .config import INGEST_WORKERS
        from .ingest import ingest_runs
        ingest_runs(client, runs, min(workers or 1, INGEST_WORKERS))
        return
    for run in runs:
        _stage_fn(stage)(client, run, **_stage_kwargs(stage, workers))


def _stage_steps(client, *, date_val, limit, workers, force) -> list[tuple[str, Callable[[], object]]]:
    from .config import SYNC_API_BASE_URL
    stages = ['download', 'compute', 'image', 'ingest'] + (['sync'] if SYNC_API_BASE_URL else [])
    if date_val:
        return [(s, (lambda s=s: _run_stage_for_date(client, s, date_val, workers, force))) for s in stages]
    return [(s, (lambda s=s: _sweep_stage(client, s, limit, workers))) for s in stages]


def pipeline_steps(client, *, date_val: date | None = None, limit: int = 10, workers: int = 4,
                   force: bool = False, init_days: int = 3, backfill: bool = False,
                   start: date | None = None) -> list[tuple[str, Callable[[], object]]]:
    """The `run` command's steps, in order, as (name, thunk) pairs — the one
    definition of the pipeline; LO/flows.py wraps each in a Prefect task."""
    from .downloader import check
    steps = [('check', lambda: check(client, init_days=init_days, backfill=backfill,
                                     start=start, date_override=date_val))]
    steps += _stage_steps(client, date_val=date_val, limit=limit, workers=workers, force=force)
    return steps


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

def cmd_check(args) -> None:
    from .downloader import check
    client = _get_client()
    try:
        found = check(client, init_days=args.init_days, backfill=args.backfill,
                      start=_parse_date(args.start) if args.start else None,
                      date_override=_parse_date(args.date) if args.date else None)
        logger.info('check: %d new run(s) queued', len(found))
    finally:
        client.close()


def _make_stage_cmd(stage: str):
    def cmd(args) -> None:
        client = _get_client()
        try:
            workers = getattr(args, 'workers', None)
            if args.date:
                _run_stage_for_date(client, stage, _parse_date(args.date), workers, args.force)
            else:
                _sweep_stage(client, stage, args.limit, workers)
        finally:
            client.close()
    return cmd


def cmd_run(args) -> None:
    client = _get_client()
    date_val = _parse_date(args.date) if args.date else None
    try:
        for name, step in pipeline_steps(
            client, date_val=date_val, limit=args.limit, workers=args.workers, force=args.force,
            init_days=args.init_days, backfill=args.backfill,
            start=_parse_date(args.start) if args.start else None,
        ):
            logger.info('=== %s%s ===', name, f' ({date_val})' if date_val else '')
            step()
    finally:
        client.close()


def cmd_status(args) -> None:
    from shared import liveocean_schema as los
    from .config import STATUS_TABLE
    client = _get_client()
    try:
        if args.date:
            where = f"run_date = toDate('{args.date}')"
        else:
            where = f"run_date >= toDate('{(datetime.utcnow().date() - timedelta(days=14)).isoformat()}')"
        rows = client.query(
            f'SELECT run_date, status, steps, attempts, error_message FROM {STATUS_TABLE} FINAL '
            f'WHERE {where} ORDER BY run_date DESC'
        ).result_rows
        owners = los.day_owners(client)
        if not rows:
            print('No status rows found.')
            return
        header = f"{'run':<12} {'status':<18} {'steps':>5} {'tries':>5}  {'owns days':<24} error"
        print(header)
        print('-' * len(header))
        for run_date, status, steps, attempts, err in rows:
            days = ','.join(d.strftime('%m-%d') for d, o in sorted(owners.items()) if o == run_date)
            print(f'{str(run_date):<12} {status:<18} {steps:>5} {attempts:>5}  {days:<24} {err[:50]}')
    finally:
        client.close()


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(prog='python -m LO.cli', description='LiveOcean pipeline CLI')
    sub = parser.add_subparsers(dest='cmd', required=True)

    def _add_date(p):
        p.add_argument('--date', metavar='YYYY-MM-DD', help='Target one run date only')

    def _add_limit(p, default=10):
        p.add_argument('--limit', type=int, default=default, metavar='N',
                       help=f'Max runs per step, 0 for unlimited (default {default})')

    def _add_workers(p, default=None):
        p.add_argument('--workers', type=int, default=default, metavar='N',
                       help='Worker processes for CPU-bound steps')

    def _add_force(p):
        p.add_argument('--force', action='store_true', help='Act regardless of current status')

    def _add_check_opts(p):
        p.add_argument('--init-days', type=int, default=3, metavar='N',
                       help='Days back to start from when no run is known (default 3)')
        p.add_argument('--backfill', action='store_true',
                       help='Probe the whole archive (from --from) instead of only newer runs')
        p.add_argument('--from', dest='start', metavar='YYYY-MM-DD',
                       help='First run date for --backfill (default: archive start)')

    p = sub.add_parser('check', help='Probe the archive and queue new runs')
    _add_date(p); _add_check_opts(p)
    p.set_defaults(func=cmd_check)

    for stage, help_ in (
        ('download', 'Fetch queued runs (HTTP range reads) into local NetCDF'),
        ('compute', 'TEOS-10 conversion + carbonate system (PyCO2SYS)'),
        ('image', 'Render WebP tiles'),
        ('ingest', 'Replace the run\'s valid days in ClickHouse'),
        ('sync', 'Push ingested runs to the API machine'),
    ):
        p = sub.add_parser(stage, help=help_)
        _add_date(p); _add_limit(p); _add_force(p)
        if stage in ('compute', 'image', 'ingest'):
            _add_workers(p)
        p.set_defaults(func=_make_stage_cmd(stage))

    p = sub.add_parser('run', help='check, then every stage in order')
    _add_date(p); _add_limit(p); _add_workers(p, default=4); _add_force(p); _add_check_opts(p)
    p.set_defaults(func=cmd_run)

    p = sub.add_parser('status', help='Print run status (default: last 14 days)')
    _add_date(p)
    p.set_defaults(func=cmd_status)

    args = parser.parse_args(argv)
    try:
        args.func(args)
    except PipelineError as e:
        logger.error('%s', e)
        sys.exit(1)


if __name__ == '__main__':
    main()
