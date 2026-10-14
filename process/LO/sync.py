"""Push an ingested LiveOcean run to the API machine.

Same transport as SSC/sync.py (whose SSH/rsync helpers this reuses): Native
exports + WebP tiles rsynced through `cloudflared access ssh`, then
POST /admin/syncLiveOcean to import. Per valid day the run owns:

  1. export its LiveOcean_4h partition (and LiveOcean_daily, if any),
  2. rsync those files plus the day's tile directories,
  3. ask the API to import them — it replaces the day's partitions under the
     same latest-wins rule (a 409 means it already has the day from this or
     a newer run, which counts as success),
  4. drop the day's local partitions and tiles.

The grid is pushed alongside (rsync --checksum makes that free once the API
has it; the API skips the import when its row count already matches).
"""
from __future__ import annotations

import logging
import os
import shlex
import shutil
from datetime import date, datetime, timedelta

import requests

from SSC.sync import _api_path, _require_configured, _rsync, _rsync_native_file, _ssh_exec, _ssh_opts_parts, _API_TARGET
from shared import liveocean_schema as los

from .config import (
    ALL_VARIABLES, DAILY_TABLE, DATA_TABLE, GRID_TABLE, IMAGE_BASE_DIR, STEP_HOURS,
    STATUS_FAILED_SYNC, STATUS_SUCCESS_SYNC, STATUS_SYNCING,
    SYNC_API_BASE_URL, SYNC_API_TOKEN, SYNC_STAGING_DIR,
)
from .db import pending_runs, set_status
from .imaging import dt_folder

logger = logging.getLogger('LiveOcean.sync')

ENDPOINT = '/admin/syncLiveOcean'


def _export(client, sql: str, params: dict, out_path: str) -> int:
    rows = client.query(f'SELECT count() FROM ({sql})', parameters=params).result_rows[0][0]
    data = client.raw_query(sql, parameters=params, fmt='Native')
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'wb') as f:
        f.write(data)
    logger.info('Exported %s (%d bytes, %d rows)', out_path, len(data), rows)
    return rows


def _notify(payload: dict) -> None:
    resp = requests.post(
        f'{SYNC_API_BASE_URL.rstrip("/")}{ENDPOINT}', json=payload,
        headers={'Authorization': f'Bearer {SYNC_API_TOKEN}'},
        timeout=1800,  # > the API's ClickHouse send/receive timeout, see SSC/sync.py
    )
    if resp.status_code == 409:
        logger.info('API already has %s — treating as success (%s)', payload, resp.text)
        return
    if not resp.ok:
        logger.error('API rejected %s (%d): %s', payload, resp.status_code, resp.text)
    resp.raise_for_status()


def _day_image_dirs(day: date, image_dir: str) -> list[str]:
    """Relative tile directories of `day` (every 4-hourly step, every variable)."""
    start = datetime(day.year, day.month, day.day)
    folders = [dt_folder((start + timedelta(hours=h) - datetime(1970, 1, 1)).total_seconds())
               for h in range(0, 24, STEP_HOURS)]
    return [os.path.join(var, f) for var in ALL_VARIABLES for f in folders
            if os.path.isdir(os.path.join(image_dir, var, f))]


def _rsync_images(rel_dirs: list[str], image_dir: str) -> None:
    if not rel_dirs:
        return
    dest_root = _api_path(image_dir)
    _ssh_exec(f'mkdir -p {shlex.quote(dest_root)}')
    _rsync(['-az', '--partial', '--relative', '--files-from=-', '-e', shlex.join(_ssh_opts_parts()),
            f'{image_dir}/', f'{_API_TARGET}:{dest_root}/'],
           input_text='\n'.join(f'{d}/' for d in rel_dirs) + '\n')


def sync_grid(client) -> None:
    path = os.path.join(SYNC_STAGING_DIR, f'{GRID_TABLE}.native')
    rows = _export(client, f'SELECT * FROM {GRID_TABLE}', {}, path)
    _rsync_native_file(path)
    _notify({'kind': 'grid', 'expected_rows': rows})


def sync_day(client, day: date, run_date: date, image_dir: str) -> None:
    local = client.query(f'SELECT count() FROM {DATA_TABLE} WHERE toDate(time) = %(d)s',
                         parameters={'d': day}).result_rows[0][0]
    if not local:
        # Already pushed and dropped by an earlier (partly failed) attempt.
        logger.info('%s (run %s) has no local rows left — already synced', day, run_date)
        return
    files = []
    try:
        hourly = os.path.join(SYNC_STAGING_DIR, f'{day.isoformat()}.4h.native')
        n = _export(client, f'SELECT * FROM {DATA_TABLE} WHERE toDate(time) = %(d)s', {'d': day}, hourly)
        files.append(hourly)
        _rsync_native_file(hourly)
        daily = os.path.join(SYNC_STAGING_DIR, f'{day.isoformat()}.daily.native')
        n_daily = _export(client, f'SELECT * FROM {DAILY_TABLE} WHERE time = %(d)s', {'d': day}, daily)
        files.append(daily)
        if n_daily:
            _rsync_native_file(daily)
        rel_dirs = _day_image_dirs(day, image_dir)
        _rsync_images(rel_dirs, image_dir)
        _notify({'kind': 'day', 'date': day.isoformat(), 'run_date': run_date.isoformat(),
                 'expected_rows': n, 'expected_daily_rows': n_daily})
    finally:
        for f in files:
            if os.path.exists(f):
                os.remove(f)

    # The API has the day now: drop the local copy (the LiveOcean_days row
    # stays — it's what keeps older runs from re-claiming the day).
    for table in (DATA_TABLE, DAILY_TABLE):
        try:
            los.drop_day(client, table, day)
        except Exception:
            logger.exception('Synced %s but could not drop its local %s partition', day, table)
    for rel in rel_dirs:
        shutil.rmtree(os.path.join(image_dir, rel), ignore_errors=True)
    logger.info('Synced %s (run %s): %d rows, %d daily rows, %d tile dirs',
                day, run_date, n, n_daily, len(rel_dirs))


def sync(client, run: dict, image_dir: str = IMAGE_BASE_DIR) -> bool:
    _require_configured()
    run_date = run['run_date']
    set_status(client, run_date, STATUS_SYNCING, attempts=run['attempts'] + 1)
    try:
        sync_grid(client)
        days = [d for d, owner in los.day_owners(client).items() if owner == run_date]
        for day in sorted(days):
            sync_day(client, day, run_date, image_dir)
    except Exception as exc:
        logger.exception('Sync failed for run %s', run_date)
        set_status(client, run_date, STATUS_FAILED_SYNC, error_message=str(exc))
        return False
    set_status(client, run_date, STATUS_SUCCESS_SYNC, attempts=0)
    _remove_nc(run)
    return True


def _remove_nc(run: dict) -> None:
    path = run.get('nc_path')
    if path and os.path.exists(path) and os.getenv('LO_KEEP_NC', '').lower() not in ('1', 'true', 'yes'):
        os.remove(path)
        logger.info('Removed %s', path)


def process_pending_syncs(client, limit: int = 10, image_dir: str = IMAGE_BASE_DIR) -> None:
    runs = pending_runs(client, 'sync', limit)
    if not runs:
        logger.info('No pending LiveOcean sync jobs')
    # Oldest first here: a newer run's import must land last on the API side.
    for run in sorted(runs, key=lambda r: r['run_date']):
        sync(client, run, image_dir)
