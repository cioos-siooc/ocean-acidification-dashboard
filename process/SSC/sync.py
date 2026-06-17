"""Sync stage for the SalishSeaCast pipeline.

This pipeline may run on a remote server (heavy download/compute/image work),
while the canonical ClickHouse + WebP images live on a home server. Once a
date finishes ingest, this stage:

  1. Exports that date's SalishSeaCast_hourly rows to a Native-format file.
  2. rsyncs the export file and the date's WebP image directories home.
  3. Calls the home API to import the export file into its own ClickHouse.

Idempotency: the home side maintains its own sync log and is the source of
truth for "has this date actually been committed" (see api/modules/sync_hourly.py)
— this stage's own pending_sync/success_sync status is only for the remote's
own orchestration (what's left to push), not a correctness guarantee. A 409
from home means "already imported" and is treated as success here.

Requires SYNC_HOME_SSH_TARGET, SYNC_HOME_DATA_DIR, SYNC_HOME_API_BASE and
SYNC_API_TOKEN to be configured (see config.py) — left unset by default so a
local-only deployment never tries to sync to itself.
"""
from __future__ import annotations

import fnmatch
import logging
import os
import shlex
import subprocess
from datetime import date, datetime, timedelta

import requests

from .config import (
    ALL_VARIABLES, IMAGE_BASE_DIR, SYNC_API_TOKEN, SYNC_HOME_API_BASE,
    SYNC_HOME_DATA_DIR, SYNC_HOME_SSH_TARGET, SYNC_SSH_KEY, SYNC_SSH_PORT,
    SYNC_STAGING_DIR,
    STATUS_FAILED_SYNC, STATUS_SUCCESS_SYNC, STATUS_SYNCING,
)
from .db import get_dates_pending_sync, get_row, mark_failed, mark_running, mark_success

logger = logging.getLogger('SalishSeaCast.sync')

# All of NC_BASE_DIR / IMAGE_BASE_DIR / SYNC_STAGING_DIR live under this root
# by convention (see CLAUDE.md storage layout) — used to translate a local
# /opt/data/... path to the equivalent path on the home host's filesystem.
_CONTAINER_DATA_ROOT = '/opt/data'


def _require_configured() -> None:
    missing = [
        name for name, val in (
            ('SYNC_HOME_SSH_TARGET', SYNC_HOME_SSH_TARGET),
            ('SYNC_HOME_DATA_DIR', SYNC_HOME_DATA_DIR),
            ('SYNC_HOME_API_BASE', SYNC_HOME_API_BASE),
            ('SYNC_API_TOKEN', SYNC_API_TOKEN),
        ) if not val
    ]
    if missing:
        raise RuntimeError(f'Sync is not configured: missing {", ".join(missing)}')


def _ssh_args() -> list[str]:
    args = ['-p', str(SYNC_SSH_PORT)]
    if SYNC_SSH_KEY:
        args += ['-i', SYNC_SSH_KEY]
    return args


def _home_path(local_path: str) -> str:
    """Map a local /opt/data/... path to its equivalent on the home host."""
    rel = os.path.relpath(local_path, _CONTAINER_DATA_ROOT)
    return os.path.join(SYNC_HOME_DATA_DIR, rel)


def _ssh_exec(remote_cmd: str) -> None:
    proc = subprocess.run(
        ['ssh', *_ssh_args(), SYNC_HOME_SSH_TARGET, remote_cmd],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f'ssh command failed (exit {proc.returncode}): {proc.stderr.strip()}')


def _rsync(args: list[str], input_text: str = None) -> None:
    proc = subprocess.run(
        ['rsync', *args], input=input_text, capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f'rsync failed (exit {proc.returncode}): {proc.stderr.strip()}')


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def _export_native(client, date_val: date, out_path: str) -> None:
    start = datetime(date_val.year, date_val.month, date_val.day)
    end   = start + timedelta(days=1)
    data = client.raw_query(
        'SELECT * FROM SalishSeaCast_hourly WHERE time >= %(start)s AND time < %(end)s',
        parameters={'start': start, 'end': end},
        fmt='Native',
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'wb') as f:
        f.write(data)
    logger.info('Exported %s (%d bytes) for %s', out_path, len(data), date_val)


# ---------------------------------------------------------------------------
# Transfer
# ---------------------------------------------------------------------------

def _rsync_native_file(local_path: str) -> None:
    dest_path = _home_path(local_path)
    _ssh_exec(f'mkdir -p {shlex.quote(os.path.dirname(dest_path))}')
    _rsync(['-az', '-e', f'ssh {" ".join(_ssh_args())}', local_path, f'{SYNC_HOME_SSH_TARGET}:{dest_path}'])


def _date_image_dirs(date_val: date, image_base_dir: str) -> list[str]:
    """Relative paths (under image_base_dir) of every per-hour image dir for date_val."""
    pattern = date_val.strftime('%Y-%m-%d') + 'T*'
    dirs = []
    for var in ALL_VARIABLES:
        var_dir = os.path.join(image_base_dir, var)
        if not os.path.isdir(var_dir):
            continue
        for name in sorted(os.listdir(var_dir)):
            if fnmatch.fnmatch(name, pattern) and os.path.isdir(os.path.join(var_dir, name)):
                dirs.append(os.path.join(var, name))
    return dirs


def _rsync_images(date_val: date, image_base_dir: str) -> None:
    rel_dirs = _date_image_dirs(date_val, image_base_dir)
    if not rel_dirs:
        logger.warning('No image directories found for %s — skipping image rsync', date_val)
        return
    dest_root = _home_path(image_base_dir)
    _ssh_exec(f'mkdir -p {shlex.quote(dest_root)}')
    _rsync(
        ['-az', '--relative', '--files-from=-', '-e', f'ssh {" ".join(_ssh_args())}',
         f'{image_base_dir}/', f'{SYNC_HOME_SSH_TARGET}:{dest_root}/'],
        input_text='\n'.join(rel_dirs) + '\n',
    )
    logger.info('rsynced %d image directories for %s', len(rel_dirs), date_val)


# ---------------------------------------------------------------------------
# Home notification
# ---------------------------------------------------------------------------

def _notify_home(date_val: date) -> None:
    url = f'{SYNC_HOME_API_BASE.rstrip("/")}/admin/syncHourly'
    resp = requests.post(
        url,
        json={'date': date_val.isoformat()},
        headers={'Authorization': f'Bearer {SYNC_API_TOKEN}'},
        timeout=600,
    )
    if resp.status_code == 409:
        logger.info('Home already has %s synced (409) — treating as success', date_val)
        return
    resp.raise_for_status()


# ---------------------------------------------------------------------------
# Core sync
# ---------------------------------------------------------------------------

def sync_date(client, date_val: date, image_base_dir: str = IMAGE_BASE_DIR) -> bool:
    """Export, transfer and trigger the home import for date_val. Returns success."""
    _require_configured()

    for var in ALL_VARIABLES:
        row = get_row(client, date_val, var) or {}
        mark_running(client, date_val, var, STATUS_SYNCING, row.get('attempts', 0))

    native_path = os.path.join(SYNC_STAGING_DIR, f'{date_val.isoformat()}.native')
    try:
        _export_native(client, date_val, native_path)
        _rsync_native_file(native_path)
        _rsync_images(date_val, image_base_dir)
        _notify_home(date_val)

        for var in ALL_VARIABLES:
            row = get_row(client, date_val, var) or {}
            mark_success(client, date_val, var, STATUS_SUCCESS_SYNC, attempts=row.get('attempts', 0))
        logger.info('Sync complete for %s', date_val)
        return True
    except Exception as exc:
        logger.exception('Sync failed for %s', date_val)
        for var in ALL_VARIABLES:
            row = get_row(client, date_val, var) or {}
            mark_failed(client, date_val, var, STATUS_FAILED_SYNC, str(exc), attempts=row.get('attempts', 0))
        return False
    finally:
        if os.path.exists(native_path):
            os.remove(native_path)


# ---------------------------------------------------------------------------
# Batch processor
# ---------------------------------------------------------------------------

def process_pending_syncs(client, limit: int = 5, image_base_dir: str = IMAGE_BASE_DIR) -> None:
    dates = get_dates_pending_sync(client, limit=limit)
    if not dates:
        logger.info('No pending sync jobs')
        return
    for date_val in dates:
        sync_date(client, date_val, image_base_dir=image_base_dir)
