"""Sync stage for the SalishSeaCast pipeline.

This pipeline runs on the PROCESS machine (heavy download/compute/image
work), while the canonical ClickHouse + WebP images live on the API
machine. Once a date finishes ingest, this stage:

  1. Exports that date's SalishSeaCast_hourly rows to a Native-format file.
  2. rsyncs the export file and the date's WebP image directories to the
     API machine.
  3. Calls the API machine to import the export file into its own ClickHouse.
  4. Repeats 1-3 for that date's SalishSeaCast_daily row(s) — the per-cell
     mean/min/max ingest.py derived from the same hourly NC read — against
     a separate native file and POST /admin/syncDaily.

Idempotency: the API machine maintains its own sync log and is the source of
truth for "has this date actually been committed" (see api/modules/sync_hourly.py)
— this stage's own pending_sync/success_sync status is only for the PROCESS
machine's own orchestration (what's left to push), not a correctness
guarantee. A 409 from the API machine means "already imported" and is
treated as success here.

Requires SYNC_API_DATA_DIR, SYNC_API_BASE_URL and SYNC_API_TOKEN to be
configured (see config.py) — left unset by default so a local-only
deployment never tries to sync to itself. SSH/rsync connect to the API
machine via `cloudflared access ssh` as a ProxyCommand (CF_TOKEN_ID /
CF_TOKEN_SECRET + API_SSH_HOST), not a persistent tunnel container.
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
    ALL_VARIABLES, IMAGE_BASE_DIR, API_SSH_HOST, API_SSH_KEY_PATH,
    API_SSH_KNOWN_HOSTS, API_SSH_USER, CF_TOKEN_ID, CF_TOKEN_SECRET,
    SYNC_API_TOKEN, SYNC_API_BASE_URL, SYNC_API_DATA_DIR, SYNC_STAGING_DIR,
    STATUS_FAILED_SYNC, STATUS_SUCCESS_SYNC, STATUS_SYNCING,
)
from .db import get_dates_pending_sync, get_row, mark_failed, mark_running, mark_success

logger = logging.getLogger('SalishSeaCast.sync')

# All of NC_BASE_DIR / IMAGE_BASE_DIR / SYNC_STAGING_DIR live under this root
# by convention (see CLAUDE.md storage layout) — used to translate a local
# /opt/data/... path to the equivalent path on the API machine's filesystem.
_CONTAINER_DATA_ROOT = '/opt/data'

_API_TARGET = f'{API_SSH_USER}@{API_SSH_HOST}'


def _require_configured() -> None:
    missing = [
        name for name, val in (
            ('SYNC_API_DATA_DIR', SYNC_API_DATA_DIR),
            ('SYNC_API_BASE_URL', SYNC_API_BASE_URL),
            ('SYNC_API_TOKEN', SYNC_API_TOKEN),
        ) if not val
    ]
    if missing:
        raise RuntimeError(f'Sync is not configured: missing {", ".join(missing)}')


def _ssh_opts_parts() -> list[str]:
    # BatchMode=yes: fail fast instead of hanging on an unattended password
    # prompt. StrictHostKeyChecking=accept-new: auto-trust a host's key on
    # first connect (no interactive yes/no prompt) but still reject if it
    # ever changes later. ProxyCommand routes the connection through
    # Cloudflare Access via `cloudflared access ssh` (service-token auth) —
    # `cloudflared access tcp`'s listener mode 400s/bad-handshakes against
    # this Access application, so this is a per-invocation subprocess instead
    # of a persistent tunnel container.
    proxy_command = (
        f'cloudflared access ssh --hostname {API_SSH_HOST} '
        f'--service-token-id {CF_TOKEN_ID} --service-token-secret {CF_TOKEN_SECRET}'
    )
    parts = [
        'ssh',
        '-o', 'BatchMode=yes',
        '-o', 'StrictHostKeyChecking=accept-new',
        '-o', f'ProxyCommand={proxy_command}',
    ]
    if API_SSH_KEY_PATH and os.path.exists(API_SSH_KEY_PATH):
        parts += ['-i', API_SSH_KEY_PATH]
    else:
        logger.warning(
            'API_SSH_KEY_PATH not found at %s; rsync may prompt/fail if '
            'passwordless auth is not already configured.', API_SSH_KEY_PATH,
        )
    if API_SSH_KNOWN_HOSTS:
        parts += ['-o', f'UserKnownHostsFile={API_SSH_KNOWN_HOSTS}']
    return parts


def _api_path(local_path: str) -> str:
    """Map a local /opt/data/... path to its equivalent on the API machine."""
    rel = os.path.relpath(local_path, _CONTAINER_DATA_ROOT)
    return os.path.join(SYNC_API_DATA_DIR, rel)


def _ssh_exec(remote_cmd: str) -> None:
    ssh_opts_parts = _ssh_opts_parts()
    proc = subprocess.run(
        ssh_opts_parts + [_API_TARGET, remote_cmd],
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

def _export_native(client, date_val: date, out_path: str) -> int:
    """Export this date's SalishSeaCast_hourly rows. Returns the row count —
    sent to the API machine alongside the file so it can verify its import
    actually completed instead of just checking "any rows exist" (a partial
    upload that dies mid-transfer can leave a nonzero-but-incomplete count)."""
    start = datetime(date_val.year, date_val.month, date_val.day)
    end   = start + timedelta(days=1)
    params = {'start': start, 'end': end}
    row_count = client.query(
        'SELECT count() FROM SalishSeaCast_hourly WHERE time >= %(start)s AND time < %(end)s',
        parameters=params,
    ).result_rows[0][0]
    data = client.raw_query(
        'SELECT * FROM SalishSeaCast_hourly WHERE time >= %(start)s AND time < %(end)s',
        parameters=params,
        fmt='Native',
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'wb') as f:
        f.write(data)
    logger.info('Exported %s (%d bytes, %d rows) for %s', out_path, len(data), row_count, date_val)
    return row_count


def _daily_native_path(date_val: date) -> str:
    return os.path.join(SYNC_STAGING_DIR, f'{date_val.isoformat()}.daily.native')


def _export_native_daily(client, date_val: date, out_path: str) -> int:
    """Export this date's SalishSeaCast_daily row(s) — written by ingest.py's
    per-cell mean/min/max accumulation, one row set per date (time is a Date
    column there, not DateTime), so an equality match is enough. Returns the
    row count, same purpose as _export_native's."""
    params = {'d': date_val}
    row_count = client.query(
        'SELECT count() FROM SalishSeaCast_daily WHERE time = %(d)s',
        parameters=params,
    ).result_rows[0][0]
    data = client.raw_query(
        'SELECT * FROM SalishSeaCast_daily WHERE time = %(d)s',
        parameters=params,
        fmt='Native',
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'wb') as f:
        f.write(data)
    logger.info('Exported %s (%d bytes, %d rows) for %s', out_path, len(data), row_count, date_val)
    return row_count


# ---------------------------------------------------------------------------
# Transfer
# ---------------------------------------------------------------------------

def _rsync_native_file(local_path: str) -> None:
    dest_path = _api_path(local_path)
    _ssh_exec(f'mkdir -p {shlex.quote(os.path.dirname(dest_path))}')
    ssh_opts = shlex.join(_ssh_opts_parts())
    # --checksum: the local export is rewritten fresh (new mtime) on every
    # retry even when the underlying data hasn't changed, so the default
    # size+mtime quick-check can't tell it's identical to what's already on
    # the API machine — without this, a retry re-sends the whole multi-GB
    # file every time. --partial keeps whatever did transfer so an
    # interrupted attempt (e.g. a dropped tunnel mid-transfer) can resume
    # instead of restarting from zero.
    _rsync(['-az', '--checksum', '--partial', '-e', ssh_opts, local_path, f'{_API_TARGET}:{dest_path}'])


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
    dest_root = _api_path(image_base_dir)
    _ssh_exec(f'mkdir -p {shlex.quote(dest_root)}')
    ssh_opts = shlex.join(_ssh_opts_parts())
    _rsync(
        ['-az', '--partial', '--relative', '--files-from=-', '-e', ssh_opts,
         f'{image_base_dir}/', f'{_API_TARGET}:{dest_root}/'],
        # --files-from only copies a listed directory's *contents* if the
        # entry has a trailing slash — without it, rsync creates the empty
        # directory and stops (confirmed: 192 empty dirs, 0 files).
        input_text='\n'.join(f'{d}/' for d in rel_dirs) + '\n',
    )
    logger.info('rsynced %d image directories for %s', len(rel_dirs), date_val)


# ---------------------------------------------------------------------------
# API machine notification
# ---------------------------------------------------------------------------

def _notify_api(date_val: date, expected_rows: int, endpoint: str = '/admin/syncHourly') -> None:
    url = f'{SYNC_API_BASE_URL.rstrip("/")}{endpoint}'
    resp = requests.post(
        url,
        json={'date': date_val.isoformat(), 'expected_rows': expected_rows},
        headers={'Authorization': f'Bearer {SYNC_API_TOKEN}'},
        # Must exceed the API's own ClickHouse client timeout
        # (api/modules/clickhouse_helpers.py's _SEND_RECEIVE_TIMEOUT) — otherwise
        # this request gives up first and reports a false failure for a
        # multi-GB insert that's still legitimately in progress server-side.
        timeout=1800,
    )
    if resp.status_code == 409:
        logger.info('API machine already has %s synced via %s (409) — treating as success', date_val, endpoint)
        return
    if not resp.ok:
        # raise_for_status()'s own exception text doesn't include the response
        # body, so the actual SyncError detail (api/SERVER.py's HTTPException)
        # would otherwise be lost — log it explicitly before raising.
        logger.error('API machine rejected %s via %s (%d): %s', date_val, endpoint, resp.status_code, resp.text)
    resp.raise_for_status()


def _delete_local_hourly(client, date_val: date) -> None:
    """Delete date_val's rows from this machine's OWN SalishSeaCast_hourly.

    Only called after the API machine has confirmed it already has the data
    (post _notify_api). Without this, the local table accumulates every
    synced date forever — SalishSeaCast_hourly's ORDER BY (gridX, gridY,
    depth, time) doesn't have `time` as a leading key, so _export_native's
    per-date WHERE-time scan gets slower as a partition (one calendar month)
    fills up with old, already-synced days it still has to scan past.
    """
    start = datetime(date_val.year, date_val.month, date_val.day)
    end   = start + timedelta(days=1)
    # ALTER ... DELETE (not the newer lightweight DELETE FROM) for
    # compatibility with any ClickHouse version, not just ones with that
    # feature enabled.
    client.command(
        'ALTER TABLE SalishSeaCast_hourly DELETE WHERE time >= %(start)s AND time < %(end)s',
        parameters={'start': start, 'end': end},
    )
    logger.info('Deleted local SalishSeaCast_hourly rows for %s (API machine already has them)', date_val)


def _delete_local_daily(client, date_val: date) -> None:
    """Delete date_val's rows from this machine's OWN SalishSeaCast_daily.

    Same rationale as _delete_local_hourly: keeps the local table from
    growing forever with data the API machine already has.
    """
    client.command(
        'ALTER TABLE SalishSeaCast_daily DELETE WHERE time = %(d)s',
        parameters={'d': date_val},
    )
    logger.info('Deleted local SalishSeaCast_daily rows for %s (API machine already has them)', date_val)


# ---------------------------------------------------------------------------
# Core sync
# ---------------------------------------------------------------------------

def sync_date(client, date_val: date, image_base_dir: str = IMAGE_BASE_DIR) -> bool:
    """Export, transfer and trigger the API machine's import for date_val. Returns success."""
    _require_configured()

    for var in ALL_VARIABLES:
        row = get_row(client, date_val, var) or {}
        mark_running(client, date_val, var, STATUS_SYNCING, row.get('attempts', 0))

    native_path = os.path.join(SYNC_STAGING_DIR, f'{date_val.isoformat()}.native')
    daily_native_path = _daily_native_path(date_val)
    try:
        hourly_rows = _export_native(client, date_val, native_path)
        _rsync_native_file(native_path)
        _rsync_images(date_val, image_base_dir)
        _notify_api(date_val, hourly_rows)

        try:
            _delete_local_hourly(client, date_val)
        except Exception:
            # Sync itself already succeeded (API machine confirmed import) —
            # don't fail/retry the whole date over a cleanup error, that
            # would force a needless multi-GB re-export and re-transfer.
            logger.exception(
                'Synced %s but failed to delete its local SalishSeaCast_hourly rows — '
                'needs manual cleanup, will not be retried automatically', date_val,
            )

        daily_rows = _export_native_daily(client, date_val, daily_native_path)
        _rsync_native_file(daily_native_path)
        _notify_api(date_val, daily_rows, endpoint='/admin/syncDaily')

        try:
            _delete_local_daily(client, date_val)
        except Exception:
            logger.exception(
                'Synced %s but failed to delete its local SalishSeaCast_daily rows — '
                'needs manual cleanup, will not be retried automatically', date_val,
            )

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
        if os.path.exists(daily_native_path):
            os.remove(daily_native_path)


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
