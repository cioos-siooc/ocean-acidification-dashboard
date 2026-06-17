"""Home-side import logic for the SalishSeaCast sync stage.

A remote processing server (see process/SSC/sync.py) exports a date's
SalishSeaCast_hourly rows to a Native-format file, rsyncs it here under
SYNC_STAGING_DIR, then calls POST /admin/syncHourly with just the date.

This module is the source of truth for "has date X actually been committed"
— it must not trust the remote's own bookkeeping, since the remote can be
wrong (e.g. it inserted successfully here but never saw the HTTP response
due to a network blip, and would otherwise retry and double-insert).
SalishSeaCast_sync_log records what *this* server has actually done.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from .clickhouse_helpers import get_ch_client

logger = logging.getLogger('api.sync_hourly')

SYNC_STAGING_DIR = os.environ.get('SSC_SYNC_STAGING_DIR', '/opt/data/SalishSeaCast/sync_staging')
SYNC_API_TOKEN   = os.environ.get('SYNC_API_TOKEN', '')

# A 'syncing' claim older than this is treated as abandoned (e.g. the server
# crashed mid-import) and may be retried rather than permanently blocking.
_STALE_CLAIM_SECONDS = 30 * 60

_CREATE_SYNC_LOG = """
CREATE TABLE IF NOT EXISTS SalishSeaCast_sync_log (
    date       Date,
    status     LowCardinality(String),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY date
"""


class SyncConflict(Exception):
    """Date is already synced or a sync is already in progress (-> HTTP 409)."""


class SyncError(Exception):
    """Any other sync failure: bad date, missing file, insert failure (-> HTTP 400)."""


def ensure_schema(client) -> None:
    client.command(_CREATE_SYNC_LOG)


def _native_path(date_val: date) -> str:
    return os.path.join(SYNC_STAGING_DIR, f'{date_val.isoformat()}.native')


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _get_log_row(client, date_val: date) -> Optional[dict]:
    result = client.query(
        'SELECT status, updated_at FROM SalishSeaCast_sync_log FINAL WHERE date = %(d)s LIMIT 1',
        parameters={'d': date_val},
    )
    if not result.result_rows:
        return None
    status, updated_at = result.result_rows[0]
    return {'status': status, 'updated_at': updated_at}


def _set_log_status(client, date_val: date, status: str) -> None:
    client.insert(
        'SalishSeaCast_sync_log', [[date_val, status, _now()]],
        column_names=['date', 'status', 'updated_at'],
    )
    # Table is tiny (one row per date ever synced) — FINAL over the whole
    # table is cheap and keeps it readable for manual inspection.
    client.command('OPTIMIZE TABLE SalishSeaCast_sync_log FINAL')


def import_native_file(date_str: str) -> dict:
    """Claim, import and finish the sync for date_str.

    Raises SyncConflict if already synced / in progress, SyncError for any
    other failure. Returns a small summary dict on success.
    """
    try:
        date_val = datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError as exc:
        raise SyncError(f'Invalid date {date_str!r}: {exc}') from exc

    client = get_ch_client()
    ensure_schema(client)

    log_row = _get_log_row(client, date_val)
    if log_row:
        if log_row['status'] == 'success':
            raise SyncConflict(f'{date_val} already synced')
        if log_row['status'] == 'syncing':
            age_seconds = (_now() - log_row['updated_at']).total_seconds()
            if age_seconds < _STALE_CLAIM_SECONDS:
                raise SyncConflict(f'{date_val} sync already in progress')
            logger.warning('Stale syncing claim for %s (age %.0fs) — retrying',
                           date_val, age_seconds)

    _set_log_status(client, date_val, 'syncing')

    try:
        native_path = _native_path(date_val)
        if not os.path.exists(native_path):
            raise SyncError(f'Expected export file not found: {native_path}')

        start = datetime(date_val.year, date_val.month, date_val.day)
        end   = start + timedelta(days=1)
        existing = client.query(
            'SELECT count() FROM SalishSeaCast_hourly WHERE time >= %(start)s AND time < %(end)s',
            parameters={'start': start, 'end': end},
        ).result_rows[0][0]

        if existing:
            # Self-heal: data is already there even though our log didn't know it
            # (e.g. a manual insert, or a crash after insert but before this point).
            logger.info('%s already has %d hourly rows — skipping insert', date_val, existing)
        else:
            with open(native_path, 'rb') as f:
                data = f.read()
            client.raw_insert('SalishSeaCast_hourly', insert_block=data, fmt='Native')
            logger.info('Imported %s into SalishSeaCast_hourly from %s', date_val, native_path)

        _set_log_status(client, date_val, 'success')
        os.remove(native_path)
        return {'date': date_str, 'rows_existing': existing}

    except SyncError:
        _set_log_status(client, date_val, 'failed')
        raise
    except Exception as exc:
        _set_log_status(client, date_val, 'failed')
        raise SyncError(str(exc)) from exc
