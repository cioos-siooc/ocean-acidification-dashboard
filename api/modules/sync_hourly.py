"""Home-side import logic for the SalishSeaCast sync stage.

A remote processing server (see process/SSC/sync.py) exports a date's
SalishSeaCast_hourly and SalishSeaCast_daily rows to Native-format files,
rsyncs them here under SYNC_STAGING_DIR, then calls POST /admin/syncHourly
and POST /admin/syncDaily (respectively) with just the date.

This module is the source of truth for "has date X actually been committed"
— it must not trust the remote's own bookkeeping, since the remote can be
wrong (e.g. it inserted successfully here but never saw the HTTP response
due to a network blip, and would otherwise retry and double-insert).
SalishSeaCast_sync_log / SalishSeaCast_daily_sync_log record what *this*
server has actually done — one log table per data table.
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

_CREATE_DAILY_SYNC_LOG = """
CREATE TABLE IF NOT EXISTS SalishSeaCast_daily_sync_log (
    date       Date,
    status     LowCardinality(String),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY date
"""

# Same 8 variables / mean-min-max layout as process/SSC/db.py and the
# historical bulk-load table this feeds (clickhouse_test/scripts/SSC.py).
# Duplicated here rather than imported because api/ and process/ are
# separate deployables with their own dependency trees (see CLAUDE.md).
_DAILY_VARIABLES = [
    'temperature', 'salinity', 'total_alkalinity', 'dissolved_oxygen',
    'dissolved_inorganic_carbon', 'ph_total', 'omega_arag', 'omega_cal',
]
_DAILY_VARIABLE_COLS = '\n    '.join(
    f'{var}_{stat} Float32 CODEC(Gorilla, ZSTD(4)),'
    for var in _DAILY_VARIABLES for stat in ('mean', 'min', 'max')
)

# IF NOT EXISTS: SalishSeaCast_daily already exists on deployments that ran
# the historical bulk-load script — this is a no-op there, and a safety net
# on a fresh ClickHouse instance that's never seen it.
_CREATE_DAILY_DATA = f"""
CREATE TABLE IF NOT EXISTS SalishSeaCast_daily (
    time     Date      CODEC(DoubleDelta, ZSTD(4)),
    depth    Float32   CODEC(Gorilla, ZSTD(4)),
    gridX    UInt16    CODEC(DoubleDelta, ZSTD(4)),
    gridY    UInt16    CODEC(DoubleDelta, ZSTD(4)),
    {_DAILY_VARIABLE_COLS}
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (gridX, gridY, time, depth)
"""


class SyncConflict(Exception):
    """Date is already synced or a sync is already in progress (-> HTTP 409)."""


class SyncError(Exception):
    """Any other sync failure: bad date, missing file, insert failure (-> HTTP 400)."""


def ensure_schema(client) -> None:
    client.command(_CREATE_SYNC_LOG)
    client.command(_CREATE_DAILY_SYNC_LOG)
    client.command(_CREATE_DAILY_DATA)


def _native_path(date_val: date, suffix: str = '') -> str:
    return os.path.join(SYNC_STAGING_DIR, f'{date_val.isoformat()}{suffix}.native')


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _parse_date(date_str: str) -> date:
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError as exc:
        raise SyncError(f'Invalid date {date_str!r}: {exc}') from exc


def _get_log_row(client, log_table: str, date_val: date) -> Optional[dict]:
    result = client.query(
        f'SELECT status, updated_at FROM {log_table} FINAL WHERE date = %(d)s LIMIT 1',
        parameters={'d': date_val},
    )
    if not result.result_rows:
        return None
    status, updated_at = result.result_rows[0]
    return {'status': status, 'updated_at': updated_at}


def _set_log_status(client, log_table: str, date_val: date, status: str) -> None:
    client.insert(
        log_table, [[date_val, status, _now()]],
        column_names=['date', 'status', 'updated_at'],
    )
    # Table is tiny (one row per date ever synced) — FINAL over the whole
    # table is cheap and keeps it readable for manual inspection.
    client.command(f'OPTIMIZE TABLE {log_table} FINAL')


def _claim_and_import(
    client, date_val: date, *, log_table: str, data_table: str,
    native_path: str, existing_check_sql: str, existing_check_params: dict,
) -> int:
    """Claim date_val in log_table, import native_path into data_table unless
    it's already there, and record the outcome.

    Shared by the hourly and daily sync paths below — same claim/import/
    finish skeleton, differing only in which table/log/file/existence-check
    each operates on. Returns the pre-existing row count (0 on a fresh
    import). Raises SyncConflict / SyncError.
    """
    log_row = _get_log_row(client, log_table, date_val)
    if log_row:
        if log_row['status'] == 'success':
            raise SyncConflict(f'{date_val} already synced into {data_table}')
        if log_row['status'] == 'syncing':
            age_seconds = (_now() - log_row['updated_at']).total_seconds()
            if age_seconds < _STALE_CLAIM_SECONDS:
                raise SyncConflict(f'{date_val} sync already in progress for {data_table}')
            logger.warning('Stale syncing claim for %s/%s (age %.0fs) — retrying',
                           data_table, date_val, age_seconds)

    _set_log_status(client, log_table, date_val, 'syncing')
    try:
        if not os.path.exists(native_path):
            raise SyncError(f'Expected export file not found: {native_path}')

        existing = client.query(existing_check_sql, parameters=existing_check_params).result_rows[0][0]
        if existing:
            # Self-heal: data is already there even though our log didn't know it
            # (e.g. a manual insert, or a crash after insert but before this point).
            logger.info('%s already has %d rows for %s — skipping insert', data_table, existing, date_val)
        else:
            with open(native_path, 'rb') as f:
                data = f.read()
            client.raw_insert(data_table, insert_block=data, fmt='Native')
            logger.info('Imported %s into %s from %s', date_val, data_table, native_path)

        _set_log_status(client, log_table, date_val, 'success')
        os.remove(native_path)
        return existing
    except SyncError:
        _set_log_status(client, log_table, date_val, 'failed')
        raise
    except Exception as exc:
        _set_log_status(client, log_table, date_val, 'failed')
        raise SyncError(str(exc)) from exc


def import_native_file(date_str: str) -> dict:
    """Claim, import and finish the SalishSeaCast_hourly sync for date_str.

    Raises SyncConflict if already synced / in progress, SyncError for any
    other failure. Returns a small summary dict on success.
    """
    date_val = _parse_date(date_str)
    client = get_ch_client()
    ensure_schema(client)

    start = datetime(date_val.year, date_val.month, date_val.day)
    end   = start + timedelta(days=1)
    existing = _claim_and_import(
        client, date_val,
        log_table='SalishSeaCast_sync_log',
        data_table='SalishSeaCast_hourly',
        native_path=_native_path(date_val),
        existing_check_sql='SELECT count() FROM SalishSeaCast_hourly WHERE time >= %(start)s AND time < %(end)s',
        existing_check_params={'start': start, 'end': end},
    )
    return {'date': date_str, 'rows_existing': existing}


def import_daily_native_file(date_str: str) -> dict:
    """Claim, import and finish the SalishSeaCast_daily sync for date_str.

    Same contract as import_native_file, against the daily mean/min/max
    table instead (time is a Date column there, so the existence check is
    an equality match rather than a range).
    """
    date_val = _parse_date(date_str)
    client = get_ch_client()
    ensure_schema(client)

    existing = _claim_and_import(
        client, date_val,
        log_table='SalishSeaCast_daily_sync_log',
        data_table='SalishSeaCast_daily',
        native_path=_native_path(date_val, suffix='.daily'),
        existing_check_sql='SELECT count() FROM SalishSeaCast_daily WHERE time = %(d)s',
        existing_check_params={'d': date_val},
    )
    return {'date': date_str, 'rows_existing': existing}
