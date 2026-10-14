"""API-side import for the LiveOcean sync stage (process/LO/sync.py).

The PROCESS machine rsyncs a valid day's Native exports into
LO_SYNC_STAGING_DIR and calls POST /admin/syncLiveOcean. Unlike
SalishSeaCast, a day is synced more than once: every new forecast run
re-sends the days it covers. So a day is never "done" — it's owned by a run
(LiveOcean_days, shared/liveocean_schema.py), and an import:

  * is refused (409) when this server already holds the day from a newer run,
    or from the same run with the same row counts (a retried request);
  * otherwise loads the files into the staging tables, checks the row counts
    against what PROCESS exported, and swaps the day's partitions in with
    REPLACE PARTITION — readers see the old day or the new one, never a mix;
  * then drops any on-demand daily/monthly tiles rendered from the old data
    (GET /png caches those to disk; the 4-hourly tiles were just rsynced).

The grid (grid_LO) comes the same way, imported only when its row count
differs from what's here.
"""
from __future__ import annotations

import logging
import os
import shutil
from datetime import date

from shared import liveocean_schema as los

from .clickhouse_helpers import get_ch_client
from .sync_hourly import SyncConflict, SyncError, _read_in_chunks

logger = logging.getLogger('api.sync_liveocean')

STAGING_DIR = os.environ.get('LO_SYNC_STAGING_DIR', '/opt/data/LiveOcean/sync_staging')
IMAGE_DIR = os.environ.get('LO_IMAGE_DIR', '/opt/data/LiveOcean/images')


def _path(name: str) -> str:
    path = os.path.join(STAGING_DIR, name)
    if not os.path.exists(path):
        raise SyncError(f'Expected export file not found: {path}')
    return path


def _load(client, table: str, path: str) -> None:
    # insert_deduplicate=0 for the same reason as sync_hourly: a re-sent day
    # can contain blocks identical to ones already inserted once.
    client.raw_insert(table, insert_block=_read_in_chunks(path), fmt='Native',
                      settings={'insert_deduplicate': 0})


def import_grid(expected_rows: int) -> dict:
    client = get_ch_client()
    los.ensure_schema(client)
    have = client.query(f'SELECT count() FROM {los.GRID_TABLE}').result_rows[0][0]
    if have == expected_rows:
        raise SyncConflict(f'{los.GRID_TABLE} already has {have} rows')
    path = _path(f'{los.GRID_TABLE}.native')
    client.command(f'TRUNCATE TABLE {los.GRID_TABLE}')
    _load(client, los.GRID_TABLE, path)
    have = client.query(f'SELECT count() FROM {los.GRID_TABLE}').result_rows[0][0]
    if have != expected_rows:
        raise SyncError(f'{los.GRID_TABLE} has {have} rows after import, expected {expected_rows}')
    os.remove(path)
    return {'grid_rows': have}


def _staged_rows(client, table: str, day: date) -> int:
    time_col = 'toDate(time)' if table == los.DATA_TABLE else 'time'
    return client.query(
        f'SELECT count() FROM {los.staging_table(table)} WHERE {time_col} = %(d)s',
        parameters={'d': day},
    ).result_rows[0][0]


def _drop_cached_tiles(day: date) -> None:
    """On-demand daily/monthly tiles built from the replaced data."""
    if not os.path.isdir(IMAGE_DIR):
        return
    for var in os.listdir(IMAGE_DIR):
        for folder in (day.isoformat(), day.strftime('%Y-%m')):
            shutil.rmtree(os.path.join(IMAGE_DIR, var, folder), ignore_errors=True)


def import_day(day_str: str, run_date_str: str, expected_rows: int, expected_daily_rows: int) -> dict:
    try:
        day, run_date = date.fromisoformat(day_str), date.fromisoformat(run_date_str)
    except ValueError as exc:
        raise SyncError(str(exc)) from exc

    client = get_ch_client()
    los.ensure_schema(client)
    owner = los.day_owner(client, day)
    if owner is not None and owner > run_date:
        raise SyncConflict(f'{day} is already owned by newer run {owner}')
    if owner == run_date:
        have = client.query(
            f"SELECT rows, daily_rows FROM {los.DAYS_TABLE} FINAL WHERE day = %(d)s",
            parameters={'d': day},
        ).result_rows
        if have and have[0] == (expected_rows, expected_daily_rows):
            raise SyncConflict(f'{day} already imported from run {run_date}')

    hourly_path = _path(f'{day.isoformat()}.4h.native')
    daily_path = _path(f'{day.isoformat()}.daily.native') if expected_daily_rows else None

    for table, path, expected in ((los.DATA_TABLE, hourly_path, expected_rows),
                                  (los.DAILY_TABLE, daily_path, expected_daily_rows)):
        los.clear_staging_day(client, table, day)
        if not path:
            continue
        _load(client, los.staging_table(table), path)
        staged = _staged_rows(client, table, day)
        if staged != expected:
            los.clear_staging_day(client, table, day)
            raise SyncError(f'{table}: staged {staged} rows for {day}, expected {expected}')

    los.promote_staging_day(client, los.DATA_TABLE, day)
    if daily_path:
        los.promote_staging_day(client, los.DAILY_TABLE, day)
    steps = client.query(
        f'SELECT uniqExact(time) FROM {los.DATA_TABLE} WHERE toDate(time) = %(d)s',
        parameters={'d': day},
    ).result_rows[0][0]
    los.record_day(client, day, run_date, expected_rows, expected_daily_rows, steps)
    _drop_cached_tiles(day)
    for path in (hourly_path, daily_path):
        if path:
            os.remove(path)
    logger.info('Imported LiveOcean %s from run %s (%d rows, %d daily rows)',
                day, run_date, expected_rows, expected_daily_rows)
    return {'date': day_str, 'run_date': run_date_str, 'rows': expected_rows, 'daily_rows': expected_daily_rows}
