"""Load a LiveOcean run into ClickHouse, one valid day at a time.

For each UTC day the run covers (skipping days a newer run owns, see
db.superseded_days): its 4-hourly rows go into LiveOcean_4h_staging, and —
when the run has the whole day — per-cell mean/min/max into
LiveOcean_daily_staging. The staged day then replaces the live partition in
one REPLACE PARTITION (shared/liveocean_schema.py), and LiveOcean_days
records the run as the day's owner. Re-running ingest for a run is therefore
idempotent: it rebuilds the same partitions.

The trailing 00Z step of a 19-step run lands alone in its day's partition
(no daily row) until the next run brings the full day.
"""
from __future__ import annotations

import logging
import warnings
from collections import defaultdict
from datetime import date, datetime, timezone

import netCDF4
import numpy as np
import pandas as pd

from shared import liveocean_schema as los

from .config import (
    ALL_VARIABLES, DAILY_TABLE, DATA_TABLE, STEPS_PER_DAY,
    STATUS_FAILED_INGEST, STATUS_INGESTING, STATUS_SUCCESS_INGEST,
)
from .db import pending_runs, set_status, superseded_days

logger = logging.getLogger('LiveOcean.ingest')

_BATCH_SIZE = 1_000_000


def _utc(epoch_seconds: float) -> datetime:
    return datetime.fromtimestamp(float(epoch_seconds), tz=timezone.utc).replace(tzinfo=None)


def _insert(client, table: str, columns: dict) -> int:
    n = len(columns['depth'])
    for start in range(0, n, _BATCH_SIZE):
        sl = slice(start, min(n, start + _BATCH_SIZE))
        df = pd.DataFrame({k: (v[sl] if isinstance(v, np.ndarray) else v) for k, v in columns.items()})
        client.insert(table, df, column_names=list(df.columns))
    return n


def ingest_run(client, path: str, run_date: date, skip_days: set[date] = frozenset()) -> int:
    """Returns the number of 4-hourly rows written."""
    total = 0
    with netCDF4.Dataset(path) as ds:
        ds.set_auto_mask(False)
        times = ds['time'][:]
        depths = ds['depth'][:].astype(np.float32)
        wet = ds['mask'][:].astype(bool)
        eta, xi = np.nonzero(wet)
        n_wet, n_d = eta.size, depths.size
        gx = np.tile(xi.astype(np.uint16), n_d)
        gy = np.tile(eta.astype(np.uint16), n_d)
        dep = np.repeat(depths, n_wet)

        steps_by_day: dict[date, list[int]] = defaultdict(list)
        for ti, t in enumerate(times):
            steps_by_day[_utc(t).date()].append(ti)

        for day, steps in sorted(steps_by_day.items()):
            if day in skip_days:
                logger.info('Run %s: skipping %s (owned by a newer run)', run_date, day)
                continue
            los.clear_staging_day(client, DATA_TABLE, day)
            los.clear_staging_day(client, DAILY_TABLE, day)

            day_rows = 0
            for ti in steps:
                fields = {var: ds[var][ti][:, eta, xi].ravel() for var in ALL_VARIABLES}
                valid = np.isfinite(fields['temperature'])
                day_rows += _insert(client, los.staging_table(DATA_TABLE), {
                    'time': _utc(times[ti]),
                    'depth': dep[valid], 'gridX': gx[valid], 'gridY': gy[valid],
                    **{f'{var}_q': los.quantize(var, fields[var][valid]) for var in ALL_VARIABLES},
                    'run_date': run_date,
                })

            daily_rows = 0
            full_day = len(steps) == STEPS_PER_DAY
            if full_day:
                stats = {}
                # nanmean/nanmin of an all-NaN column (a level below the
                # seabed) warns; those cells are dropped right after.
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore', RuntimeWarning)
                    for var in ALL_VARIABLES:
                        block = ds[var][steps[0]:steps[-1] + 1][:, :, eta, xi].reshape(len(steps), -1)
                        stats[f'{var}_mean'] = np.nanmean(block, axis=0)
                        stats[f'{var}_min'] = np.nanmin(block, axis=0)
                        stats[f'{var}_max'] = np.nanmax(block, axis=0)
                        if var == 'temperature':
                            valid = np.isfinite(block).any(axis=0)
                daily_rows = _insert(client, los.staging_table(DAILY_TABLE), {
                    'time': day,
                    'depth': dep[valid], 'gridX': gx[valid], 'gridY': gy[valid],
                    **{f'{k}_q': los.quantize(k, v[valid]) for k, v in stats.items()},
                    'run_date': run_date,
                })

            los.promote_staging_day(client, DATA_TABLE, day)
            if full_day:
                los.promote_staging_day(client, DAILY_TABLE, day)
            los.record_day(client, day, run_date, day_rows, daily_rows, len(steps))
            total += day_rows
            logger.info('Run %s: %s ingested (%d steps, %d rows, %d daily rows)',
                        run_date, day, len(steps), day_rows, daily_rows)
    return total


def ingest(client, run: dict) -> bool:
    run_date = run['run_date']
    set_status(client, run_date, STATUS_INGESTING, attempts=run['attempts'] + 1)
    try:
        n = ingest_run(client, run['nc_path'], run_date, superseded_days(client, run_date))
    except Exception as exc:
        logger.exception('Ingest failed for run %s', run_date)
        set_status(client, run_date, STATUS_FAILED_INGEST, error_message=str(exc))
        return False
    set_status(client, run_date, STATUS_SUCCESS_INGEST, attempts=0)
    logger.info('Ingest complete for run %s: %d rows', run_date, n)
    return True


def process_pending_ingests(client, limit: int = 10) -> None:
    runs = pending_runs(client, 'ingest', limit)
    if not runs:
        logger.info('No pending LiveOcean ingest jobs')
    for run in runs:
        ingest(client, run)
