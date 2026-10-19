"""Find published LiveOcean runs and fetch the part of each one we keep.

Nothing is downloaded whole. layers.nc is NetCDF4/HDF5 chunked one time step
per chunk, and the S3 host honours HTTP Range requests, so netCDF-C's
`#mode=bytes` reads exactly the steps asked for — ~1 GB of a 3 GB file for a
day-0 slice. A run already in the local mirror (LO_ARCHIVE_DIR) is read from
disk instead. Only the variables the pipeline needs (config.READ_VARIABLES)
are read. The steps are written to one compact local NetCDF per run
(canonical variable names, float32, dims (time, depth, eta, xi)) that the
compute/image/ingest steps then work from.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Optional

import netCDF4
import numpy as np
import pandas as pd
import requests

from .config import (
    ALL_VARIABLES, ARCHIVE_START, GRID_TABLE, LEVELS, LO_ARCHIVE_DIR, LO_BASE_URL,
    NATIVE_VARIABLES, NC_BASE_DIR, READ_VARIABLES, STEPS_PER_DAY, STEPS_PER_RUN,
    STATUS_DOWNLOADING, STATUS_FAILED_DOWNLOAD, STATUS_SUCCESS_DOWNLOAD,
)
from .db import known_runs, pending_runs, seed_runs, set_status

logger = logging.getLogger('LiveOcean.downloader')

_FILL_LIMIT = 1e19  # layers.nc's _FillValue is 1e20


def run_url(run_date: date) -> str:
    return f'{LO_BASE_URL}/f{run_date:%Y.%m.%d}/layers.nc'


def local_path(run_date: date) -> Optional[str]:
    """The run's layers.nc in the local mirror, if it's there."""
    if not LO_ARCHIVE_DIR:
        return None
    path = os.path.join(LO_ARCHIVE_DIR, f'f{run_date:%Y.%m.%d}', 'layers.nc')
    return path if os.path.isfile(path) else None


def run_source(run_date: date) -> str:
    return local_path(run_date) or run_url(run_date)


def nc_path(run_date: date, nc_dir: str = NC_BASE_DIR) -> str:
    return os.path.join(nc_dir, f'LiveOcean_{run_date:%Y%m%d}.nc')


def _open_run(run_date: date, retries: int = 3) -> netCDF4.Dataset:
    local = local_path(run_date)
    if local:
        return netCDF4.Dataset(local)
    url = run_url(run_date) + '#mode=bytes'
    for attempt in range(1, retries + 1):
        try:
            return netCDF4.Dataset(url)
        except OSError:
            if attempt == retries:
                raise
            logger.warning('Opening %s failed (attempt %d/%d), retrying', url, attempt, retries)
            time.sleep(10 * attempt)
    raise AssertionError('unreachable')


def is_available(run_date: date) -> bool:
    """In the local mirror, or a 2-byte range GET answers 206 (403 when the
    run isn't published)."""
    if local_path(run_date):
        return True
    try:
        resp = requests.get(run_url(run_date), headers={'Range': 'bytes=0-1'}, timeout=30)
    except requests.RequestException:
        logger.warning('Availability probe failed for %s', run_date, exc_info=True)
        return False
    return resp.status_code in (200, 206)


def plan_steps(available: Iterable[date]) -> dict[date, int]:
    """How many steps of each run to keep: up to the next available run's
    00Z (its day 0 takes over from there), all 19 for the newest run."""
    runs = sorted(set(available))
    plan = {}
    for run, nxt in zip(runs, runs[1:] + [None]):
        plan[run] = STEPS_PER_RUN if nxt is None else min(STEPS_PER_RUN, STEPS_PER_DAY * (nxt - run).days)
    return plan


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def check(client, init_days: int = 3, backfill: bool = False,
          start: Optional[date] = None, date_override: Optional[date] = None) -> list[date]:
    """Probe the archive for runs newer than what's queued and seed them.

    Default: from the day after the newest known run (or `init_days` back when
    nothing is known) to today. `backfill`: from `start` (default the archive
    start) — already-known runs aren't re-probed. `date_override`: just that
    run (used by `run --date`). Returns the newly found runs.
    """
    known = known_runs(client)
    today = _today_utc()
    if date_override:
        candidates = [date_override]
    else:
        if backfill:
            first = max(start or ARCHIVE_START, ARCHIVE_START)
        elif known:
            first = max(known) + timedelta(days=1)
        else:
            first = today - timedelta(days=init_days)
        candidates = [first + timedelta(days=i) for i in range((today - first).days + 1)]
        candidates = [d for d in candidates if d not in known]

    found = [d for d in candidates if is_available(d)]
    if not found:
        logger.info('check: no new LiveOcean runs (%d probed)', len(candidates))
        return []
    missing = sorted(set(candidates) - set(found))
    if missing and not date_override:
        logger.info('check: %d probed date(s) not published: %s', len(missing),
                    ', '.join(str(d) for d in missing[:20]))

    plan = plan_steps(list(known) + found)
    # Only (re)plan runs that are new or still waiting to download — a run
    # already downloaded keeps the steps it was fetched with.
    plan = {d: n for d, n in plan.items()
            if d in found or known.get(d, {}).get('status') == 'pending_download'}
    seed_runs(client, plan)
    ensure_grid(client)
    return found


# ---------------------------------------------------------------------------
# Grid
# ---------------------------------------------------------------------------

def ensure_grid(client) -> None:
    """Populate grid_LO from the newest run's lon/lat/mask/h if it's empty.
    Older runs have no `h`, so the grid always comes from a recent one."""
    if client.query(f'SELECT count() FROM {GRID_TABLE}').result_rows[0][0]:
        return
    for back in range(0, 10):
        run_date = _today_utc() - timedelta(days=back)
        if is_available(run_date):
            break
    else:
        raise RuntimeError('No LiveOcean run in the last 10 days to build the grid from')
    ds = _open_run(run_date)
    try:
        lon = np.asarray(ds['lon_rho'][:], dtype=np.float64)
        lat = np.asarray(ds['lat_rho'][:], dtype=np.float64)
        mask = np.asarray(ds['mask_rho'][:]) == 1
        h = np.asarray(ds['h'][:], dtype=np.float32)
    finally:
        ds.close()
    eta, xi = np.nonzero(mask)
    df = pd.DataFrame({
        'gridX': xi.astype(np.uint16), 'gridY': eta.astype(np.uint16),
        'longitude': lon[mask], 'latitude': lat[mask], 'h': h[mask],
    })
    client.insert(GRID_TABLE, df, column_names=list(df.columns))
    logger.info('Populated %s with %d wet cells from run %s', GRID_TABLE, len(df), run_date)


def load_grid_arrays(client, shape: tuple[int, int]) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """(lon, lat, mask, h) as full (eta, xi) arrays rebuilt from grid_LO —
    for runs whose own file predates the `h` variable. Land is NaN/False."""
    rows = client.query(f'SELECT gridY, gridX, longitude, latitude, h FROM {GRID_TABLE}').result_rows
    lon = np.full(shape, np.nan); lat = np.full(shape, np.nan)
    h = np.full(shape, np.nan, dtype=np.float32); mask = np.zeros(shape, dtype=bool)
    arr = np.array(rows, dtype=np.float64)
    j, i = arr[:, 0].astype(int), arr[:, 1].astype(int)
    lon[j, i], lat[j, i], h[j, i], mask[j, i] = arr[:, 2], arr[:, 3], arr[:, 4], True
    return lon, lat, mask, h


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def _read_field(ds: netCDF4.Dataset, name: str, steps: int, retries: int = 3) -> np.ndarray:
    for attempt in range(1, retries + 1):
        try:
            raw = ds[name][:steps]
            a = np.asarray(np.ma.filled(raw, np.nan), dtype=np.float32)
            a[~np.isfinite(a) | (np.abs(a) > _FILL_LIMIT)] = np.nan
            return a
        except (OSError, RuntimeError):
            if attempt == retries:
                raise
            logger.warning('Reading %s failed (attempt %d/%d), retrying', name, attempt, retries)
            time.sleep(5 * attempt)
    raise AssertionError('unreachable')


def download_run(client, run_date: date, steps: int, nc_dir: str = NC_BASE_DIR) -> str:
    """Fetch the first `steps` time steps of run_date into a local NetCDF.
    Returns its path. Written under a temp name and renamed when complete."""
    out = nc_path(run_date, nc_dir)
    tmp = out + '.part'
    os.makedirs(nc_dir, exist_ok=True)
    src = _open_run(run_date)
    try:
        src.set_auto_mask(True)
        steps = min(steps, src.dimensions['ocean_time'].size)
        levels = [lvl for lvl in LEVELS if f'temp_{lvl}' in src.variables]
        if not levels:
            raise RuntimeError(f'No recognised levels in {run_source(run_date)}')
        depths = [LEVELS[lvl] for lvl in levels]
        ny, nx = src.dimensions['eta_rho'].size, src.dimensions['xi_rho'].size

        times = np.asarray(src['ocean_time'][:steps], dtype=np.float64)
        if 'h' in src.variables:
            lon = np.asarray(src['lon_rho'][:], dtype=np.float64)
            lat = np.asarray(src['lat_rho'][:], dtype=np.float64)
            mask = np.asarray(src['mask_rho'][:]) == 1
            h = np.asarray(src['h'][:], dtype=np.float32)
        else:
            lon, lat, mask, h = load_grid_arrays(client, (ny, nx))

        logger.info('Downloading run %s from %s: %d steps x %d levels (%s), %s', run_date,
                    run_source(run_date), steps, len(levels), ','.join(levels), ','.join(READ_VARIABLES))
        with netCDF4.Dataset(tmp, 'w') as dst:
            dst.createDimension('time', steps)
            dst.createDimension('depth', len(depths))
            dst.createDimension('eta', ny)
            dst.createDimension('xi', nx)
            t = dst.createVariable('time', 'f8', ('time',))
            t.units = 'seconds since 1970-01-01 00:00:00'
            t[:] = times
            dst.createVariable('depth', 'f4', ('depth',))[:] = np.asarray(depths, dtype=np.float32)
            dst.createVariable('lon', 'f8', ('eta', 'xi'), zlib=True, complevel=1)[:] = lon
            dst.createVariable('lat', 'f8', ('eta', 'xi'), zlib=True, complevel=1)[:] = lat
            dst.createVariable('mask', 'u1', ('eta', 'xi'), zlib=True, complevel=1)[:] = mask.astype(np.uint8)
            dst.createVariable('h', 'f4', ('eta', 'xi'), zlib=True, complevel=1)[:] = h
            dst.run_date = run_date.isoformat()
            dst.source_url = run_url(run_date)
            dst.variables_read = ','.join(READ_VARIABLES)

            for var in ALL_VARIABLES:
                dst.createVariable(var, 'f4', ('time', 'depth', 'eta', 'xi'), zlib=True, complevel=1,
                                   chunksizes=(1, 1, ny, nx), fill_value=np.float32(np.nan))
            for stem, var in NATIVE_VARIABLES.items():
                if var not in READ_VARIABLES:
                    continue
                for k, lvl in enumerate(levels):
                    a = _read_field(src, f'{stem}_{lvl}', steps)
                    a[:, ~mask] = np.nan
                    dst[var][:, k] = a
                logger.debug('  %s done', var)
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    finally:
        src.close()
    os.replace(tmp, out)
    logger.info('Run %s saved to %s (%.0f MB)', run_date, out, os.path.getsize(out) / 1e6)
    return out


def download(client, run: dict, nc_dir: str = NC_BASE_DIR) -> bool:
    run_date = run['run_date']
    set_status(client, run_date, STATUS_DOWNLOADING, attempts=run['attempts'] + 1)
    try:
        path = download_run(client, run_date, run['steps'], nc_dir)
    except Exception as exc:
        logger.exception('Download failed for run %s', run_date)
        set_status(client, run_date, STATUS_FAILED_DOWNLOAD, error_message=str(exc))
        return False
    set_status(client, run_date, STATUS_SUCCESS_DOWNLOAD, nc_path=path, attempts=0)
    return True


def process_pending_downloads(client, limit: int = 10, nc_dir: str = NC_BASE_DIR) -> None:
    runs = pending_runs(client, 'download', limit)
    if not runs:
        logger.info('No pending LiveOcean downloads')
    for run in runs:
        download(client, run, nc_dir)
