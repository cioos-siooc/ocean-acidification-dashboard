"""WebP tiles for a LiveOcean run: one per (variable, time step, level).

Renders straight from the run's NetCDF arrays with nc2tile.render_values (the
lon/lat come from the same file, so values and coordinates can't be
misaligned), into {LO_IMAGE_DIR}/{var}/{YYYY-MM-DDTHHMMSS}/{depth}.webp —
the layout GET /png/LiveOcean/... serves. Depth names follow the frontend's
formatDepthLabel: "10.0", "0.0", and "bottom" for the -1 sentinel.

A forecast step's tiles are re-rendered by every newer run that covers it;
render_values writes via rename, so a tile is never served half-written.
"""
from __future__ import annotations

import logging
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from datetime import date, datetime, timezone
from typing import Iterable, Optional

import netCDF4
import numpy as np

from shared.variable_config import load_variable_config

from .config import (
    ALL_VARIABLES, BOTTOM_DEPTH, IMAGE_BASE_DIR,
    STATUS_FAILED_IMAGE, STATUS_IMAGING, STATUS_SUCCESS_IMAGE,
)
from .db import pending_runs, set_status, superseded_days

logger = logging.getLogger('LiveOcean.imaging')

_PROCESS_DIR = os.path.join(os.path.dirname(__file__), '..')


def _nc2tile():
    if _PROCESS_DIR not in sys.path:
        sys.path.insert(0, _PROCESS_DIR)
    import shared.nc2tile as nc2tile  # noqa: PLC0415
    return nc2tile


def depth_label(depth: float) -> str:
    return 'bottom' if depth == BOTTOM_DEPTH else f'{depth:.1f}'


def dt_folder(epoch_seconds: float) -> str:
    return datetime.fromtimestamp(float(epoch_seconds), tz=timezone.utc).strftime('%Y-%m-%dT%H%M%S')


# Worker state, set once per process by _init_worker (lon/lat are ~14 MB).
_W: dict = {}


def _init_worker(path: str) -> None:
    with netCDF4.Dataset(path) as ds:
        ds.set_auto_mask(False)
        _W['lon'] = ds['lon'][:]
        _W['lat'] = ds['lat'][:]
    _W['path'] = path


def _render(var: str, ti: int, di: int, out_png: str) -> str:
    with netCDF4.Dataset(_W['path']) as ds:
        ds.set_auto_mask(False)
        values = ds[var][ti, di].astype(np.float64)
    valid = np.isfinite(values)
    if not valid.any():
        return ''
    # nc2tile treats an exact 0 as "no data" (transparent). Concentrations
    # like nitrate legitimately reach 0, so lift them to the colormap floor —
    # the tile clamps to that range anyway, and point queries read ClickHouse.
    floor = load_variable_config()['variables'][var].get('colormapMin')
    if floor is not None:
        values[valid] = np.maximum(values[valid], floor)
    os.makedirs(os.path.dirname(out_png), exist_ok=True)
    _nc2tile().render_values(_W['lon'], _W['lat'], values, valid, var, out_png)
    return out_png


def step_day(epoch_seconds: float) -> date:
    return datetime.fromtimestamp(float(epoch_seconds), tz=timezone.utc).date()


def image_run(path: str, image_dir: str = IMAGE_BASE_DIR, workers: Optional[int] = None,
              variables: Optional[list[str]] = None, skip_days: Iterable[date] = ()) -> int:
    """Render every tile of the run, except steps on `skip_days` (valid days
    a newer run already owns — their tiles are that run's)."""
    variables = variables or ALL_VARIABLES
    skip = set(skip_days)
    with netCDF4.Dataset(path) as ds:
        times = ds['time'][:]
        depths = [float(d) for d in ds['depth'][:]]
    tasks = [
        (var, ti, di, os.path.join(image_dir, var, dt_folder(t), f'{depth_label(d)}.webp'))
        for ti, t in enumerate(times) if step_day(t) not in skip
        for di, d in enumerate(depths) for var in variables
    ]
    if not tasks:
        logger.info('Imaging %s: every step is owned by a newer run — nothing to render', path)
        return 0
    # Build (or load) the cached interpolators once here, so forked workers
    # inherit them instead of each building its own and racing to save it.
    _init_worker(path)
    _render(*tasks[0])
    workers = workers or max(1, (os.cpu_count() or 2) - 1)
    logger.info('Imaging %s: %d tiles on %d workers', path, len(tasks), workers)
    with ProcessPoolExecutor(max_workers=workers, initializer=_init_worker, initargs=(path,)) as pool:
        written = [p for p in pool.map(_render, *zip(*tasks[1:]), chunksize=8) if p]
    logger.info('Imaging complete for %s: %d tiles', path, len(written) + 1)
    return len(written) + 1


def image(client, run: dict, image_dir: str = IMAGE_BASE_DIR, workers: Optional[int] = None) -> bool:
    run_date = run['run_date']
    set_status(client, run_date, STATUS_IMAGING, attempts=run['attempts'] + 1)
    try:
        image_run(run['nc_path'], image_dir, workers, skip_days=superseded_days(client, run_date))
    except Exception as exc:
        logger.exception('Imaging failed for run %s', run_date)
        set_status(client, run_date, STATUS_FAILED_IMAGE, error_message=str(exc))
        return False
    set_status(client, run_date, STATUS_SUCCESS_IMAGE, attempts=0)
    return True


def process_pending_images(client, limit: int = 10, image_dir: str = IMAGE_BASE_DIR,
                           workers: Optional[int] = None) -> None:
    runs = pending_runs(client, 'image', limit)
    if not runs:
        logger.info('No pending LiveOcean image jobs')
    for run in runs:
        image(client, run, image_dir, workers)
