"""WebP tile generation step for the SalishSeaCast pipeline.

Calls nc2tile for each variable to reproject the curvilinear NetCDF grid into
Web Mercator WebP tiles for the Mapbox frontend.

Picks up variables at pending_image status (set by db.check_image_ready() once
all 8 variables for a date have succeeded download/compute).  After success,
marks success_image.  Call db.promote_ready_dates() afterwards to advance
fully-imaged dates to pending_ingest.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import date
from typing import Optional

import xarray as xr

from .config import (
    ALL_VARIABLES, IMAGE_BASE_DIR, NC_BASE_DIR, VARIABLE_PRECISION,
    STATUS_FAILED_IMAGE, STATUS_IMAGING, STATUS_SUCCESS_IMAGE,
)
from .db import get_pending_images, get_row, mark_failed, mark_running, mark_success

logger = logging.getLogger('SalishSeaCast.imaging')

_PROCESS_DIR = os.path.join(os.path.dirname(__file__), '..')


def _get_nc2tile():
    if _PROCESS_DIR not in sys.path:
        sys.path.insert(0, _PROCESS_DIR)
    import nc2tile  # noqa: PLC0415
    return nc2tile


def _nc_path(variable: str, date_val: date, nc_base_dir: str) -> str:
    return os.path.join(nc_base_dir, variable, f'{variable}_{date_val.strftime("%Y%m%d")}.nc')


def _bottom_nc_path(nc_path: str) -> str:
    dirname  = os.path.dirname(nc_path)
    basename, ext = os.path.splitext(os.path.basename(nc_path))
    variable = os.path.basename(dirname)
    if basename.startswith(f'{variable}_'):
        date_suffix  = basename[len(variable) + 1:]
        new_basename = f'{variable}_{date_suffix}_bottom'
    else:
        new_basename = f'{basename}_bottom'
    return os.path.join(dirname, new_basename + ext)


def _get_all_depths(nc_path: str) -> tuple[list[int], list[str]]:
    with xr.open_dataset(nc_path, engine='netcdf4') as ds:
        depth_coord = next(
            (ds.coords[c].values for c in ds.coords
             if any(k in str(c).lower() for k in ('depth', 'deptht', 'z'))),
            None,
        )
    if depth_coord is None:
        raise ValueError(f'No depth coordinate found in {nc_path}')
    # Round to 1 decimal so filenames are e.g. 18.0.webp, not 18.00713539123535.webp
    return list(range(len(depth_coord))), [f'{round(float(v), 1):.1f}' for v in depth_coord]


# ---------------------------------------------------------------------------
# Single-variable image generation
# ---------------------------------------------------------------------------

def image_variable(
    client,
    date_val: date,
    variable: str,
    workers: Optional[int] = None,
    nc_base_dir: str = NC_BASE_DIR,
    image_base_dir: str = IMAGE_BASE_DIR,
) -> bool:
    # nc2tile reads its output root from this env var (no CLI flag is exposed for it).
    os.environ['IMAGE_ROOT'] = image_base_dir
    src = _nc_path(variable, date_val, nc_base_dir)
    if not os.path.exists(src):
        msg = f'Source NC not found: {src}'
        logger.error(msg)
        row      = get_row(client, date_val, variable) or {}
        attempts = row.get('attempts', 0)
        mark_failed(client, date_val, variable, STATUS_FAILED_IMAGE, msg, attempts=attempts + 1)
        return False

    row      = get_row(client, date_val, variable) or {}
    attempts = row.get('attempts', 0)
    mark_running(client, date_val, variable, STATUS_IMAGING, attempts)

    try:
        nc2tile   = _get_nc2tile()
        precision = VARIABLE_PRECISION.get(variable, 0.1)

        depth_indices, depth_labels = _get_all_depths(src)
        args = [
            '--data', src,
            '--vars', variable,
            '--precision', str(precision),
            '--depth-indices', ','.join(str(i) for i in depth_indices),
            '--depth-images',  ','.join(depth_labels),
        ]
        if workers is not None:
            args += ['--workers', str(int(workers))]

        try:
            nc2tile.main(args)
        except SystemExit as exc:
            if not isinstance(exc.code, list) and exc.code not in (0, None):
                raise RuntimeError(f'nc2tile exited with code {exc.code}') from exc

        # Process companion bottom-layer file if present
        bottom_src = _bottom_nc_path(src)
        if os.path.exists(bottom_src):
            bottom_args = [
                '--data', bottom_src, '--vars', variable,
                '--precision', str(precision),
                '--depth-indices', '0', '--depth-images', 'bottom',
            ]
            if workers is not None:
                bottom_args += ['--workers', str(int(workers))]
            try:
                nc2tile.main(bottom_args)
            except SystemExit as exc:
                if not isinstance(exc.code, list) and exc.code not in (0, None):
                    logger.warning('nc2tile failed for bottom layer %s (non-fatal)', bottom_src)

        mark_success(client, date_val, variable, STATUS_SUCCESS_IMAGE,
                     nc_path=src, attempts=attempts + 1)
        logger.info('Imaging complete: %s %s', variable, date_val)
        return True

    except Exception as exc:
        logger.exception('Imaging failed for %s %s', variable, date_val)
        mark_failed(client, date_val, variable, STATUS_FAILED_IMAGE,
                    str(exc), attempts=attempts + 1)
        return False


# ---------------------------------------------------------------------------
# Batch processor
# ---------------------------------------------------------------------------

def process_pending_images(
    client,
    limit: int = 40,
    workers: Optional[int] = None,
    nc_base_dir: str = NC_BASE_DIR,
    image_base_dir: str = IMAGE_BASE_DIR,
) -> None:
    pending = get_pending_images(client, limit=limit)
    if not pending:
        logger.info('No pending image jobs')
        return
    for date_val, variable in pending:
        image_variable(client, date_val, variable, workers=workers,
                       nc_base_dir=nc_base_dir, image_base_dir=image_base_dir)
