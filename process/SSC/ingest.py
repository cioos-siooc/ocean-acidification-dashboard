"""ClickHouse ingestion step for the SalishSeaCast pipeline.

Reads all 8 NetCDF files for a given date, flattens the 4D arrays into rows,
filters out NaN / land cells, and inserts into SalishSeaCast_hourly in batches.

Picks up dates where all 8 variables are at pending_ingest (set by
db.promote_ready_dates() after all images complete).
"""
from __future__ import annotations

import logging
import os
from datetime import date

import numpy as np
import pandas as pd
import xarray as xr

from .config import (
    ALL_VARIABLES, NC_BASE_DIR,
    STATUS_FAILED_INGEST, STATUS_INGESTING, STATUS_SUCCESS_INGEST,
)
from .db import get_dates_pending_ingest, get_row, mark_failed, mark_running, mark_success

logger = logging.getLogger('SalishSeaCast.ingest')

_BATCH_SIZE = 500_000


def _nc_path(variable: str, date_val: date, nc_base_dir: str) -> str:
    return os.path.join(nc_base_dir, variable, f'{variable}_{date_val.strftime("%Y%m%d")}.nc')


# ---------------------------------------------------------------------------
# Core ingestion
# ---------------------------------------------------------------------------

def ingest_date(client, date_val: date, nc_base_dir: str = NC_BASE_DIR) -> int:
    """Insert all hourly rows for date_val into SalishSeaCast_hourly.

    Processes one time step (hour) at a time: a full day's grid (24 x 40 x 898 x
    398 for SSC) is ~343M cells, so building whole-day meshgrid/variable arrays
    at once (the original approach) peaks at 20+ GB RAM and gets OOM-killed.
    Per-timestep, peak RAM is bounded by one hour's worth of data (~500 MB).

    Returns the total number of rows inserted.
    """
    paths = {var: _nc_path(var, date_val, nc_base_dir) for var in ALL_VARIABLES}
    missing = [var for var, p in paths.items() if not os.path.exists(p)]
    if missing:
        raise FileNotFoundError(f'Missing NC files for {date_val}: {missing}')

    datasets = {var: xr.open_dataset(paths[var], engine='netcdf4') for var in ALL_VARIABLES}
    try:
        base_ds = datasets[ALL_VARIABLES[0]]

        t_dim = next(d for d in base_ds.dims if 'time' in str(d).lower())
        d_dim = next(
            d for d in base_ds.dims
            if any(k in str(d).lower() for k in ('depth', 'deptht', 'z'))
        )
        spatial = [d for d in base_ds.dims if d not in (t_dim, d_dim)]
        y_dim, x_dim = spatial[0], spatial[1]

        times  = base_ds[t_dim].values
        depths = base_ds[d_dim].values.astype(np.float32)
        gridY  = base_ds[y_dim].values.astype(np.uint16)
        gridX  = base_ds[x_dim].values.astype(np.uint16)

        n_t, n_d, n_y, n_x = len(times), len(depths), len(gridY), len(gridX)
        logger.info('Ingesting %s: %d t × %d d × %d×%d grid', date_val, n_t, n_d, n_y, n_x)

        # Depth/grid coordinates are identical across all time steps — build once.
        D, Y, X = np.meshgrid(depths, gridY, gridX, indexing='ij')
        D = D.ravel(); Y = Y.ravel(); X = X.ravel()

        total_inserted = 0
        for ti in range(n_t):
            var_slices = {
                var: datasets[var][var].isel({t_dim: ti}).values.astype(np.float32).ravel()
                for var in ALL_VARIABLES
            }
            # temperature is NaN on land/out-of-grid cells, and exactly 0.0 below the
            # local seafloor (z-level padding) — both are non-ocean and share this mask
            # across all 5 download variables, so gating on temperature alone is sufficient.
            valid_mask = ~np.isnan(var_slices['temperature']) & (var_slices['temperature'] != 0)
            n_valid = int(valid_mask.sum())
            if n_valid == 0:
                continue

            t_val      = pd.Timestamp(times[ti])
            valid_D    = D[valid_mask]
            valid_Y    = Y[valid_mask]
            valid_X    = X[valid_mask]
            valid_vars = {var: var_slices[var][valid_mask] for var in ALL_VARIABLES}

            for batch_start in range(0, n_valid, _BATCH_SIZE):
                sl = slice(batch_start, min(n_valid, batch_start + _BATCH_SIZE))
                df = pd.DataFrame({
                    'time':  t_val,
                    'depth': valid_D[sl],
                    'gridX': valid_X[sl],
                    'gridY': valid_Y[sl],
                    **{var: valid_vars[var][sl] for var in ALL_VARIABLES},
                })
                client.insert('SalishSeaCast_hourly', df, column_names=list(df.columns))
                total_inserted += len(df)

            logger.debug('t=%d: inserted %d rows (%d valid cells)', ti, n_valid, n_valid)

    finally:
        for ds in datasets.values():
            ds.close()

    logger.info('Ingestion complete for %s: %d rows', date_val, total_inserted)
    return total_inserted


# ---------------------------------------------------------------------------
# Batch processor
# ---------------------------------------------------------------------------

def process_pending_ingests(client, limit: int = 5, nc_base_dir: str = NC_BASE_DIR) -> None:
    dates = get_dates_pending_ingest(client, limit=limit)
    if not dates:
        logger.info('No pending ingest jobs')
        return

    for date_val in dates:
        # Mark all 8 variables as ingesting
        for var in ALL_VARIABLES:
            row      = get_row(client, date_val, var) or {}
            attempts = row.get('attempts', 0)
            mark_running(client, date_val, var, STATUS_INGESTING, attempts)

        try:
            n_rows = ingest_date(client, date_val, nc_base_dir=nc_base_dir)
            for var in ALL_VARIABLES:
                row      = get_row(client, date_val, var) or {}
                attempts = row.get('attempts', 0)
                mark_success(client, date_val, var, STATUS_SUCCESS_INGEST,
                             nc_path=str(n_rows), attempts=attempts)
            logger.info('Ingest complete for %s: %d rows', date_val, n_rows)
        except Exception as exc:
            logger.exception('Ingest failed for %s', date_val)
            for var in ALL_VARIABLES:
                row      = get_row(client, date_val, var) or {}
                attempts = row.get('attempts', 0)
                mark_failed(client, date_val, var, STATUS_FAILED_INGEST,
                            str(exc), attempts=attempts)
