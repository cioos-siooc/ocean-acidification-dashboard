"""PyCO2SYS computation step for the SalishSeaCast pipeline.

Reads total_alkalinity, dissolved_inorganic_carbon, temperature, and salinity
NetCDF files for a given date, derives ph_total, omega_arag, and omega_cal
via PyCO2SYS, and writes one NetCDF file per output variable.

Uses memory-mapped temporary files so each time step is processed by a worker
process independently, keeping peak RAM bounded regardless of grid size.

After successful computation, all three compute variables are marked
success_compute.  Call db.check_image_ready() once all 8 variables for the
date have succeeded to advance them together to pending_image.
"""
from __future__ import annotations

import logging
import multiprocessing
import os
import tempfile
from concurrent.futures import ProcessPoolExecutor
from datetime import date

import numpy as np
import xarray as xr

from .config import (
    COMPUTE_VARIABLES, DEPTH_TO_PRESSURE_FACTOR, NC_BASE_DIR,
    PYCO2SYS_INPUTS, SEAWATER_DENSITY,
    STATUS_COMPUTING, STATUS_FAILED_COMPUTE, STATUS_SUCCESS_COMPUTE,
)
from .db import (
    downloads_complete, get_dates_pending_compute, get_row,
    mark_failed, mark_running, mark_success,
)

logger = logging.getLogger('SalishSeaCast.compute')


# ---------------------------------------------------------------------------
# Worker (subprocess — no shared state with parent)
# ---------------------------------------------------------------------------

def _worker(mem_info: dict) -> tuple[int, int]:
    """Compute ph_total, omega_arag, omega_cal for one time step via PyCO2SYS."""
    import numpy as np
    import PyCO2SYS as pyco2

    ti         = mem_info['time_index']
    shape      = tuple(mem_info['shape'])   # (depth, ny, nx)
    dtype      = mem_info['dtype']
    d, ny, nx  = shape
    depth_vals = np.array(mem_info['depth_vals'])
    density    = mem_info.get('density', 1025.0)
    batch_size = mem_info.get('depth_batch_size', d)

    ta_m  = np.memmap(mem_info['ta_path'],  dtype=dtype,      mode='r',  shape=shape)
    dic_m = np.memmap(mem_info['dic_path'], dtype=dtype,      mode='r',  shape=shape)
    t_m   = np.memmap(mem_info['t_path'],   dtype=dtype,      mode='r',  shape=shape)
    sal_m = np.memmap(mem_info['sal_path'], dtype=dtype,      mode='r',  shape=shape)
    ph_m  = np.memmap(mem_info['ph_path'],  dtype=np.float32, mode='r+', shape=shape)
    ar_m  = np.memmap(mem_info['ar_path'],  dtype=np.float32, mode='r+', shape=shape)
    cal_m = np.memmap(mem_info['cal_path'], dtype=np.float32, mode='r+', shape=shape)

    n_valid_total = 0

    for start in range(0, d, batch_size):
        stop   = min(d, start + batch_size)
        dcount = stop - start

        ta_flat  = (ta_m[start:stop]  * (1000.0 / density)).ravel()
        dic_flat = (dic_m[start:stop] * (1000.0 / density)).ravel()
        t_flat   = t_m[start:stop].ravel()
        sal_flat = sal_m[start:stop].ravel()

        p_flat = (
            (depth_vals[start:stop] / DEPTH_TO_PRESSURE_FACTOR)[:, np.newaxis, np.newaxis]
            .repeat(ny, axis=1)
            .repeat(nx, axis=2)
            .ravel()
        )

        mask = ~(
            np.isnan(ta_flat) | np.isnan(dic_flat) |
            np.isnan(t_flat)  | np.isnan(sal_flat) |
            (sal_flat <= 0)
        )
        n_valid = int(mask.sum())
        n_valid_total += n_valid

        if n_valid == 0:
            continue

        res = pyco2.sys(
            par1=ta_flat[mask], par2=dic_flat[mask],
            par1_type=1, par2_type=2,
            salinity=sal_flat[mask], temperature=t_flat[mask],
            pressure=p_flat[mask],
            opt_pH_scale=1, opt_k_carbonic=10,
        )

        ph_flat  = np.full(len(ta_flat), np.nan, dtype=np.float32)
        ar_flat  = np.full(len(ta_flat), np.nan, dtype=np.float32)
        cal_flat = np.full(len(ta_flat), np.nan, dtype=np.float32)

        ph_flat[mask]  = res['pH'].astype(np.float32)
        ar_flat[mask]  = res['saturation_aragonite'].astype(np.float32)
        cal_flat[mask] = res['saturation_calcite'].astype(np.float32)

        ph_m[start:stop]  = ph_flat.reshape(dcount, ny, nx)
        ar_m[start:stop]  = ar_flat.reshape(dcount, ny, nx)
        cal_m[start:stop] = cal_flat.reshape(dcount, ny, nx)
        ph_m.flush(); ar_m.flush(); cal_m.flush()

    del ta_m, dic_m, t_m, sal_m, ph_m, ar_m, cal_m
    return ti, n_valid_total


# ---------------------------------------------------------------------------
# Output NC writer
# ---------------------------------------------------------------------------

def _write_output_nc(out_path: str, variable: str, data: np.ndarray,
                     template_ds: xr.Dataset) -> None:
    coords = {k: template_ds[k] for k in template_ds.coords if k in template_ds.dims}
    dims   = list(template_ds[list(template_ds.data_vars)[0]].dims)
    da     = xr.DataArray(data, dims=dims, coords=coords, name=variable)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    da.to_dataset().to_netcdf(out_path, encoding={variable: {'zlib': True, 'complevel': 4,
                                                              'dtype': 'float32'}})


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def compute_date(
    date_val: date,
    workers: int = 4,
    depth_batch_size: int = 8,
    nc_base_dir: str = NC_BASE_DIR,
) -> dict[str, str]:
    """Derive ph_total, omega_arag, omega_cal for date_val. Returns {variable: nc_path}."""
    suffix = date_val.strftime('%Y%m%d')

    def _path(var: str) -> str:
        return os.path.join(nc_base_dir, var, f'{var}_{suffix}.nc')

    missing = [v for v in PYCO2SYS_INPUTS if not os.path.exists(_path(v))]
    if missing:
        raise FileNotFoundError(f'Missing input NC files for {date_val}: {missing}')

    with xr.open_dataset(_path('temperature'), engine='netcdf4') as tmpl:
        t_var   = next(v for v in tmpl.dims if 'time' in str(v).lower())
        d_var   = next(v for v in tmpl.dims
                       if any(k in str(v).lower() for k in ('depth', 'deptht', 'z')))
        n_time  = tmpl.sizes[t_var]
        n_depth = tmpl.sizes[d_var]
        spatial = [v for v in list(tmpl.dims) if v not in (t_var, d_var)]
        ny, nx  = tmpl.sizes[spatial[0]], tmpl.sizes[spatial[1]]
        depth_vals = tmpl[d_var].values.astype(np.float64)

    shape_3d   = (n_depth, ny, nx)
    full_shape = (n_time,) + shape_3d
    dtype      = 'float32'
    logger.info('Grid: %d t × %d d × %d×%d  (~%.0f MB output/var)',
                n_time, n_depth, ny, nx,
                n_time * n_depth * ny * nx * 4 / 1e6)

    with tempfile.TemporaryDirectory(prefix='ssc_compute_') as tmpdir:

        def _write_mm(arr: np.ndarray, name: str) -> str:
            path = os.path.join(tmpdir, name)
            mm = np.memmap(path, dtype=dtype, mode='w+', shape=shape_3d)
            mm[:] = arr; mm.flush(); del mm
            return path

        # Phase 1: write per-time-step input memmaps then close all xarray datasets.
        # Workers must not inherit open datasets via fork.
        job_paths: list[tuple] = []
        ta_ds  = xr.open_dataset(_path('total_alkalinity'),           engine='netcdf4')
        dic_ds = xr.open_dataset(_path('dissolved_inorganic_carbon'),  engine='netcdf4')
        t_ds   = xr.open_dataset(_path('temperature'),                 engine='netcdf4')
        sal_ds = xr.open_dataset(_path('salinity'),                    engine='netcdf4')
        try:
            for ti in range(n_time):
                ta_p  = _write_mm(ta_ds['total_alkalinity'].isel({t_var: ti}).values.astype(dtype),          f'ta_{ti}.mm')
                dic_p = _write_mm(dic_ds['dissolved_inorganic_carbon'].isel({t_var: ti}).values.astype(dtype), f'dic_{ti}.mm')
                t_p   = _write_mm(t_ds['temperature'].isel({t_var: ti}).values.astype(dtype),                  f't_{ti}.mm')
                sal_p = _write_mm(sal_ds['salinity'].isel({t_var: ti}).values.astype(dtype),                   f'sal_{ti}.mm')
                ph_p  = os.path.join(tmpdir, f'ph_{ti}.mm')
                ar_p  = os.path.join(tmpdir, f'ar_{ti}.mm')
                cal_p = os.path.join(tmpdir, f'cal_{ti}.mm')
                for p in (ph_p, ar_p, cal_p):
                    mm = np.memmap(p, dtype=np.float32, mode='w+', shape=shape_3d)
                    mm[:] = np.nan; mm.flush(); del mm
                job_paths.append((ti, ta_p, dic_p, t_p, sal_p, ph_p, ar_p, cal_p))
        finally:
            ta_ds.close(); dic_ds.close(); t_ds.close(); sal_ds.close()

        # Phase 2: file-backed output memmaps — avoids holding GB of output in RAM.
        out_ph_mm  = np.memmap(os.path.join(tmpdir, 'out_ph.mm'),  dtype=np.float32, mode='w+', shape=full_shape)
        out_ar_mm  = np.memmap(os.path.join(tmpdir, 'out_ar.mm'),  dtype=np.float32, mode='w+', shape=full_shape)
        out_cal_mm = np.memmap(os.path.join(tmpdir, 'out_cal.mm'), dtype=np.float32, mode='w+', shape=full_shape)
        out_ph_mm[:] = np.nan; out_ph_mm.flush()
        out_ar_mm[:] = np.nan; out_ar_mm.flush()
        out_cal_mm[:] = np.nan; out_cal_mm.flush()

        # Phase 3: spawn workers — 'spawn' avoids inheriting parent address space.
        mp_ctx = multiprocessing.get_context('spawn')
        with ProcessPoolExecutor(max_workers=workers, mp_context=mp_ctx) as pool:
            futures = []
            for ti, ta_p, dic_p, t_p, sal_p, ph_p, ar_p, cal_p in job_paths:
                futures.append((ti, ph_p, ar_p, cal_p, pool.submit(_worker, {
                    'time_index': ti, 'shape': shape_3d, 'dtype': dtype,
                    'depth_vals': depth_vals.tolist(), 'density': SEAWATER_DENSITY,
                    'depth_batch_size': depth_batch_size,
                    'ta_path': ta_p, 'dic_path': dic_p, 't_path': t_p, 'sal_path': sal_p,
                    'ph_path': ph_p, 'ar_path': ar_p, 'cal_path': cal_p,
                })))

            for ti, ph_p, ar_p, cal_p, fut in futures:
                _, n_valid = fut.result()
                logger.debug('t=%d  n_valid=%d', ti, n_valid)
                out_ph_mm[ti]  = np.memmap(ph_p,  dtype=np.float32, mode='r', shape=shape_3d)
                out_ar_mm[ti]  = np.memmap(ar_p,  dtype=np.float32, mode='r', shape=shape_3d)
                out_cal_mm[ti] = np.memmap(cal_p, dtype=np.float32, mode='r', shape=shape_3d)
                out_ph_mm.flush(); out_ar_mm.flush(); out_cal_mm.flush()

        # Phase 4: write output NC files.
        output_paths: dict[str, str] = {}
        with xr.open_dataset(_path('temperature'), engine='netcdf4') as tmpl:
            for var, arr in [('ph_total', out_ph_mm), ('omega_arag', out_ar_mm), ('omega_cal', out_cal_mm)]:
                out_path = _path(var)
                _write_output_nc(out_path, var, arr, tmpl)
                output_paths[var] = out_path
                logger.info('Wrote %s → %s', var, out_path)

        del out_ph_mm, out_ar_mm, out_cal_mm

    return output_paths


# ---------------------------------------------------------------------------
# Batch processor
# ---------------------------------------------------------------------------

def process_pending_compute(
    client,
    limit: int = 10,
    workers: int = 4,
    nc_base_dir: str = NC_BASE_DIR,
) -> None:
    dates = get_dates_pending_compute(client, limit=limit)
    if not dates:
        logger.info('No pending compute jobs')
        return

    for date_val in dates:
        # Mark all three compute vars as computing
        for var in COMPUTE_VARIABLES:
            row      = get_row(client, date_val, var) or {}
            attempts = row.get('attempts', 0)
            mark_running(client, date_val, var, STATUS_COMPUTING, attempts)

        try:
            paths = compute_date(date_val, workers=workers, nc_base_dir=nc_base_dir)
            for var in COMPUTE_VARIABLES:
                row      = get_row(client, date_val, var) or {}
                attempts = row.get('attempts', 0)
                mark_success(client, date_val, var, STATUS_SUCCESS_COMPUTE,
                             nc_path=paths.get(var, ''), attempts=attempts)
            logger.info('Compute complete for %s', date_val)
        except Exception as exc:
            logger.exception('Compute failed for %s', date_val)
            for var in COMPUTE_VARIABLES:
                row      = get_row(client, date_val, var) or {}
                attempts = row.get('attempts', 0)
                mark_failed(client, date_val, var, STATUS_FAILED_COMPUTE,
                            str(exc), attempts=attempts)
