"""Derived variables for a downloaded LiveOcean run.

1. TEOS-10: LiveOcean ships potential temperature and practical salinity;
   SalishSeaCast (and so the app's `temperature`/`salinity`) is Conservative
   Temperature and Absolute Salinity (g/kg). Converted with gsw so the two
   models — and the sensors compared against them — mean the same thing.

2. Carbonate system: LiveOcean ships pH (total scale) and Ω aragonite but not
   TA/DIC/Ω calcite. PyCO2SYS solves the system from the pH + Ω-aragonite
   pair (par types 3 and 11) at in-situ temperature, practical salinity and
   pressure — same constants as SSC/compute.py (opt_k_carbonic=10, Lueker
   2000). Checked on real fields: plausible TA/DIC (e.g. ~2290 µmol/kg at
   500 m); a 0.001 pH error moves TA/DIC by ~4-5 µmol/kg, which is why it
   needs LiveOcean's full-precision pH, not a rounded copy. TA/DIC are stored
   in mmol/m³ like SalishSeaCast's, using in-situ density.

The carbonate solve is skipped when LO_VARIABLES selects none of its outputs
(config.NEEDS_CARBONATE); the TEOS-10 conversion always runs.

Parallelised over (time step, level) pairs with spawned workers; each worker
reads its slice from the run's NetCDF and returns results through a .npz in
a temp dir, and the parent writes them back (one writer, no HDF5 contention).
"""
from __future__ import annotations

import logging
import multiprocessing
import os
import tempfile
from concurrent.futures import ProcessPoolExecutor

import netCDF4
import numpy as np

from .config import (
    BOTTOM_DEPTH, NEEDS_CARBONATE,
    STATUS_COMPUTING, STATUS_FAILED_COMPUTE, STATUS_SUCCESS_COMPUTE,
)
from .db import pending_runs, set_status

logger = logging.getLogger('LiveOcean.compute')

_TS_OUTPUTS = ('temperature', 'salinity')
_CARBONATE_OUTPUTS = ('omega_cal', 'total_alkalinity', 'dissolved_inorganic_carbon')


def _worker(task: dict) -> str:
    os.environ.setdefault('HDF5_USE_FILE_LOCKING', 'FALSE')
    import gsw
    import netCDF4 as nc
    import numpy as np

    ti, di, carbonate = task['ti'], task['di'], task['carbonate']
    with nc.Dataset(task['path']) as ds:
        ds.set_auto_mask(False)
        depth = float(ds['depth'][di])
        lon = ds['lon'][:]; lat = ds['lat'][:]
        pt = ds['temperature'][ti, di]; sp = ds['salinity'][ti, di]
        if carbonate:
            ph = ds['ph_total'][ti, di]; om = ds['omega_arag'][ti, di]
        h = ds['h'][:] if depth == BOTTOM_DEPTH else None

    # T/S convert wherever they're valid; the carbonate solve additionally
    # needs a usable pH/Ω pair (Ω is 0 in a few hundred cells).
    ts_ok = (np.isfinite(pt) & np.isfinite(sp) & (sp >= 0)).ravel()
    idx = np.flatnonzero(ts_ok)
    n = idx.size
    out = {'idx': idx}
    nan = np.full(n, np.nan)
    if n:
        lon_v, lat_v = lon.ravel()[idx], lat.ravel()[idx]
        z = h.ravel()[idx].astype(np.float64) if h is not None else np.full(n, depth)
        p = gsw.p_from_z(-z, lat_v)
        sp_v, pt_v = sp.ravel()[idx].astype(np.float64), pt.ravel()[idx].astype(np.float64)
        sa = gsw.SA_from_SP(sp_v, p, lon_v, lat_v)
        ct = gsw.CT_from_pt(sa, pt_v)
        out.update(temperature=ct, salinity=sa)

    if n and carbonate:
        import PyCO2SYS as pyco2
        out.update(omega_cal=nan.copy(), total_alkalinity=nan.copy(), dissolved_inorganic_carbon=nan.copy())
        ph_v, om_v = ph.ravel()[idx].astype(np.float64), om.ravel()[idx].astype(np.float64)
        carb = np.isfinite(ph_v) & np.isfinite(om_v) & (om_v > 0) & (sp_v > 0)
        if carb.any():
            t_insitu = gsw.t_from_CT(sa[carb], ct[carb], p[carb])
            rho = gsw.rho(sa[carb], ct[carb], p[carb])
            r = pyco2.sys(
                par1=ph_v[carb], par2=om_v[carb], par1_type=3, par2_type=11,
                salinity=sp_v[carb], temperature=t_insitu, pressure=p[carb],
                opt_pH_scale=1, opt_k_carbonic=10,
            )
            out['omega_cal'][carb] = r['saturation_calcite']
            # µmol/kg -> mmol/m³
            out['total_alkalinity'][carb] = r['alkalinity'] * rho / 1000.0
            out['dissolved_inorganic_carbon'][carb] = r['dic'] * rho / 1000.0
    path = os.path.join(task['tmpdir'], f'{ti}_{di}.npz')
    np.savez(path, **{k: np.asarray(v, dtype=np.int64 if k == 'idx' else np.float32) for k, v in out.items()})
    return path


def compute_run(path: str, workers: int = 4) -> None:
    with netCDF4.Dataset(path) as ds:
        if getattr(ds, 'computed', '') == 'yes':
            logger.info('%s already computed — skipping', path)
            return
        n_t, n_d = ds.dimensions['time'].size, ds.dimensions['depth'].size
        shape = (ds.dimensions['eta'].size, ds.dimensions['xi'].size)

    with tempfile.TemporaryDirectory(prefix='lo_compute_') as tmpdir:
        tasks = [{'path': path, 'ti': ti, 'di': di, 'tmpdir': tmpdir, 'carbonate': NEEDS_CARBONATE}
                 for ti in range(n_t) for di in range(n_d)]
        logger.info('Computing %s: %d step x level tasks on %d workers', path, len(tasks), workers)
        ctx = multiprocessing.get_context('spawn')
        with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as pool:
            results = list(pool.map(_worker, tasks))

        with netCDF4.Dataset(path, 'a') as ds:
            for task, res_path in zip(tasks, results):
                res = np.load(res_path)
                idx = res['idx']
                for var in _TS_OUTPUTS + (_CARBONATE_OUTPUTS if NEEDS_CARBONATE else ()):
                    field = np.full(shape[0] * shape[1], np.nan, dtype=np.float32)
                    if idx.size:
                        field[idx] = res[var]
                    ds[var][task['ti'], task['di']] = field.reshape(shape)
            ds.computed = 'yes'
            ds.temperature_kind = 'conservative temperature (TEOS-10)'
            ds.salinity_kind = 'absolute salinity, g/kg (TEOS-10)'
    logger.info('Compute complete for %s', path)


def compute(client, run: dict, workers: int = 4) -> bool:
    run_date = run['run_date']
    set_status(client, run_date, STATUS_COMPUTING, attempts=run['attempts'] + 1)
    try:
        compute_run(run['nc_path'], workers=workers)
    except Exception as exc:
        logger.exception('Compute failed for run %s', run_date)
        set_status(client, run_date, STATUS_FAILED_COMPUTE, error_message=str(exc))
        return False
    set_status(client, run_date, STATUS_SUCCESS_COMPUTE, attempts=0)
    return True


def process_pending_compute(client, limit: int = 10, workers: int = 4) -> None:
    runs = pending_runs(client, 'compute', limit)
    if not runs:
        logger.info('No pending LiveOcean compute jobs')
    for run in runs:
        compute(client, run, workers)
