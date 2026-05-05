#!/usr/bin/env python3
"""Carbonate computation using shared memory or memory-mapped files to reduce RAM usage.

Modes:
 - sharedmem: uses multiprocessing.shared_memory to place a time-slice in RAM once and avoid pickling when sending to workers.
 - memmap: writes time-slice to a temporary memmap file on disk and workers read/write that file (lower peak RAM, more IO).

Design decisions:
 - Work is batched per time-step. Each worker processes one time-step, but internally processes depth batches to bound peak memory.
 - Workers write outputs to shared memory or memmap outputs to avoid sending very large arrays back to the parent process.
 - Parent process writes results immediately to NetCDF (append) and deletes temp resources to keep peak RAM low.

Usage:
 python process/calc_carbon_grid_shm_memmap.py --mode sharedmem --base-dir /opt/data/nc --date 20260105 --workers 4 --depth-batch-size 8
"""

from __future__ import annotations
import argparse
import logging
import os
import sys
import time
import tempfile
import uuid
from glob import glob
import re
import shutil

import numpy as np
import xarray as xr
import PyCO2SYS as pyco2

from concurrent.futures import ProcessPoolExecutor, as_completed

import multiprocessing as mp
try:
    from multiprocessing import shared_memory
    HAS_SHM = True
except Exception:
    HAS_SHM = False

import zipfile
import netCDF4 as nc4

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("calc_carbon_grid_shm_memmap")

# Constants
DEFAULT_DENSITY = 1025.0
DEPTH_TO_PRESSURE_FACTOR = 1.019716

VARS_MAP = {
    "TA": "total_alkalinity",
    "DIC": "dissolved_inorganic_carbon",
    "Temp": "temperature",
    "Sal": "salinity"
}

# ----------------------------------------------------------------------------
# Utilities
# ----------------------------------------------------------------------------

def depth_to_pressure(depth_m: np.ndarray) -> np.ndarray:
    return depth_m / DEPTH_TO_PRESSURE_FACTOR


def find_vars(ds: xr.Dataset, key: str) -> str:
    target = VARS_MAP[key]
    if target in ds:
        return target
    for v in ds.data_vars:
        if key == 'DIC' and 'inorganic_carbon' in v:
            return v
        if key == 'TA' and 'alkalinity' in v:
            return v
        if key == 'Temp' and 'temp' in v.lower():
            return v
        if key == 'Sal' and 'sal' in v.lower():
            return v
    # fallback
    return list(ds.data_vars)[0]

# ----------------------------------------------------------------------------
# Worker implementations
# ----------------------------------------------------------------------------

def worker_shm_compute_time(shm_info):
    """
    Worker that reads input arrays from shared memory and writes outputs to shared memory.
    shm_info: dict with keys: ta_name,dic_name,temp_name,sal_name,out_ph_name,out_arag_name,out_cal_name,
    shapes, dshape, depth_vals, depth_batch_size
    Returns: diagnostic tuple (time_idx, elapsed_total, elapsed_pyco, n_valid, tm_out_name)
    """
    err_file = f"/tmp/shm_worker_error_{shm_info.get('time_index','unknown')}_{os.getpid()}.log"
    try:
        start_total = time.time()
        ti = shm_info['time_index']

        # Attach to input shared memory
        def attach(name, dtype, shape):
            # Normalize dtype and validate buffer size to avoid SIGBUS
            try:
                dtype = np.dtype(dtype)
            except Exception:
                logger.error(f"Invalid dtype provided for shared memory attach: {dtype}")
                raise

            sh = shared_memory.SharedMemory(name=name)
            buflen = len(sh.buf)
            expected = int(np.prod(shape)) * dtype.itemsize
            if buflen < expected:
                logger.error(f"Shared memory {name} buffer too small: got {buflen}, expected {expected}")
                raise RuntimeError("Shared memory buffer size mismatch")
            arr = np.ndarray(shape, dtype=dtype, buffer=sh.buf)
            logger.debug(f"Attached shared memory {name} shape={shape} dtype={dtype} bytes={buflen}")
            return sh, arr

        ta_sh, ta_arr = attach(shm_info['ta_name'], shm_info['dtype'], tuple(shm_info['shape']))
        dic_sh, dic_arr = attach(shm_info['dic_name'], shm_info['dtype'], tuple(shm_info['shape']))
        temp_sh, temp_arr = attach(shm_info['temp_name'], shm_info['dtype'], tuple(shm_info['shape']))
        sal_sh, sal_arr = attach(shm_info['sal_name'], shm_info['dtype'], tuple(shm_info['shape']))

        # Create / attach output shared memory
        out_shape = tuple(shm_info['shape'])
        ph_sh = shared_memory.SharedMemory(name=shm_info['out_ph_name'])
        ph_arr = np.ndarray(out_shape, dtype=np.float32, buffer=ph_sh.buf)
        ar_sh = shared_memory.SharedMemory(name=shm_info['out_arag_name'])
        ar_arr = np.ndarray(out_shape, dtype=np.float32, buffer=ar_sh.buf)
        cal_sh = shared_memory.SharedMemory(name=shm_info['out_cal_name'])
        cal_arr = np.ndarray(out_shape, dtype=np.float32, buffer=cal_sh.buf)

        # Ensure shapes (depth, y, x)
        d, ny, nx = shm_info['shape']

        # Loop in depth batches to keep peak memory down
        dbs = shm_info.get('depth_batch_size', d)
        elapsed_pyco_total = 0.0
        n_valid_total = 0

        for start in range(0, d, dbs):
            stop = min(d, start + dbs)
            # slice views
            s_ta = ta_arr[start:stop]
            s_dic = dic_arr[start:stop]
            s_temp = temp_arr[start:stop]
            s_sal = sal_arr[start:stop]

            # Flatten
            ta_flat = (s_ta * (1000.0 / DEFAULT_DENSITY)).ravel()
            dic_flat = (s_dic * (1000.0 / DEFAULT_DENSITY)).ravel()
            temp_flat = s_temp.ravel()
            sal_flat = s_sal.ravel()

            # depth grid for this batch
            depth_vals = np.array(shm_info['depth_vals'][start:stop])
            # build p grid
            d_count = stop - start
            p_grid = depth_to_pressure(depth_vals)[:, None, None].repeat(ny, axis=1).repeat(nx, axis=2).ravel()

            mask = ~(np.isnan(ta_flat) | np.isnan(dic_flat) | np.isnan(temp_flat) | np.isnan(sal_flat) | (sal_flat <= 0))
            n_valid = int(mask.sum())
            n_valid_total += n_valid

            if n_valid == 0:
                continue

            t0 = time.time()
            res = pyco2.sys(par1=ta_flat[mask], par2=dic_flat[mask], par1_type=1, par2_type=2,
                            salinity=sal_flat[mask], temperature=temp_flat[mask], pressure=p_grid[mask],
                            opt_pH_scale=1, opt_k_carbonic=10)
            t1 = time.time()
            elapsed_pyco_total += (t1 - t0)

            # assign back into sliced output buffers
            ph_slice = np.full_like(ta_flat, np.nan, dtype=np.float32)
            ar_slice = np.full_like(ta_flat, np.nan, dtype=np.float32)
            cal_slice = np.full_like(ta_flat, np.nan, dtype=np.float32)

            ph_slice[mask] = res.get('pH')
            ar_slice[mask] = res.get('saturation_aragonite')
            cal_slice[mask] = res.get('saturation_calcite')

            # reshape and assign to the output shared mem arrays
            ph_arr[start:stop] = ph_slice.reshape((d_count, ny, nx))
            ar_arr[start:stop] = ar_slice.reshape((d_count, ny, nx))
            cal_arr[start:stop] = cal_slice.reshape((d_count, ny, nx))

        elapsed_total = time.time() - start_total

        # Cleanup attachments (but not unlinking shared memories here, parent will handle)
        for sh in (ta_sh, dic_sh, temp_sh, sal_sh, ph_sh, ar_sh, cal_sh):
            try:
                sh.close()
            except Exception:
                pass

        return (ti, elapsed_total, elapsed_pyco_total, n_valid_total, shm_info['out_ph_name'], shm_info['out_arag_name'], shm_info['out_cal_name'])

    except Exception as e:
        logger.exception(f"Error in worker_shm_compute_time: {e}")
        return (shm_info.get('time_index', -1), -1.0, -1.0, 0, None, None, None)


def worker_memmap_compute_time(mem_info):
    """
    Worker that reads from memmap files and writes outputs to memmap files.
    mem_info contains input and output file paths and shapes.
    Returns (time_idx, elapsed_total, elapsed_pyco, n_valid, out_ph_path, out_arag_path, out_cal_path)
    """
    tmpdir = os.path.dirname(mem_info['ta_path']) if 'ta_path' in mem_info else tempfile.gettempdir()
    err_file = os.path.join(tmpdir, 'worker_error.log')
    try:
        start_total = time.time()
        ti = mem_info['time_index']

        # Open memmaps
        shape = tuple(mem_info['shape'])
        dtype = mem_info['dtype']

        ta_m = np.memmap(mem_info['ta_path'], dtype=dtype, mode='r', shape=shape)
        dic_m = np.memmap(mem_info['dic_path'], dtype=dtype, mode='r', shape=shape)
        temp_m = np.memmap(mem_info['temp_path'], dtype=dtype, mode='r', shape=shape)
        sal_m = np.memmap(mem_info['sal_path'], dtype=dtype, mode='r', shape=shape)

        # prepare outputs memmaps
        ph_m = np.memmap(mem_info['out_ph_path'], dtype=np.float32, mode='r+', shape=shape)
        ar_m = np.memmap(mem_info['out_arag_path'], dtype=np.float32, mode='r+', shape=shape)
        cal_m = np.memmap(mem_info['out_cal_path'], dtype=np.float32, mode='r+', shape=shape)

        d, ny, nx = shape
        dbs = mem_info.get('depth_batch_size', d)
        elapsed_py = 0.0
        n_valid_total = 0

        for start in range(0, d, dbs):
            stop = min(d, start + dbs)
            s_ta = ta_m[start:stop]
            s_dic = dic_m[start:stop]
            s_temp = temp_m[start:stop]
            s_sal = sal_m[start:stop]

            ta_flat = (s_ta * (1000.0 / DEFAULT_DENSITY)).ravel()
            dic_flat = (s_dic * (1000.0 / DEFAULT_DENSITY)).ravel()
            temp_flat = s_temp.ravel()
            sal_flat = s_sal.ravel()

            depth_vals = np.array(mem_info['depth_vals'][start:stop])
            p_grid = depth_to_pressure(depth_vals)[:, None, None].repeat(ny, axis=1).repeat(nx, axis=2).ravel()

            mask = ~(np.isnan(ta_flat) | np.isnan(dic_flat) | np.isnan(temp_flat) | np.isnan(sal_flat) | (sal_flat <= 0))
            n_valid = int(mask.sum())
            n_valid_total += n_valid

            if n_valid == 0:
                continue

            t0 = time.time()
            res = pyco2.sys(par1=ta_flat[mask], par2=dic_flat[mask], par1_type=1, par2_type=2,
                            salinity=sal_flat[mask], temperature=temp_flat[mask], pressure=p_grid[mask],
                            opt_pH_scale=1, opt_k_carbonic=10)
            t1 = time.time()
            elapsed_py += (t1 - t0)

            ph_flat = np.full_like(ta_flat, np.nan, dtype=np.float32)
            ar_flat = np.full_like(ta_flat, np.nan, dtype=np.float32)
            cal_flat = np.full_like(ta_flat, np.nan, dtype=np.float32)

            ph_flat[mask] = res.get('pH')
            ar_flat[mask] = res.get('saturation_aragonite')
            cal_flat[mask] = res.get('saturation_calcite')

            dcount = stop - start
            ph_m[start:stop] = ph_flat.reshape((dcount, ny, nx))
            ar_m[start:stop] = ar_flat.reshape((dcount, ny, nx))
            cal_m[start:stop] = cal_flat.reshape((dcount, ny, nx))

            # flush to disk
            ph_m.flush()
            ar_m.flush()
            cal_m.flush()

        elapsed_total = time.time() - start_total

        # close memmaps
        del ta_m, dic_m, temp_m, sal_m, ph_m, ar_m, cal_m

        return (ti, elapsed_total, elapsed_py, n_valid_total, mem_info['out_ph_path'], mem_info['out_arag_path'], mem_info['out_cal_path'])

    except Exception as e:
        # Write traceback to per-task error file for post-mortem
        try:
            import traceback
            with open(err_file, 'a') as fh:
                fh.write(f"Error in worker (time_index={mem_info.get('time_index')}, pid={os.getpid()}):\n")
                traceback.print_exc(file=fh)
                fh.flush()
            # try to sync to disk
            try:
                os.sync()
            except Exception:
                pass
        except Exception:
            pass
        logger.exception(f"Error in worker_memmap_compute_time: {e}")
        return (mem_info.get('time_index', -1), -1.0, -1.0, 0, None, None, None)

# ----------------------------------------------------------------------------
# High-level processing using the two modes
# ----------------------------------------------------------------------------

def create_netcdf_outputs(base_dir: str, filename_base: str, coords, dims, outputs):
    """Create netCDF files for outputs with proper dimensions and variables."""
    files = {}
    for key, info in outputs.items():
        out_subdir = os.path.join(base_dir, key)
        os.makedirs(out_subdir, exist_ok=True)
        fpath = os.path.join(out_subdir, info['filename'])
        files[key] = fpath

        # Create file with netCDF4 and define dims/vars
        if os.path.exists(fpath):
            os.remove(fpath)

        root = nc4.Dataset(fpath, 'w')
        for cname, coord in coords.items():
            if np.issubdtype(coord.dtype, np.datetime64):
                root.createDimension(cname, len(coord))
                var = root.createVariable(cname, 'i8', (cname,))
                var[:] = coord.values.astype('datetime64[s]').astype('int64')
                var.units = 'seconds since 1970-01-01 00:00:00'
            else:
                root.createDimension(cname, len(coord))
                var = root.createVariable(cname, coord.dtype, (cname,))
                var[:] = coord.values
        # Create variable
        varname = info['var_name']
        root.createVariable(varname, 'f4', dims, zlib=True, complevel=4, fill_value=np.nan)
        root.variables[varname].long_name = info['name']
        root.variables[varname].units = info['unit']
        root.close()

    return files


def process_file_set_with_mode(files, out_base_dir, mode='sharedmem', workers=2, overwrite=False, depth_batch_size=8):
    assert mode in ('sharedmem', 'memmap')
    if mode == 'sharedmem' and not HAS_SHM:
        raise RuntimeError('Shared memory not available in this python environment')

    dic_path = files['DIC']
    fname_base = os.path.basename(dic_path)

    outputs = {
        'ph_total': {'name': 'pH Total Scale', 'unit': '1', 'filename': fname_base.replace('dissolved_inorganic_carbon', 'ph_total'), 'var_name': 'ph_total'},
        'omega_arag': {'name': 'Omega Aragonite', 'unit': '1', 'filename': fname_base.replace('dissolved_inorganic_carbon', 'omega_arag'), 'var_name': 'omega_arag'},
        'omega_cal': {'name': 'Omega Calcite', 'unit': '1', 'filename': fname_base.replace('dissolved_inorganic_carbon', 'omega_cal'), 'var_name': 'omega_cal'}
    }

    out_files = create_netcdf_outputs(out_base_dir, fname_base, {} , (), outputs)  # we'll update coords/dims later after opening

    ds_map = {}
    try:
        # open inputs
        for k, p in files.items():
            ds_map[k] = xr.open_dataset(p)

        var_names = {k: find_vars(ds_map[k], k) for k in ds_map}

        dims = ds_map['DIC'][var_names['DIC']].dims
        coords = ds_map['DIC'].coords
        time_dim = next((d for d in dims if 'time' in d.lower()), None)
        depth_dim = next((d for d in dims if 'depth' in d.lower()), None)

        t_size = ds_map['DIC'].sizes[time_dim] if time_dim else 1
        d_size = ds_map['DIC'].sizes[depth_dim] if depth_dim else 1

        depth_vals = ds_map['DIC'][depth_dim].values if depth_dim else np.array([0.0])

        # Recreate outputs with proper coords/dims
        out_files = create_netcdf_outputs(out_base_dir, fname_base, coords, dims, outputs)

        logger.info(f"Mode={mode}. Processing {fname_base} time_size={t_size}, depth_size={d_size}, workers={workers}, depth_batch={depth_batch_size}")

        # Timing counters
        overall_start = time.time()
        total_valid_points = 0
        total_pyco_time = 0.0
        total_worker_elapsed = 0.0

        # Executor
        tasks = []

        # If using shared memory, prefer spawn start method to avoid fork+SHM issues. Set before creating processes.
        try:
            if mp.get_start_method(allow_none=True) != 'spawn':
                mp.set_start_method('spawn')
                logger.info('Set multiprocessing start method to spawn (recommended)')
        except Exception as e:
            logger.warning(f'Could not set start method to spawn: {e}')

        # Helper to process a completed task result
        def _handle_task_result(fut, mode_type, resource):
            nonlocal total_valid_points, total_pyco_time, total_worker_elapsed
            try:
                res = fut.result(timeout=300)  # 5 minute timeout per worker task
            except Exception as worker_exc:
                # Child process crashed. Check if it wrote an error log.
                if mode_type == 'memmap' and 'tmpdir' in resource:
                    tmpdir = resource['tmpdir']
                    err_file = os.path.join(tmpdir, 'worker_error.log')
                    if os.path.exists(err_file):
                        logger.error(f"Worker error log found at {err_file}:")
                        try:
                            with open(err_file, 'r') as fh:
                                logger.error(fh.read())
                        except Exception as e:
                            logger.error(f"Could not read error log: {e}")
                # Check system state: OOM, disk space
                try:
                    import subprocess
                    dmesg = subprocess.run(['dmesg', '-T'], capture_output=True, text=True, timeout=5).stdout
                    recent_dmesg = '\n'.join(dmesg.split('\n')[-20:])  # last 20 lines
                    if 'Killed' in recent_dmesg or 'Out of memory' in recent_dmesg:
                        logger.error(f"Recent dmesg (OOM check):\n{recent_dmesg}")
                except Exception as e:
                    logger.debug(f"Could not check dmesg: {e}")
                # Check disk space
                try:
                    tmpdir_to_check = resource.get('tmpdir', tempfile.gettempdir())
                    stat_result = shutil.disk_usage(tmpdir_to_check)
                    logger.error(f"Tmp disk state: {stat_result.free / (1024**3):.1f} GB free, {stat_result.total / (1024**3):.1f} GB total")
                except Exception as e:
                    logger.debug(f"Could not check disk space: {e}")
                raise worker_exc
            res = fut.result()
            ti, elapsed_total, elapsed_pyco, n_valid, out_ph, out_arag, out_cal = res
            # Accumulate totals
            try:
                total_valid_points += int(n_valid)
            except Exception:
                pass
            try:
                total_pyco_time += float(elapsed_pyco)
            except Exception:
                pass
            try:
                total_worker_elapsed += float(elapsed_total)
            except Exception:
                pass

            if mode_type == 'sharedmem':
                resource_obj = resource
                ta_sh, dic_sh, temp_sh, sal_sh, ph_sh, ar_sh, cal_sh = resource_obj['sh_objs']
                shape_tuple = tuple(resource_obj['shape'])

                def read_and_close(name):
                    try:
                        sh = shared_memory.SharedMemory(name=name)
                        arr = np.ndarray(shape_tuple, dtype=np.float32, buffer=sh.buf)
                        data = arr.copy()
                        sh.close()
                        try:
                            sh.unlink()
                        except Exception:
                            pass
                        return data
                    except Exception as e:
                        logger.exception(f"Failed reading shared memory {name}: {e}")
                        return np.full(shape_tuple, np.nan, dtype=np.float32)

                ph_arr = read_and_close(out_ph)
                ar_arr = read_and_close(out_arag)
                cal_arr = read_and_close(out_cal)

                # write to netcdf using netCDF4
                with nc4.Dataset(out_files['ph_total'], 'a') as root:
                    var = root.variables[outputs['ph_total']['var_name']]
                    if time_dim:
                        var[ti, :, :, :] = ph_arr
                    else:
                        var[:] = ph_arr
                with nc4.Dataset(out_files['omega_arag'], 'a') as root:
                    var = root.variables[outputs['omega_arag']['var_name']]
                    if time_dim:
                        var[ti, :, :, :] = ar_arr
                    else:
                        var[:] = ar_arr
                with nc4.Dataset(out_files['omega_cal'], 'a') as root:
                    var = root.variables[outputs['omega_cal']['var_name']]
                    if time_dim:
                        var[ti, :, :, :] = cal_arr
                    else:
                        var[:] = cal_arr

                # attempt best-effort cleanup of any remaining shared memory
                try:
                    for name in list(resource_obj.get('shm_names', [])):
                        try:
                            s = shared_memory.SharedMemory(name=name)
                            s.close(); s.unlink()
                        except FileNotFoundError:
                            pass
                        except Exception:
                            pass
                except Exception:
                    pass

            elif mode_type == 'memmap':
                meminfo = resource
                tmpdir = meminfo['tmpdir']
                shape_tuple = tuple(meminfo['shape'])
                # read memmaps and write to netcdf
                try:
                    mm_ph = np.memmap(os.path.join(tmpdir, 'ph.dat'), dtype=np.float32, mode='r', shape=shape_tuple)
                    mm_ar = np.memmap(os.path.join(tmpdir, 'arag.dat'), dtype=np.float32, mode='r', shape=shape_tuple)
                    mm_cal = np.memmap(os.path.join(tmpdir, 'cal.dat'), dtype=np.float32, mode='r', shape=shape_tuple)

                    with nc4.Dataset(out_files['ph_total'], 'a') as root:
                        var = root.variables[outputs['ph_total']['var_name']]
                        if time_dim:
                            var[ti, :, :, :] = mm_ph
                        else:
                            var[:] = mm_ph
                    with nc4.Dataset(out_files['omega_arag'], 'a') as root:
                        var = root.variables[outputs['omega_arag']['var_name']]
                        if time_dim:
                            var[ti, :, :, :] = mm_ar
                        else:
                            var[:] = mm_ar
                    with nc4.Dataset(out_files['omega_cal'], 'a') as root:
                        var = root.variables[outputs['omega_cal']['var_name']]
                        if time_dim:
                            var[ti, :, :, :] = mm_cal
                        else:
                            var[:] = mm_cal
                finally:
                    # cleanup tmpdir
                    try:
                        shutil.rmtree(tmpdir)
                    except Exception:
                        pass

            logger.info(f"Completed time {ti+1}/{t_size} (mode={mode_type}): elapsed_total={elapsed_total:.2f}s, pyco2_time={elapsed_pyco:.2f}s, valid_points={n_valid}")

        with ProcessPoolExecutor(max_workers=workers) as executor:
            for t in range(t_size):
                # Read time slices
                def get_slice(k):
                    v = var_names[k]
                    da = ds_map[k][v]
                    if time_dim:
                        return da.isel({time_dim: t}).values
                    return da.values

                t_dic = get_slice('DIC')
                t_ta = get_slice('TA')
                t_temp = get_slice('Temp')
                t_sal = get_slice('Sal')

                shape = t_dic.shape if t_dic.ndim == 3 else (1,) + t_dic.shape

                if mode == 'sharedmem':
                    # create shared memory blocks for inputs and outputs for this time slice
                    dtype = t_dic.dtype
                    shape_tuple = shape

                    # Create unique names
                    ta_name = f"ta_{os.getpid()}_{t}_{uuid.uuid4().hex}"
                    dic_name = f"dic_{os.getpid()}_{t}_{uuid.uuid4().hex}"
                    temp_name = f"temp_{os.getpid()}_{t}_{uuid.uuid4().hex}"
                    sal_name = f"sal_{os.getpid()}_{t}_{uuid.uuid4().hex}"
                    out_ph_name = f"ph_{os.getpid()}_{t}_{uuid.uuid4().hex}"
                    out_arag_name = f"arag_{os.getpid()}_{t}_{uuid.uuid4().hex}"
                    out_cal_name = f"cal_{os.getpid()}_{t}_{uuid.uuid4().hex}"

                    # allocate and copy
                    ta_sh = shared_memory.SharedMemory(create=True, size=t_ta.nbytes, name=ta_name)
                    np.ndarray(shape_tuple, dtype=t_ta.dtype, buffer=ta_sh.buf)[:] = t_ta
                    dic_sh = shared_memory.SharedMemory(create=True, size=t_dic.nbytes, name=dic_name)
                    np.ndarray(shape_tuple, dtype=t_dic.dtype, buffer=dic_sh.buf)[:] = t_dic
                    temp_sh = shared_memory.SharedMemory(create=True, size=t_temp.nbytes, name=temp_name)
                    np.ndarray(shape_tuple, dtype=t_temp.dtype, buffer=temp_sh.buf)[:] = t_temp
                    sal_sh = shared_memory.SharedMemory(create=True, size=t_sal.nbytes, name=sal_name)
                    np.ndarray(shape_tuple, dtype=t_sal.dtype, buffer=sal_sh.buf)[:] = t_sal

                    # output shared mem (float32)
                    ph_sh = shared_memory.SharedMemory(create=True, size=np.prod(shape_tuple) * np.dtype(np.float32).itemsize, name=out_ph_name)
                    ar_sh = shared_memory.SharedMemory(create=True, size=np.prod(shape_tuple) * np.dtype(np.float32).itemsize, name=out_arag_name)
                    cal_sh = shared_memory.SharedMemory(create=True, size=np.prod(shape_tuple) * np.dtype(np.float32).itemsize, name=out_cal_name)

                    shm_info = {
                        'time_index': t,
                        'ta_name': ta_name, 'dic_name': dic_name, 'temp_name': temp_name, 'sal_name': sal_name,
                        'out_ph_name': out_ph_name, 'out_arag_name': out_arag_name, 'out_cal_name': out_cal_name,
                        'shape': shape_tuple, 'dtype': t_dic.dtype.name, 'depth_vals': depth_vals.tolist(), 'depth_batch_size': depth_batch_size
                    }

                    # Track created shared memory names for robust cleanup on errors
                    if 'created_shm_names' not in locals():
                        created_shm_names = []
                    created_shm_names.extend([ta_name, dic_name, temp_name, sal_name, out_ph_name, out_arag_name, out_cal_name])

                    logger.info(f"Submitting time {t+1}/{t_size} to sharedmem worker (shm names: {ta_name} ...)")
                    fut = executor.submit(worker_shm_compute_time, shm_info)
                    resource = {'sh_objs': (ta_sh, dic_sh, temp_sh, sal_sh, ph_sh, ar_sh, cal_sh), 'shm_names': [ta_name, dic_name, temp_name, sal_name, out_ph_name, out_arag_name, out_cal_name], 'shape': shape_tuple}
                    tasks.append((fut, 'sharedmem', resource))

                elif mode == 'memmap':
                    # estimate disk space needed for this task (inputs + outputs)
                    elems = int(np.prod(shape))
                    needed_bytes = elems * (np.dtype(t_dic.dtype).itemsize * 4 + np.dtype(np.float32).itemsize * 3)
                    tmp_root = tempfile.gettempdir()
                    try:
                        disk_usage = shutil.disk_usage(tmp_root)
                        free = disk_usage.free
                    except Exception:
                        free = None
                    
                    # Check both free space and inodes. If low on either, reduce concurrency or warn.
                    if free is not None and free < needed_bytes * 1.1:
                        logger.error("Insufficient tmp disk space for memmap: need approx %d bytes, have %s", needed_bytes, free)
                        raise RuntimeError("Insufficient tmp disk space for memmap files")
                    
                    # Also check inode availability (if possible)
                    try:
                        stat_result = os.statvfs(tmp_root)
                        available_inodes = stat_result.f_favail
                        needed_inodes = 10  # 4 input files + 3 output + margin
                        if available_inodes < needed_inodes:
                            logger.warning(f"Low on inodes in {tmp_root}: {available_inodes} available, need {needed_inodes}")
                    except Exception as e:
                        logger.debug(f"Could not check inodes: {e}")

                    # create temporary memmap files
                    tmpdir = tempfile.mkdtemp(prefix=f"carbon_memmap_{t}_")
                    ta_path = os.path.join(tmpdir, 'ta.dat')
                    dic_path = os.path.join(tmpdir, 'dic.dat')
                    temp_path = os.path.join(tmpdir, 'temp.dat')
                    sal_path = os.path.join(tmpdir, 'sal.dat')

                    out_ph_path = os.path.join(tmpdir, 'ph.dat')
                    out_arag_path = os.path.join(tmpdir, 'arag.dat')
                    out_cal_path = os.path.join(tmpdir, 'cal.dat')

                    # write memmaps
                    mm_ta = np.memmap(ta_path, dtype=t_dic.dtype, mode='w+', shape=shape)
                    mm_dic = np.memmap(dic_path, dtype=t_dic.dtype, mode='w+', shape=shape)
                    mm_temp = np.memmap(temp_path, dtype=t_temp.dtype, mode='w+', shape=shape)
                    mm_sal = np.memmap(sal_path, dtype=t_sal.dtype, mode='w+', shape=shape)

                    mm_ta[:] = t_ta
                    mm_dic[:] = t_dic
                    mm_temp[:] = t_temp
                    mm_sal[:] = t_sal
                    mm_ta.flush(); mm_dic.flush(); mm_temp.flush(); mm_sal.flush()

                    # create empty output memmaps
                    mm_ph = np.memmap(out_ph_path, dtype=np.float32, mode='w+', shape=shape)
                    mm_arag = np.memmap(out_arag_path, dtype=np.float32, mode='w+', shape=shape)
                    mm_cal = np.memmap(out_cal_path, dtype=np.float32, mode='w+', shape=shape)
                    mm_ph[:] = np.nan; mm_arag[:] = np.nan; mm_cal[:] = np.nan
                    mm_ph.flush(); mm_arag.flush(); mm_cal.flush()

                    mem_info = {
                        'time_index': t,
                        'ta_path': ta_path, 'dic_path': dic_path, 'temp_path': temp_path, 'sal_path': sal_path,
                        'out_ph_path': out_ph_path, 'out_arag_path': out_arag_path, 'out_cal_path': out_cal_path,
                        'shape': shape, 'dtype': str(t_dic.dtype), 'depth_vals': depth_vals.tolist(), 'depth_batch_size': depth_batch_size
                    }

                    logger.info(f"Submitting time {t+1}/{t_size} to memmap worker (tmpdir: {tmpdir})")
                    try:
                        fut = executor.submit(worker_memmap_compute_time, mem_info)
                    except Exception as e:
                        # cleanup tempdir on failure
                        logger.exception("Failed to submit memmap worker: %s", e)
                        try:
                            shutil.rmtree(tmpdir)
                        except Exception:
                            pass
                        raise
                    resource = {'tmpdir': tmpdir, 'shape': shape}
                    tasks.append((fut, 'memmap', resource))

                # If we have reached the concurrency limit, wait for the oldest task to finish and process it
                if len(tasks) >= workers:
                    old_fut, old_mode, old_res = tasks.pop(0)
                    try:
                        _handle_task_result(old_fut, old_mode, old_res)
                    except Exception:
                        logger.exception("Error while handling completed task")

            # After submitting all tasks, process any remaining
            for fut, mode_type, resource in tasks:
                try:
                    _handle_task_result(fut, mode_type, resource)
                except Exception:
                    logger.exception("Error while handling completed task")

            # Post-run validation: check we processed any valid points
            if total_valid_points == 0:
                logger.error("No valid points were processed for this file — outputs may be empty or invalid.")

            # Verify output NetCDFs have non-NaN values and report counts
            try:
                for key, fpath in out_files.items():
                    with xr.open_dataset(fpath) as ods:
                        varname = outputs[key]['var_name']
                        data = ods[varname].values
                        n_non_nan = int(np.isfinite(data).sum())
                        logger.info(f"Output {os.path.basename(fpath)} contains {n_non_nan} finite values for variable {varname}")
                        if n_non_nan == 0:
                            logger.error(f"Output {fpath} appears empty (all NaNs).")
            except Exception as e:
                logger.exception(f"Failed post-run validation: {e}")

            # Timing summary
            overall_elapsed = time.time() - overall_start
            logger.info("Timing summary for %s: total_valid_points=%d, total_pyco_time=%.2fs, total_worker_elapsed=%.2fs, wall_clock=%.2fs", fname_base, total_valid_points, total_pyco_time, total_worker_elapsed, overall_elapsed)


    finally:
        # Ensure datasets closed
        for ds in ds_map.values():
            ds.close()
        # Ensure any leftover shared memory is unlinked
        if 'created_shm_names' in locals() and isinstance(created_shm_names, list) and len(created_shm_names) > 0:
            logger.info(f"Cleaning up {len(created_shm_names)} leftover shared memory objects")
            for name in list(created_shm_names):
                try:
                    s = shared_memory.SharedMemory(name=name)
                    s.close(); s.unlink()
                    created_shm_names.remove(name)
                except FileNotFoundError:
                    try:
                        created_shm_names.remove(name)
                    except ValueError:
                        pass
                except Exception:
                    pass

# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--base-dir', default='/opt/data/nc')
    parser.add_argument('--date')
    parser.add_argument('--mode', choices=['sharedmem', 'memmap'], default='sharedmem')
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--depth-batch-size', type=int, default=8)
    parser.add_argument('--overwrite', action='store_true')
    args = parser.parse_args()

    dic_dir = os.path.join(args.base_dir, 'dissolved_inorganic_carbon')
    if not os.path.exists(dic_dir):
        logger.error('DIC directory not found')
        sys.exit(1)

    files = sorted(glob(os.path.join(dic_dir, '*.nc')))
    if args.date:
        files = [f for f in files if args.date in os.path.basename(f)]

    for f in files:
        # find matching set
        base = os.path.basename(f)
        match = None
        # reuse matching logic from other scripts: search for TA/Temp/Sal by token
        m = re.search(r"\d{8}T\d{4}", base)
        token = m.group(0) if m else None
        if token:
            candidates = glob(os.path.join(args.base_dir, '*', f'*{token}*.nc'))
            found = {}
            for c in candidates:
                if 'dissolved_inorganic_carbon' in c: found['DIC'] = c
                if 'total_alkalinity' in c: found['TA'] = c
                if 'temperature' in c.lower() or 'temp' in c.lower(): found['Temp'] = c
                if 'salinity' in c.lower() or 'salt' in c.lower(): found['Sal'] = c
            if len(found) == 4:
                match = {k: found[k] for k in ['DIC','TA','Temp','Sal']}
        if match:
            process_file_set_with_mode(match, args.base_dir, mode=args.mode, workers=args.workers, overwrite=args.overwrite, depth_batch_size=args.depth_batch_size)
        else:
            logger.warning(f"Could not find full input set for {f}")

if __name__ == '__main__':
    main()
