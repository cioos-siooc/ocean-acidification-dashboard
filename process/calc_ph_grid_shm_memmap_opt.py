#!/usr/bin/env python3
"""Optimized pH and carbonate saturation computation script.

Computes pH_total, omega_arag, and omega_cal from input NetCDF files containing:
  - Temperature
  - Salinity
  - Total Alkalinity (TA)
  - Dissolved Inorganic Carbon (DIC)

Outputs are written to separate NetCDF files.

Architecture:
 - One timestep per worker task (not batched)
 - Shared memory or memmap for zero-copy/disk-based data sharing
 - Immediate NetCDF write and resource cleanup per timestep
 - Depth batching within workers to manage memory

Usage:
 python calc_ph_grid_shm_memmap_opt.py --dic-file DIC.nc --ta-file TA.nc --temp-file Temp.nc --sal-file Sal.nc --output-dir /opt/data/nc --mode sharedmem --workers 4
"""

# pip3 install numpy xarray PyCO2SYS netCDF4

from __future__ import annotations
import argparse
import logging
import os
import sys
import time
import tempfile
import uuid
import shutil

import numpy as np
import xarray as xr
import PyCO2SYS as pyco2
import netCDF4 as nc4
from concurrent.futures import ProcessPoolExecutor, as_completed

import multiprocessing as mp
try:
    from multiprocessing import shared_memory
    HAS_SHM = True
except Exception:
    HAS_SHM = False

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("calc_ph_grid_opt")

# Constants
DEFAULT_DENSITY = 1025.0
DEPTH_TO_PRESSURE_FACTOR = 1.019716


def depth_to_pressure(depth_m: np.ndarray) -> np.ndarray:
    """Convert depth in meters to pressure in dbar."""
    return depth_m / DEPTH_TO_PRESSURE_FACTOR


def worker_shm_compute_time(shm_info):
    """
    Worker function to process a single timestep from shared memory.
    Attaches to input shared memory blocks, computes, and writes outputs to output shared memory.
    
    Returns: (time_index, elapsed_total, elapsed_pyco, n_valid)
    """
    try:
        start_total = time.time()
        ti = shm_info['time_index']

        # Attach to input shared memory
        def attach(name, dtype, shape):
            try:
                dtype = np.dtype(dtype)
            except Exception:
                logger.error(f"Invalid dtype for shared memory: {dtype}")
                raise
            sh = shared_memory.SharedMemory(name=name)
            buflen = len(sh.buf)
            expected = int(np.prod(shape)) * dtype.itemsize
            if buflen < expected:
                logger.error(f"Shared memory {name} buffer too small: got {buflen}, expected {expected}")
                raise RuntimeError("Shared memory buffer size mismatch")
            arr = np.ndarray(shape, dtype=dtype, buffer=sh.buf)
            return sh, arr

        ta_sh, ta_arr = attach(shm_info['ta_name'], shm_info['dtype'], tuple(shm_info['shape']))
        dic_sh, dic_arr = attach(shm_info['dic_name'], shm_info['dtype'], tuple(shm_info['shape']))
        temp_sh, temp_arr = attach(shm_info['temp_name'], shm_info['dtype'], tuple(shm_info['shape']))
        sal_sh, sal_arr = attach(shm_info['sal_name'], shm_info['dtype'], tuple(shm_info['shape']))

        # Attach to output shared memory
        out_shape = tuple(shm_info['shape'])
        ph_sh = shared_memory.SharedMemory(name=shm_info['out_ph_name'])
        ph_arr = np.ndarray(out_shape, dtype=np.float32, buffer=ph_sh.buf)
        ar_sh = shared_memory.SharedMemory(name=shm_info['out_arag_name'])
        ar_arr = np.ndarray(out_shape, dtype=np.float32, buffer=ar_sh.buf)
        cal_sh = shared_memory.SharedMemory(name=shm_info['out_cal_name'])
        cal_arr = np.ndarray(out_shape, dtype=np.float32, buffer=cal_sh.buf)

        # Process depth batches
        d, _, _ = shm_info['shape']
        ny, nx = shm_info['shape'][1], shm_info['shape'][2]
        dbs = shm_info.get('depth_batch_size', d)
        elapsed_pyco_total = 0.0
        n_valid_total = 0

        for start in range(0, d, dbs):
            stop = min(d, start + dbs)
            s_ta = ta_arr[start:stop]
            s_dic = dic_arr[start:stop]
            s_temp = temp_arr[start:stop]
            s_sal = sal_arr[start:stop]

            # Flatten and convert to µmol/kg
            ta_flat = (s_ta * (1000.0 / DEFAULT_DENSITY)).ravel()
            dic_flat = (s_dic * (1000.0 / DEFAULT_DENSITY)).ravel()
            temp_flat = s_temp.ravel()
            sal_flat = s_sal.ravel()

            # Build pressure grid
            depth_vals = np.array(shm_info['depth_vals'][start:stop])
            p_grid = depth_to_pressure(depth_vals)[:, None, None].repeat(ny, axis=1).repeat(nx, axis=2).ravel()

            # Valid data mask
            mask = ~(np.isnan(ta_flat) | np.isnan(dic_flat) | np.isnan(temp_flat) | np.isnan(sal_flat) | (sal_flat <= 0))
            n_valid = int(mask.sum())
            n_valid_total += n_valid

            if n_valid == 0:
                continue

            # Compute carbonate system
            t0 = time.time()
            res = pyco2.sys(
                par1=ta_flat[mask], par2=dic_flat[mask],
                par1_type=1, par2_type=2,
                salinity=sal_flat[mask], temperature=temp_flat[mask],
                pressure=p_grid[mask],
                opt_pH_scale=1, opt_k_carbonic=10
            )
            t1 = time.time()
            elapsed_pyco_total += (t1 - t0)

            # Fill temporary arrays and assign to shared memory
            ph_flat = np.full_like(ta_flat, np.nan, dtype=np.float32)
            ar_flat = np.full_like(ta_flat, np.nan, dtype=np.float32)
            cal_flat = np.full_like(ta_flat, np.nan, dtype=np.float32)

            ph_flat[mask] = res.get('pH')
            ar_flat[mask] = res.get('saturation_aragonite')
            cal_flat[mask] = res.get('saturation_calcite')

            d_count = stop - start
            ph_arr[start:stop] = ph_flat.reshape((d_count, ny, nx))
            ar_arr[start:stop] = ar_flat.reshape((d_count, ny, nx))
            cal_arr[start:stop] = cal_flat.reshape((d_count, ny, nx))

        elapsed_total = time.time() - start_total

        # Close (but don't unlink - main process will do that after reading)
        for sh in (ta_sh, dic_sh, temp_sh, sal_sh, ph_sh, ar_sh, cal_sh):
            try:
                sh.close()
            except Exception:
                pass

        return (ti, elapsed_total, elapsed_pyco_total, n_valid_total)

    except Exception as e:
        logger.exception(f"Error in worker_shm_compute_time: {e}")
        return (shm_info.get('time_index', -1), -1.0, -1.0, 0)


def worker_memmap_compute_time(mem_info):
    """
    Worker function to process a single timestep from memmap files.
    Reads inputs from memmap, computes, and writes outputs to memmap.
    
    Returns: (time_index, elapsed_total, elapsed_pyco, n_valid)
    """
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

        # Prepare output memmaps
        ph_m = np.memmap(mem_info['out_ph_path'], dtype=np.float32, mode='r+', shape=shape)
        ar_m = np.memmap(mem_info['out_arag_path'], dtype=np.float32, mode='r+', shape=shape)
        cal_m = np.memmap(mem_info['out_cal_path'], dtype=np.float32, mode='r+', shape=shape)

        d, ny, nx = shape
        dbs = mem_info.get('depth_batch_size', d)
        elapsed_pyco = 0.0
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
            res = pyco2.sys(
                par1=ta_flat[mask], par2=dic_flat[mask],
                par1_type=1, par2_type=2,
                salinity=sal_flat[mask], temperature=temp_flat[mask],
                pressure=p_grid[mask],
                opt_pH_scale=1, opt_k_carbonic=10
            )
            t1 = time.time()
            elapsed_pyco += (t1 - t0)

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

            # Flush to disk
            ph_m.flush()
            ar_m.flush()
            cal_m.flush()

        elapsed_total = time.time() - start_total

        # Close memmaps
        del ta_m, dic_m, temp_m, sal_m, ph_m, ar_m, cal_m

        return (ti, elapsed_total, elapsed_pyco, n_valid_total)

    except Exception as e:
        logger.exception(f"Error in worker_memmap_compute_time: {e}")
        return (mem_info.get('time_index', -1), -1.0, -1.0, 0)


def _create_empty_netcdf(filepath, var_name, time_size, coords, depth_dim, time_dim):
    """Create empty NetCDF file with time dimension and pre-initialized data."""
    if os.path.exists(filepath):
        os.remove(filepath)
    
    root = nc4.Dataset(filepath, 'w')
    
    # Create time dimension (unlimited)
    root.createDimension(time_dim, None)
    
    # Create time coordinate variable
    if time_dim in coords:
        time_var = root.createVariable(time_dim, 'i8', (time_dim,))
        time_var[:] = coords[time_dim].values.astype('datetime64[s]').astype('int64')
        time_var.units = 'seconds since 1970-01-01 00:00:00'
    
    # Create other coordinates
    for cname, coord in coords.items():
        if cname == time_dim or 'time' in cname.lower():
            continue
        elif cname == depth_dim or 'depth' in cname.lower():
            root.createDimension(cname, len(coord))
            var = root.createVariable(cname, coord.dtype, (cname,))
            var[:] = coord.values
        else:
            if cname not in root.dimensions and cname not in ['y', 'x']:
                root.createDimension(cname, len(coord) if hasattr(coord, '__len__') else 1)
                if cname not in root.variables:
                    try:
                        var = root.createVariable(cname, coord.dtype, (cname,))
                        var[:] = coord.values if hasattr(coord, '__len__') else [coord.values]
                    except Exception:
                        pass
    
    # Create y, x dimensions (get from first slice)
    if 'y' not in root.dimensions:
        root.createDimension('y', 720)  # Default grid size
    if 'x' not in root.dimensions:
        root.createDimension('x', 480)  # Default grid size
    
    # Create output variable
    dims = (time_dim, depth_dim, 'y', 'x')
    var = root.createVariable(var_name, 'f4', dims, zlib=True, complevel=4, fill_value=np.nan)
    
    # Add metadata
    if var_name == 'ph_total':
        var.long_name = 'pH Total Scale'
        var.units = '1'
    elif var_name == 'omega_arag':
        var.long_name = 'Omega Aragonite'
        var.units = '1'
    elif var_name == 'omega_cal':
        var.long_name = 'Omega Calcite'
        var.units = '1'
    
    root.close()


def _append_netcdf_timeseries(filepath, var_name, data, time_idx):
    """Append data to existing timeseries NetCDF file at specific time index."""
    with nc4.Dataset(filepath, 'a') as ds:
        ds[var_name][time_idx] = data


def _write_netcdf_snapshot(filepath, data, var_name, coords, depth_dim):
    """Write a single snapshot (no time dimension) to NetCDF file."""
    if os.path.exists(filepath):
        os.remove(filepath)
    
    root = nc4.Dataset(filepath, 'w')
    
    # Create dimensions and coordinates
    for cname, coord in coords.items():
        if cname == depth_dim or 'depth' in cname.lower():
            root.createDimension(cname, len(coord))
            var = root.createVariable(cname, coord.dtype, (cname,))
            var[:] = coord.values
        else:
            root.createDimension(cname, len(coord) if hasattr(coord, '__len__') else 1)
            if cname not in root.variables:
                try:
                    var = root.createVariable(cname, coord.dtype, (cname,))
                    var[:] = coord.values if hasattr(coord, '__len__') else [coord.values]
                except Exception:
                    pass
    
    # Create y, x dimensions
    if 'y' not in root.dimensions:
        root.createDimension('y', data.shape[1])
    if 'x' not in root.dimensions:
        root.createDimension('x', data.shape[2])
    
    # Create output variable
    dims = (depth_dim, 'y', 'x')
    var = root.createVariable(var_name, 'f4', dims, zlib=True, complevel=4, fill_value=np.nan)
    var[:] = data
    
    # Add metadata
    if var_name == 'ph_total':
        var.long_name = 'pH Total Scale'
        var.units = '1'
    elif var_name == 'omega_arag':
        var.long_name = 'Omega Aragonite'
        var.units = '1'
    elif var_name == 'omega_cal':
        var.long_name = 'Omega Calcite'
        var.units = '1'
    
    root.close()


def main():
    parser = argparse.ArgumentParser(description='Optimized pH and carbonate saturation computation.')
    parser.add_argument('--dic-file', required=True, help='Path to DIC NetCDF file')
    parser.add_argument('--ta-file', required=True, help='Path to TA NetCDF file')
    parser.add_argument('--temp-file', required=True, help='Path to Temperature NetCDF file')
    parser.add_argument('--sal-file', required=True, help='Path to Salinity NetCDF file')
    parser.add_argument('--output-dir', required=True, help='Output directory for computed NetCDF files')
    parser.add_argument('--depth-batch-size', type=int, default=8, help='Process depths in batches (default: 8)')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers (default: 4)')
    parser.add_argument('--mode', choices=['sharedmem', 'memmap'], default='sharedmem', help='Data sharing mode: sharedmem (RAM) or memmap (disk). Default: sharedmem')
    args = parser.parse_args()
    
    if args.mode == 'sharedmem' and not HAS_SHM:
        logger.error("sharedmem mode requested but multiprocessing.shared_memory not available. Use --mode memmap instead.")
        sys.exit(1)
    
    # Open input files
    logger.info("Loading input files...")
    ds_dic = xr.open_dataset(args.dic_file)
    ds_ta = xr.open_dataset(args.ta_file)
    ds_temp = xr.open_dataset(args.temp_file)
    ds_sal = xr.open_dataset(args.sal_file)
    
    try:
        # Infer variable names
        def get_var(ds, patterns):
            for pattern in patterns:
                for var in ds.data_vars:
                    if pattern.lower() in var.lower():
                        return var
            return list(ds.data_vars)[0]
        
        dic_var = get_var(ds_dic, ['dic', 'inorganic_carbon', 'co2'])
        ta_var = get_var(ds_ta, ['ta', 'alkalinity'])
        temp_var = get_var(ds_temp, ['temp', 'temperature'])
        sal_var = get_var(ds_sal, ['sal', 'salinity'])
        
        logger.info(f"Using variables: DIC={dic_var}, TA={ta_var}, Temp={temp_var}, Sal={sal_var}")
        
        # Determine dimensions
        dims = ds_dic[dic_var].dims
        coords = ds_dic.coords
        
        time_dim = next((d for d in dims if 'time' in d.lower()), None)
        depth_dim = next((d for d in dims if 'depth' in d.lower()), None)
        
        t_size = ds_dic.sizes[time_dim] if time_dim else 1
        d_size = ds_dic.sizes[depth_dim] if depth_dim else 1
        
        depth_vals = ds_dic[depth_dim].values if depth_dim else np.array([0.0])
        
        logger.info(f"Data shape: time={t_size}, depth={d_size}")
        logger.info(f"Processing mode: {args.mode} with {args.workers} workers")
        
        # Ensure output directory exists
        os.makedirs(args.output_dir, exist_ok=True)
        
        # Get base filename for output
        base_filename = os.path.basename(args.dic_file)
        
        # Output filenames
        out_ph = os.path.join(args.output_dir, base_filename.replace('dissolved_inorganic_carbon', 'ph_total'))
        out_ar = os.path.join(args.output_dir, base_filename.replace('dissolved_inorganic_carbon', 'omega_arag'))
        out_cal = os.path.join(args.output_dir, base_filename.replace('dissolved_inorganic_carbon', 'omega_cal'))
        
        # Process data
        start_time = time.time()
        
        if time_dim is None:
            # Single time snapshot - compute directly in main process
            logger.info("Processing single time snapshot...")
            dic_data = ds_dic[dic_var].values
            ta_data = ds_ta[ta_var].values
            temp_data = ds_temp[temp_var].values
            sal_data = ds_sal[sal_var].values
            
            # Ensure 3D
            if dic_data.ndim < 3:
                dic_data = dic_data[np.newaxis, :, :]
                ta_data = ta_data[np.newaxis, :, :]
                temp_data = temp_data[np.newaxis, :, :]
                sal_data = sal_data[np.newaxis, :, :]
            
            d, ny, nx = dic_data.shape
            dbs = args.depth_batch_size
            elapsed_pyco_total = 0.0
            n_valid_total = 0
            
            ph_arr = np.full_like(dic_data, np.nan, dtype=np.float32)
            ar_arr = np.full_like(dic_data, np.nan, dtype=np.float32)
            cal_arr = np.full_like(dic_data, np.nan, dtype=np.float32)
            
            logger.info("Computing pH snapshot with depth batching...")
            start_total = time.time()
            
            # Process depth batches
            for start in range(0, d, dbs):
                stop = min(d, start + dbs)
                s_ta = ta_data[start:stop]
                s_dic = dic_data[start:stop]
                s_temp = temp_data[start:stop]
                s_sal = sal_data[start:stop]
                
                ta_flat = (s_ta * (1000.0 / DEFAULT_DENSITY)).ravel()
                dic_flat = (s_dic * (1000.0 / DEFAULT_DENSITY)).ravel()
                temp_flat = s_temp.ravel()
                sal_flat = s_sal.ravel()
                
                depth_vals_batch = depth_vals[start:stop]
                p_grid = depth_to_pressure(depth_vals_batch)[:, None, None].repeat(ny, axis=1).repeat(nx, axis=2).ravel()
                
                mask = ~(np.isnan(ta_flat) | np.isnan(dic_flat) | np.isnan(temp_flat) | np.isnan(sal_flat) | (sal_flat <= 0))
                n_valid = int(mask.sum())
                n_valid_total += n_valid
                
                if n_valid > 0:
                    t0 = time.time()
                    res = pyco2.sys(
                        par1=ta_flat[mask], par2=dic_flat[mask],
                        par1_type=1, par2_type=2,
                        salinity=sal_flat[mask], temperature=temp_flat[mask],
                        pressure=p_grid[mask],
                        opt_pH_scale=1, opt_k_carbonic=10
                    )
                    t1 = time.time()
                    elapsed_pyco_total += (t1 - t0)
                    
                    ph_flat = np.full_like(ta_flat, np.nan, dtype=np.float32)
                    ar_flat = np.full_like(ta_flat, np.nan, dtype=np.float32)
                    cal_flat = np.full_like(ta_flat, np.nan, dtype=np.float32)
                    
                    ph_flat[mask] = res.get('pH')
                    ar_flat[mask] = res.get('saturation_aragonite')
                    cal_flat[mask] = res.get('saturation_calcite')
                    
                    dcount = stop - start
                    ph_arr[start:stop] = ph_flat.reshape((dcount, ny, nx))
                    ar_arr[start:stop] = ar_flat.reshape((dcount, ny, nx))
                    cal_arr[start:stop] = cal_flat.reshape((dcount, ny, nx))
            
            elapsed_total = time.time() - start_total
            
            # Write outputs
            _write_netcdf_snapshot(out_ph, ph_arr, 'ph_total', coords, depth_dim)
            _write_netcdf_snapshot(out_ar, ar_arr, 'omega_arag', coords, depth_dim)
            _write_netcdf_snapshot(out_cal, cal_arr, 'omega_cal', coords, depth_dim)
            
            logger.info(f"Snapshot complete: {n_valid_total} valid points, elapsed={elapsed_total:.2f}s, pyco2_time={elapsed_pyco_total:.2f}s")
        
        else:
            # Time series: one timestep per worker task
            logger.info(f"Processing {t_size} timesteps...")
            
            # Create empty output files
            _create_empty_netcdf(out_ph, 'ph_total', t_size, coords, depth_dim, time_dim)
            _create_empty_netcdf(out_ar, 'omega_arag', t_size, coords, depth_dim, time_dim)
            _create_empty_netcdf(out_cal, 'omega_cal', t_size, coords, depth_dim, time_dim)
            
            total_valid_points = 0
            total_pyco_time = 0.0
            total_worker_elapsed = 0.0
            
            # If using shared memory, prefer spawn start method to avoid fork+SHM issues
            try:
                import multiprocessing as mp
                if args.mode == 'sharedmem' and mp.get_start_method(allow_none=True) != 'spawn':
                    mp.set_start_method('spawn', force=True)
                    logger.info('Set multiprocessing start method to spawn (recommended for shared memory)')
            except Exception as e:
                logger.warning(f'Could not set start method to spawn: {e}')
            
            # Helper to process a completed task result
            def _handle_task_result(fut, mode_type, resource):
                nonlocal total_valid_points, total_pyco_time, total_worker_elapsed
                try:
                    ti, elapsed_total, elapsed_pyco, n_valid = fut.result(timeout=300)
                except Exception as worker_exc:
                    logger.exception(f"Worker failed: {worker_exc}")
                    raise
                
                total_valid_points += n_valid
                total_pyco_time += elapsed_pyco
                total_worker_elapsed += elapsed_total
                
                if mode_type == 'sharedmem':
                    sh_objs = resource['sh_objs']
                    shape_tuple = tuple(resource['shape'])
                    shm_names = resource['shm_names']
                    
                    def read_output(sh_obj):
                        try:
                            arr = np.ndarray(shape_tuple, dtype=np.float32, buffer=sh_obj.buf)
                            return arr.copy()
                        except Exception as e:
                            logger.exception(f"Failed reading from shared memory: {e}")
                            return np.full(shape_tuple, np.nan, dtype=np.float32)
                    
                    ph_arr = read_output(sh_objs[4])
                    ar_arr = read_output(sh_objs[5])
                    cal_arr = read_output(sh_objs[6])
                    
                    _append_netcdf_timeseries(out_ph, 'ph_total', ph_arr, ti)
                    _append_netcdf_timeseries(out_ar, 'omega_arag', ar_arr, ti)
                    _append_netcdf_timeseries(out_cal, 'omega_cal', cal_arr, ti)
                    
                    for i, sh in enumerate(sh_objs):
                        try:
                            sh.close()
                            sh.unlink()
                        except FileNotFoundError:
                            pass
                        except Exception as e:
                            logger.debug(f"Error cleaning {shm_names[i]}: {e}")
                
                elif mode_type == 'memmap':
                    tmpdir = resource['tmpdir']
                    shape_tuple = tuple(resource['shape'])
                    
                    try:
                        mm_ph = np.memmap(os.path.join(tmpdir, 'ph.dat'), dtype=np.float32, mode='r', shape=shape_tuple)
                        mm_ar = np.memmap(os.path.join(tmpdir, 'arag.dat'), dtype=np.float32, mode='r', shape=shape_tuple)
                        mm_cal = np.memmap(os.path.join(tmpdir, 'cal.dat'), dtype=np.float32, mode='r', shape=shape_tuple)
                        
                        _append_netcdf_timeseries(out_ph, 'ph_total', mm_ph, ti)
                        _append_netcdf_timeseries(out_ar, 'omega_arag', mm_ar, ti)
                        _append_netcdf_timeseries(out_cal, 'omega_cal', mm_cal, ti)
                    finally:
                        try:
                            shutil.rmtree(tmpdir)
                        except Exception:
                            pass
                
                logger.info(f"Completed time {ti+1}/{t_size} (mode={mode_type}): elapsed={elapsed_total:.2f}s, pyco2={elapsed_pyco:.2f}s, valid={n_valid}")
            
            with ProcessPoolExecutor(max_workers=args.workers) as executor:
                tasks = []  # FIFO queue of (future, mode_type, resource)
                
                for t in range(t_size):
                    # Load single timestep
                    sel_dict = {time_dim: t}
                    t_dic = ds_dic[dic_var].isel(sel_dict).values
                    t_ta = ds_ta[ta_var].isel(sel_dict).values
                    t_temp = ds_temp[temp_var].isel(sel_dict).values
                    t_sal = ds_sal[sal_var].isel(sel_dict).values
                    
                    # Ensure 3D (depth, y, x)
                    shape = t_dic.shape if t_dic.ndim == 3 else (1,) + t_dic.shape
                    if t_dic.ndim < 3:
                        t_dic = t_dic[np.newaxis, :, :] if t_dic.ndim == 2 else t_dic
                        t_ta = t_ta[np.newaxis, :, :] if t_ta.ndim == 2 else t_ta
                        t_temp = t_temp[np.newaxis, :, :] if t_temp.ndim == 2 else t_temp
                        t_sal = t_sal[np.newaxis, :, :] if t_sal.ndim == 2 else t_sal
                    
                    if args.mode == 'sharedmem':
                        dtype = t_dic.dtype
                        ta_name = f"ta_{uuid.uuid4().hex}"
                        dic_name = f"dic_{uuid.uuid4().hex}"
                        temp_name = f"temp_{uuid.uuid4().hex}"
                        sal_name = f"sal_{uuid.uuid4().hex}"
                        out_ph_name = f"ph_{uuid.uuid4().hex}"
                        out_arag_name = f"arag_{uuid.uuid4().hex}"
                        out_cal_name = f"cal_{uuid.uuid4().hex}"
                        
                        ta_sh_obj = shared_memory.SharedMemory(create=True, size=t_ta.nbytes, name=ta_name)
                        np.ndarray(shape, dtype=dtype, buffer=ta_sh_obj.buf)[:] = t_ta
                        dic_sh_obj = shared_memory.SharedMemory(create=True, size=t_dic.nbytes, name=dic_name)
                        np.ndarray(shape, dtype=dtype, buffer=dic_sh_obj.buf)[:] = t_dic
                        temp_sh_obj = shared_memory.SharedMemory(create=True, size=t_temp.nbytes, name=temp_name)
                        np.ndarray(shape, dtype=dtype, buffer=temp_sh_obj.buf)[:] = t_temp
                        sal_sh_obj = shared_memory.SharedMemory(create=True, size=t_sal.nbytes, name=sal_name)
                        np.ndarray(shape, dtype=dtype, buffer=sal_sh_obj.buf)[:] = t_sal
                        
                        ph_sh_obj = shared_memory.SharedMemory(create=True, size=np.prod(shape) * 4, name=out_ph_name)
                        np.ndarray(shape, dtype=np.float32, buffer=ph_sh_obj.buf)[:] = np.nan
                        ar_sh_obj = shared_memory.SharedMemory(create=True, size=np.prod(shape) * 4, name=out_arag_name)
                        np.ndarray(shape, dtype=np.float32, buffer=ar_sh_obj.buf)[:] = np.nan
                        cal_sh_obj = shared_memory.SharedMemory(create=True, size=np.prod(shape) * 4, name=out_cal_name)
                        np.ndarray(shape, dtype=np.float32, buffer=cal_sh_obj.buf)[:] = np.nan
                        
                        shm_info = {
                            'time_index': t,
                            'ta_name': ta_name, 'dic_name': dic_name, 'temp_name': temp_name, 'sal_name': sal_name,
                            'out_ph_name': out_ph_name, 'out_arag_name': out_arag_name, 'out_cal_name': out_cal_name,
                            'shape': shape, 'dtype': dtype.name, 'depth_vals': depth_vals.tolist(), 'depth_batch_size': args.depth_batch_size
                        }
                        
                        fut = executor.submit(worker_shm_compute_time, shm_info)
                        resource = {
                            'sh_objs': [ta_sh_obj, dic_sh_obj, temp_sh_obj, sal_sh_obj, ph_sh_obj, ar_sh_obj, cal_sh_obj],
                            'shape': shape,
                            'shm_names': [ta_name, dic_name, temp_name, sal_name, out_ph_name, out_arag_name, out_cal_name]
                        }
                        tasks.append((fut, 'sharedmem', resource))
                    
                    elif args.mode == 'memmap':
                        tmpdir = tempfile.mkdtemp(prefix=f"ph_memmap_{t}_")
                        ta_path = os.path.join(tmpdir, 'ta.dat')
                        dic_path = os.path.join(tmpdir, 'dic.dat')
                        temp_path = os.path.join(tmpdir, 'temp.dat')
                        sal_path = os.path.join(tmpdir, 'sal.dat')
                        
                        out_ph_path = os.path.join(tmpdir, 'ph.dat')
                        out_arag_path = os.path.join(tmpdir, 'arag.dat')
                        out_cal_path = os.path.join(tmpdir, 'cal.dat')
                        
                        mm_ta = np.memmap(ta_path, dtype=t_dic.dtype, mode='w+', shape=shape)
                        mm_dic = np.memmap(dic_path, dtype=t_dic.dtype, mode='w+', shape=shape)
                        mm_temp = np.memmap(temp_path, dtype=t_temp.dtype, mode='w+', shape=shape)
                        mm_sal = np.memmap(sal_path, dtype=t_sal.dtype, mode='w+', shape=shape)
                        
                        mm_ta[:] = t_ta
                        mm_dic[:] = t_dic
                        mm_temp[:] = t_temp
                        mm_sal[:] = t_sal
                        mm_ta.flush()
                        mm_dic.flush()
                        mm_temp.flush()
                        mm_sal.flush()
                        
                        mm_ph = np.memmap(out_ph_path, dtype=np.float32, mode='w+', shape=shape)
                        mm_arag = np.memmap(out_arag_path, dtype=np.float32, mode='w+', shape=shape)
                        mm_cal = np.memmap(out_cal_path, dtype=np.float32, mode='w+', shape=shape)
                        mm_ph[:] = np.nan
                        mm_arag[:] = np.nan
                        mm_cal[:] = np.nan
                        mm_ph.flush()
                        mm_arag.flush()
                        mm_cal.flush()
                        
                        mem_info = {
                            'time_index': t,
                            'ta_path': ta_path, 'dic_path': dic_path, 'temp_path': temp_path, 'sal_path': sal_path,
                            'out_ph_path': out_ph_path, 'out_arag_path': out_arag_path, 'out_cal_path': out_cal_path,
                            'shape': shape, 'dtype': str(t_dic.dtype), 'depth_vals': depth_vals.tolist(), 'depth_batch_size': args.depth_batch_size
                        }
                        
                        fut = executor.submit(worker_memmap_compute_time, mem_info)
                        resource = {'tmpdir': tmpdir, 'shape': shape}
                        tasks.append((fut, 'memmap', resource))
                    
                    # Bounded queue: when we have workers worth of pending tasks, pop oldest and wait for it
                    if len(tasks) >= args.workers:
                        old_fut, old_mode, old_res = tasks.pop(0)
                        try:
                            _handle_task_result(old_fut, old_mode, old_res)
                        except Exception:
                            logger.exception("Error processing completed task")
                
                # Process any remaining tasks
                for fut, mode_type, resource in tasks:
                    try:
                        _handle_task_result(fut, mode_type, resource)
                    except Exception:
                        logger.exception("Error processing final batch of tasks")
        logger.info(f"Complete! Elapsed time: {elapsed:.2f}s ({elapsed/t_size:.2f}s per timestep)")
        if time_dim:
            logger.info(f"Total valid points: {total_valid_points}, PyC02SYS time: {total_pyco_time:.2f}s")
        logger.info(f"Output files written to {args.output_dir}")
        
    finally:
        ds_dic.close()
        ds_ta.close()
        ds_temp.close()
        ds_sal.close()


if __name__ == '__main__':
    main()
