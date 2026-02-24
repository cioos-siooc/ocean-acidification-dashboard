#!/usr/bin/env python3
"""Standalone pH and carbonate saturation computation script.

Computes pH_total, omega_arag, and omega_cal from input NetCDF files containing:
  - Temperature
  - Salinity
  - Total Alkalinity (TA)
  - Dissolved Inorganic Carbon (DIC)

Outputs are written to separate NetCDF files.

Usage:
 python calc_ph_grid_shm_memmap.py --dic-file DIC.nc --ta-file TA.nc --temp-file Temp.nc --sal-file Sal.nc --output-dir /opt/data/nc
"""

from __future__ import annotations
import argparse
import logging
import os
import sys
import time

import numpy as np
import xarray as xr
import PyCO2SYS as pyco2
import netCDF4 as nc4
from concurrent.futures import ProcessPoolExecutor, as_completed

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("calc_ph_grid")

# Constants
DEFAULT_DENSITY = 1025.0
DEPTH_TO_PRESSURE_FACTOR = 1.019716


def depth_to_pressure(depth_m: np.ndarray) -> np.ndarray:
    """Convert depth in meters to pressure in dbar."""
    return depth_m / DEPTH_TO_PRESSURE_FACTOR


def _worker_process_timestep(args_dict):
    """
    Worker function to process a single timestep. 
    Executed in a separate process.
    
    Returns: (timestep_index, ph_arr, ar_arr, cal_arr)
    """
    dic_data = args_dict['dic_data']
    ta_data = args_dict['ta_data']
    temp_data = args_dict['temp_data']
    sal_data = args_dict['sal_data']
    depth_vals = args_dict['depth_vals']
    depth_batch_size = args_dict['depth_batch_size']
    time_index = args_dict['time_index']
    
    ph_arr, ar_arr, cal_arr = compute_ph_carbonate(
        dic_data, ta_data, temp_data, sal_data, depth_vals, depth_batch_size
    )
    
    return (time_index, ph_arr, ar_arr, cal_arr)


def _worker_process_timestep_batch(args_dict):
    """
    Worker function to process a batch of timesteps together.
    This allows PyC02SYS to process a larger array for better vectorization.
    
    Returns: list of (timestep_index, ph_arr, ar_arr, cal_arr) tuples
    """
    dic_data_batch = args_dict['dic_data_batch']  # (batch_size, depth, y, x)
    ta_data_batch = args_dict['ta_data_batch']
    temp_data_batch = args_dict['temp_data_batch']
    sal_data_batch = args_dict['sal_data_batch']
    depth_vals = args_dict['depth_vals']
    depth_batch_size = args_dict['depth_batch_size']
    time_indices = args_dict['time_indices']
    
    results = []
    
    # Process all timesteps in this batch
    for batch_idx, time_idx in enumerate(time_indices):
        dic_data = dic_data_batch[batch_idx]
        ta_data = ta_data_batch[batch_idx]
        temp_data = temp_data_batch[batch_idx]
        sal_data = sal_data_batch[batch_idx]
        
        ph_arr, ar_arr, cal_arr = compute_ph_carbonate(
            dic_data, ta_data, temp_data, sal_data, depth_vals, depth_batch_size
        )
        results.append((time_idx, ph_arr, ar_arr, cal_arr))
    
    return results


# ----------------------------------------------------------------------------
# Main computation
# ----------------------------------------------------------------------------

def compute_ph_carbonate(dic_data, ta_data, temp_data, sal_data, depth_vals, depth_batch_size=8):
    """
    Compute pH and carbonate saturation states using pyco2sys.
    
    Args:
        dic_data: DIC array (depth, y, x)
        ta_data: TA array (depth, y, x)
        temp_data: Temperature array (depth, y, x)
        sal_data: Salinity array (depth, y, x)
        depth_vals: Depth values in meters
        depth_batch_size: Process depths in batches to manage memory
        
    Returns:
        ph_arr, omega_arag, omega_cal: Output arrays
    """
    
    shape = dic_data.shape
    d, ny, nx = shape
    
    ph_arr = np.full(shape, np.nan, dtype=np.float32)
    ar_arr = np.full(shape, np.nan, dtype=np.float32)
    cal_arr = np.full(shape, np.nan, dtype=np.float32)
    
    n_valid_total = 0
    
    for start in range(0, d, depth_batch_size):
        stop = min(d, start + depth_batch_size)
        
        s_ta = ta_data[start:stop]
        s_dic = dic_data[start:stop]
        s_temp = temp_data[start:stop]
        s_sal = sal_data[start:stop]
        
        # Convert to µmol/kg
        ta_flat = (s_ta * (1000.0 / DEFAULT_DENSITY)).ravel()
        dic_flat = (s_dic * (1000.0 / DEFAULT_DENSITY)).ravel()
        temp_flat = s_temp.ravel()
        sal_flat = s_sal.ravel()
        
        # Build pressure grid
        depth_batch = np.array(depth_vals[start:stop])
        p_grid = depth_to_pressure(depth_batch)[:, None, None].repeat(ny, axis=1).repeat(nx, axis=2).ravel()
        
        # Valid data mask
        mask = ~(np.isnan(ta_flat) | np.isnan(dic_flat) | np.isnan(temp_flat) | np.isnan(sal_flat) | (sal_flat <= 0))
        n_valid = int(mask.sum())
        n_valid_total += n_valid
        
        if n_valid == 0:
            continue
        
        # Compute carbonate system
        res = pyco2.sys(
            par1=ta_flat[mask], par2=dic_flat[mask],
            par1_type=1, par2_type=2,
            salinity=sal_flat[mask], temperature=temp_flat[mask],
            pressure=p_grid[mask],
            opt_pH_scale=1, opt_k_carbonic=10
        )
        
        # Fill output arrays
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
    
    logger.info(f"Processed {n_valid_total} valid points")
    return ph_arr, ar_arr, cal_arr

# ----------------------------------------------------------------------------
# Main entry point
# ----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Compute pH and carbonate saturation states from ocean data.')
    parser.add_argument('--dic-file', required=True, help='Path to DIC NetCDF file')
    parser.add_argument('--ta-file', required=True, help='Path to TA NetCDF file')
    parser.add_argument('--temp-file', required=True, help='Path to Temperature NetCDF file')
    parser.add_argument('--sal-file', required=True, help='Path to Salinity NetCDF file')
    parser.add_argument('--output-dir', required=True, help='Output directory for computed NetCDF files')
    parser.add_argument('--depth-batch-size', type=int, default=8, help='Process depths in batches (default: 8)')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers (default: 4)')
    args = parser.parse_args()
    
    # Open input files
    logger.info("Loading input files...")
    ds_dic = xr.open_dataset(args.dic_file)
    ds_ta = xr.open_dataset(args.ta_file)
    ds_temp = xr.open_dataset(args.temp_file)
    ds_sal = xr.open_dataset(args.sal_file)
    
    try:
        # Infer variable names (support common naming conventions)
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
        
        # Get data arrays
        dic_data = ds_dic[dic_var].values
        ta_data = ds_ta[ta_var].values
        temp_data = ds_temp[temp_var].values
        sal_data = ds_sal[sal_var].values
        
        # Determine dimensions
        dims = ds_dic[dic_var].dims
        coords = ds_dic.coords
        
        time_dim = next((d for d in dims if 'time' in d.lower()), None)
        depth_dim = next((d for d in dims if 'depth' in d.lower()), None)
        
        t_size = ds_dic.sizes[time_dim] if time_dim else 1
        d_size = ds_dic.sizes[depth_dim] if depth_dim else 1
        
        depth_vals = ds_dic[depth_dim].values if depth_dim else np.array([0.0])
        
        logger.info(f"Data shape: time={t_size}, depth={d_size}")
        
        # Ensure output directory exists
        os.makedirs(args.output_dir, exist_ok=True)
        
        # Get base filename for output
        base_filename = os.path.basename(args.dic_file)
        
        # Output filenames
        out_ph = os.path.join(args.output_dir, base_filename.replace('dissolved_inorganic_carbon', 'ph_total'))
        out_ar = os.path.join(args.output_dir, base_filename.replace('dissolved_inorganic_carbon', 'omega_arag'))
        out_cal = os.path.join(args.output_dir, base_filename.replace('dissolved_inorganic_carbon', 'omega_cal'))
        
        # Process each time step
        start_time = time.time()
        
        if time_dim is None:
            # Single time snapshot
            logger.info("Processing single time snapshot...")
            ph_arr, ar_arr, cal_arr = compute_ph_carbonate(
                dic_data, ta_data, temp_data, sal_data, depth_vals, args.depth_batch_size
            )
            
            # Write outputs
            _write_netcdf(out_ph, ph_arr, 'ph_total', coords, depth_dim)
            _write_netcdf(out_ar, ar_arr, 'omega_arag', coords, depth_dim)
            _write_netcdf(out_cal, cal_arr, 'omega_cal', coords, depth_dim)
        else:
            # Time series processing - submit timesteps in batches to workers
            logger.info(f"Submitting {t_size} timesteps to {args.workers} workers (batching timesteps)...")
            
            # Determine batch size: aim for ~2-4 batches per worker for better load balancing
            batch_size = max(1, t_size // (args.workers * 3))
            logger.info(f"Using batch size: {batch_size} timesteps per worker")
            
            # Track which output files have been created
            created_files = set()
            
            with ProcessPoolExecutor(max_workers=args.workers) as executor:
                futures = {}
                
                # Submit batches of timesteps
                for batch_start in range(0, t_size, batch_size):
                    batch_end = min(batch_start + batch_size, t_size)
                    batch_indices = list(range(batch_start, batch_end))
                    
                    # Load all timesteps in this batch
                    dic_batch = []
                    ta_batch = []
                    temp_batch = []
                    sal_batch = []
                    
                    for t in batch_indices:
                        sel_dict = {time_dim: t}
                        dic_slice = ds_dic[dic_var].isel(sel_dict).values
                        ta_slice = ds_ta[ta_var].isel(sel_dict).values
                        temp_slice = ds_temp[temp_var].isel(sel_dict).values
                        sal_slice = ds_sal[sal_var].isel(sel_dict).values
                        
                        # Ensure 3D (depth, y, x)
                        if dic_slice.ndim == 2:
                            dic_slice = dic_slice[np.newaxis, :, :]
                            ta_slice = ta_slice[np.newaxis, :, :]
                            temp_slice = temp_slice[np.newaxis, :, :]
                            sal_slice = sal_slice[np.newaxis, :, :]
                        
                        dic_batch.append(dic_slice)
                        ta_batch.append(ta_slice)
                        temp_batch.append(temp_slice)
                        sal_batch.append(sal_slice)
                    
                    # Convert to arrays and submit
                    args_dict = {
                        'dic_data_batch': np.array(dic_batch),
                        'ta_data_batch': np.array(ta_batch),
                        'temp_data_batch': np.array(temp_batch),
                        'sal_data_batch': np.array(sal_batch),
                        'depth_vals': depth_vals,
                        'depth_batch_size': args.depth_batch_size,
                        'time_indices': batch_indices
                    }
                    future = executor.submit(_worker_process_timestep_batch, args_dict)
                    futures[future] = batch_indices
                
                # Collect results as they complete and write to files
                completed = 0
                for future in as_completed(futures):
                    batch_indices = futures[future]
                    try:
                        batch_results = future.result()
                        
                        # Process results from this batch
                        for time_idx, ph_arr, ar_arr, cal_arr in batch_results:
                            # Write to output files - create on first write
                            if 'ph_total' not in created_files:
                                _write_netcdf_timeseries(out_ph, 'ph_total', ph_arr, time_idx, t_size, coords, depth_dim, time_dim)
                                created_files.add('ph_total')
                            else:
                                _append_netcdf_timeseries(out_ph, 'ph_total', ph_arr, time_idx)
                            
                            if 'omega_arag' not in created_files:
                                _write_netcdf_timeseries(out_ar, 'omega_arag', ar_arr, time_idx, t_size, coords, depth_dim, time_dim)
                                created_files.add('omega_arag')
                            else:
                                _append_netcdf_timeseries(out_ar, 'omega_arag', ar_arr, time_idx)
                            
                            if 'omega_cal' not in created_files:
                                _write_netcdf_timeseries(out_cal, 'omega_cal', cal_arr, time_idx, t_size, coords, depth_dim, time_dim)
                                created_files.add('omega_cal')
                            else:
                                _append_netcdf_timeseries(out_cal, 'omega_cal', cal_arr, time_idx)
                            
                            completed += 1
                        
                        if completed % 50 == 0 or completed == t_size:
                            logger.info(f"Completed {completed}/{t_size} timesteps")
                    except Exception as e:
                        logger.error(f"Error processing batch {batch_indices}: {e}", exc_info=True)
        
        elapsed = time.time() - start_time
        logger.info(f"Complete! Elapsed time: {elapsed:.2f}s ({elapsed/t_size:.2f}s per timestep)")
        logger.info(f"Output files written to {args.output_dir}")
        
    finally:
        ds_dic.close()
        ds_ta.close()
        ds_temp.close()
        ds_sal.close()




def _create_empty_netcdf(filepath, var_name, time_size, coords, depth_dim, time_dim):
    """Create empty NetCDF file with time dimension initialized to NaN."""
    if os.path.exists(filepath):
        os.remove(filepath)
    
    root = nc4.Dataset(filepath, 'w')
    
    # Create time dimension and coordinate
    for cname, coord in coords.items():
        if cname == time_dim or 'time' in cname.lower():
            root.createDimension(cname, time_size)
            var = root.createVariable(cname, 'i8', (cname,))
            var[:] = coord.values.astype('datetime64[s]').astype('int64')
            var.units = 'seconds since 1970-01-01 00:00:00'
        elif cname == depth_dim or 'depth' in cname.lower():
            root.createDimension(cname, len(coord))
            var = root.createVariable(cname, coord.dtype, (cname,))
            var[:] = coord.values
        else:
            if cname not in root.dimensions:
                root.createDimension(cname, len(coord))
            if cname not in root.variables:
                var = root.createVariable(cname, coord.dtype, (cname,))
                var[:] = coord.values
    
    # Create y, x dimensions if needed (rough estimate from coordinates)
    if 'y' not in root.dimensions:
        root.createDimension('y', coords.get('y', np.arange(1)).size if hasattr(coords.get('y'), '__len__') else 1)
    if 'x' not in root.dimensions:
        root.createDimension('x', coords.get('x', np.arange(1)).size if hasattr(coords.get('x'), '__len__') else 1)
    
    # Get actual grid dimensions from any existing variable
    y_size = root.dimensions['y'].size if 'y' in root.dimensions else 1
    x_size = root.dimensions['x'].size if 'x' in root.dimensions else 1
    d_size = root.dimensions[depth_dim].size if depth_dim in root.dimensions else 1
    
    # Create output variable
    dims = (time_dim, depth_dim, 'y', 'x')
    var = root.createVariable(var_name, 'f4', dims, zlib=True, complevel=4, fill_value=np.nan)
    
    # Initialize with NaN
    for t in range(time_size):
        var[t, :, :, :] = np.full((d_size, y_size, x_size), np.nan, dtype=np.float32)
    
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


def _write_netcdf_timeseries(filepath, var_name, data, time_idx, total_times, coords, depth_dim, time_dim):
    """Create NetCDF file on first write with unlimited time dimension."""
    if os.path.exists(filepath):
        os.remove(filepath)
    
    root = nc4.Dataset(filepath, 'w')
    
    # Create time dimension (unlimited)
    root.createDimension(time_dim, None)
    
    # Create time coordinate
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
            if cname not in root.dimensions:
                root.createDimension(cname, len(coord))
            if cname not in root.variables:
                var = root.createVariable(cname, coord.dtype, (cname,))
                var[:] = coord.values
    
    # Create y, x dimensions
    if 'y' not in root.dimensions:
        root.createDimension('y', data.shape[1])
    if 'x' not in root.dimensions:
        root.createDimension('x', data.shape[2])
    
    # Create output variable
    dims = (time_dim, depth_dim, 'y', 'x')
    var = root.createVariable(var_name, 'f4', dims, zlib=True, complevel=4, fill_value=np.nan)
    
    # Write the first timestep
    var[time_idx] = data
    
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
    """Append data to existing timeseries NetCDF file."""
    with nc4.Dataset(filepath, 'a') as ds:
        ds[var_name][time_idx] = data


def _write_netcdf(filepath, data, var_name, coords, depth_dim, time_dim=None):
    """Write a variable to NetCDF file."""
    if os.path.exists(filepath):
        os.remove(filepath)
    
    root = nc4.Dataset(filepath, 'w')
    
    # Create dimensions and coordinates
    for cname, coord in coords.items():
        if cname == time_dim or 'time' in cname.lower():
            root.createDimension(cname, len(coord))
            var = root.createVariable(cname, 'i8', (cname,))
            var[:] = coord.values.astype('datetime64[s]').astype('int64')
            var.units = 'seconds since 1970-01-01 00:00:00'
        elif cname == depth_dim or 'depth' in cname.lower():
            root.createDimension(cname, len(coord))
            var = root.createVariable(cname, coord.dtype, (cname,))
            var[:] = coord.values
        else:
            root.createDimension(cname, len(coord))
            var = root.createVariable(cname, coord.dtype, (cname,))
            var[:] = coord.values
    
    # Create output variable
    if time_dim:
        dims = (time_dim, depth_dim, 'y', 'x')
        root.createDimension('y', data.shape[1])
        root.createDimension('x', data.shape[2])
    else:
        dims = (depth_dim, 'y', 'x')
        root.createDimension('y', data.shape[1])
        root.createDimension('x', data.shape[2])
    
    var = root.createVariable(var_name, 'f4', dims, zlib=True, complevel=4, fill_value=np.nan)
    
    if time_dim:
        var[0] = data
    else:
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


if __name__ == '__main__':
    main()
