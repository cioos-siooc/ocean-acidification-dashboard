#!/usr/bin/env python3
"""Merge and rechunk NetCDF climatology files for optimal pixel timeseries extraction.

Reads all .nc files from current directory, merges them by time dimension,
and saves with (T, 20, 20) chunking for fast pixel extraction.

Usage:
    python rechunk_climatology.py                                  # Fast (complevel=1)
    python rechunk_climatology.py --complevel 4                    # Better compression
    python rechunk_climatology.py --chunk-size 32                  # Different chunk size
    python rechunk_climatology.py --output merged_result.nc        # Custom output name
"""

import os
import sys
import xarray as xr
import argparse
from pathlib import Path
from datetime import datetime

def find_nc_files(directory='.'):
    """Find all .nc files in directory, sorted by modification time."""
    nc_files = sorted(Path(directory).glob('*.nc'))
    return [str(f) for f in nc_files]

def get_time_range(filepath):
    """Get min/max time values from a NetCDF file."""
    try:
        with xr.open_dataset(filepath, engine='netcdf4') as ds:
            # Find time dimension
            time_dim = None
            for dim in ds.dims:
                if 'time' in dim.lower():
                    time_dim = dim
                    break
            
            if time_dim is None:
                return None, None, 0
            
            time_values = ds[time_dim].values
            file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
            return time_values[0], time_values[-1], file_size_mb
    except Exception as e:
        print(f"  Warning: Could not read time from {Path(filepath).name}: {e}")
        return None, None, 0

def merge_files(filepaths, output_path=None, chunk_size=20, complevel=1):
    """Merge multiple NetCDF files by time and rechunk.
    
    Args:
        filepaths: List of NC file paths to merge
        output_path: Output file path (default: merged_TIMESTAMP.nc)
        chunk_size: Spatial chunk size (default: 20)
        complevel: Compression level (default: 1 for speed)
    """
    
    if not filepaths:
        print("Error: No NetCDF files found")
        return False
    
    if output_path is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = f'merged_{timestamp}.nc'
    
    print(f"Found {len(filepaths)} NetCDF files:")
    
    # Calculate total size upfront
    total_size_mb = 0
    for fp in filepaths:
        size_mb = os.path.getsize(fp) / (1024 * 1024)
        total_size_mb += size_mb
        print(f"  - {Path(fp).name:50} {size_mb:8.1f} MB")
    print(f"  Total input size: {total_size_mb:.1f} MB")
    
    try:
        # Sort files by time dimension
        print("\n[1/4] Sorting files by time dimension...")
        file_times = []
        for i, fp in enumerate(filepaths, 1):
            print(f"  [{i}/{len(filepaths)}] Reading {Path(fp).name}...", end=" ", flush=True)
            t_min, t_max, size_mb = get_time_range(fp)
            if t_min is not None:
                file_times.append((fp, t_min, t_max))
                print(f"OK ({t_min} to {t_max}, {size_mb:.1f} MB)")
            else:
                print(f"SKIP (no time dimension)")
        
        if not file_times:
            print("Error: No files with time dimensions found")
            return False
        
        # Sort by start time
        print(f"\n  Sorting {len(file_times)} files by start time...")
        file_times.sort(key=lambda x: x[1])
        sorted_files = [ft[0] for ft in file_times]
        
        for i, (fp, t_min, t_max) in enumerate(file_times, 1):
            print(f"  {i:2}. {Path(fp).name:50} {t_min} to {t_max}")
        
        print(f"\n[2/4] Inspecting structure from first file...")
        import netCDF4
        import time as time_module

        first_file = sorted_files[0]
        with netCDF4.Dataset(first_file, 'r') as src0:
            unlimited_dims = {name for name, dim in src0.dimensions.items() if dim.isunlimited()}
            if not unlimited_dims:
                # Fall back: treat the time-like dim as unlimited
                time_dim_nc = next((n for n in src0.dimensions if 'time' in n.lower()), None)
                unlimited_dims = {time_dim_nc} if time_dim_nc else set()
            time_dim_nc = next(iter(unlimited_dims)) if unlimited_dims else None
            if time_dim_nc is None:
                print("Error: No time dimension found")
                return False

            all_dim_names  = list(src0.dimensions.keys())
            all_var_names  = list(src0.variables.keys())
            spatial_dims   = [d for d in all_dim_names if d not in unlimited_dims]
            global_attrs   = src0.__dict__
            dim_sizes      = {name: dim.size for name, dim in src0.dimensions.items()}  # actual sizes
            var_meta       = {}  # var_name -> (dtype, dims, attrs)
            for vn in all_var_names:
                v = src0.variables[vn]
                var_meta[vn] = (v.dtype, v.dimensions, v.__dict__)
            total_timesteps_per_file = src0.dimensions[time_dim_nc].size

        total_timesteps = total_timesteps_per_file * len(sorted_files)
        print(f"  Time dim : {time_dim_nc}")
        print(f"  Spatial  : {spatial_dims}")
        print(f"  Files    : {len(sorted_files)}  ×  {total_timesteps_per_file} timesteps  =  {total_timesteps} total")

        # Chunk time = timesteps per source file so each write fills complete HDF5 chunks.
        # Writing partial time-chunks forces HDF5 to buffer ALL spatial chunks simultaneously,
        # which is what caused the OOM. TIME_COPY_BLOCK must equal chunk_time.
        chunk_time = total_timesteps_per_file
        TIME_COPY_BLOCK = chunk_time

        start_write = time_module.time()
        print(f"\n[3/4] Creating output file: {output_path}")
        print(f"  Chunks  : time={chunk_time} (per source file), spatial={chunk_size}×{chunk_size}, complevel={complevel}")

        with netCDF4.Dataset(output_path, 'w', format='NETCDF4') as dst_ds:
            # Dimensions
            for name in all_dim_names:
                if name in unlimited_dims:
                    dst_ds.createDimension(name, None)
                else:
                    with netCDF4.Dataset(first_file, 'r') as src0:
                        dst_ds.createDimension(name, src0.dimensions[name].size)

            # Global attributes
            dst_ds.setncatts(global_attrs)

            # Create variables with target chunking (don't write data yet)
            is_coord = lambda vn: vn in all_dim_names
            for vn, (dtype, dims, attrs) in var_meta.items():
                ndim = len(dims)
                chunksizes = None
                if not is_coord(vn) and ndim >= 3:
                    chunksizes = tuple(
                        chunk_time if d in unlimited_dims else min(chunk_size, dim_sizes.get(d, chunk_size))
                        for d in dims
                    )
                dst_var = dst_ds.createVariable(
                    vn, dtype, dims,
                    chunksizes=chunksizes,
                    zlib=(complevel > 0),
                    complevel=complevel,
                )
                dst_var.setncatts(attrs)

            # Stream data: one source file at a time, one time-block at a time
            print(f"\n[4/4] Streaming data ({TIME_COPY_BLOCK} timesteps/block)...")
            t_offset = 0
            for file_idx, fp in enumerate(sorted_files, 1):
                print(f"  [{file_idx}/{len(sorted_files)}] {Path(fp).name}")
                with netCDF4.Dataset(fp, 'r') as src_ds:
                    t_len = src_ds.dimensions[time_dim_nc].size
                    for vn in all_var_names:
                        src_var = src_ds.variables[vn]
                        dst_var = dst_ds.variables[vn]
                        if not is_coord(vn) and src_var.ndim >= 3:
                            # Find time axis index
                            t_axis = src_var.dimensions.index(time_dim_nc)
                            slc_src = [slice(None)] * src_var.ndim
                            slc_dst = [slice(None)] * src_var.ndim
                            for t_start in range(0, t_len, TIME_COPY_BLOCK):
                                t_end = min(t_start + TIME_COPY_BLOCK, t_len)
                                slc_src[t_axis] = slice(t_start, t_end)
                                slc_dst[t_axis] = slice(t_offset + t_start, t_offset + t_end)
                                dst_var[tuple(slc_dst)] = src_var[tuple(slc_src)]
                            print(f"    {vn}: {t_len} timesteps written (offset {t_offset})")
                        elif is_coord(vn) and file_idx == 1:
                            # Write coordinate vars only from first file (except time)
                            if time_dim_nc not in src_var.dimensions:
                                dst_var[:] = src_var[:]
                        # Time coordinate: append from every file
                        if vn == time_dim_nc or (src_var.ndim == 1 and src_var.dimensions == (time_dim_nc,)):
                            dst_var[t_offset:t_offset + t_len] = src_var[:]
                t_offset += t_len

            write_time = time_module.time() - start_write
            output_size_mb = os.path.getsize(output_path) / (1024 * 1024)
            print(f"\n  Write completed in {write_time:.1f}s, output size: {output_size_mb:.1f} MB")

        # Verify
        print(f"\nVerifying output file...")
        ds_verify = xr.open_dataset(output_path, engine='netcdf4')
        print(f"  Dimensions:")
        for dim_name, dim_size in ds_verify.dims.items():
            print(f"    {dim_name:20} = {dim_size}")
        print(f"  Data variables: {len(ds_verify.data_vars)}")
        for var_name in list(ds_verify.data_vars)[:3]:
            var = ds_verify[var_name]
            chunks = var.encoding.get('chunksizes', 'N/A')
            print(f"    {var_name:30} {str(var.shape):30} chunks={chunks}")
        if len(ds_verify.data_vars) > 3:
            print(f"    ... and {len(ds_verify.data_vars) - 3} more")
        ds_verify.close()
        print(f"\n  ✓ Verification successful!")

        return True
    
    except Exception as e:
        print(f"\nERROR during merge: {e}")
        import traceback
        traceback.print_exc()
        return False

def rechunk_file_single(filepath, output_path=None, chunk_size=20, complevel=1):
    """Rechunk a single NetCDF file to (time, chunk_size, chunk_size) for all variables.
    
    If output_path is None, uses input_file.rechunked
    complevel: zlib compression level (0=none, 1=fastest, 4=default, 9=best)
    
    Note: This is kept for backwards compatibility but merge_files is preferred.
    """
    pass  # Not used in current workflow

def main():
    import time as time_module
    start_total = time_module.time()
    
    parser = argparse.ArgumentParser(description='Merge all .nc files from current directory with optimal chunking')
    parser.add_argument('-o', '--output', help='Output file path (default: merged_TIMESTAMP.nc)')
    parser.add_argument('--chunk-size', type=int, default=20, help='Spatial chunk size (default: 20)')
    parser.add_argument('--complevel', type=int, default=1, help='Compression level 0-9 (default: 1 for speed)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without merging')
    
    args = parser.parse_args()
    
    # Find all .nc files in current directory
    nc_files = find_nc_files('.')
    
    if not nc_files:
        print("Error: No .nc files found in current directory")
        sys.exit(1)
    
    if args.dry_run:
        print(f"[DRY RUN - Files will not be modified]")
        print(f"Found {len(nc_files)} files:")
        for f in nc_files:
            print(f"  {Path(f).name}")
        return
    
    if merge_files(nc_files, output_path=args.output, chunk_size=args.chunk_size, complevel=args.complevel):
        total_time = time_module.time() - start_total
        output = args.output or f'merged_*.nc'
        print(f"\n{'='*70}")
        print(f"✓ Successfully merged and rechunked files")
        print(f"  Output: {output}")
        print(f"  Total time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
        print(f"{'='*70}")
    else:
        print(f"\n✗ Failed to merge files")
        sys.exit(1)

if __name__ == '__main__':
    main()
