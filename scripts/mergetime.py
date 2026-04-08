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
        
        print(f"\n[2/4] Loading and concatenating {len(sorted_files)} files...")
        datasets = []
        total_loaded_mb = 0
        for i, fp in enumerate(sorted_files, 1):
            size_mb = os.path.getsize(fp) / (1024 * 1024)
            print(f"  [{i}/{len(sorted_files)}] Loading {Path(fp).name}...", end=" ", flush=True)
            ds = xr.open_dataset(fp, engine='netcdf4')
            datasets.append(ds)
            total_loaded_mb += size_mb
            n_time = ds.dims.get('virtual_time') or ds.dims.get('time') or 0
            n_vars = len(ds.data_vars)
            print(f"OK ({n_time} timesteps, {n_vars} variables)")
        
        print(f"\n  Concatenating {len(datasets)} datasets along time dimension...")
        # Concatenate along time dimension
        merged_ds = xr.concat(datasets, dim=None)  # Let xarray auto-detect time dim
        
        # Find actual time dimension name
        time_dim = None
        for dim in merged_ds.dims:
            if 'time' in dim.lower():
                time_dim = dim
                break
        
        if time_dim is None:
            print("Error: No time dimension found in merged dataset")
            for ds in datasets:
                ds.close()
            return False
        
        total_timesteps = merged_ds.dims[time_dim]
        total_vars = len(merged_ds.data_vars)
        print(f"  ✓ Successfully merged into {total_timesteps} timesteps, {total_vars} variables")
        
        # Close source datasets to free memory
        print(f"\n  Closing source datasets...")
        for ds in datasets:
            ds.close()
        print(f"  ✓ Memory released")
        
        # Save merged file
        print(f"\n[3/4] Creating output file with rechunking...")
        print(f"  Output: {output_path}")
        print(f"  Format: NetCDF4 (HDF5)")
        print(f"  Compression: zlib level {complevel}")
        
        # Use netCDF4 to save with proper chunking
        import netCDF4
        
        # Determine target chunks
        spatial_dims = [d for d in merged_ds.dims if d not in [time_dim]]
        chunks = {time_dim: total_timesteps}
        for sd in spatial_dims:
            chunks[sd] = chunk_size
        print(f"  Chunk scheme: time={total_timesteps}, spatial={chunk_size}x{chunk_size}")
        
        # Now use netCDF4 to write with chunking (merged_ds already in memory)
        import time as time_module
        start_write = time_module.time()
        
        with netCDF4.Dataset(output_path, 'w', format='NETCDF4') as dst_ds:
            print(f"\n[4/4] Writing variables to disk...")
            
            # Get unlimited dims
            unlimited_dims = {time_dim}  # Time is unlimited
            
            # Copy dimensions
            print(f"  Creating {len(merged_ds.dims)} dimensions...")
            for name in merged_ds.dims:
                size = merged_ds.dims[name]
                if name in unlimited_dims:
                    dst_ds.createDimension(name, None)
                    print(f"    {name:20} = unlimited ({size} current)")
                else:
                    dst_ds.createDimension(name, size)
                    print(f"    {name:20} = {size}")
            
            # Copy global attributes
            print(f"\n  Copying global attributes ({len(merged_ds.attrs)} items)...")
            dst_ds.setncatts(merged_ds.attrs)
            
            # Copy and rechunk variables
            print(f"\n  Writing {len(merged_ds.data_vars)} data variables:")
            for var_idx, var_name in enumerate(merged_ds.data_vars, 1):
                src_var = merged_ds[var_name]
                is_coord = var_name in merged_ds.dims
                
                # Build chunksizes for data variables
                chunksizes = None
                if not is_coord and src_var.ndim >= 3:
                    chunksizes = []
                    for i, dim_name in enumerate(src_var.dims):
                        if dim_name == time_dim:
                            chunksizes.append(total_timesteps)
                        else:
                            chunksizes.append(min(chunk_size, src_var.shape[i]))
                    chunksizes = tuple(chunksizes)
                
                # Create variable
                try:
                    print(f"    [{var_idx}/{len(merged_ds.data_vars)}] {var_name:30}", end=" ", flush=True)
                    dst_var = dst_ds.createVariable(
                        var_name,
                        src_var.dtype,
                        src_var.dims,
                        chunksizes=chunksizes,
                        zlib=True,
                        complevel=complevel
                    )
                    
                    # Copy attributes
                    dst_var.setncatts(src_var.attrs)
                    
                    # Copy data
                    dst_var[:] = src_var.values
                    
                    chunk_str = f"chunks={chunksizes}" if chunksizes else "(no chunking)"
                    size_mb = (src_var.size * src_var.dtype.itemsize) / (1024 * 1024)
                    print(f"OK ({chunk_str}, {size_mb:.1f} MB)")
                except Exception as e:
                    print(f"ERROR - {e}")
            
            # Copy coordinate variables
            print(f"\n  Writing {len(merged_ds.coords)} coordinate variables:")
            for coord_idx, coord_name in enumerate(merged_ds.coords, 1):
                if coord_name not in dst_ds.variables:
                    coord_var = merged_ds[coord_name]
                    try:
                        print(f"    [{coord_idx}/{len(merged_ds.coords)}] {coord_name:30}", end=" ", flush=True)
                        dst_coord = dst_ds.createVariable(
                            coord_name,
                            coord_var.dtype,
                            coord_var.dims
                        )
                        dst_coord.setncatts(coord_var.attrs)
                        dst_coord[:] = coord_var.values
                        print(f"OK")
                    except Exception as e:
                        print(f"ERROR - {e}")
            
            merged_ds.close()
            
            write_time = time_module.time() - start_write
            output_size_mb = os.path.getsize(output_path) / (1024 * 1024)
            print(f"\n  Write completed in {write_time:.1f}s, output size: {output_size_mb:.1f} MB")
        
        # Cleanup
        for ds in datasets:
            ds.close()
        
        # Verify
        print(f"\nVerifying output file...")
        ds_verify = xr.open_dataset(output_path, engine='netcdf4')
        
        print(f"  Dimensions:")
        for dim_name, dim_size in ds_verify.dims.items():
            print(f"    {dim_name:20} = {dim_size}")
        
        print(f"  Data variables: {len(ds_verify.data_vars)}")
        for var_name in list(ds_verify.data_vars)[:3]:  # Show first 3
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
