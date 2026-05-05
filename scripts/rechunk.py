#!/usr/bin/env python3
"""Rechunk NetCDF climatology files for optimal pixel timeseries extraction.

Changes chunks from (1, H, W) to (T, 20, 20) so extracting a single pixel
loads just one chunk instead of every chunk for every timestep.

Usage:
    python rechunk_climatology.py /path/to/file.nc --complevel 1          # Fast (complevel=1)
    python rechunk_climatology.py /path/to/file.nc --complevel 4          # Better compression
    python rechunk_climatology.py /path/to/file.nc --complevel 0          # No compression (fastest)
"""

import os
import sys
import xarray as xr
import argparse
from pathlib import Path

# Number of timesteps to copy at a time during rechunking.
# 100 timesteps x 40 × (898×398) float32 ≈ 5 GB — safe on most machines.
# Increase for faster I/O if you have more RAM.
TIME_COPY_BLOCK = 500

def is_already_optimally_chunked(filepath, chunk_size=20):
    """Check if file is already chunked optimally (T, 20, 20) for data variables.
    
    Returns True if all data variables have the target chunking scheme.
    """
    try:
        import netCDF4
        with netCDF4.Dataset(filepath, 'r') as ds:
            # Get unlimited dims
            unlimited_dims = {name for name, dim in ds.dimensions.items() if dim.isunlimited()}
            
            if not unlimited_dims:
                return False  # No unlimited dimension found
            
            time_dim = list(unlimited_dims)[0]
            time_len = ds.dimensions[time_dim].size
            
            # Check all data variables (3D+, not coordinates)
            for var_name in ds.variables:
                var = ds.variables[var_name]
                is_coord = var_name in ds.dimensions
                
                if is_coord or var.ndim < 3:
                    continue  # Skip coordinate variables and <3D data
                
                # Get current chunks
                current_chunks = var.chunking()
                if current_chunks is None:
                    return False  # Variable is not chunked
                
                # Build expected chunks
                expected_chunks = []
                for i, dim_name in enumerate(var.dimensions):
                    if dim_name in unlimited_dims:
                        expected_chunks.append(time_len)  # Full time dimension
                    else:
                        expected_chunks.append(min(chunk_size, var.shape[i]))  # 20 or less for spatial
                expected_chunks = tuple(expected_chunks)
                
                # Compare
                if current_chunks != expected_chunks:
                    return False
            
            return True
    except Exception as e:
        return False  # If we can't check, assume not optimal

def rechunk_file(filepath, output_path=None, chunk_size=20, complevel=4):
    """Rechunk a NetCDF file to (time, chunk_size, chunk_size) for all variables.
    
    If output_path is None, uses input_file.rechunked
    complevel: zlib compression level (0=none, 1=fastest, 4=default, 9=best)
    """
    
    if output_path is None:
        output_path = filepath + '.rechunked'
    
    print(f"\nProcessing: {filepath}")
    print(f"Output: {output_path}")
    
    try:
        # Open with xarray to inspect structure
        ds = xr.open_dataset(filepath, engine='netcdf4')
        
        # Get time dimension length
        time_dim = None
        for dim in ds.dims:
            if 'time' in dim.lower():
                time_dim = dim
                break
        
        if time_dim is None:
            print(f"  WARNING: No time dimension found, skipping")
            ds.close()
            return False
        
        time_len = ds.dims[time_dim]
        print(f"  Time length: {time_len}")
        
        # Find spatial dimensions
        spatial_dims = [d for d in ds.dims if d not in [time_dim]]
        print(f"  Spatial dimensions: {spatial_dims}")
        
        if len(spatial_dims) < 2:
            print(f"  WARNING: Expected 2+ spatial dims, found {len(spatial_dims)}")
            ds.close()
            return False
        
        # Build chunk dict: time dimension = full length, spatial = chunk_size
        chunks = {
            time_dim: time_len,  # Keep full time in memory per chunk
        }
        for spatial_dim in spatial_dims:
            chunks[spatial_dim] = chunk_size
        
        print(f"  Target chunks: {chunks}")
        
        # Check if already optimally chunked
        if is_already_optimally_chunked(filepath, chunk_size):
            print(f"  ✓ File already optimally chunked, skipping")
            ds.close()
            return True
        
        # Close dataset and rechunk using netCDF4 directly for more control
        ds.close()
        
        print(f"  Rechunking with netCDF4...")
        import netCDF4
        
        # Open with netCDF4 to properly set chunking
        with netCDF4.Dataset(filepath, 'r') as src_ds:
            with netCDF4.Dataset(output_path, 'w', format='NETCDF4') as dst_ds:
                # Get unlimited dimensions upfront
                unlimited_dims = {name for name, dim in src_ds.dimensions.items() if dim.isunlimited()}
                
                # Copy dimensions
                for name, dim in src_ds.dimensions.items():
                    if name in unlimited_dims:
                        dst_ds.createDimension(name, None)
                    else:
                        dst_ds.createDimension(name, dim.size)
                
                # Copy global attributes
                dst_ds.setncatts(src_ds.__dict__)
                
                # Copy and rechunk variables
                for var_name in src_ds.variables:
                    src_var = src_ds.variables[var_name]
                    
                    # Skip coordinate variables (1D vars with same name as dimension)
                    is_coord = var_name in src_ds.dimensions
                    
                    # Build chunksizes only for data variables (ndim >= 3, not coordinates)
                    chunksizes = None
                    if not is_coord and src_var.ndim >= 3:
                        chunksizes = []
                        for i, dim_name in enumerate(src_var.dimensions):
                            if dim_name in unlimited_dims:
                                # For unlimited dims (time), use full dimension
                                chunksizes.append(src_var.shape[i])
                            else:
                                # For spatial dims, use chunk_size
                                chunksizes.append(min(chunk_size, src_var.shape[i]))
                        chunksizes = tuple(chunksizes)
                    
                    # Create variable (without chunksizes for 1D coordinate vars)
                    dst_var = dst_ds.createVariable(
                        var_name, 
                        src_var.dtype, 
                        src_var.dimensions,
                        chunksizes=chunksizes,
                        zlib=True,
                        complevel=complevel
                    )
                    
                    # Copy attributes
                    dst_var.setncatts(src_var.__dict__)

                    # Copy data in time-sliced blocks to avoid loading the full array into RAM.
                    # Coordinate/1-D vars are small enough to copy whole.
                    if chunksizes is not None and src_var.ndim >= 3:
                        # Find which axis is the unlimited (time) dimension
                        time_axis = None
                        for i, dim_name in enumerate(src_var.dimensions):
                            if dim_name in unlimited_dims:
                                time_axis = i
                                break
                        if time_axis is not None:
                            t_len = src_var.shape[time_axis]
                            block = max(1, TIME_COPY_BLOCK)
                            slices_src = [slice(None)] * src_var.ndim
                            slices_dst = [slice(None)] * src_var.ndim
                            for t_start in range(0, t_len, block):
                                t_end = min(t_start + block, t_len)
                                slices_src[time_axis] = slice(t_start, t_end)
                                slices_dst[time_axis] = slice(t_start, t_end)
                                dst_var[tuple(slices_dst)] = src_var[tuple(slices_src)]
                                print(f"      {var_name}: copied timesteps {t_start}–{t_end-1}/{t_len-1}", end='\r')
                            print()  # newline after progress
                        else:
                            dst_var[:] = src_var[:]
                    else:
                        dst_var[:] = src_var[:]

                    chunk_str = chunksizes if chunksizes else "(no chunking - coordinate)"
                    print(f"    {var_name}: {chunk_str}")
        
        # Verify
        print(f"  Verifying...")
        ds_verify = xr.open_dataset(output_path)
        sample_var = list(ds_verify.data_vars)[0]
        actual_chunks = ds_verify[sample_var].encoding.get('chunksizes')
        print(f"  ✓ Verified chunks for {sample_var}: {actual_chunks}")
        ds_verify.close()
        
        return True
        
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description='Rechunk NetCDF climatology file for fast pixel extraction')
    parser.add_argument('input_file', help='NetCDF file to rechunk')
    parser.add_argument('-o', '--output', help='Output file path (default: input_file.rechunked)')
    parser.add_argument('--chunk-size', type=int, default=20, help='Spatial chunk size (default: 20)')
    parser.add_argument('--complevel', type=int, default=4, help='Compression level 0-9 (default: 4 for better compression)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without modifying file')
    
    args = parser.parse_args()
    
    input_file = Path(args.input_file)
    if not input_file.is_file():
        print(f"Error: {input_file} is not a file")
        sys.exit(1)
    
    output_file = args.output or (str(input_file) + '.rechunked')
    
    if args.dry_run:
        print(f"[DRY RUN - File will not be modified]")
        print(f"  Input:  {input_file.name}")
        print(f"  Output: {Path(output_file).name}")
        return
    
    if rechunk_file(str(input_file), output_path=output_file, chunk_size=args.chunk_size, complevel=args.complevel):
        print(f"\n✓ Successfully rechunked file to: {output_file}")
    else:
        print(f"\n✗ Failed to rechunk file")
        sys.exit(1)

if __name__ == '__main__':
    main()
