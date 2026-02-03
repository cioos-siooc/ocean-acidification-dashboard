import xarray as xr
import glob
import pandas as pd
from dask.diagnostics import ProgressBar
import os
import time
import argparse
import sys

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi']:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

parser = argparse.ArgumentParser(description="Compute climatology stats")
parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
parser.add_argument("--step", type=int, default=100, help="Batch size (gridY rows)")
parser.add_argument("--smoothing-days", type=int, default=5, help="Smoothing window in days")
args = parser.parse_args()
verbose = args.verbose
stats_file = "SalishSea_Surface_Climatology_Stats.nc"
smoothing_window = args.smoothing_days

start_time = time.time()
if verbose:
    print(f"Starting statistics run. verbose={verbose}, step={args.step}, smoothing_days={smoothing_window}")

if os.path.exists(stats_file):
    print(f"Found intermediate stats file '{stats_file}'. Loading...")
    ds_stats = xr.open_dataset(stats_file)
else:
    # 1. Select January Files
    files = sorted(glob.glob("output_*.nc"))
    print(f"Processing {len(files)} files ...")
    if verbose:
        print(f"Found {len(files)} files. Opening dataset lazily...")
        print(f"Batch size: {args.step}, smoothing: {smoothing_window} days")

    # 2. Load Data (Lazy)
    # chunk={} preserves native chunking to avoid warnings/issues during load
    ds = xr.open_mfdataset(
        files, 
        combine='by_coords', 
        parallel=True, 
        chunks={} 
    )

    if verbose:
        print(f"Dataset dims: time={ds.sizes.get('time')}, gridY={ds.sizes.get('gridY')}, gridX={ds.sizes.get('gridX')}")

    # 3. Virtual Year Setup
    print("Aligning timestamps...")
    # This is a metadata operation, safe to do globally
    virtual_times = ds.indexes['time'].map(lambda x: x.replace(year=2020))
    ds.coords['virtual_time'] = ('time', virtual_times) # Assign to ds, da will inherit

    # 4. Process in Spatial Batches to save RAM
    # Quantiles/Median require loading full time series into memory.
    # We do this for small spatial strips (gridY slices) to prevent OOM.
    
    total_y = ds.sizes['gridY']
    step = args.step
    total_batches = (total_y + step - 1) // step
    part_files = []
    
    if verbose:
        print(f"Starting batched processing for {total_y} Y-rows (Batch size: {step}, {total_batches} batches)...")
    else:
        print(f"Starting batched processing for {total_y} Y-rows (Batch size: {step})...")

    # Ensure tmp directory exists
    os.makedirs("tmp_stats", exist_ok=True)

    for start_y in range(0, total_y, step):
        end_y = min(start_y + step, total_y)
        part_filename = f"tmp_stats/part_y{start_y}_{end_y}.nc"
        part_files.append(part_filename)

        if os.path.exists(part_filename):
            try:
                # Validate the existing part
                with xr.open_dataset(part_filename) as tmp_ds:
                    # force load of time to check decoding
                    _ = tmp_ds.virtual_time.values 
                print(f"Skipping existing valid batch: {part_filename}")
                continue
            except Exception as e:
                print(f"Found corrupted/incompatible batch {part_filename}: {e}")
                print("Deleting and regenerating...")
                os.remove(part_filename)

        batch_idx = (start_y // step) + 1
        print(f"Processing batch Y: {start_y} to {end_y} ... ({batch_idx}/{total_batches})")
        batch_start = time.time()

        # Select spatial slice
        ds_slice = ds.isel(gridY=slice(start_y, end_y))
        da_slice = ds_slice['temperature']

        # Load into RAM
        if verbose:
            print("Loading batch into memory...")
        load_start = time.time()
        da_slice = da_slice.load()
        load_time = time.time() - load_start
        # Try to get approx memory size
        try:
            nbytes = int(getattr(da_slice, 'nbytes', getattr(da_slice.data, 'nbytes', None)))
        except Exception:
            nbytes = None
        if verbose and nbytes is not None:
            print(f"  Loaded {sizeof_fmt(nbytes)} in {load_time:.1f}s")

        # Group & Calc (runs on numpy arrays in RAM)
        compute_start = time.time()
        grouped = da_slice.groupby('virtual_time')
        ds_stats_part = xr.Dataset({
            'mean':   grouped.mean(),
            'median': grouped.median(),
            'q1':     grouped.quantile(0.25).drop_vars('quantile'),
            'q3':     grouped.quantile(0.75).drop_vars('quantile'),
            'min':    grouped.min(),
            'max':    grouped.max()
        })
        compute_time = time.time() - compute_start
        if verbose:
            print(f"  Stats computed in {compute_time:.1f}s")

        # Ensure correct write ordering/chunking
        ds_stats_part = ds_stats_part.sortby('virtual_time')

        # Reset encoding to ensure standard units
        if 'units' in ds_stats_part.virtual_time.encoding:
            del ds_stats_part.virtual_time.encoding['units']
        ds_stats_part.virtual_time.encoding['units'] = 'seconds since 2020-01-01 00:00:00'

        # Save part and report
        save_start = time.time()
        ds_stats_part.to_netcdf(part_filename)
        save_time = time.time() - save_start
        if verbose:
            try:
                fsize = os.path.getsize(part_filename)
                print(f"  Saved {part_filename} ({sizeof_fmt(fsize)}) in {save_time:.1f}s")
            except Exception:
                print(f"  Saved {part_filename} in {save_time:.1f}s")

        # Explicit clean up
        ds_stats_part.close()
        del ds_stats_part, da_slice, grouped, ds_slice

        # Progress/ETA
        batch_elapsed = time.time() - batch_start
        total_elapsed = time.time() - start_time
        avg = total_elapsed / batch_idx
        remaining = total_batches - batch_idx
        eta = avg * remaining
        if verbose:
            print(f"  Batch done in {batch_elapsed:.1f}s — elapsed {total_elapsed:.1f}s — ETA {eta/60:.1f} min")

    print("Merging spatial batches...")
    
    # Combine parts
    ds_stats = xr.open_mfdataset(part_files, combine='by_coords')
    
    # Save the full merged stats file (so we have it for next time)
    # This should be fast as it's just copying pre-computed files
    print(f"Saving merged intermediate stats to {stats_file}...")
    merge_save_start = time.time()
    # Ensure merged dataset also has clean encoding
    if 'units' in ds_stats.virtual_time.encoding:
        del ds_stats.virtual_time.encoding['units']
    ds_stats.virtual_time.encoding['units'] = 'seconds since 2020-01-01 00:00:00'
    ds_stats.to_netcdf(stats_file)
    merge_save_time = time.time() - merge_save_start
    if verbose:
        print(f"Merged stats saved in {merge_save_time:.1f}s — total elapsed {time.time() - start_time:.1f}s")
    
    # Cleanup parts (optional, but good practice)
    # import shutil
    # shutil.rmtree("tmp_stats")

    # Reload fresh
    ds_stats = xr.open_dataset(stats_file)

# 5. Smoothing
# We use min_periods=1 to allow edges to be calculated even if window is cut off
print("Applying smoothing...")
smooth_start = time.time()
ds_smoothed = ds_stats.rolling(virtual_time=smoothing_window*24, center=True, min_periods=1).mean()
smooth_time = time.time() - smooth_start
if verbose:
    print(f"Smoothing computed in {smooth_time:.1f}s")

# 6. Save
output_file = "SalishSea_Surface_Climatology.nc"
print(f"Saving to {output_file}...")
save_start = time.time()
with ProgressBar():
    ds_smoothed.to_netcdf(output_file)
save_time = time.time() - save_start
if verbose:
    print(f"Saved final file in {save_time:.1f}s — total run time {time.time() - start_time:.1f}s")

print("Done.")