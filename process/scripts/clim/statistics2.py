"""Fast climatology stats (NumPy-backed)

This script mirrors the behavior of `statistics.py` but computes median/quantiles
using NumPy on in-memory slices which is much faster than repeated xarray.groupby
for large in-memory chunks.

Usage:
  python process/scripts/statistics2.py --verbose --jobs 1
  python process/scripts/statistics2.py --depth-index 0

Features:
- Full-grid monthly processing (requires sufficient RAM)
- Support for specific depth index processing
- In-memory numpy group-by compute for mean, median, q1, q3, min, max
- Optional simple parallelization (jobs>1) if joblib is available (may increase memory)
- Verbose timing and progress/ETA
"""

# pip3 install xarray bottleneck netCDF4 joblib dask numpy

import xarray as xr
import glob
import numpy as np
import os
import time
import argparse
import warnings
import re
import shutil
from dask.diagnostics import ProgressBar
from netCDF4 import Dataset as NC4Dataset


def validate_netcdf_file(filepath):
    """Check if a NetCDF file is valid and readable."""
    try:
        with NC4Dataset(filepath, 'r') as ds:
            _ = ds.dimensions
            return True
    except Exception as e:
        return False


def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi']:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

parser = argparse.ArgumentParser(description="Compute climatology stats (fast, numpy-based)")
parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
parser.add_argument("--depth-index", type=int, default=None, help="Process specific depth index (default: process all depths or single layer if no depth dim)")
parser.add_argument("--months", type=str, default=None, help="Comma-separated months to process (01,02,..) or 'all' (default)")
parser.add_argument("--jobs", type=int, default=1, help="Number of parallel jobs for group loop (optional)")
args = parser.parse_args()
verbose = args.verbose
jobs = max(1, args.jobs)
months_arg = args.months
depth_index = args.depth_index

start_time = time.time()
if verbose:
    print(f"Starting statistics2 run. verbose={verbose}, jobs={jobs}")

# 1. Select files with date pattern and filter to valid ones
all_files = sorted(glob.glob("*.nc"))

# Only keep files matching YYYYMM pattern and are valid NetCDF
files = []
files_by_month = {}
for f in all_files:
    basename = os.path.basename(f)
    match = re.search(r'_(\d{6})_', basename)
    if match:
        # Validate that it's a readable NetCDF file
        if validate_netcdf_file(f):
            files.append(f)
            yyyymm = match.group(1)
            month = yyyymm[4:6]  # Extract MM from YYYYMM
            files_by_month.setdefault(month, []).append(f)
        else:
            print(f"Warning: Skipping invalid/unreadable NetCDF file: {f}")

print(f"Found {len(all_files)} .nc files, using {len(files)} with valid date patterns and readable...")
if not files:
    print("ERROR: No files with YYYYMM date pattern found. Expected format: variable_YYYYMM_resolution.nc")
    exit(1)

if verbose:
    print(f"Opening {len(files)} files lazily...")

variable = '_'.join(files[0].split('_')[:-2])  # Extract variable name from first file (assumes consistent naming)

# 2. Load dataset lazily (respect native chunks)
# Use parallel=False for the initial open to avoid HDF5/NetCDF thread-safety issues 
# which frequently cause segfaults in Docker/Linux environments.
try:
    ds = xr.open_mfdataset(files, combine='by_coords', parallel=False, chunks={})
except Exception as e:
    print(f"ERROR: Failed to open dataset: {e}")
    exit(1)

# Identify depth dimension candidates
depth_dim = 'depth'

if verbose:
    print(f"Dataset dims: {dict(ds.sizes)}")
    if depth_dim:
        print(f"Found depth dimension: {depth_dim}")
    else:
        print("No depth dimension found.")

# 3. Virtual time setup (metadata operation)
print("Aligning timestamps (virtual year)...")
virtual_times = ds.indexes['time'].map(lambda x: x.replace(year=2020))
ds.coords['virtual_time'] = ('time', virtual_times)

if months_arg:
    if months_arg.strip().lower() == 'all':
        months_to_process = sorted(files_by_month.keys())
    else:
        months_to_process = [m.zfill(2) for m in months_arg.split(',')]
else:
    months_to_process = sorted(files_by_month.keys())

stats_month_files = []
for month in months_to_process:
    month_files = files_by_month.get(month, [])
    if not month_files:
        print(f"No files found for month {month}, skipping.")
        continue
    
    print(f"Processing month {month} with {len(month_files)} files")
    
    # Validate all files in this month batch
    valid_month_files = [f for f in month_files if validate_netcdf_file(f)]
    if len(valid_month_files) != len(month_files):
        invalid_count = len(month_files) - len(valid_month_files)
        print(f"  Warning: {invalid_count} invalid files removed from month {month}")
        if not valid_month_files:
            print(f"  All files for month {month} are invalid, skipping.")
            continue
        month_files = valid_month_files

    # open month dataset lazily and set virtual_time
    try:
        print(f"  Opening {len(month_files)} files with xarray...")
        # Defaulting to parallel=False to prevent "NetCDF: HDF error" and Segfaults
        ds_m = xr.open_mfdataset(month_files, combine='by_coords', parallel=False, chunks={})
    except Exception as e:
        print(f"  Error opening files for month {month}: {e}")
        continue
    
    # Handle depth dimension
    
    # Handle depth dimension
    if depth_dim in ds_m.dims:
        if depth_index is not None:
            # Validate depth index
            if depth_index < 0 or depth_index >= ds_m.sizes[depth_dim]:
                print(f"Error: depth index {depth_index} out of range for {depth_dim} size {ds_m.sizes[depth_dim]}")
                continue
            if verbose:
                print(f"  Selecting depth index {depth_index} on dimension '{depth_dim}'")
            ds_m = ds_m.isel({depth_dim: depth_index})
            stats_month_dir = f"depth_idx/{depth_index}"
        else:
            # Process all depths
            if verbose:
                print(f"  Processing all depths ({ds_m.sizes[depth_dim]} layers)")
            stats_month_dir = "all_depths"
    else:
        if depth_index is not None:
            print(f"Warning: --depth-index specified but no depth dimension found. Ignoring.")
        if verbose:
            print(f"  No depth dimension found, processing single layer")
        stats_month_dir = "single_layer"
    
    os.makedirs(stats_month_dir, exist_ok=True)
    stats_month_file = os.path.join(stats_month_dir, f"month_{month}.nc")

    # Skip if month file already exists (resume capability after crash)
    if os.path.exists(stats_month_file):
        print(f"  Skipping month {month} (output file already exists)")
        stats_month_files.append(stats_month_file)
        continue

    virtual_times = ds_m.indexes['time'].map(lambda x: x.replace(year=2020))
    ds_m.coords['virtual_time'] = ('time', virtual_times)

    # Optional joblib import
    have_joblib = False
    if jobs > 1:
        try:
            from joblib import Parallel, delayed
            have_joblib = True
        except Exception:
            have_joblib = False

    # Try to use bottleneck for speed if available
    try:
        import bottleneck as bn
        have_bn = True
        if verbose: print("  Using bottleneck for accelerated stats.")
    except ImportError:
        have_bn = False
        if verbose: print("  Bottleneck not found, using standard NumPy (slower).")

    da_m = ds_m[variable].squeeze()

    # load into memory
    print(f"  Loading full month {month} into memory...")
    load_start = time.time()
    with ProgressBar():
        arr = da_m.values
    print(f"  Converting to contiguous array...")
    arr = np.ascontiguousarray(arr, dtype=np.float32)
    load_time = time.time() - load_start
    bytes_ = arr.nbytes
    print(f"    Loaded {sizeof_fmt(bytes_)} in {load_time:.1f}s; shape={arr.shape}")

    # group prep
    print(f"  Extracting virtual_time and computing groups...")
    vt = ds_m['virtual_time'].values
    unique_vt, inverse = np.unique(vt, return_inverse=True)
    ng = unique_vt.size
    ny = arr.shape[1]
    nx = arr.shape[2]
    print(f"    Groups: {ng}, Grid: {ny}x{nx}")

    stats_dtype = np.float32 

    mean_a = np.full((ng, ny, nx), np.nan, dtype=stats_dtype)
    median_a = np.full((ng, ny, nx), np.nan, dtype=stats_dtype)
    q1_a = np.full((ng, ny, nx), np.nan, dtype=stats_dtype)
    q3_a = np.full((ng, ny, nx), np.nan, dtype=stats_dtype)
    min_a = np.full((ng, ny, nx), np.nan, dtype=stats_dtype)
    max_a = np.full((ng, ny, nx), np.nan, dtype=stats_dtype)

    def compute_group(g):
        sel = (inverse == g)
        if not sel.any():
            return g, None
        grp = arr[sel, :, :]
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            if have_bn:
                # Bottleneck is much faster for nan-stats
                mean_g = bn.nanmean(grp, axis=0)
                median_g = bn.nanmedian(grp, axis=0)
                min_g = bn.nanmin(grp, axis=0)
                max_g = bn.nanmax(grp, axis=0)
                # Bottleneck doesn't have nanpercentile, use numpy for just these 2
                q1_g = np.nanpercentile(grp, 25, axis=0)
                q3_g = np.nanpercentile(grp, 75, axis=0)
            else:
                mean_g = np.nanmean(grp, axis=0)
                min_g = np.nanmin(grp, axis=0)
                max_g = np.nanmax(grp, axis=0)
                # Compute multiple percentiles in ONE pass (huge speedup)
                percentiles = np.nanpercentile(grp, [25, 50, 75], axis=0)
                q1_g = percentiles[0]
                median_g = percentiles[1]
                q3_g = percentiles[2]
                
        return g, (mean_g.astype(np.float32), 
                   median_g.astype(np.float32), 
                   q1_g.astype(np.float32), 
                   q3_g.astype(np.float32), 
                   min_g.astype(np.float32), 
                   max_g.astype(np.float32))

    compute_start = time.time()
    if have_joblib and jobs > 1:
        print(f"  Computing stats with joblib ({jobs} jobs)...")
        # Added verbose=10 to Parallel to show progress in the terminal
        results = Parallel(n_jobs=jobs, verbose=10)(delayed(compute_group)(g) for g in range(ng))
        for g, data in results:
            if data is None:
                continue
            mean_a[g], median_a[g], q1_a[g], q3_a[g], min_a[g], max_a[g] = data
    else:
        print(f"  Computing stats for {ng} groups (single-threaded)...")
        # Higher resolution progress reporting
        report_interval = max(1, ng // 20)
        for g in range(ng):
            if g % report_interval == 0:
                elapsed = time.time() - compute_start
                eta = (elapsed / g * (ng - g)) if g > 0 else 0
                print(f"    Progress: {g}/{ng} ({g/ng*100:.1f}%) | Elapsed: {elapsed:.1f}s | ETA: {eta:.1f}s")
            
            _, data = compute_group(g)
            if data is None:
                continue
            mean_a[g], median_a[g], q1_a[g], q3_a[g], min_a[g], max_a[g] = data
    compute_time = time.time() - compute_start
    print(f"    Stats computed in {compute_time:.1f}s")

    print(f"  Creating xarray Dataset...")
    coords = {
        'virtual_time': unique_vt,
        'gridY': ds_m['gridY'].values,
        'gridX': ds_m['gridX'].values
    }
    ds_stats_month = xr.Dataset(
        {
            'mean': (('virtual_time', 'gridY', 'gridX'), mean_a),
            'median': (('virtual_time', 'gridY', 'gridX'), median_a),
            'q1': (('virtual_time', 'gridY', 'gridX'), q1_a),
            'q3': (('virtual_time', 'gridY', 'gridX'), q3_a),
            'min': (('virtual_time', 'gridY', 'gridX'), min_a),
            'max': (('virtual_time', 'gridY', 'gridX'), max_a),
        },
        coords=coords
    )

    if 'units' in ds_stats_month.virtual_time.encoding:
        del ds_stats_month.virtual_time.encoding['units']
    ds_stats_month.virtual_time.encoding['units'] = 'seconds since 2020-01-01 00:00:00'

    # Set up compression for all data variables to save disk space for intermediate files
    comp = dict(zlib=True, complevel=4)
    encoding = {var: comp for var in ds_stats_month.data_vars}
    # Ensure virtual_time keeps its specific encoding (like units)
    encoding['virtual_time'] = ds_stats_month.virtual_time.encoding

    print(f"  Saving month stats (compressed) to {stats_month_file}...")
    with ProgressBar():
        ds_stats_month.to_netcdf(stats_month_file, encoding=encoding)
    
    print(f"  Closing datasets and cleaning up...")
    ds_stats_month.close()
    del ds_stats_month, mean_a, median_a, q1_a, q3_a, min_a, max_a, arr
    # Note: don't explicitly close ds_m as it can cause issues with lazy-loaded data

    stats_month_files.append(stats_month_file)
    print(f"  Month {month} done.")

print("Done.")
