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


def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi']:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

parser = argparse.ArgumentParser(description="Compute climatology stats (fast, numpy-based)")
parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
parser.add_argument("--depth-index", type=int, required=True, help="Process specific depth index (default 0 if depth dim exists)")
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

# 1. Select files
files = sorted(glob.glob("output_*.nc"))
print(f"Processing {len(files)} files ...")
if verbose:
    print(f"Found {len(files)} files. Opening dataset lazily...")

# 2. Load dataset lazily (respect native chunks)
ds = xr.open_mfdataset(files, combine='by_coords', parallel=True, chunks={})

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

# Process per-month using filenames (e.g., output_200701.nc -> month 01)
# Build month groups
files_by_month = {}
for f in files:
    # expect filenames like output_YYYYMM.nc
    if len(f) >= 10 and f.endswith('.nc') and f.startswith('output_'):
        month = f[-5:-3]
        files_by_month.setdefault(month, []).append(f)

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
    
    stats_month_dir = f"depth_idx/{depth_index}"
    os.makedirs(stats_month_dir, exist_ok=True)
    stats_month_file = os.path.join(stats_month_dir, f"month_{month}.nc")

    # If full month stats already exist and valid, skip
    if os.path.exists(stats_month_file):
        try:
            with xr.open_dataset(stats_month_file) as tmp_ds:
                _ = tmp_ds.virtual_time.values
            print(f"Skipping existing month stats: {stats_month_file}")
            stats_month_files.append(stats_month_file)
            continue
        except Exception as e:
            print(f"Existing month stats {stats_month_file} invalid: {e}, regenerating...")
            os.remove(stats_month_file)

    print(f"Processing month {month} with {len(month_files)} files")

    # open month dataset lazily and set virtual_time
    ds_m = xr.open_mfdataset(month_files, combine='by_coords', parallel=True, chunks={})
    
    if depth_index >= ds_m.sizes[depth_dim]:
        print(f"Error: depth index {depth_index} out of range for {depth_dim} size {ds_m.sizes[depth_dim]}")
        continue
    if verbose:
        print(f"  Selecting depth index {depth_index} on dimension '{depth_dim}'")
    ds_m = ds_m.isel({depth_dim: depth_index})

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

    da_m = ds_m['temperature'].squeeze()

    # load into memory
    if verbose:
        print(f"  Loading full month {month} into memory...")
    load_start = time.time()
    with ProgressBar():
        arr = da_m.values
    arr = np.ascontiguousarray(arr)
    load_time = time.time() - load_start
    if verbose:
        bytes_ = arr.nbytes
        print(f"    Loaded {sizeof_fmt(bytes_)} in {load_time:.1f}s; shape={arr.shape}")

    # group prep
    vt = ds_m['virtual_time'].values
    unique_vt, inverse = np.unique(vt, return_inverse=True)
    ng = unique_vt.size
    ny = arr.shape[1]
    nx = arr.shape[2]

    dtype = arr.dtype
    stats_dtype = np.float32 if np.issubdtype(dtype, np.floating) else np.float64

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
            mean_g = np.nanmean(grp, axis=0)
            median_g = np.nanmedian(grp, axis=0)
            q1_g = np.nanpercentile(grp, 25, axis=0)
            q3_g = np.nanpercentile(grp, 75, axis=0)
            min_g = np.nanmin(grp, axis=0)
            max_g = np.nanmax(grp, axis=0)
        return g, (mean_g, median_g, q1_g, q3_g, min_g, max_g)

    compute_start = time.time()
    if have_joblib and jobs > 1:
        results = Parallel(n_jobs=jobs)(delayed(compute_group)(g) for g in range(ng))
        for g, data in results:
            if data is None:
                continue
            mean_a[g], median_a[g], q1_a[g], q3_a[g], min_a[g], max_a[g] = data
    else:
        for g in range(ng):
            _, data = compute_group(g)
            if data is None:
                continue
            mean_a[g], median_a[g], q1_a[g], q3_a[g], min_a[g], max_a[g] = data
    compute_time = time.time() - compute_start
    if verbose:
        print(f"    Stats computed in {compute_time:.1f}s")

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
    
    ds_stats_month.close()
    del ds_stats_month, mean_a, median_a, q1_a, q3_a, min_a, max_a, arr

    stats_month_files.append(stats_month_file)
    print(f"  Month {month} done.")

print("Done.")
