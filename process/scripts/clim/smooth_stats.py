"""Smooth monthly stats into a final climatology NetCDF

Reads monthly stats files (default pattern: month*.nc)
and computes a centered rolling mean along `virtual_time` with a configurable
window (in days). Writes final smoothed climatology to NetCDF.

Usage:
  python process/scripts/smooth_stats.py --verbose --smoothing-days 5

Options:
  --input-pattern   Glob pattern for month stat files (default: month*.nc)
  --output          Output file (default: SalishSea_Surface_Climatology.nc)
  --smoothing-days  Smoothing window in days (default: 5)
  --gridY           chunk size for gridY (default: 50)
  --gridX           chunk size for gridX (default: 50)
  --verbose / -v    Verbose output
"""

import xarray as xr
import glob
import time
import argparse
from dask.diagnostics import ProgressBar
import os

parser = argparse.ArgumentParser(description="Smooth monthly stats into final climatology")
parser.add_argument("--input-pattern", default="month*.nc", help="Glob pattern for month stats files")
parser.add_argument("--output", default="SalishSea_Surface_Climatology.nc", help="Output smoothed climatology file")
parser.add_argument("--smoothing-days", type=int, default=5, help="Smoothing window in days")
parser.add_argument("--gridY", type=int, default=50, help="Chunk size for gridY")
parser.add_argument("--gridX", type=int, default=50, help="Chunk size for gridX")
parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
args = parser.parse_args()
# smoothing is always circular (wraps Dec->Jan) per project policy

files = sorted(glob.glob(args.input_pattern))
if not files:
    raise SystemExit(f"No input files found with pattern '{args.input_pattern}'")

if args.verbose:
    print(f"Found {len(files)} files. Using pattern: {args.input_pattern}")
    print("Files (first 10):")
    for f in files[:10]:
        print("  ", f)

start = time.time()
# Open monthly stats as a single dataset (no merging to disk required)
ds = xr.open_mfdataset(files, combine='by_coords', parallel=True, chunks={})
if args.verbose:
    print(f"Opened dataset dims: time={ds.sizes.get('time')}, virtual_time={ds.sizes.get('virtual_time')}, gridY={ds.sizes.get('gridY')}, gridX={ds.sizes.get('gridX')}")

# Ensure virtual_time is sorted
ds = ds.sortby('virtual_time')

# Re-chunk to ensure memory-friendly writes
ds = ds.chunk({'virtual_time': -1, 'gridY': args.gridY, 'gridX': args.gridX})

# Rolling window (days -> assume hourly data)
window_steps = args.smoothing_days * 24
half = window_steps // 2
if args.verbose:
    print(f"Applying centered rolling mean with window {args.smoothing_days} days ({window_steps} steps)")

smooth_start = time.time()
# Circular wrap: pad end and start so rolling includes Dec->Jan neighbors
if args.verbose:
    print("Using circular wrap over virtual_time (including Dec/Jan in window)")
# Ensure sorted
ds_sorted = ds.sortby('virtual_time')
pre = ds_sorted.isel(virtual_time=slice(-half, None)) if half > 0 else None
post = ds_sorted.isel(virtual_time=slice(0, half)) if half > 0 else None
parts = [p for p in (pre, ds_sorted, post) if p is not None]
ds_ext = xr.concat(parts, dim='virtual_time')
ds_smoothed_ext = ds_ext.rolling(virtual_time=window_steps, center=True, min_periods=1).mean()
# slice back to original range
ds_smoothed = ds_smoothed_ext.isel(virtual_time=slice(half, half + ds_sorted.sizes['virtual_time']))
# restore original virtual_time coords
ds_smoothed = ds_smoothed.assign_coords(virtual_time=ds_sorted['virtual_time'].values)

smooth_time = time.time() - smooth_start
if args.verbose:
    print(f"Smoothing computed in {smooth_time:.1f}s")

# Ensure encoding for safe decoding later
if 'units' in ds_smoothed.virtual_time.encoding:
    del ds_smoothed.virtual_time.encoding['units']
ds_smoothed.virtual_time.encoding['units'] = 'seconds since 2020-01-01 00:00:00'

# Define compression and chunking encoding
# We use small spatial chunks and large time chunks to optimize for timeseries extraction
dim_sizes = ds_smoothed.sizes
v_time_len = dim_sizes.get('virtual_time', 1)
# 20x20 spatial chunks are usually a good balance for tiled/point access
chunk_y = min(20, dim_sizes.get('gridY', 20))
chunk_x = min(20, dim_sizes.get('gridX', 20))

comp = dict(
    zlib=True, 
    complevel=5,
    chunksizes=(v_time_len, chunk_y, chunk_x)
)
encoding = {var: comp for var in ds_smoothed.data_vars}
# Preserve time units/encoding
encoding['virtual_time'] = ds_smoothed.virtual_time.encoding

# Write
if args.verbose:
    print(f"Saving smoothed climatology to {args.output} (with zlib compression)...")
save_start = time.time()
with ProgressBar():
    ds_smoothed.to_netcdf(args.output, encoding=encoding)
save_time = time.time() - save_start

if args.verbose:
    print(f"Saved file in {save_time:.1f}s — total script time {(time.time() - start):.1f}s")

print("Done.")
