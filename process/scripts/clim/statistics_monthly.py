#!/usr/bin/env python3
"""Compute monthly climatology statistics for monthly ERDDAP yearly files.

This script mirrors the behavior of `statistics2.py` but operates on the
monthly (1mo) yearly files produced by the downloader. Each variable has its
own input directory (e.g., `data/nc/monthly/dissolved_inorganic_carbon/`) with
one file per year (e.g., `ubcSSg3DChemistryFields1moV21-11_2007.nc`).

For each variable the script computes, for each calendar month (1..12), the
following statistics across years: mean, median, q1 (25%), q3 (75%), min, max.
The results preserve the depth dimension (all depths), gridY/gridX and produce
an output dataset with a `month` and `virtual_time` coordinate (virtual times
use year 2020 for convenience).

Usage:
    python process/scripts/clim/statistics_monthly.py --in-dir data/nc/monthly --outdir data/nc/monthly_stats

Options:
  --variables VAR [VAR ...]   Subset of variables to process (default: all)
  --months 01,02,... or 'all' Months to process (default: all)
  --deflate-level N          NetCDF zlib deflate level (default: 4)
  --jobs N                   Not used for now (kept for interface parity)
  --verbose, -v              Verbose output

"""

from __future__ import annotations

import argparse
import glob
import os
import sys
import time
from datetime import datetime

import numpy as np
import xarray as xr

try:
    from dask.diagnostics import ProgressBar
except Exception:
    ProgressBar = None


def sizeof_fmt(num, suffix="B"):
    for unit in ["","Ki","Mi","Gi","Ti","Pi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compute monthly climatology stats from yearly monthly files")
    p.add_argument("--in-dir", default="./erddap_monthly", help="Directory containing per-variable subdirectories")
    p.add_argument("--outdir", default="./monthly_stats", help="Directory to write monthly climatology files")
    p.add_argument("--variables", nargs="+", default=None, help="Subset of variable names to process. Defaults to all subdirs in --in-dir")
    p.add_argument("--months", type=str, default="all", help="Comma-separated months to process (01,02,..) or 'all' (default)")
    p.add_argument("--deflate-level", type=int, default=4, help="Deflate level for zlib compression (1-9)")
    p.add_argument("--jobs", type=int, default=1, help="Jobs placeholder (not used, for interface parity)")
    p.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    return p.parse_args()


def find_variable_dirs(indir: str) -> list[str]:
    # variable dirs are immediate subdirectories of indir
    if not os.path.isdir(indir):
        raise FileNotFoundError(indir)
    entries = sorted(os.listdir(indir))
    return [os.path.join(indir, e) for e in entries if os.path.isdir(os.path.join(indir, e))]


def collect_files_for_var(var_dir: str) -> list[str]:
    # Look for .nc files in var_dir
    files = sorted(glob.glob(os.path.join(var_dir, "*.nc")))
    return files


def build_virtual_times(months: list[int]) -> np.ndarray:
    # Use 15th of each month at noon UTC in year 2020 to represent virtual_time
    times = [np.datetime64(datetime(2020, m, 15, 12, 0, 0)) for m in months]
    return np.array(times, dtype="datetime64[ns]")


def process_variable(var_dir: str, outdir: str, months: list[int], deflate: int = 4, verbose: bool = False, jobs: int = 1):
    var_name = os.path.basename(var_dir)
    files = collect_files_for_var(var_dir)
    if not files:
        if verbose:
            print(f"No files for variable '{var_name}' in {var_dir}, skipping.")
        return

    if verbose:
        print(f"Processing variable '{var_name}' with {len(files)} files")

    # Open all yearly files (concatenate along time)
    ds = xr.open_mfdataset(files, combine="by_coords", parallel=True, chunks={})

    if "time" not in ds.coords:
        print(f"Variable '{var_name}' has no 'time' coordinate, skipping.")
        ds.close()
        return

    da = ds[var_name] if var_name in ds.data_vars else None
    if da is None:
        # try common variable naming: sometimes variable name differs from directory name
        # fall back to first data variable
        if len(ds.data_vars) == 1:
            da = list(ds.data_vars.values())[0]
            actual_name = list(ds.data_vars.keys())[0]
            if verbose:
                print(f"Using data variable '{actual_name}' for directory '{var_name}'")
        else:
            print(f"Could not determine data variable for '{var_name}' (found: {list(ds.data_vars.keys())}), skipping")
            ds.close()
            return

    # Group by calendar month
    grouped = da.groupby("time.month")

    # Prepare containers for stats
    months_out = months
    n_months = len(months_out)

    # Compute stats using xarray groupby; result dims include 'month' then remaining dims
    if verbose:
        print("  Computing mean...")
    mean = grouped.mean(dim="time")
    if verbose:
        print("  Computing median...")
    median = grouped.median(dim="time")
    if verbose:
        print("  Computing 25th/75th percentiles...")
    try:
        q1 = grouped.quantile(0.25, dim="time", skipna=True)
        q3 = grouped.quantile(0.75, dim="time", skipna=True)
    except Exception as exc:
        # Fallback: compute via numpy percentiles (may be slower), parallelized across months
        if verbose:
            print(f"grouped.quantile failed: {exc}. Falling back to numpy np.nanpercentile (jobs={jobs})")

        # Load into memory (required for numpy percentile) -- be mindful of memory use
        arr = da.values  # shape (time, ...)
        months_idx = ds.indexes['time'].month.values
        full_months = np.arange(1, 13)

        # Prepare result arrays
        out_shape = (len(full_months),) + arr.shape[1:]
        q1_arr = np.full(out_shape, np.nan, dtype=np.float64)
        q3_arr = np.full(out_shape, np.nan, dtype=np.float64)

        def compute_month(m):
            sel = months_idx == m
            if not sel.any():
                return m, None, None
            subset = arr[sel, ...]
            # Ensure float for percentile computation
            subset = np.ascontiguousarray(subset)
            q1_m = np.nanpercentile(subset, 25, axis=0)
            q3_m = np.nanpercentile(subset, 75, axis=0)
            return m, q1_m, q3_m

        # Try to parallelize using joblib if available and jobs > 1
        results = []
        if jobs and jobs > 1:
            try:
                from joblib import Parallel, delayed
                results = Parallel(n_jobs=jobs)(delayed(compute_month)(m) for m in full_months)
            except Exception as ji_exc:
                if verbose:
                    print(f"joblib parallelization failed ({ji_exc}), falling back to serial computation")
                results = [compute_month(m) for m in full_months]
        else:
            results = [compute_month(m) for m in full_months]

        # Fill arrays with results
        month_to_idx = {m: i for i, m in enumerate(full_months)}
        for m, q1_m, q3_m in results:
            if q1_m is None:
                continue
            idx = month_to_idx[int(m)]
            q1_arr[idx] = q1_m
            q3_arr[idx] = q3_m

        # Build xarray DataArrays with correct dims and coords
        coords = {'month': full_months}
        for dim in da.dims[1:]:
            coords[dim] = da.coords[dim].values

        q1 = xr.DataArray(q1_arr, dims=('month',) + tuple(da.dims[1:]), coords=coords)
        q3 = xr.DataArray(q3_arr, dims=('month',) + tuple(da.dims[1:]), coords=coords)

    if verbose:
        print("  Computing min/max...")
    mn = grouped.min(dim="time")
    mx = grouped.max(dim="time")

    # The groupby result has 'month' coordinate for months that exist in input; reindex to full months list
    month_coord = mean['month'].values

    # Build a template dataset with month as coordinate (1..12), and reindex all results to requested months
    full_months = np.arange(1, 13)

    # Convert to datasets and reindex
    ds_stats = xr.Dataset()
    ds_stats['mean'] = mean.reindex(month=full_months).rename({'month': 'month'})
    ds_stats['median'] = median.reindex(month=full_months)
    ds_stats['q1'] = q1.reindex(month=full_months)
    ds_stats['q3'] = q3.reindex(month=full_months)
    ds_stats['min'] = mn.reindex(month=full_months)
    ds_stats['max'] = mx.reindex(month=full_months)

    # Keep only the requested months (may be subset)
    ds_stats = ds_stats.sel(month=months_out)

    # Add virtual_time coordinate (datetimes in 2020 for each month)
    vt = build_virtual_times(months_out)
    ds_stats = ds_stats.assign_coords(virtual_time=("month", vt))

    # Set virtual_time encoding (like statistics2.py)
    ds_stats['virtual_time'].encoding['units'] = 'seconds since 2020-01-01 00:00:00'

    # Compression encoding
    comp = dict(zlib=True, complevel=int(deflate))
    encoding = {var: comp for var in ds_stats.data_vars}
    # Ensure virtual_time keeps its specific encoding if present
    encoding['virtual_time'] = ds_stats['virtual_time'].encoding

    os.makedirs(outdir, exist_ok=True)
    out_file = os.path.join(outdir, f"{var_name}_monthly_climatology.nc")

    if verbose:
        print(f"  Saving result to {out_file} ...")
    if ProgressBar:
        with ProgressBar():
            ds_stats.to_netcdf(out_file, encoding=encoding)
    else:
        ds_stats.to_netcdf(out_file, encoding=encoding)

    if verbose:
        # Report size
        size = os.path.getsize(out_file)
        print(f"  Saved {out_file} ({sizeof_fmt(size)})")

    ds.close()
    ds_stats.close()


def main() -> int:
    args = parse_args()
    verbose = args.verbose

    # Determine variables to process
    var_dirs = find_variable_dirs(args.in_dir)
    var_names = [os.path.basename(d) for d in var_dirs]

    if args.variables:
        selected = [v for v in args.variables if v in var_names]
        unknown = [v for v in args.variables if v not in var_names]
        if unknown:
            print(f"Unknown variables requested: {unknown}. Known variables: {sorted(var_names)}")
            return 2
        var_dirs = [os.path.join(args.in_dir, v) for v in selected]

    # Parse months
    if args.months.strip().lower() == 'all':
        months = list(range(1, 13))
    else:
        months = [int(m) for m in args.months.split(',')]

    if verbose:
        print(f"Processing variables: {[os.path.basename(d) for d in var_dirs]}")
        print(f"Months: {months}")

    for var_dir in var_dirs:
        try:
            process_variable(var_dir, args.outdir, months, deflate=args.deflate_level, verbose=verbose, jobs=args.jobs)
        except Exception as exc:
            print(f"Error processing {var_dir}: {exc}")

    print("All done.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
