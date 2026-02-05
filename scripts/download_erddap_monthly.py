#!/usr/bin/env python3
"""Download monthly ERDDAP griddap files for UBC Salish Sea chemistry fields.

Creates one .nc file per year per variable using the URL pattern provided by the user.

Usage:
    python scripts/download_erddap_monthly.py --start 2007 --end 2025 --outdir data/nc/ubcSSg3DChemistryFields1moV21-11

Features:
- Builds the ERDDAP URL per year per variable
- Streams download with retries and optional overwrite
- Shows progress bar if tqdm is installed
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime

import requests

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

BASE = "https://salishsea.eos.ubc.ca/erddap/griddap/"
GROUPS = [
    {"dataset": "ubcSSg3DChemistryFields1moV21-11", "VARS": ["dissolved_inorganic_carbon", "total_alkalinity", "dissolved_oxygen"]},
    {"dataset": "ubcSSg3DPhysicsFields1moV21-11", "VARS": ["temperature", "salinity"]},
]

VAR_TEMPLATE = "{var}[{time}][(0.5000003):1:(441.4661)][(0.0):1:(897.0)][(0.0):1:(397.0)]"

# Use mid-month timestamps (as in example) to avoid edge alignment issues
TIME_FMT = "({start}):1:({end})"

CHUNK_SIZE = 1024 * 32


def build_url(dataset: str, year: int, variables: list[str]) -> str:
    """Build the ERDDAP query URL for a specific dataset, year, and list of variables.

    `variables` must be a non-empty list of variable names. The function builds the
    ERDDAP griddap query: BASE + dataset + "?" + comma-separated variable slices.
    """
    if not variables:
        raise ValueError("variables must be a non-empty list")
    start = f"{year}-01-15T12:00:00Z"
    end = f"{year}-12-15T12:00:00Z"
    time_part = TIME_FMT.format(start=start, end=end)
    vars_part = ",".join(VAR_TEMPLATE.format(var=v, time=time_part) for v in variables)
    return BASE + dataset + ".nc?" + vars_part


def download_url(url: str, outpath: str, retries: int = 3, timeout: int = 60, overwrite: bool = False, apply_nc_compression: bool = False, deflate: int = 5) -> bool:
    """Download URL to outpath. Returns True on success.

    If `apply_nc_compression` is True, the downloaded file will be read with xarray
    and re-saved with internal netCDF compression (zlib) at the provided `deflate` level.
    """
    if os.path.exists(outpath) and not overwrite:
        print(f"Skip existing: {outpath}")
        return True

    tmp = outpath + ".part"

    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                total = r.headers.get("content-length")
                total = int(total) if total and total.isdigit() else None

                with open(tmp, "wb") as f:
                    if tqdm and total:
                        for chunk in tqdm(r.iter_content(CHUNK_SIZE), total=total // CHUNK_SIZE, unit="KB"):
                            if chunk:
                                f.write(chunk)
                    else:
                        for chunk in r.iter_content(CHUNK_SIZE):
                            if chunk:
                                f.write(chunk)

            # Move the downloaded temporary file into place
            os.replace(tmp, outpath)

            # Optionally apply internal netCDF compression using xarray
            if apply_nc_compression:
                print(f"Applying internal netCDF compression (zlib, deflate={deflate}) to {outpath}")
                try:
                    compress_nc_with_xarray(outpath, deflate=deflate)
                    print(f"Compressed internal netCDF saved: {outpath}")
                except Exception as exc:
                    print(f"Internal netCDF compression failed: {exc}")
                    # Keep the uncompressed file but signal failure
                    return False

            print(f"Saved: {outpath}")
            return True
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            print(f"Attempt {attempt}/{retries} failed: {exc}")
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except Exception:
                    pass
            if attempt < retries:
                sleep = 2 ** (attempt - 1)
                print(f"Retrying in {sleep}s...")
                time.sleep(sleep)
            else:
                print(f"Failed to download after {retries} attempts: {url}")
                return False


def compress_nc_with_xarray(src: str, deflate: int = 5) -> str:
    """Read `src` with xarray and re-save it with zlib compression applied per variable.

    Writes a temporary file and replaces the original on success. Returns `src`.
    Requires xarray and netCDF4 to be installed.
    """
    try:
        import xarray as xr
    except Exception as exc:
        raise RuntimeError("xarray is required for internal netCDF compression: %s" % exc)

    tmp = src + ".nc.tmp"
    ds = xr.open_dataset(src, engine="netcdf4")
    encoding = {}
    for name in ds.data_vars:
        # apply zlib compression and a deflate level
        encoding[name] = {"zlib": True, "complevel": int(deflate), "shuffle": True}
    try:
        ds.to_netcdf(tmp, format="NETCDF4_CLASSIC", engine="netcdf4", encoding=encoding)
    finally:
        ds.close()

    os.replace(tmp, src)
    return src


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download monthly ERDDAP files for UBC SalishSea chemistry fields")
    p.add_argument("--start", type=int, default=2007, help="Start year (inclusive)")
    p.add_argument("--end", type=int, default=2025, help="End year (inclusive)")
    p.add_argument("--outdir", default="data/nc/monthly", help="Directory to save .nc files")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    p.add_argument("--retries", type=int, default=3, help="Number of retries per file")
    p.add_argument("--timeout", type=int, default=60, help="HTTP timeout in seconds")
    p.add_argument("--variables", nargs="+", default=None, help="Subset of variable names to download. Defaults to all.")
    # Downloads are always performed per-variable (one file per variable per year)
    p.set_defaults(per_variable=True)
    p.add_argument("--nc-compress", action="store_true", default=True, help="Apply internal netCDF compression using xarray after download (zlib)")
    p.add_argument("--deflate-level", type=int, default=5, help="Deflate level (1-9) for internal netCDF compression")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    failed = []

    # Validate requested variables across all groups
    requested = args.variables if args.variables else None
    all_group_vars = set(v for g in GROUPS for v in g["VARS"])
    if requested:
        unknown = [v for v in requested if v not in all_group_vars]
        if unknown:
            print(f"Unknown variables requested: {unknown}. Known variables: {sorted(all_group_vars)}")
            return 3

    for year in range(args.start, args.end + 1):
        for group in GROUPS:
            dataset = group["dataset"]
            group_vars = group["VARS"]

            # Determine which variables to handle for this group
            if requested:
                selected = [v for v in requested if v in group_vars]
                if not selected:
                    # nothing to do for this group
                    continue
            else:
                selected = group_vars

            dataset_basename = os.path.splitext(dataset)[0]

            # Always download per-variable (one file per variable per year), storing each
            # variable in its own directory under `outdir`.
            for var in selected:
                var_dir = os.path.join(args.outdir, var)
                os.makedirs(var_dir, exist_ok=True)
                url = build_url(dataset, year, [var])
                fname = f"{var}_{year}.nc"
                outpath = os.path.join(var_dir, fname)
                print(f"Downloading {dataset} {year} {var} -> {os.path.join(var, fname)}")
                ok = download_url(
                    url,
                    outpath,
                    retries=args.retries,
                    timeout=args.timeout,
                    overwrite=args.overwrite,
                    apply_nc_compression=args.nc_compress,
                    deflate=args.deflate_level,
                )
                if not ok:
                    failed.append((dataset, year, var))

    if failed:
        print("Finished with failures:", failed)
        return 2

    print("All downloads completed successfully ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())