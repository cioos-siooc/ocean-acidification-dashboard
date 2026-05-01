#!/usr/bin/env python3
"""
check_chunks.py — Inspect chunking and basic metadata of a netCDF4 file.

Usage:
    python check_chunks.py <file.nc> [--var VARNAME]
"""

import argparse
import sys
import netCDF4
import numpy as np


def sizeof_fmt(num_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num_bytes) < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} PB"


def chunk_info(var: netCDF4.Variable) -> str:
    storage = var.chunking()
    if not storage or storage == "contiguous":
        return "contiguous"
    return str(tuple(storage))


def chunk_size_bytes(var: netCDF4.Variable) -> int | None:
    storage = var.chunking()
    if not storage or storage == "contiguous":
        return None
    dtype_size = np.dtype(var.dtype).itemsize
    return int(np.prod(storage)) * dtype_size


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect netCDF4 variable chunking.")
    parser.add_argument("file", help="Path to the .nc file")
    parser.add_argument(
        "--var", "-v",
        metavar="VARNAME",
        help="Show detailed info for a specific variable only",
    )
    args = parser.parse_args()

    try:
        ds = netCDF4.Dataset(args.file, "r")
    except FileNotFoundError:
        print(f"Error: file not found: {args.file}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error opening file: {e}", file=sys.stderr)
        sys.exit(1)

    with ds:
        print(f"\nFile : {args.file}")
        print(f"Format : {ds.file_format}")
        print()

        # Dimensions
        print("Dimensions:")
        for name, dim in ds.dimensions.items():
            unlim = " (unlimited)" if dim.isunlimited() else ""
            print(f"  {name:20s}  size={len(dim)}{unlim}")
        print()

        # Variables to inspect
        target_vars = (
            {args.var: ds.variables[args.var]} if args.var else ds.variables
        )

        if args.var and args.var not in ds.variables:
            print(f"Error: variable '{args.var}' not found.", file=sys.stderr)
            print("Available variables:", ", ".join(ds.variables.keys()))
            sys.exit(1)

        col_w = max(len(n) for n in target_vars) + 2

        header = (
            f"{'Variable':{col_w}}  {'Shape':30}  {'Dtype':8}  "
            f"{'Chunks':30}  {'Chunk size':12}  Dims"
        )
        print(header)
        print("-" * len(header))

        for name, var in target_vars.items():
            shape_str   = str(var.shape)
            chunks_str  = chunk_info(var)
            cb          = chunk_size_bytes(var)
            cb_str      = sizeof_fmt(cb) if cb is not None else "—"
            dims_str    = ", ".join(var.dimensions)
            print(
                f"{name:{col_w}}  {shape_str:30}  {str(var.dtype):8}  "
                f"{chunks_str:30}  {cb_str:12}  {dims_str}"
            )

        # Highlight potential performance issues for 4-D variables
        print()
        issues: list[str] = []
        for name, var in target_vars.items():
            if var.ndim < 4:
                continue
            storage = var.chunking()
            if not storage or storage == "contiguous":
                issues.append(f"  {name}: contiguous storage — very slow for point/time-series queries")
                continue
            time_dim_idx = next(
                (i for i, d in enumerate(var.dimensions) if d in ("time", "t", "ocean_time")),
                None,
            )
            if time_dim_idx is not None and storage[time_dim_idx] == 1:
                issues.append(
                    f"  {name}: chunk_time=1 — one full spatial slice per timestep read; "
                    "consider rechunking to chunk_time≥24"
                )

        if issues:
            print("⚠  Potential performance issues detected:")
            for msg in issues:
                print(msg)
        else:
            print("✓  No obvious chunking issues found.")
        print()


if __name__ == "__main__":
    main()
