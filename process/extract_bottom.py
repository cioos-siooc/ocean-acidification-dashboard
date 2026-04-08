#!/usr/bin/env python3
"""Standalone bottom-layer extractor.

Processes one or more 4-D NetCDF files and writes a companion bottom-layer
file next to each one, using the same naming and structure as the pipeline:

    {var}_{YYYYMMDD}_bottom.nc  (depth coordinate = -1.0, same variable name)

No database connection required.

Usage
-----
    python extract_bottom.py /path/to/temperature_20260326.nc
    python extract_bottom.py /path/to/nc/temperature/*.nc
    python extract_bottom.py /path/to/nc/temperature/*.nc --overwrite
    python extract_bottom.py /path/to/nc/temperature/*.nc --var temperature
"""

import argparse
import logging
import os
import sys

import numpy as np
import xarray as xr

# Allow running from the process/ directory or from the repo root
sys.path.insert(0, os.path.dirname(__file__))


def _extract_bottom_layer_4d(data_4d: np.ndarray) -> np.ndarray:
    """Vectorized bottom-layer extraction over all time steps at once.

    Parameters
    ----------
    data_4d : array (time, depth, lat, lon)

    Returns
    -------
    array (time, lat, lon)
    """
    data = np.ma.filled(data_4d, np.nan).astype(float)
    data = np.where(data == 0, np.nan, data)

    n_time, n_depth, n_lat, n_lon = data.shape

    valid_mask = ~np.isnan(data)  # (time, depth, lat, lon)
    flipped_mask = np.flip(valid_mask, axis=1)  # flip depth axis
    first_valid_flipped = np.argmax(flipped_mask, axis=1)  # (time, lat, lon)
    last_valid_idx = n_depth - 1 - first_valid_flipped

    has_valid = np.any(valid_mask, axis=1)  # (time, lat, lon)
    last_valid_idx = np.where(has_valid, last_valid_idx, 0)

    t_idx, lat_idx, lon_idx = np.meshgrid(
        np.arange(n_time), np.arange(n_lat), np.arange(n_lon), indexing="ij"
    )
    bottom = data[t_idx, last_valid_idx, lat_idx, lon_idx]
    return np.where(has_valid, bottom, np.nan)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("extract_bottom")


def _get_bottom_nc_path(nc_path: str) -> str:
    """Derive the companion bottom-layer path.

    /data/nc/temperature/temperature_20260326.nc
    → /data/nc/temperature/temperature_20260326_bottom.nc
    """
    dirname = os.path.dirname(nc_path)
    basename, ext = os.path.splitext(os.path.basename(nc_path))
    variable = os.path.basename(dirname)
    if basename.startswith(variable + "_"):
        date_suffix = basename[len(variable) + 1:]
        new_basename = f"{variable}_{date_suffix}_bottom"
    else:
        new_basename = f"{basename}_bottom"
    return os.path.join(dirname, new_basename + ext)


def _infer_variable(nc_path: str) -> str:
    """Infer the variable name from the parent directory name."""
    return os.path.basename(os.path.dirname(nc_path))


def process_file(nc_path: str, var: str | None = None, overwrite: bool = False) -> bool:
    out_path = _get_bottom_nc_path(nc_path)

    if os.path.exists(out_path) and not overwrite:
        logger.info("Already exists, skipping: %s", out_path)
        return True

    base_var = var or _infer_variable(nc_path)

    logger.info("Processing %s (var=%s)", nc_path, base_var)

    try:
        with xr.open_dataset(nc_path) as ds:
            if base_var not in ds:
                raise ValueError(
                    f"Variable '{base_var}' not found in {nc_path}. "
                    f"Available: {list(ds.data_vars)}. Use --var to specify."
                )

            var_data = ds[base_var]
            dims = list(var_data.dims)

            time_dim = next((d for d in dims if "time" in d.lower()), None)
            depth_dim = next(
                (d for d in dims if any(k in d.lower() for k in ("depth", "lev", "z", "deptht"))),
                None,
            )

            if time_dim is None:
                raise ValueError(f"No time dimension found in {nc_path}")
            if depth_dim is None:
                raise ValueError(f"No depth dimension found in {nc_path}")

            spatial_dims = [d for d in dims if d not in (time_dim, depth_dim)]

            # Load and reorder to (time, depth, *spatial) for vectorized processing
            ordered_dims = [time_dim, depth_dim] + spatial_dims
            data_4d = var_data.transpose(*ordered_dims).values

            bottom_arr = _extract_bottom_layer_4d(data_4d)  # (time, lat, lon)
            bottom_arr = bottom_arr[:, np.newaxis, :, :]     # (time, 1, lat, lon)

            coords = {time_dim: ds[time_dim], depth_dim: np.array([-1.0], dtype=float)}
            for dim in spatial_dims:
                if dim in ds.coords:
                    coords[dim] = ds[dim]

            da = xr.DataArray(
                bottom_arr,
                dims=[time_dim, depth_dim] + spatial_dims,
                coords=coords,
                name=base_var,
                attrs={**var_data.attrs, "long_name": f"Bottom layer {base_var}"},
            )

            os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
            encoding = {base_var: {"zlib": True, "complevel": 4}}
            da.to_dataset().to_netcdf(out_path, encoding=encoding)

        logger.info("Wrote %s", out_path)
        return True

    except Exception as e:
        logger.error("Failed %s: %s", nc_path, e)
        return False


def main():
    parser = argparse.ArgumentParser(description="Extract bottom layer from 4-D NetCDF files.")
    parser.add_argument("files", nargs="+", help="Input NetCDF file(s)")
    parser.add_argument(
        "--var",
        default=None,
        help="Variable name to extract (default: inferred from parent directory name)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing bottom files",
    )
    args = parser.parse_args()

    ok = 0
    fail = 0
    for f in args.files:
        if "_bottom_" in os.path.basename(f):
            logger.info("Skipping bottom file: %s", f)
            continue
        if process_file(f, var=args.var, overwrite=args.overwrite):
            ok += 1
        else:
            fail += 1

    logger.info("Done: %d succeeded, %d failed", ok, fail)
    sys.exit(1 if fail else 0)


if __name__ == "__main__":
    main()
