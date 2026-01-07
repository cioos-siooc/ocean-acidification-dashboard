#!/usr/bin/env python3
"""Reproject NetCDF variables to Web-Mercator and write per-timestep grayscale PNGs.

Behavior (defaults chosen according to your inputs):
- Uses xarray to open `ds_grid` and `ds_data` NetCDF files.
- Reads lat/lon arrays from `ds_grid` (supports 2D or 1D coords named like 'lat', 'latitude', 'lon', 'longitude').
- For each variable in `ds_data` (or a selected subset) and for each time step,
  regrids the variable from the source curvilinear grid to a regular Web-Mercator (EPSG:3857) grid
  using `scipy.interpolate.griddata` (linear interpolation by default).
- Preserves aspect ratio; target max dimension defaults to 2048 px.
- Scales values to grayscale using a *global* min/max per variable across all times.
- Writes PNGs with transparent alpha for NaN values and a JSON sidecar containing lon/lat bounds for Mapbox placement.

Dependencies:
- xarray, numpy, scipy, pyproj, pillow (PIL), tqdm (optional)

Example usage:

    python nc2tile.py \ 
      --grid ds_grid.nc --data ds_data.nc \ 
      --vars dissolved_inorganic_carbon,total_alkalinity \ 
      --outdir output --max-dim 2048 --interp linear

If you'd like different defaults (resolution, interpolation method, etc.) change options.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from functools import partial
from typing import List, Tuple, Optional
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import xarray as xr
from scipy.interpolate import griddata
from PIL import Image

try:
    from pyproj import Transformer
except Exception:
    Transformer = None

try:
    from tqdm import tqdm
except Exception:  # optional
    tqdm = None


def find_latlon(ds_grid: xr.Dataset) -> Tuple[np.ndarray, np.ndarray]:
    """Return lon, lat 2D arrays from ds_grid.

    Handles cases where lon/lat are 1D coordinates or 2D variables named
    'lon','longitude','lat','latitude'. Raises ValueError if not found.
    """
    # Candidate names
    lon_names = ["lon", "longitude", "x", "lon_0"]
    lat_names = ["lat", "latitude", "y", "lat_0"]

    # First check for 2D variables
    for ln in lon_names:
        for la in lat_names:
            if ln in ds_grid and la in ds_grid:
                lon = ds_grid[ln].values
                lat = ds_grid[la].values
                if lon.ndim == 2 and lat.ndim == 2:
                    return lon, lat

    # Next check for 1D coords (gridX, gridY mapping)
    # Common pattern: lat(gridY), lon(gridX) OR lat(gridY, gridX) already covered
    # Search coords for 1D
    lon1 = None
    lat1 = None
    for name in ds_grid.coords:
        if name.lower() in lon_names and ds_grid.coords[name].ndim == 1:
            lon1 = ds_grid.coords[name].values
        if name.lower() in lat_names and ds_grid.coords[name].ndim == 1:
            lat1 = ds_grid.coords[name].values
    if lon1 is not None and lat1 is not None:
        # Build meshgrid
        lon2d, lat2d = np.meshgrid(lon1, lat1)
        return lon2d, lat2d

    # Sometimes variables are named 'longitude' and 'latitude' but 1D
    # Try variables too
    for name in ds_grid.variables:
        if name.lower() in lon_names and ds_grid[name].ndim == 1:
            lon1 = ds_grid[name].values
        if name.lower() in lat_names and ds_grid[name].ndim == 1:
            lat1 = ds_grid[name].values
    if lon1 is not None and lat1 is not None:
        lon2d, lat2d = np.meshgrid(lon1, lat1)
        return lon2d, lat2d

    raise ValueError("Unable to locate latitude/longitude in ds_grid. Looked for common names.")


def compute_mercator_grid_bounds(lon: np.ndarray, lat: np.ndarray) -> Tuple[float, float, float, float]:
    """Compute bounding box in Web-Mercator meters (minx,miny,maxx,maxy) for given lon/lat arrays."""
    if Transformer is None:
        raise RuntimeError("pyproj.Transformer is required but not installed")
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    lon_flat = np.array([lon.min(), lon.max()])
    lat_flat = np.array([lat.min(), lat.max()])
    xs, ys = transformer.transform(lon_flat, lat_flat)
    minx, maxx = min(xs), max(xs)
    miny, maxy = min(ys), max(ys)
    return float(minx), float(miny), float(maxx), float(maxy)


def build_target_grid(minx: float, miny: float, maxx: float, maxy: float, max_dim: int = 2048) -> Tuple[np.ndarray, np.ndarray, int, int]:
    """Create target Web-Mercator grid coordinates (xs, ys mesh) and return also width, height.

    Preserves aspect ratio and keeps the largest dimension <= max_dim.
    Returns arrays of x (width) and y (height) coordinates (2D meshgrid) and (width, height).
    """
    width_m = maxx - minx
    height_m = maxy - miny
    if width_m <= 0 or height_m <= 0:
        raise ValueError("Invalid mercator bounds")

    # Compute target pixel sizes preserving aspect ratio
    if width_m >= height_m:
        w = max_dim
        h = max(1, int(round((height_m / width_m) * max_dim)))
    else:
        h = max_dim
        w = max(1, int(round((width_m / height_m) * max_dim)))

    xs = np.linspace(minx, maxx, w)
    ys = np.linspace(maxy, miny, h)  # top-to-bottom (y decreases)
    xx, yy = np.meshgrid(xs, ys)
    return xx, yy, w, h


def reproject_and_interpolate(
    lon_src: np.ndarray,
    lat_src: np.ndarray,
    vals_src: np.ndarray,
    xx_merc: np.ndarray,
    yy_merc: np.ndarray,
    method: str = "linear",
) -> np.ndarray:
    """Interpolate vals_src (shape gridY x gridX) defined at lon_src/lat_src onto target mercator grid.

    Steps:
    - inverse-transform target mercator grid back to lon/lat
    - use scipy.griddata to interpolate
    """
    if Transformer is None:
        raise RuntimeError("pyproj is required")
    # transformer: 3857 -> 4326
    transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    flat_x = xx_merc.ravel()
    flat_y = yy_merc.ravel()
    tgt_lon, tgt_lat = transformer.transform(flat_x, flat_y)

    # Source points
    pts_src = np.column_stack((lon_src.ravel(), lat_src.ravel()))
    vals = vals_src.ravel()

    # Mask invalid sources
    mask = ~np.isnan(vals)
    if mask.sum() == 0:
        return np.full_like(xx_merc, np.nan, dtype=float)

    pts_valid = pts_src[mask]
    vals_valid = vals[mask]

    # Interpolate; note this can be slow for large grids
    tgt_pts = np.column_stack((tgt_lon, tgt_lat))
    interpolated = griddata(pts_valid, vals_valid, tgt_pts, method=method, fill_value=np.nan)
    return interpolated.reshape(xx_merc.shape)


def _process_task(task: Tuple) -> str:
    """Worker function executed in a separate process. Returns the path of the written PNG."""
    (
        ds_grid_path,
        ds_data_path,
        varname,
        t_idx,
        t_str,
        d_idx,
        d_val,
        vmin,
        vmax,
        minx,
        miny,
        maxx,
        maxy,
        w,
        h,
        method,
        clip,
        out_var_dir,
        verbose,
    ) = task

    # Open datasets in worker process
    ds_grid = xr.open_dataset(ds_grid_path)
    ds_data = xr.open_dataset(ds_data_path)

    lon_src, lat_src = find_latlon(ds_grid)

    # build mercator grid in worker to avoid large pickled arrays
    xs = np.linspace(minx, maxx, int(w))
    ys = np.linspace(maxy, miny, int(h))
    xx_merc, yy_merc = np.meshgrid(xs, ys)

    # find time/depth dims locally
    time_dim = None
    depth_dim = None
    for d in ds_data[varname].dims:
        ld = d.lower()
        if "time" in ld and time_dim is None:
            time_dim = d
        if any(k in ld for k in ("depth", "lev", "z", "deptht")) and depth_dim is None:
            depth_dim = d

    sel = {time_dim: int(t_idx)}
    if depth_dim is not None and d_idx is not None:
        sel[depth_dim] = int(d_idx)

    tslice = ds_data[varname].isel(sel).values

    # Interpolate
    interp = reproject_and_interpolate(lon_src, lat_src, tslice, xx_merc, yy_merc, method=method)

    # compute vmin/vmax if not provided
    if vmin is None or vmax is None:
        fin = np.isfinite(interp)
        if fin.sum() == 0:
            vmin_local, vmax_local = 0.0, 1.0
        else:
            nonzero = fin & (interp != 0)
            if nonzero.sum() > 0:
                vmin_local = float(np.nanmin(interp[nonzero]))
            else:
                vmin_local = float(np.nanmin(interp[fin]))
            vmax_local = float(np.nanmax(interp[fin]))
    else:
        vmin_local, vmax_local = vmin, vmax

    gray = scale_to_uint8(interp, vmin_local, vmax_local, clip_percentile=clip)
    alpha = np.where(np.isnan(interp) | (interp == 0), 0, 255).astype(np.uint8)

    # filename
    if d_val is None:
        out_png = os.path.join(out_var_dir, f"{varname}_{t_str}.png")
    else:
        depth_s = str(d_val).replace('.', 'p').replace('-', 'm')
        out_png = os.path.join(out_var_dir, f"{varname}_{t_str}_depth{depth_s}.png")

    write_png_rgba(gray, alpha, out_png)

    # compute bbox and sidecar
    if Transformer is None:
        raise RuntimeError("pyproj is required in worker")
    tmerc2ll = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    corners_x = [xx_merc[0, 0], xx_merc[0, -1], xx_merc[-1, -1], xx_merc[-1, 0]]
    corners_y = [yy_merc[0, 0], yy_merc[0, -1], yy_merc[-1, -1], yy_merc[-1, 0]]
    lons, lats = tmerc2ll.transform(corners_x, corners_y)
    lonmin, lonmax = float(np.min(lons)), float(np.max(lons))
    latmin, latmax = float(np.min(lats)), float(np.max(lats))

    if verbose:
        if d_val is None:
            print(f"Wrote (worker): {out_png}")
        else:
            print(f"Wrote (worker): {out_png} (depth={d_val})")

    ds_grid.close()
    ds_data.close()

    return out_png


def scale_to_uint8(arr: np.ndarray, vmin: float, vmax: float, clip_percentile: Optional[Tuple[float, float]] = None) -> np.ndarray:
    """Scale a 2D array to uint8 grayscale 0..255. NaNs -> 0 (we'll set alpha separately).

    If clip_percentile is (pmin, pmax) then clip to those percentiles first.
    """
    a = np.array(arr, dtype=float)
    if clip_percentile is not None:
        pmin, pmax = clip_percentile
        lo = np.nanpercentile(a, pmin)
        hi = np.nanpercentile(a, pmax)
        vmin = float(lo)
        vmax = float(hi)

    if vmin >= vmax:
        # flat field -> constant mid-gray
        out = np.full(a.shape, 127, dtype=np.uint8)
        out[np.isnan(a)] = 0
        return out

    norm = (a - vmin) / (vmax - vmin)
    norm = np.clip(norm, 0.0, 1.0)
    # Replace NaNs/inf before casting to avoid warnings
    norm = np.nan_to_num(norm, nan=0.0, posinf=1.0, neginf=0.0)
    out = (norm * 255.0).astype(np.uint8)
    out[np.isnan(a)] = 0
    return out


def write_png_rgba(gray_u8: np.ndarray, alpha_mask: np.ndarray, outpath: str) -> None:
    """Write 2D uint8 grayscale + alpha mask (0..255) to RGBA PNG using Pillow."""
    # gray_u8: HxW, alpha_mask: HxW uint8
    h, w = gray_u8.shape
    rgba = np.zeros((h, w, 4), dtype=np.uint8)
    rgba[..., 0] = gray_u8
    rgba[..., 1] = gray_u8
    rgba[..., 2] = gray_u8
    rgba[..., 3] = alpha_mask
    img = Image.fromarray(rgba, mode="RGBA")
    img.save(outpath, compress_level=1)


def ensure_out_dirs(outdir: str, varname: str) -> str:
    d = os.path.join(outdir, varname)
    os.makedirs(d, exist_ok=True)
    return d


def write_sidecar_json(outpath: str, lonmin: float, latmin: float, lonmax: float, latmax: float, depth: Optional[float] = None) -> None:
    meta = {
        "bounds": [float(lonmin), float(latmin), float(lonmax), float(latmax)],
        "crs": "EPSG:4326",
    }
    if depth is not None:
        # include depth information (numeric)
        meta["depth"] = float(depth)
    with open(outpath, "w") as fh:
        json.dump(meta, fh)


def compute_global_minmax_exclude_zero(ds_data: xr.Dataset, varname: str) -> Tuple[float, float]:
    arr = ds_data[varname].values
    # Flatten and consider finite values only
    fin = np.isfinite(arr)
    if fin.sum() == 0:
        return 0.0, 1.0

    # Prefer min over non-zero values (exclude exact zeros)
    nonzero = fin & (arr != 0)
    if nonzero.sum() > 0:
        mn = float(np.min(arr[nonzero]))
    else:
        mn = float(np.min(arr[fin]))

    mx = float(np.max(arr[fin]))
    return mn, mx

# Backwards-compatible alias if other modules call compute_global_minmax
def compute_global_minmax(ds_data: xr.Dataset, varname: str) -> Tuple[float, float]:
    return compute_global_minmax_exclude_zero(ds_data, varname)


def process_variable(
    ds_grid_path: str,
    ds_data_path: str,
    varname: str,
    outdir: str,
    max_dim: int = 2048,
    method: str = "linear",
    clip_percentile: Optional[Tuple[float, float]] = None,
    global_scale: bool = True,
    verbose: bool = True,
    workers: int = 1,
):
    ds_grid = xr.open_dataset(ds_grid_path)
    ds_data = xr.open_dataset(ds_data_path)

    lon_src, lat_src = find_latlon(ds_grid)

    # Mercator bounds
    minx, miny, maxx, maxy = compute_mercator_grid_bounds(lon_src, lat_src)
    xx_merc, yy_merc, w, h = build_target_grid(minx, miny, maxx, maxy, max_dim=max_dim)

    if verbose:
        print(f"Target grid size: {w}x{h} (max_dim={max_dim})")

    # global scale (min is computed excluding exact zeros)
    if global_scale:
        vmin, vmax = compute_global_minmax(ds_data, varname)
        if verbose:
            print(f"Global scale for {varname}: vmin={vmin} (zeros excluded), vmax={vmax}")
    else:
        vmin, vmax = None, None

    out_var_dir = ensure_out_dirs(outdir, varname)

    # compute overall bounds for the variable (don't write meta.json yet - wait until we know time/depth info)
    if Transformer is None:
        raise RuntimeError("pyproj.Transformer is required but not installed")
    tmerc2ll = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    corners_x = [xx_merc[0, 0], xx_merc[0, -1], xx_merc[-1, -1], xx_merc[-1, 0]]
    corners_y = [yy_merc[0, 0], yy_merc[0, -1], yy_merc[-1, -1], yy_merc[-1, 0]]
    lons, lats = tmerc2ll.transform(corners_x, corners_y)
    lonmin, lonmax = float(np.min(lons)), float(np.max(lons))
    latmin, latmax = float(np.min(lats)), float(np.max(lats))

    # meta.json will be written after we have the time/depth details
    meta = { 'bounds': [lonmin, latmin, lonmax, latmax], 'vmin': vmin, 'vmax': vmax }

    time_dim = None
    depth_dim = None
    for d in ds_data[varname].dims:
        ld = d.lower()
        if "time" in ld and time_dim is None:
            time_dim = d
        if any(k in ld for k in ("depth", "lev", "z", "deptht")) and depth_dim is None:
            depth_dim = d

    if time_dim is None:
        raise ValueError("Could not find time dimension for variable")

    times = ds_data[time_dim].values

    # depth values if present
    depth_vals = None
    if depth_dim is not None:
        depth_vals = ds_data[depth_dim].values

    # write per-variable meta.json (bounds, scale, depths, time count)
    if depth_vals is not None:
        meta['depths'] = [float(x) for x in depth_vals]
    try:
        meta['time_count'] = int(len(times))
    except Exception:
        meta['time_count'] = None
    with open(os.path.join(out_var_dir, 'meta.json'), 'w') as fh:
        json.dump(meta, fh)

    # Prepare task list (time x depth) to be executed either locally or in parallel workers
    tasks = []
    for idx, tval in enumerate(times):
        tstr = np.datetime_as_string(tval, unit="s").replace(":", "")
        depth_iter = [(None, None)]
        if depth_vals is not None:
            depth_iter = list(enumerate(depth_vals))

        for didx, dval in depth_iter:
            # prepare vmin/vmax to send to worker if using global scale; else pass None
            send_vmin = vmin if global_scale else None
            send_vmax = vmax if global_scale else None

            tasks.append((
                ds_grid_path,
                ds_data_path,
                varname,
                int(idx),
                str(tstr),
                (int(didx) if dval is not None else None),
                (float(dval) if dval is not None else None),
                send_vmin,
                send_vmax,
                float(minx),
                float(miny),
                float(maxx),
                float(maxy),
                int(w),
                int(h),
                method,
                clip_percentile,
                out_var_dir,
                verbose,
            ))

    if workers <= 1:
        # run sequentially in-process (use same worker fn)
        for task in tasks:
            _process_task(task)
    else:
        total = len(tasks)
        if total == 0:
            return
        if verbose:
            print(f"Dispatching {total} tasks to {workers} workers...")
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(_process_task, t): t for t in tasks}
            if tqdm:
                futures_iter = tqdm(as_completed(futures), total=len(futures), desc=f"{varname}")
            else:
                futures_iter = as_completed(futures)

            for fut in futures_iter:
                try:
                    out = fut.result()
                except Exception as exc:
                    # log and continue
                    print(f"Task failed: {exc}")
                else:
                    if verbose:
                        print(f"Completed: {out}")
    ds_grid.close()
    ds_data.close()


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Regrid NetCDF variables to Web-Mercator PNG tiles")
    p.add_argument("--grid", required=True, help="Path to ds_grid NetCDF (contains lat/lon)")
    p.add_argument("--data", required=True, help="Path to ds_data NetCDF (contains variables with gridY/gridX + time)")
    p.add_argument("--vars", default=None, help="Comma-separated variable names to process (default: all variables in ds_data)")
    p.add_argument("--outdir", default="out_tiles", help="Output directory")
    p.add_argument("--max-dim", type=int, default=2048, help="Maximum pixel dimension for output (preserves aspect ratio)")
    p.add_argument("--interp", choices=["linear", "nearest", "cubic"], default="linear", help="Interpolation method")
    p.add_argument("--clip-pct", default=None, help="Clip percentiles e.g. 1,99 for contrast stretching (optional)")
    p.add_argument("--no-global-scale", action="store_true", help="Disable global per-variable scaling (use per-image) ")
    p.add_argument("--workers", type=int, default=None, help="Number of parallel worker processes to use (default: cpu_count()-1)")
    p.add_argument("--quiet", action="store_true", help="Less verbose")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    varnames = None
    if args.vars:
        varnames = [v.strip() for v in args.vars.split(",") if v.strip()]

    clip = None
    if args.clip_pct:
        try:
            pmin, pmax = [float(x.strip()) for x in args.clip_pct.split(",")]
            clip = (pmin, pmax)
        except Exception:
            raise SystemExit("Invalid --clip-pct value; use format e.g. 1,99")

    ds_data = xr.open_dataset(args.data)
    all_vars = list(ds_data.data_vars.keys())
    ds_data.close()

    if varnames is None:
        varnames = all_vars

    # determine workers
    if args.workers is None:
        workers = max(1, multiprocessing.cpu_count() - 1)
    else:
        workers = max(1, int(args.workers))

    for var in varnames:
        print(f"Processing variable: {var} (workers={workers})")
        process_variable(
            args.grid,
            args.data,
            var,
            args.outdir,
            max_dim=args.max_dim,
            method=args.interp,
            clip_percentile=clip,
            global_scale=not args.no_global_scale,
            verbose=not args.quiet,
            workers=workers,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())