#!/usr/bin/env python3
"""Standalone nc2tile — no database required.

Identical to the shared/nc2tile.py pipeline tool except the curvilinear grid
and variable bounds are loaded from local files produced by export_grid.py.

Required files (produce them once with export_grid.py while DB is accessible)
---------------------------------------------------------------------------
grid.npz    -- lon/lat arrays for the curvilinear source grid
fields.json -- per-variable min/max bounds (optional; used for value scaling)

Usage
-----
    python nc2tile.py \\
        --data temperature_20260326.nc \\
        --vars temperature \\
        --depth-indices 0,1,2 \\
        --grid grid.npz \\
        --fields fields.json \\
        --outdir /output/webp

    # Bottom layer file (depth index 0 maps to depth value -1 → bottom.webp):
    python nc2tile.py \\
        --data temperature_bottom_20260326.nc \\
        --vars temperature \\
        --depth-indices 0 \\
        --grid grid.npz \\
        --outdir /output/webp
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
from scipy.spatial import Delaunay, cKDTree
from PIL import Image

try:
    from pyproj import Transformer
except Exception:
    Transformer = None

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

import logging

logger = logging.getLogger("nc2tile")

# Module-level grid cache (populated on first call to load_grid)
_GRID_CACHE: Tuple[np.ndarray, np.ndarray] | None = None
_GRID_PATH: str | None = None

# Interpolator disk-cache (same as upstream)
INTERP_CACHE: dict = {}
INTERP_CACHE_DIR = os.getenv("INTERP_CACHE_DIR", "/tmp/nc2tile_interp_cache")


# ---------------------------------------------------------------------------
# Grid loading (file-based, no DB)
# ---------------------------------------------------------------------------

def load_grid(path: str) -> Tuple[np.ndarray, np.ndarray]:
    """Load lon/lat arrays from a grid.npz file produced by export_grid.py."""
    global _GRID_CACHE, _GRID_PATH
    if _GRID_CACHE is not None and _GRID_PATH == path:
        return _GRID_CACHE
    if not path.endswith(".npz"):
        raise ValueError(
            f"--grid expects a .npz file (produced by export_grid.py), got: {path}\n"
            "  Run: python export_grid.py --out-dir ."
        )
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Grid file not found: {path}\n"
            "  Run: python export_grid.py --out-dir ."
        )
    data = np.load(path, allow_pickle=True)
    lon = data["lon"]
    lat = data["lat"]
    _GRID_CACHE = (lon, lat)
    _GRID_PATH = path
    logger.info("Loaded grid from %s  (shape: %s)", path, lon.shape)
    return _GRID_CACHE


# ---------------------------------------------------------------------------
# Interpolator helpers (identical to shared/nc2tile.py)
# ---------------------------------------------------------------------------

class _PrecomputedLinearInterpolator:
    def __init__(self, pts_src: np.ndarray, tgt_pts: np.ndarray):
        self.tri = Delaunay(pts_src)
        simplices = self.tri.find_simplex(tgt_pts)
        self.valid = simplices >= 0
        self.vertices = np.where(self.valid[:, None], self.tri.simplices[simplices], 0)
        X = self.tri.transform[simplices, :2]
        Y = tgt_pts - self.tri.transform[simplices, 2]
        b = np.einsum("ijk,ik->ij", X, Y)
        w = np.c_[b, 1 - b.sum(axis=1)]
        self.weights = np.where(self.valid[:, None], w, 0.0)

    def apply(self, vals_src: np.ndarray) -> np.ndarray:
        vals = vals_src[self.vertices]
        out = (vals * self.weights).sum(axis=1)
        out[~self.valid] = np.nan
        return out


class _PrecomputedNearestInterpolator:
    def __init__(self, pts_src: np.ndarray, tgt_pts: np.ndarray):
        self.kdtree = cKDTree(pts_src)
        _, self.idx = self.kdtree.query(tgt_pts, k=1)

    def apply(self, vals_src: np.ndarray) -> np.ndarray:
        return vals_src[self.idx]


def _interp_cache_path(key: tuple, method: str) -> str:
    import hashlib
    key_hash = hashlib.md5(str(key).encode()).hexdigest()[:12]
    return os.path.join(INTERP_CACHE_DIR, f"interp_{method}_{key_hash}.npz")


def _load_interp_from_disk(path: str, method: str):
    try:
        if os.path.exists(path):
            data = np.load(path)
            if method == "linear":
                interp = _PrecomputedLinearInterpolator.__new__(_PrecomputedLinearInterpolator)
                interp.valid = data["valid"]
                interp.vertices = data["vertices"]
                interp.weights = data["weights"]
                logger.info("Loaded linear interpolator from disk cache %s", path)
                return interp
            elif method == "nearest":
                interp = _PrecomputedNearestInterpolator.__new__(_PrecomputedNearestInterpolator)
                interp.idx = data["idx"]
                logger.info("Loaded nearest interpolator from disk cache %s", path)
                return interp
    except Exception:
        logger.exception("Failed to load interp cache from %s", path)
    return None


def _save_interp_to_disk(path: str, interp, method: str) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if method == "linear":
            np.savez_compressed(path, valid=interp.valid, vertices=interp.vertices, weights=interp.weights)
        elif method == "nearest":
            np.savez_compressed(path, idx=interp.idx)
        logger.info("Saved %s interpolator to disk cache %s", method, path)
    except Exception:
        logger.exception("Failed to save interp cache to %s", path)


def _get_interpolator(method: str, pts_src: np.ndarray, tgt_pts: np.ndarray, grid_sig: Tuple):
    key = (method, grid_sig, pts_src.shape[0], tgt_pts.shape[0])
    if key in INTERP_CACHE:
        return INTERP_CACHE[key]
    disk_path = _interp_cache_path(key, method)
    interp = _load_interp_from_disk(disk_path, method)
    if interp is not None:
        INTERP_CACHE[key] = interp
        return interp
    logger.info("Building %s interpolator from scratch (will cache to disk)", method)
    if method == "linear":
        interp = _PrecomputedLinearInterpolator(pts_src, tgt_pts)
    elif method == "nearest":
        interp = _PrecomputedNearestInterpolator(pts_src, tgt_pts)
    else:
        return None
    INTERP_CACHE[key] = interp
    _save_interp_to_disk(disk_path, interp, method)
    return interp


# ---------------------------------------------------------------------------
# Mercator helpers (identical to shared/nc2tile.py)
# ---------------------------------------------------------------------------

def compute_mercator_grid_bounds(lon: np.ndarray, lat: np.ndarray) -> Tuple[float, float, float, float]:
    if Transformer is None:
        raise RuntimeError("pyproj.Transformer is required but not installed")
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    lon_flat = np.array([np.nanmin(lon), np.nanmax(lon)])
    lat_flat = np.array([np.nanmin(lat), np.nanmax(lat)])
    xs, ys = transformer.transform(lon_flat, lat_flat)
    return float(min(xs)), float(min(ys)), float(max(xs)), float(max(ys))


def build_target_grid(minx, miny, maxx, maxy, max_dim=2048) -> Tuple[np.ndarray, np.ndarray, int, int]:
    width_m = maxx - minx
    height_m = maxy - miny
    if width_m <= 0 or height_m <= 0:
        raise ValueError("Invalid mercator bounds")
    if width_m >= height_m:
        w = max_dim
        h = max(1, int(round((height_m / width_m) * max_dim)))
    else:
        h = max_dim
        w = max(1, int(round((width_m / height_m) * max_dim)))
    xs = np.linspace(minx, maxx, w)
    ys = np.linspace(maxy, miny, h)
    xx, yy = np.meshgrid(xs, ys)
    return xx, yy, w, h


# ---------------------------------------------------------------------------
# Image helpers (identical to shared/nc2tile.py)
# ---------------------------------------------------------------------------

def scale_to_uint8(arr, vmin, vmax, clip_percentile=None):
    a = np.array(arr, dtype=float)
    if clip_percentile is not None:
        pmin, pmax = clip_percentile
        vmin = float(np.nanpercentile(a, pmin))
        vmax = float(np.nanpercentile(a, pmax))
    if vmin >= vmax:
        out = np.full(a.shape, 127, dtype=np.uint8)
        out[np.isnan(a)] = 0
        return out
    norm = np.clip((a - vmin) / (vmax - vmin), 0.0, 1.0)
    norm = np.nan_to_num(norm, nan=0.0, posinf=1.0, neginf=0.0)
    out = (norm * 255.0).astype(np.uint8)
    out[np.isnan(a)] = 0
    return out


def cap_to_range(arr, vmin, vmax):
    a = np.array(arr, dtype=float, copy=True)
    fin = np.isfinite(a)
    if vmin is not None:
        mask = fin & (a < float(vmin))
        if mask.any():
            a[mask] = float(vmin)
    if vmax is not None:
        mask = fin & (a > float(vmax))
        if mask.any():
            if vmin is None:
                candidates = a[fin & (a <= float(vmax))]
                cap_value = float(np.max(candidates)) if candidates.size > 0 else float(vmax)
            else:
                cap_value = float(vmax)
            a[mask] = cap_value
    return a


def write_webp_packed(float_arr, alpha_mask, outpath, precision=0.1, base=0.0):
    h, w = float_arr.shape
    q = np.rint((float_arr - base) / float(precision)).astype(np.int64)
    q = np.where(np.isnan(float_arr), 0, q)
    q = np.clip(q, 0, 2 ** 24 - 1).astype(np.int64)
    r = ((q >> 16) & 255).astype(np.uint8)
    g = ((q >> 8) & 255).astype(np.uint8)
    b = (q & 255).astype(np.uint8)
    rgba = np.zeros((h, w, 4), dtype=np.uint8)
    rgba[..., 0] = r
    rgba[..., 1] = g
    rgba[..., 2] = b
    rgba[..., 3] = alpha_mask
    img = Image.fromarray(rgba, mode="RGBA")
    img.save(outpath, "WEBP", lossless=True)


# ---------------------------------------------------------------------------
# Per-task worker (identical logic to shared/nc2tile.py _process_task,
# but receives grid_path instead of reaching out to DB)
# ---------------------------------------------------------------------------

def _process_task(task: Tuple) -> Tuple[str, str]:
    (
        ds_data_path,
        varname,
        t_idx,
        t_str,
        d_idx,
        d_val,
        vmin,
        vmax,
        erddap_min,
        erddap_max,
        minx,
        miny,
        maxx,
        maxy,
        w,
        h,
        method,
        clip,
        verbose,
        pack_precision,
        grid_path,
        outdir,
    ) = task

    lon_src, lat_src = load_grid(grid_path)

    ds_data = xr.open_dataset(ds_data_path)

    xs = np.linspace(minx, maxx, int(w))
    ys = np.linspace(maxy, miny, int(h))
    xx_merc, yy_merc = np.meshgrid(xs, ys)

    time_dim = depth_dim = None
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
    tslice_vals = tslice.astype(float)

    mask_slice = None
    for m in [f"{varname}_mask", f"{varname}_valid", f"{varname}_flag", "mask"]:
        if m in ds_data:
            try:
                ms = ds_data[m].isel(sel).values
                if ms.shape == tslice_vals.shape:
                    mask_slice = ms
                    break
            except Exception:
                pass

    if mask_slice is not None:
        if mask_slice.dtype == bool:
            src_valid = mask_slice.astype(bool)
        else:
            src_valid = (mask_slice != 0) & np.isfinite(mask_slice)
    else:
        fill = None
        try:
            fill = ds_data[varname].attrs.get("_FillValue") or ds_data[varname].attrs.get("missing_value")
        except Exception:
            pass
        if fill is not None:
            try:
                fv = float(fill)
                src_valid = np.isfinite(tslice_vals) & (~np.isclose(tslice_vals, fv)) & (tslice_vals != 0)
            except Exception:
                src_valid = np.isfinite(tslice_vals) & (tslice_vals != 0)
        else:
            src_valid = np.isfinite(tslice_vals) & (np.abs(tslice_vals) < 1e29) & (tslice_vals != 0)

    total_points = tslice_vals.size
    valid_count = int(np.sum(src_valid))
    if valid_count == 0:
        interp = np.full_like(xx_merc, np.nan, dtype=float)
        mask_tgt_bool = np.zeros_like(interp, dtype=bool)
    else:
        coord_valid = np.isfinite(lon_src.ravel()) & np.isfinite(lat_src.ravel())
        pts_src_full = np.column_stack((lon_src.ravel(), lat_src.ravel()))[coord_valid]
        src_valid_flat = src_valid.ravel()[coord_valid]
        if src_valid_flat.sum() < max(1, int(0.01 * total_points)):
            src_valid_flat = (np.isfinite(tslice_vals.ravel()) & (tslice_vals.ravel() != 0))[coord_valid]

        if Transformer is None:
            raise RuntimeError("pyproj is required")
        transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
        tgt_lon, tgt_lat = transformer.transform(xx_merc.ravel(), yy_merc.ravel())
        tgt_pts = np.column_stack((tgt_lon, tgt_lat))

        grid_sig = (
            lon_src.shape,
            float(np.nanmin(lon_src)),
            float(np.nanmax(lon_src)),
            float(np.nanmin(lat_src)),
            float(np.nanmax(lat_src)),
            int(w),
            int(h),
        )
        interp_engine = _get_interpolator(method, pts_src_full, tgt_pts, grid_sig)

        mask_src_f = src_valid_flat.astype(float)
        try:
            mask_engine = _get_interpolator("nearest", pts_src_full, tgt_pts, grid_sig)
            if mask_engine is not None:
                mask_tgt_flat = mask_engine.apply(mask_src_f)
            else:
                mask_tgt_flat = griddata(pts_src_full, mask_src_f, tgt_pts, method="nearest", fill_value=0.0)
            mask_tgt_flat = np.nan_to_num(mask_tgt_flat, nan=0.0)
            mask_tgt_bool = (mask_tgt_flat.reshape(xx_merc.shape) >= 0.5)
        except Exception:
            mask_tgt_bool = np.zeros_like(xx_merc, dtype=bool)

        vals_full = tslice_vals.ravel()[coord_valid].astype(float)
        vals_full[~src_valid_flat] = np.nan
        vals_valid = vals_full[src_valid_flat]
        try:
            if interp_engine is not None:
                interp_flat = interp_engine.apply(vals_full)
            else:
                pts_valid = pts_src_full[src_valid_flat]
                interp_flat = griddata(pts_valid, vals_valid, tgt_pts, method=method, fill_value=np.nan)
            interp = interp_flat.reshape(xx_merc.shape)
        except Exception:
            interp = np.full_like(xx_merc, np.nan, dtype=float)

        if not mask_tgt_bool.any():
            mask_tgt_bool = np.isfinite(interp)
        interp[~mask_tgt_bool] = np.nan

    if vmin is None or vmax is None:
        fin = np.isfinite(interp)
        if fin.sum() == 0:
            vmin_local, vmax_local = 0.0, 1.0
        else:
            nonzero = fin & (interp != 0)
            vmin_local = float(np.nanmin(interp[nonzero])) if nonzero.sum() > 0 else float(np.nanmin(interp[fin]))
            vmax_local = float(np.nanmax(interp[fin]))
    else:
        vmin_local, vmax_local = vmin, vmax

    gray = scale_to_uint8(interp, vmin_local, vmax_local, clip_percentile=clip)
    alpha = np.where(np.isnan(interp) | (interp == 0), 0, 255).astype(np.uint8)

    interp_capped = cap_to_range(interp, erddap_min, erddap_max) if (erddap_min is not None or erddap_max is not None) else interp

    webp_dir = os.path.join(outdir, varname, t_str)
    os.makedirs(webp_dir, exist_ok=True)

    if d_val is None:
        fname = "time.webp"
    elif float(d_val) == -1.0:
        fname = "bottom.webp"
    else:
        depth_s = f"{round(float(d_val), 1):.1f}".replace(".", "p").replace("-", "m")
        fname = f"{depth_s}.webp"

    out_webp = os.path.join(webp_dir, fname)
    write_webp_packed(interp_capped, alpha, out_webp, precision=pack_precision, base=0.0)

    if verbose:
        depth_label = f" (depth={d_val})" if d_val is not None else ""
        print(f"Wrote: {out_webp}{depth_label}")

    ds_data.close()
    return out_webp, t_str


# ---------------------------------------------------------------------------
# Main processing entry point
# ---------------------------------------------------------------------------

def process_variable(
    ds_data_path: str,
    depth_indices: list[int],
    varname: str,
    grid_path: str,
    fields: dict,
    outdir: str,
    max_dim: int = 2048,
    method: str = "linear",
    clip_percentile: Optional[Tuple[float, float]] = None,
    global_scale: bool = True,
    verbose: bool = True,
    workers: int = 1,
    pack_precision: float = 0.1,
) -> list[str]:
    lon_src, lat_src = load_grid(grid_path)
    ds_data = xr.open_dataset(ds_data_path)

    minx, miny, maxx, maxy = compute_mercator_grid_bounds(lon_src, lat_src)
    xx_merc, yy_merc, w, h = build_target_grid(minx, miny, maxx, maxy, max_dim=max_dim)

    if verbose:
        print(f"Target grid size: {w}x{h} (max_dim={max_dim})")

    vmin = vmax = erddap_min = erddap_max = None
    if varname in fields:
        erddap_min = fields[varname]["min"]
        erddap_max = fields[varname]["max"]
        if global_scale:
            vmin, vmax = erddap_min, erddap_max
            if verbose:
                print(f"Global scale for {varname}: vmin={vmin}, vmax={vmax}")
    elif verbose:
        print(f"Warning: {varname} not found in fields file — using per-image scaling")

    if Transformer is None:
        raise RuntimeError("pyproj.Transformer is required")
    tmerc2ll = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    corners_x = [xx_merc[0, 0], xx_merc[0, -1], xx_merc[-1, -1], xx_merc[-1, 0]]
    corners_y = [yy_merc[0, 0], yy_merc[0, -1], yy_merc[-1, -1], yy_merc[-1, 0]]
    lons, lats = tmerc2ll.transform(corners_x, corners_y)
    meta = {
        "bounds": [float(np.min(lons)), float(np.min(lats)), float(np.max(lons)), float(np.max(lats))],
        "vmin": vmin,
        "vmax": vmax,
        "packed": {"precision": float(pack_precision), "base": 0.0},
    }

    time_dim = depth_dim = None
    for d in ds_data[varname].dims:
        ld = d.lower()
        if "time" in ld and time_dim is None:
            time_dim = d
        if any(k in ld for k in ("depth", "lev", "z", "deptht")) and depth_dim is None:
            depth_dim = d

    if time_dim is None:
        raise ValueError("Could not find time dimension for variable")

    times = ds_data[time_dim].values
    depths = ds_data[depth_dim].values if depth_dim else [None]

    out_var_dir = os.path.join(outdir, varname)
    os.makedirs(out_var_dir, exist_ok=True)
    with open(os.path.join(out_var_dir, "meta.json"), "w") as fh:
        json.dump(meta, fh)

    tasks = []
    tstr_to_iso: dict[str, str] = {}
    for idx, tval in enumerate(times):
        tstr = np.datetime_as_string(tval, unit="s").replace(":", "")
        tstr_to_iso[tstr] = str(np.datetime_as_string(tval, unit="s"))
        for didx in depth_indices:
            d_val = depths[int(didx)] if depth_dim else None
            tasks.append((
                ds_data_path, varname, int(idx), str(tstr),
                didx, d_val,
                vmin if global_scale else None,
                vmax if global_scale else None,
                erddap_min, erddap_max,
                float(minx), float(miny), float(maxx), float(maxy),
                int(w), int(h),
                method, clip_percentile, verbose, pack_precision,
                grid_path, outdir,
            ))

    processed_tstrs: set[str] = set()
    if workers <= 1:
        for task in tasks:
            try:
                out_webp, t_str = _process_task(task)
                processed_tstrs.add(t_str)
            except Exception as exc:
                print(f"Task failed: {exc}")
    else:
        if verbose:
            print(f"Dispatching {len(tasks)} tasks to {workers} workers...")
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(_process_task, t): t for t in tasks}
            iter_ = tqdm(as_completed(futures), total=len(futures), desc=varname) if tqdm else as_completed(futures)
            for fut in iter_:
                try:
                    out_webp, t_str = fut.result()
                    if verbose:
                        print(f"Completed: {out_webp}")
                    processed_tstrs.add(t_str)
                except Exception as exc:
                    print(f"Task failed: {exc}")

    ds_data.close()
    return [tstr_to_iso[t] for t in sorted(processed_tstrs)]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Regrid NetCDF to Web-Mercator WebP tiles (no DB required).")
    p.add_argument("--data", required=True, help="Input NetCDF file")
    p.add_argument("--vars", default=None, help="Comma-separated variable names (default: all)")
    p.add_argument("--depth-indices", required=True, help="Comma-separated depth indices to process, e.g. 0,1,2")
    p.add_argument("--grid", required=True, help="Path to grid.npz (from export_grid.py)")
    p.add_argument("--fields", default=None, help="Path to fields.json for variable bounds (optional)")
    p.add_argument("--outdir", default="webp", help="Root output directory (default: ./webp)")
    p.add_argument("--max-dim", type=int, default=2048)
    p.add_argument("--interp", choices=["linear", "nearest"], default="linear")
    p.add_argument("--clip-pct", default=None, help="e.g. 1,99")
    p.add_argument("--no-global-scale", action="store_true")
    p.add_argument("--workers", type=int, default=None)
    p.add_argument("--precision", type=float, default=0.1)
    p.add_argument("--quiet", action="store_true")
    return p.parse_args(argv)


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args(argv)

    fields: dict = {}
    if args.fields and os.path.exists(args.fields):
        with open(args.fields) as fh:
            fields = json.load(fh)
    elif args.fields:
        print(f"Warning: fields file not found: {args.fields}")

    clip = None
    if args.clip_pct:
        try:
            pmin, pmax = [float(x.strip()) for x in args.clip_pct.split(",")]
            clip = (pmin, pmax)
        except Exception:
            raise SystemExit("Invalid --clip-pct; use format e.g. 1,99")

    ds = xr.open_dataset(args.data)
    varnames = [v.strip() for v in args.vars.split(",") if v.strip()] if args.vars else list(ds.data_vars.keys())
    ds.close()

    workers = args.workers if args.workers is not None else max(1, multiprocessing.cpu_count() - 1)
    depth_indices = [int(d) for d in args.depth_indices.split(",") if d.strip().isdigit()]

    for var in varnames:
        print(f"Processing variable: {var} (workers={workers})")
        process_variable(
            ds_data_path=args.data,
            depth_indices=depth_indices,
            varname=var,
            grid_path=args.grid,
            fields=fields,
            outdir=args.outdir,
            max_dim=args.max_dim,
            method=args.interp,
            clip_percentile=clip,
            global_scale=not args.no_global_scale,
            verbose=not args.quiet,
            workers=workers,
            pack_precision=args.precision,
        )


if __name__ == "__main__":
    main()
