#!/usr/bin/env python3
"""Reproject NetCDF variables to Web-Mercator and write per-timestep grayscale PNGs.

Behavior (defaults chosen according to your inputs):
- Uses xarray to open a `ds_data` NetCDF file (contains variables and time). Lat/lon grid is read from the Postgres `grid` table.
- Reads lat/lon arrays from the Postgres `grid` table (columns: row_idx, col_idx, lat, lon). If the DB cannot be reached or the grid is missing the script will raise an error and exit.
- For each variable in `ds_data` (or a selected subset) and for each time step,
  regrids the variable from the source curvilinear grid to a regular Web-Mercator (EPSG:3857) grid
  using `scipy.interpolate.griddata` (linear interpolation by default).
- Preserves aspect ratio; target max dimension defaults to 2048 px.
- Scales values to grayscale using a *global* min/max per variable across all times.
- Writes PNGs with transparent alpha for NaN values and a JSON sidecar containing lon/lat bounds for Mapbox placement.

Dependencies:
- xarray, numpy, scipy, pyproj, pillow (PIL), tqdm (optional)

Example usage:

    python nc2tile.py  
      --data ds_data.nc  
      --vars dissolved_inorganic_carbon,total_alkalinity 
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
from scipy.spatial import Delaunay, cKDTree
from PIL import Image

# DB access for curvilinear grid
import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from pyproj import Transformer
except Exception:
    Transformer = None

# module-level cache for grid loaded from DB
GRID_CACHE = None
GRID_CACHE_PATH = os.getenv("GRID_CACHE_PATH", "/tmp/oa_grid_cache.npz")
INTERP_CACHE = {}

import logging

try:
    from tqdm import tqdm
except Exception:  # optional
    tqdm = None

logger = logging.getLogger("nc2tile")





# ---- DB helpers for curvilinear grid ---------------------------------

def get_db_conn():
    host = os.getenv('PGHOST', 'db')
    port = int(os.getenv('PGPORT', 5432))
    db = os.getenv('PGDATABASE', 'oa')
    user = os.getenv('PGUSER', 'postgres')
    pw = os.getenv('PGPASSWORD', 'postgres')
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pw)


def _load_grid_cache(path: str) -> Tuple[np.ndarray, np.ndarray] | None:
    try:
        if os.path.exists(path):
            data = np.load(path)
            lon = data.get('lon')
            lat = data.get('lat')
            if lon is not None and lat is not None:
                return lon, lat
    except Exception:
        logger.exception("Failed to load grid cache from %s", path)
    return None


def _write_grid_cache(path: str, lon: np.ndarray, lat: np.ndarray) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        np.savez_compressed(path, lon=lon, lat=lat)
    except Exception:
        logger.exception("Failed to write grid cache to %s", path)


def get_grid_from_db(table: str = 'grid') -> Tuple[np.ndarray, np.ndarray]:
    """Load curvilinear grid lon/lat arrays from Postgres table named `table`.

    Expects columns: row_idx, col_idx, lon, lat
    Returns lon2d, lat2d shaped (nrows, ncols) aligned by sorted unique row/col indices.
    Caches result in module-level GRID_CACHE.
    """
    global GRID_CACHE
    if GRID_CACHE is not None:
        return GRID_CACHE

    cached = _load_grid_cache(GRID_CACHE_PATH)
    if cached is not None:
        GRID_CACHE = cached
        logger.info("Loaded grid from cache %s", GRID_CACHE_PATH)
        return GRID_CACHE

    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"SELECT row_idx, col_idx, lon, lat FROM {table}")
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        raise ValueError(f"Grid table '{table}' is empty or missing")

    row_idxs = sorted({r['row_idx'] for r in rows})
    col_idxs = sorted({r['col_idx'] for r in rows})
    row_pos = {v: i for i, v in enumerate(row_idxs)}
    col_pos = {v: j for j, v in enumerate(col_idxs)}

    nrows = len(row_idxs)
    ncols = len(col_idxs)
    lon = np.full((nrows, ncols), np.nan, dtype=float)
    lat = np.full((nrows, ncols), np.nan, dtype=float)

    for r in rows:
        i = row_pos[r['row_idx']]
        j = col_pos[r['col_idx']]
        lon[i, j] = float(r['lon'])
        lat[i, j] = float(r['lat'])

    GRID_CACHE = (lon, lat)
    _write_grid_cache(GRID_CACHE_PATH, lon, lat)
    logger.info("Worker loaded grid from DB table 'grid' and cached to %s", GRID_CACHE_PATH)
    return GRID_CACHE


class _PrecomputedLinearInterpolator:
    def __init__(self, pts_src: np.ndarray, tgt_pts: np.ndarray):
        self.tri = Delaunay(pts_src)
        simplices = self.tri.find_simplex(tgt_pts)
        self.valid = simplices >= 0
        self.vertices = np.where(self.valid[:, None], self.tri.simplices[simplices], 0)
        X = self.tri.transform[simplices, :2]
        Y = tgt_pts - self.tri.transform[simplices, 2]
        b = np.einsum('ijk,ik->ij', X, Y)
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


def _get_interpolator(method: str, pts_src: np.ndarray, tgt_pts: np.ndarray, grid_sig: Tuple) -> object | None:
    key = (method, grid_sig, pts_src.shape[0], tgt_pts.shape[0])
    if key in INTERP_CACHE:
        return INTERP_CACHE[key]
    if method == "linear":
        interp = _PrecomputedLinearInterpolator(pts_src, tgt_pts)
    elif method == "nearest":
        interp = _PrecomputedNearestInterpolator(pts_src, tgt_pts)
    else:
        return None
    INTERP_CACHE[key] = interp
    return interp


def compute_mercator_grid_bounds(lon: np.ndarray, lat: np.ndarray) -> Tuple[float, float, float, float]:
    """Compute bounding box in Web-Mercator meters (minx,miny,maxx,maxy) for given lon/lat arrays."""
    if Transformer is None:
        raise RuntimeError("pyproj.Transformer is required but not installed")
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    lon_flat = np.array([np.nanmin(lon), np.nanmax(lon)])
    lat_flat = np.array([np.nanmin(lat), np.nanmax(lat)])
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


def _process_task(task: Tuple) -> Tuple[str, str]:
    """Worker function executed in a separate process.

    Returns (path_of_written_png, t_str) where t_str is the folder (time) string.
    """
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
    ) = task

    # Open dataset in worker process
    ds_data = xr.open_dataset(ds_data_path)

    # Load curvilinear grid from DB (no fallback). This will raise if DB not available.
    # get_grid_from_db logs a message only on the first load per process to avoid repeated DB hits.
    lon_src, lat_src = get_grid_from_db('grid')

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

    # Build a valid-data mask from the file (prefer an explicit mask variable if present)
    tslice_vals = tslice.astype(float)

    mask_slice = None
    mask_candidates = [f"{varname}_mask", f"{varname}_valid", f"{varname}_flag", "mask"]
    for m in mask_candidates:
        if m in ds_data:
            try:
                ms = ds_data[m].isel(sel).values
                if ms.shape == tslice_vals.shape:
                    mask_slice = ms
                    break
            except Exception:
                pass

    if mask_slice is not None:
        # normalize mask to boolean: non-zero/True and finite means valid
        if mask_slice.dtype == bool:
            src_valid = mask_slice.astype(bool)
        else:
            src_valid = (mask_slice != 0) & np.isfinite(mask_slice)
    else:
        # fallback: use _FillValue / missing_value attrs, or NaN/finite test
        fill = None
        try:
            fill = ds_data[varname].attrs.get('_FillValue') or ds_data[varname].attrs.get('missing_value')
        except Exception:
            fill = None
        if fill is not None:
            try:
                fv = float(fill)
                src_valid = np.isfinite(tslice_vals) & (~np.isclose(tslice_vals, fv)) & (tslice_vals != 0)
            except Exception:
                src_valid = np.isfinite(tslice_vals) & (tslice_vals != 0)
        else:
            # treat extreme sentinel values as invalid (defensive)
            src_valid = np.isfinite(tslice_vals) & (np.abs(tslice_vals) < 1e29) & (tslice_vals != 0)

    # If no valid source points, produce entirely transparent output
    total_points = tslice_vals.size
    valid_count = int(np.sum(src_valid))
    # Handle pathological case: fill==0 marking nearly all points invalid -> relax to finite-only
    if valid_count == 0:
        interp = np.full_like(xx_merc, np.nan, dtype=float)
        mask_tgt_bool = np.zeros_like(interp, dtype=bool)
    else:
        # Build source coordinate mask to exclude NaN lon/lats
        coord_valid = np.isfinite(lon_src.ravel()) & np.isfinite(lat_src.ravel())
        pts_src_full = np.column_stack((lon_src.ravel(), lat_src.ravel()))[coord_valid]

        # Align source-valid flags with coord_valid entries
        src_valid_flat = src_valid.ravel()[coord_valid]

        # If src_valid_flat has very few true points (e.g., fill==0 case), fallback to finite-only (excluding zero)
        if src_valid_flat.sum() < max(1, int(0.01 * total_points)):
            src_valid_flat = (np.isfinite(tslice_vals.ravel()) & (tslice_vals.ravel() != 0))[coord_valid]

        # Transform target mercator grid to lon/lat for interpolation
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

        # Interpolate mask using nearest-neighbor to avoid blurry boundaries that create black borders
        # (even though data is interpolated with linear/cubic, the mask must be discrete to maintain crisp alpha edges)
        mask_src_f = src_valid_flat.astype(float)
        try:
            # Always use 'nearest' for mask to create hard alpha boundaries, not soft gradients
            mask_tgt_flat = griddata(pts_src_full, mask_src_f, tgt_pts, method='nearest', fill_value=0.0)
            mask_tgt_flat = np.nan_to_num(mask_tgt_flat, nan=0.0)
            mask_tgt_bool = (mask_tgt_flat.reshape(xx_merc.shape) >= 0.5)
        except Exception:
            # Very unlikely to fail with nearest
            mask_tgt_bool = np.zeros_like(xx_merc, dtype=bool)

        # Interpolate data using only valid source points (and coords that are finite)
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

        # If mask interpolation produced nothing, fallback to marking finite interpolated values as valid
        if not mask_tgt_bool.any():
            mask_tgt_bool = np.isfinite(interp)

        # Apply interpolated mask: mark anything coming from invalid source as NaN
        interp[~mask_tgt_bool] = np.nan

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
    # Derive alpha directly from interpolated data to avoid boundary artifacts
    alpha = np.where(np.isnan(interp) | (interp == 0), 0, 255).astype(np.uint8)

    # Apply erddap-provided absolute min/max caps to the interpolated values before packing to RGB.
    # Leave NaNs untouched (they will remain transparent in alpha).
    if erddap_min is not None or erddap_max is not None:
        interp_capped = cap_to_range(interp, erddap_min, erddap_max)
    else:
        interp_capped = interp

    # filename and folder per user request: /opt/data/png/{var}/{datetime}/{depth}.png
    png_root = os.getenv('PNG_ROOT', '/opt/data/png')
    png_dir = os.path.join(png_root, varname, t_str)
    os.makedirs(png_dir, exist_ok=True)

    if d_val is None:
        fname = 'time.png'
    else:
        # round depth to 1 decimal and format like 0p5, 18p0, etc.
        depth_round = round(float(d_val), 1)
        depth_s = f"{depth_round:.1f}".replace('.', 'p').replace('-', 'm')
        fname = f"{depth_s}.png"

    out_png = os.path.join(png_dir, fname)

    # Pack values into RGB channels at fixed precision using base=0.0.
    # Use the capped array (if applicable) so values outside absolute bounds are clipped prior to packing.
    write_png_packed(interp_capped, alpha, out_png, precision=pack_precision, base=0.0)
    # per-datetime sidecar files are not written anymore; we use the per-variable meta.json
    # for bounds and packing metadata instead (fixed vmin/vmax and fixed bounds apply).
    if verbose:
        if d_val is None:
            print(f"Wrote (worker): {out_png}")
        else:
            print(f"Wrote (worker): {out_png} (depth={d_val})")

    ds_data.close()

    return out_png, t_str


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


def cap_to_range(arr: np.ndarray, vmin: Optional[float], vmax: Optional[float]) -> np.ndarray:
    """Return a copy of `arr` with values capped to [vmin, vmax].

    - Values that are NaN are left untouched.
    - If vmin or vmax is None, that bound is ignored.

    Special behavior: when `vmax` is provided, values above `vmax` will be
    capped to the largest finite value in the array that is <= `vmax` if any
    such value exists; otherwise they are capped to `vmax`. This preserves
    discrete step-like value behavior in some datasets (see tests).
    """
    a = np.array(arr, dtype=float, copy=True)
    fin = np.isfinite(a)
    if vmin is not None:
        mask = fin & (a < float(vmin))
        if mask.any():
            a[mask] = float(vmin)
    if vmax is not None:
        mask = fin & (a > float(vmax))
        if mask.any():
            # If vmin is None (partial bound provided) prefer capping to the
            # largest existing value <= vmax (if any). If vmin is provided then
            # treat vmax as an explicit numeric clamp.
            if vmin is None:
                candidates = a[fin & (a <= float(vmax))]
                if candidates.size > 0:
                    cap_value = float(np.max(candidates))
                else:
                    cap_value = float(vmax)
            else:
                cap_value = float(vmax)
            a[mask] = cap_value
    return a


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


def write_png_packed(float_arr: np.ndarray, alpha_mask: np.ndarray, outpath: str, precision: float = 0.1, base: float = 0.0) -> None:
    """Pack float values into RGB channels using fixed-point quantization.

    - precision: the smallest distinguishable unit (e.g., 0.1)
    - base: the value that maps to 0 in packed representation (quant = round((val-base)/precision))

    Quantized integers are clamped to [0, 2^24-1] and split into R,G,B bytes (big-endian).
    Alpha channel is preserved as provided (0..255 uint8).
    """
    h, w = float_arr.shape
    # quantize
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
    img.save(outpath, compress_level=1)




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
    ds_data_path: str,
    depth_indices: list[int],
    varname: str,
    max_dim: int = 2048,
    method: str = "linear",
    clip_percentile: Optional[Tuple[float, float]] = None,
    global_scale: bool = True,
    verbose: bool = True,
    workers: int = 1,
    pack_precision: float = 0.1,
):
    # Load curvilinear grid from DB table 'grid'. Fail hard if unavailable.
    lon_src, lat_src = get_grid_from_db('grid')
    # get_grid_from_db logs a message only on the first load per process to avoid repeated DB hits.

    # Open ds_data and compute mercator bounds
    ds_data = xr.open_dataset(ds_data_path)

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

    # Try to fetch any absolute min/max values stored in erddap_variables for this variable.
    erddap_min = None
    erddap_max = None
    try:
        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT min, max FROM erddap_variables WHERE variable=%s LIMIT 1",
                    (varname,),
                )
                row = cur.fetchone()
                if row:
                    erddap_min, erddap_max = row[0], row[1]
        finally:
            conn.close()
    except Exception:
        # Don't fail if DB is not available; just proceed without erddap caps
        erddap_min = None
        erddap_max = None

    if verbose and (erddap_min is not None or erddap_max is not None):
        print(f"Using erddap min/max for {varname}: min={erddap_min}, max={erddap_max}")

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
    # Include packing metadata (precision and base) so consumers know the packing format
    meta['packed'] = {'precision': float(pack_precision), 'base': 0.0}

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
    
    depths = ds_data[depth_dim].values

    # Write per-variable meta.json into PNG_ROOT/<var>/meta.json so the API can serve it
    png_root = os.getenv('PNG_ROOT', '/opt/data/png')
    out_var_dir = os.path.join(png_root, varname)
    os.makedirs(out_var_dir, exist_ok=True)
    with open(os.path.join(out_var_dir, 'meta.json'), 'w') as fh:
        json.dump(meta, fh)

    # Prepare task list (time x depth) to be executed either locally or in parallel workers
    tasks = []
    tstr_to_iso = {}
    for idx, tval in enumerate(times):
        tstr = np.datetime_as_string(tval, unit="s").replace(":", "")
        t_iso = str(np.datetime_as_string(tval, unit="s"))
        tstr_to_iso[str(tstr)] = t_iso

        for didx in depth_indices:
            # prepare vmin/vmax to send to worker if using global scale; else pass None
            send_vmin = vmin if global_scale else None
            send_vmax = vmax if global_scale else None
            
            depth = depths[int(didx)]

            # include any absolute min/max stored in erddap_variables for this variable
            tasks.append((
                ds_data_path,
                varname,
                int(idx),
                str(tstr),
                didx,
                depth,
                send_vmin,
                send_vmax,
                erddap_min,
                erddap_max,
                float(minx),
                float(miny),
                float(maxx),
                float(maxy),
                int(w),
                int(h),
                method,
                clip_percentile,
                verbose,
                pack_precision,
            ))

    processed_tstrs = set()
    if workers <= 1:
        # run sequentially in-process (use same worker fn)
        for task in tasks:
            try:
                out = _process_task(task)
            except Exception as exc:
                if verbose:
                    print(f"Task failed: {exc}")
            else:
                # out is (out_png, t_str)
                if isinstance(out, tuple) and len(out) >= 2:
                    processed_tstrs.add(out[1])
    else:
        total = len(tasks)
        if total == 0:
            ds_data.close()
            return []
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
                    if isinstance(out, tuple) and len(out) >= 2:
                        processed_tstrs.add(out[1])
    ds_data.close()

    # return ISO-format datetimes for all successfully processed time folders
    processed_iso = [tstr_to_iso[t] for t in sorted(processed_tstrs)]
    return processed_iso


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Regrid NetCDF variables to Web-Mercator PNG tiles")
    p.add_argument("--data", required=True, help="Path to ds_data NetCDF (contains variables with gridY/gridX + time)")
    p.add_argument("--vars", default=None, help="Comma-separated variable names to process (default: all variables in ds_data)")
    p.add_argument("--max-dim", type=int, default=2048, help="Maximum pixel dimension for output (preserves aspect ratio)")
    p.add_argument("--interp", choices=["linear", "nearest", "cubic"], default="linear", help="Interpolation method")
    p.add_argument("--clip-pct", default=None, help="Clip percentiles e.g. 1,99 for contrast stretching (optional)")
    p.add_argument("--no-global-scale", action="store_true", help="Disable global per-variable scaling (use per-image) ")
    p.add_argument("--workers", type=int, default=None, help="Number of parallel worker processes to use (default: cpu_count()-1)")
    p.add_argument("--precision", type=float, default=0.1, help="Precision for PNG packing (default: 0.1)")
    p.add_argument("--depth-indices", required=True, help="Comma-separated depth indices to process.")
    p.add_argument("--quiet", action="store_true", help="Less verbose")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> list[int]:
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

    all_processed = []
    for var in varnames:
        print(f"Processing variable: {var} (workers={workers})")
        processed = process_variable(
            args.data,
            [int(d) for d in args.depth_indices.split(",") if d.strip().isdigit()],
            var,
            max_dim=args.max_dim,
            method=args.interp,
            clip_percentile=clip,
            global_scale=not args.no_global_scale,
            verbose=not args.quiet,
            workers=workers,
            pack_precision=args.precision,
        )
        if processed:
            all_processed.extend(processed)

    return all_processed


if __name__ == "__main__":
    raise SystemExit(main())