#!/usr/bin/env python3
"""Image processing for Live Ocean: regrid to Web-Mercator and write WebP tiles.

Loads extracted NetCDF files and generates WebP tiles for all variables/depths/times.
Supports both serial and parallel (worker pool) processing.

Memory optimization: 
- Grid is cached to disk to avoid redundant DB queries in workers
- NC files are loaded lazily - only needed time/depth slices are loaded into memory
"""

from __future__ import annotations

import logging
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import lru_cache
from typing import List, Tuple

import numpy as np
import xarray as xr
from scipy.interpolate import griddata
from scipy.spatial import Delaunay, cKDTree
from PIL import Image
import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from pyproj import Transformer
except Exception:
    Transformer = None

logger = logging.getLogger("lo_imaging")

# Dataset ID for Live Ocean in the datasets table
LO_DATASET_ID = 4

# Grid cache file
GRID_CACHE_PATH = os.getenv("GRID_CACHE_PATH", "/opt/data/cache/lo_grid_cache.npz")

# Worker-local cache for Mercator grids and interpolator (set by worker initializer)
_worker_mercator_cache = {}


def get_db_conn(db_host: str = None, db_user: str = None, db_password: str = None, db_name: str = None):
    """Create a database connection, using environment variables if args not provided."""
    host = db_host or os.getenv("DB_HOST", "db")
    user = db_user or os.getenv("DB_USER", "postgres")
    password = db_password or os.getenv("DB_PASSWORD", "postgres")
    database = db_name or os.getenv("DB_NAME", "oa")
    
    return psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=database,
    )


def get_field_metadata(variable: str) -> Tuple[float, float, float]:
    """Fetch min, max, precision from fields table for dataset_id=4.
    
    Returns (min_val, max_val, precision).
    Raises ValueError if not found.
    """
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT min, max, precision
                FROM fields
                WHERE dataset_id = %s AND variable = %s
                LIMIT 1
                """,
                (LO_DATASET_ID, variable),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"No field metadata found for variable '{variable}' in dataset {LO_DATASET_ID}")
            return float(row['min']), float(row['max']), float(row['precision'])
    finally:
        conn.close()


def load_lo_grid() -> Tuple[np.ndarray, np.ndarray]:
    """Load lo_grid from database or cache. Returns (lon_2d, lat_2d) shaped (nrows, ncols).
    
    Tries cache first to avoid DB queries in worker processes.
    """
    # Try loading from cache first (fast for workers)
    if os.path.exists(GRID_CACHE_PATH):
        try:
            data = np.load(GRID_CACHE_PATH)
            lon = data['lon']
            lat = data['lat']
            logger.info(f"Loaded lo_grid from cache: {GRID_CACHE_PATH}")
            return lon, lat
        except Exception as e:
            logger.warning(f"Failed to load grid from cache: {e}")
    
    # Fall back to DB query
    logger.info("Loading lo_grid from database...")
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT row_idx, col_idx, lon, lat FROM lo_grid")
            rows = cur.fetchall()
    finally:
        conn.close()
    
    if not rows:
        raise ValueError("lo_grid table is empty or missing. Run lo_grid_init.py first.")
    
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
    
    # Save to cache for future loads
    try:
        os.makedirs(os.path.dirname(GRID_CACHE_PATH), exist_ok=True)
        np.savez_compressed(GRID_CACHE_PATH, lon=lon, lat=lat)
        logger.info(f"Cached lo_grid to: {GRID_CACHE_PATH}")
    except Exception as e:
        logger.warning(f"Could not cache grid: {e}")
    
    logger.info(f"Loaded lo_grid: {nrows} x {ncols}")
    return lon, lat


def compute_mercator_bounds(lon: np.ndarray, lat: np.ndarray) -> Tuple[float, float, float, float]:
    """Compute Web-Mercator bounds (minx, miny, maxx, maxy) in meters."""
    if Transformer is None:
        raise RuntimeError("pyproj is required")
    
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    lon_flat = np.array([np.nanmin(lon), np.nanmax(lon)])
    lat_flat = np.array([np.nanmin(lat), np.nanmax(lat)])
    xs, ys = transformer.transform(lon_flat, lat_flat)
    
    return float(min(xs)), float(min(ys)), float(max(xs)), float(max(ys))


def build_mercator_grid(minx: float, miny: float, maxx: float, maxy: float, max_dim: int = 2048) -> Tuple[np.ndarray, np.ndarray, int, int]:
    """Build target Web-Mercator grid. Returns (xx, yy, width, height)."""
    width_m = maxx - minx
    height_m = maxy - miny
    
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


class LinearInterpolator:
    """Precomputed linear interpolator using Delaunay triangulation."""
    
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


def regrid_to_mercator(data_src: np.ndarray, lon_src: np.ndarray, lat_src: np.ndarray,
                       xx_merc: np.ndarray, yy_merc: np.ndarray) -> np.ndarray:
    """Regrid source data to Mercator grid using linear interpolation."""
    # Build source points (lon, lat) for valid grid cells
    coord_valid = np.isfinite(lon_src.ravel()) & np.isfinite(lat_src.ravel())
    pts_src = np.column_stack((lon_src.ravel(), lat_src.ravel()))[coord_valid]
    
    # Transform target Mercator to lon/lat
    if Transformer is None:
        raise RuntimeError("pyproj is required")
    transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    xs_flat = xx_merc.ravel()
    ys_flat = yy_merc.ravel()
    lons_tgt, lats_tgt = transformer.transform(xs_flat, ys_flat)
    tgt_pts = np.column_stack((lons_tgt, lats_tgt))
    
    # Interpolate
    interp = LinearInterpolator(pts_src, tgt_pts)
    data_src_flat = data_src.ravel()[coord_valid]
    interp_flat = interp.apply(data_src_flat)
    interp_grid = interp_flat.reshape(xx_merc.shape)
    
    return interp_grid


def pack_to_rgb(float_arr: np.ndarray, vmin: float, vmax: float, precision: float, base: float = 0.0) -> np.ndarray:
    """Pack float values into RGB channels using fixed-point quantization.
    
    Values are scaled from [vmin, vmax] to [base, base + precision * 2^24).
    The quantized integer is split into 3 bytes (R, G, B).
    
    Returns array of shape (h, w, 3) with uint8 RGB values.
    """
    h, w = float_arr.shape
    
    # Clamp values to [vmin, vmax]
    clamped = np.clip(float_arr, vmin, vmax)
    
    # Scale to [0, 2^24-1] range
    # Map [vmin, vmax] -> [0, 2^24-1]
    max_uint24 = 2**24 - 1
    normalized = (clamped - vmin) / (vmax - vmin + 1e-10)
    q = np.rint(normalized * max_uint24).astype(np.int64)
    
    # Handle NaNs -> pack as 0
    q = np.where(np.isnan(float_arr), 0, q)
    q = np.clip(q, 0, max_uint24)
    
    # Split into RGB bytes (big-endian)
    r = ((q >> 16) & 0xFF).astype(np.uint8)
    g = ((q >> 8) & 0xFF).astype(np.uint8)
    b = (q & 0xFF).astype(np.uint8)
    
    rgb = np.zeros((h, w, 3), dtype=np.uint8)
    rgb[:, :, 0] = r
    rgb[:, :, 1] = g
    rgb[:, :, 2] = b
    
    return rgb


def write_webp(rgb_arr: np.ndarray, alpha_mask: np.ndarray, outpath: str) -> None:
    """Write RGB data with alpha mask as WebP (lossless)."""
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    
    h, w = rgb_arr.shape[:2]
    rgba = np.zeros((h, w, 4), dtype=np.uint8)
    rgba[:, :, :3] = rgb_arr
    rgba[:, :, 3] = alpha_mask
    
    img = Image.fromarray(rgba, mode='RGBA')
    img.save(outpath, 'WEBP', lossless=True)
    logger.info(f"Wrote {outpath}")




def image_live_ocean(outputs: List[dict], image_root: str = "/opt/data/images") -> List[str]:
    """
    Process extracted Live Ocean files serially, generating WebP tiles for all variables/depths/times.
    
    Args:
        outputs: List of dicts from write_daily_outputs, each with keys:
            - variable: variable name
            - date: YYYYMMDD
            - path: path to extracted NC file
        image_root: Root directory for image output
    
    Returns:
        List of written WebP paths
    """
    logger.info(f"Starting Live Ocean imaging (serial) for {len(outputs)} file(s)")
    
    # Load grid once
    lon_src, lat_src = load_lo_grid()
    minx, miny, maxx, maxy = compute_mercator_bounds(lon_src, lat_src)
    xx_merc, yy_merc, w, h = build_mercator_grid(minx, miny, maxx, maxy)
    
    all_paths = []
    
    for out in outputs:
        variable = out['variable']
        nc_path = out['path']
        
        if not os.path.exists(nc_path):
            logger.warning(f"NC file not found: {nc_path}")
            continue
        
        try:
            ds = xr.open_dataset(nc_path)
            
            if variable not in ds.data_vars:
                logger.warning(f"Variable {variable} not found in {nc_path}")
                ds.close()
                continue
            
            # Get field metadata
            vmin, vmax, precision = get_field_metadata(variable)
            
            # Get dimensions
            dims = ds[variable].dims
            time_dim = next((d for d in dims if 'time' in str(d).lower()), None)
            depth_dim = next((d for d in dims if 'depth' in str(d).lower()), None)
            
            if not time_dim or not depth_dim:
                logger.error(f"Could not find time/depth dims for {variable}")
                ds.close()
                continue
            
            times = ds[time_dim].values
            depths = ds[depth_dim].values
            
            # Process each time/depth slice
            for t_idx, t_val in enumerate(times):
                time_iso = str(np.datetime_as_string(t_val, unit='m')).replace(':', '')
                
                for d_idx, d_val in enumerate(depths):
                    # Determine depth filename
                    if np.isclose(d_val, 0.0):
                        depth_str = "surface"
                    elif np.isclose(d_val, 99999.0):
                        depth_str = "bottom"
                    else:
                        depth_str = f"{d_val:.0f}"
                    
                    # Load this slice
                    data_slice = ds[variable].isel({time_dim: t_idx, depth_dim: d_idx}).values
                    
                    # Regrid and write
                    regridded = regrid_to_mercator(data_slice, lon_src, lat_src, xx_merc, yy_merc)
                    alpha_mask = np.where(np.isfinite(regridded), 255, 0).astype(np.uint8)
                    rgb_arr = pack_to_rgb(regridded, vmin, vmax, precision)
                    
                    out_dir = os.path.join(image_root, variable_name, time_iso)
                    out_path = os.path.join(out_dir, f"{depth_str}.webp")
                    write_webp(rgb_arr, alpha_mask, out_path)
                    all_paths.append(out_path)
                    
                    # Cleanup
                    del data_slice, regridded, alpha_mask, rgb_arr
            
            ds.close()
            import gc
            gc.collect()
            
        except Exception as e:
            logger.error(f"Error processing {variable} from {nc_path}: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    logger.info(f"Completed serial imaging: generated {len(all_paths)} WebP tiles")
    return all_paths


def _worker_init(lon_src: np.ndarray, lat_src: np.ndarray, xx_merc: np.ndarray, yy_merc: np.ndarray):
    """Worker initializer: build interpolator once per worker process.
    
    Computes the expensive Delaunay triangulation a single time and caches it.
    All subsequent slice calls just invoke interp.apply() which is cheap.
    
    Args:
        lon_src, lat_src: Source grid coordinates
        xx_merc, yy_merc: Target Mercator grid coordinates
    """
    global _worker_mercator_cache
    
    # Build source points from valid grid cells
    coord_valid = np.isfinite(lon_src.ravel()) & np.isfinite(lat_src.ravel())
    pts_src = np.column_stack((lon_src.ravel(), lat_src.ravel()))[coord_valid]
    
    # Transform target Mercator to lon/lat
    transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    lons_tgt, lats_tgt = transformer.transform(xx_merc.ravel(), yy_merc.ravel())
    tgt_pts = np.column_stack((lons_tgt, lats_tgt))
    
    # Build interpolator once — this is the expensive Delaunay step
    interp = LinearInterpolator(pts_src, tgt_pts)
    
    _worker_mercator_cache['interp'] = interp
    _worker_mercator_cache['coord_valid'] = coord_valid
    _worker_mercator_cache['output_shape'] = xx_merc.shape
    logger.debug("Worker initialized: Delaunay triangulation built")


def _process_single_slice(args: Tuple) -> str:
    """Worker function for processing a single time/depth slice.
    
    Optimized for fine-grained parallelization: Mercator grids are loaded once
    per worker by initializer, not duplicated in every task tuple.
    
    Also updates database status as the file progresses.
    
    Args:
        args: (source_date, variable_id, start_time, end_time, nc_path, time_idx, depth_idx, image_root, 
               time_val, depth_val, vmin, vmax, precision)
    
    Returns:
        Path of written WebP file
    """
    global _worker_mercator_cache
    
    (source_date, variable_id, start_time, end_time, nc_path, time_idx, depth_idx, image_root, 
     time_val, depth_val, vmin, vmax, precision) = args
    
    # Get prebuilt interpolator from worker cache (built once by initializer)
    interp = _worker_mercator_cache['interp']
    coord_valid = _worker_mercator_cache['coord_valid']
    output_shape = _worker_mercator_cache['output_shape']
    
    try:
        # Update status to 'imaging' when first slice from this file starts
        # (Use conditional update to avoid race conditions: only update if currently extracted/pending_image)
        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE nc_jobs
                    SET status = 'imaging', updated_at = NOW()
                    WHERE misc->>'source_date' = %s AND variable_id = %s AND start_time = %s
                      AND dataset_id = 4
                      AND status IN ('extracted', 'pending_image')
                    """,
                    (source_date, variable_id, start_time)
                )
                conn.commit()
        finally:
            conn.close()
        
        # Load only this specific time/depth slice
        ds = xr.open_dataset(nc_path)
        
        # Get variable name from field_id (for accessing data)
        # We need to query the database to get the variable name from field_id
        var_conn = get_db_conn()
        try:
            with var_conn.cursor() as cur:
                cur.execute("SELECT variable FROM fields WHERE id = %s LIMIT 1", (variable_id,))
                result = cur.fetchone()
                if not result:
                    raise ValueError(f"Could not find variable name for field_id {variable_id}")
                variable_name = result[0]
        finally:
            var_conn.close()
        
        # Get dimension names
        dims = ds[variable_name].dims
        time_dim = next((d for d in dims if 'time' in str(d).lower()), None)
        depth_dim = next((d for d in dims if 'depth' in str(d).lower()), None)
        
        if not time_dim or not depth_dim:
            ds.close()
            return ""
        
        # Load only this slice
            data_slice = ds[variable_name].isel({time_dim: time_idx, depth_dim: depth_idx}).values
        ds.close()
        
        # Determine depth filename
        if np.isclose(depth_val, 0.0):
            depth_str = "surface"
        elif np.isclose(depth_val, 99999.0):
            depth_str = "bottom"
        else:
            depth_str = f"{depth_val:.0f}"
        
        # Regrid and write (fast: just apply prebuilt interpolator, no Delaunay recompute)
        data_flat = data_slice.ravel()[coord_valid]
        regridded = interp.apply(data_flat).reshape(output_shape)
        alpha_mask = np.where(np.isfinite(regridded), 255, 0).astype(np.uint8)
        rgb_arr = pack_to_rgb(regridded, vmin, vmax, precision)
        
        # Determine time string
        time_iso = str(np.datetime_as_string(time_val, unit='m')).replace(':', '')
        
        out_dir = os.path.join(image_root, variable, time_iso)
        out_path = os.path.join(out_dir, f"{depth_str}.webp")
        write_webp(rgb_arr, alpha_mask, out_path)
        
        # Cleanup
        del data_slice, regridded, alpha_mask, rgb_arr
        import gc
        gc.collect()
        
        return out_path
    
    except Exception as e:
        logger.error(f"Error processing {variable_name} slice (t={time_idx}, d={depth_idx}): {e}")
        # Mark file as imaging_failed
        try:
            conn = get_db_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE nc_jobs
                        SET status = 'imaging_failed', error_message = %s, updated_at = NOW()
                        WHERE misc->>'source_date' = %s AND variable_id = %s AND start_time = %s
                          AND dataset_id = 4
                        """,
                        (str(e), source_date, variable_id, start_time)
                    )
                    conn.commit()
            finally:
                conn.close()
        except Exception as db_err:
            logger.error(f"Failed to update error status in DB: {db_err}")
        return ""


def image_live_ocean_parallel(outputs: List[dict], workers: int = 4, image_root: str = "/opt/data/images", source_date: str = None) -> List[str]:
    """
    Process extracted Live Ocean files in parallel at the time/depth slice level.
    
    Instead of parallelizing by variable, we parallelize by individual depth slices.
    This allows multiple workers to process different depths from the same NC file,
    reducing memory duplication while improving load distribution.
    
    Memory optimization:
    - Grid caching to disk (lo_grid_cache.npz) - shared across workers
    - Each worker processes one time/depth slice, then releases memory
    - Explicit gc.collect() after each slice
    
    Database tracking:
    - Worker updates status to 'imaging' on first slice of a file
    - Main process updates to 'success_image' after all slices complete
    
    Args:
        outputs: List of dicts with keys: variable, variable_id, path, start_time, end_time
        workers: Number of parallel processes
        image_root: Root directory for image output
        source_date: Source date (YYYY-MM-DD) for database status tracking
    
    Returns:
        List of written WebP paths
    """
    logger.info(f"Starting Live Ocean imaging (parallel, {workers} processes) for {len(outputs)} file(s)")
    
    # Load grid once in main process (will use cache)
    lon_src, lat_src = load_lo_grid()
    minx, miny, maxx, maxy = compute_mercator_bounds(lon_src, lat_src)
    xx_merc, yy_merc, w, h = build_mercator_grid(minx, miny, maxx, maxy)
    
    # Build task queue: one task per (variable_id, time, depth) triple
    tasks = []
    for out in outputs:
        variable = out['variable']
        variable_id = out.get('variable_id')
        nc_path = out['path']
        start_time = out['start_time']  # ISO timestamp
        end_time = out['end_time']      # ISO timestamp
        
        # Open dataset to get time/depth info
        if not os.path.exists(nc_path):
            logger.warning(f"NC file not found: {nc_path}")
            continue
        
        try:
            ds = xr.open_dataset(nc_path)
            
            if variable not in ds.data_vars:
                logger.warning(f"Variable {variable} not found in {nc_path}")
                ds.close()
                continue
            
            # Get field metadata
            vmin, vmax, precision = get_field_metadata(variable)
            
            # Get dimensions
            dims = ds[variable].dims
            time_dim = next((d for d in dims if 'time' in str(d).lower()), None)
            depth_dim = next((d for d in dims if 'depth' in str(d).lower()), None)
            
            if not time_dim or not depth_dim:
                logger.error(f"Could not find time/depth dims for {variable}")
                ds.close()
                continue
            
            times = ds[time_dim].values
            depths = ds[depth_dim].values
            
            # Create task for each time/depth pair
            # Include source_date and start_time/end_time for database status updates in worker
            for t_idx, t_val in enumerate(times):
                for d_idx, d_val in enumerate(depths):
                    tasks.append((
                        source_date, variable_id, start_time, end_time, nc_path, t_idx, d_idx, image_root,
                        t_val, d_val, vmin, vmax, precision
                    ))
            
            ds.close()
        except Exception as e:
            logger.error(f"Error scanning {nc_path}: {e}")
            continue
    
    logger.info(f"Created {len(tasks)} tasks for {len(outputs)} file(s)")
    
    # Track task completion per file: {(source_date, variable_id, start_time, end_time): (completed_count, total_count)}
    # This allows us to update to 'success_image' as soon as each file's slices are all done
    file_task_counts = {}
    
    all_paths = []
    with ProcessPoolExecutor(
        max_workers=workers,
        initializer=_worker_init,
        initargs=(lon_src, lat_src, xx_merc, yy_merc)
    ) as executor:
        futures = {executor.submit(_process_single_slice, task): task for task in tasks}
        
        # Pre-populate task counts per file
        for task in tasks:
            source_date_t, variable_id, start_time, end_time, *_ = task
            key = (source_date_t, variable_id, start_time, end_time)
            if key not in file_task_counts:
                file_task_counts[key] = [0, 0]  # [completed, total]
            file_task_counts[key][1] += 1
        
        logger.info(f"Tracking {len(file_task_counts)} file(s) across {len(tasks)} total tasks")
        
        completed = 0
        for future in as_completed(futures):
            try:
                path = future.result()
                if path:
                    all_paths.append(path)
                
                # Extract file key from task
                task = futures[future]
                source_date_t, variable_id, start_time, end_time, *_ = task
                key = (source_date_t, variable_id, start_time, end_time)
                
                # Increment completion count for this file
                file_task_counts[key][0] += 1
                completed_for_file, total_for_file = file_task_counts[key]
                
                # If all slices for this file are done, update to 'success_image'
                if completed_for_file == total_for_file:
                    logger.info(f"All slices completed for variable_id={variable_id}/{start_time}. Updating to 'success_image'.")
                    try:
                        db_conn = get_db_conn()
                        try:
                            with db_conn.cursor() as cur:
                                cur.execute(
                                    """
                                    UPDATE nc_jobs
                                    SET status = 'success_image', updated_at = NOW()
                                    WHERE misc->>'source_date' = %s AND variable_id = %s AND start_time = %s
                                      AND dataset_id = 4
                                      AND status = 'imaging'
                                    """,
                                    (source_date_t, variable_id, start_time)
                                )
                                db_conn.commit()
                        finally:
                            db_conn.close()
                    except Exception as db_err:
                        logger.error(f"Failed to mark variable_id={variable_id}/{start_time} as success_image: {db_err}")
                
                completed += 1
                if completed % max(1, len(tasks) // 10) == 0:
                    logger.info(f"Progress: {completed}/{len(tasks)} slices processed")
            except Exception as e:
                logger.error(f"Task failed: {e}")
    
    
    logger.info(f"Completed parallel imaging: generated {len(all_paths)} WebP tiles")
    
    # Cleanup: force garbage collection after executor shutdown
    import gc
    del futures
    del tasks
    gc.collect()
    
    return all_paths
