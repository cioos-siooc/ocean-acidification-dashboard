#!/usr/bin/env python3
"""On-demand PNG generation utilities for the API.

This module handles finding NC files, determining depth indices, and orchestrating
PNG generation when requested through the /png endpoint.
"""

import os
import logging
from glob import glob
from datetime import datetime
from typing import Tuple

import numpy as np
import xarray as xr
import pandas as pd
import psycopg2
import psycopg2.extras

from shared.nc2tile import (
    get_grid_from_db,
    compute_mercator_grid_bounds,
    build_target_grid,
    _process_task,
)

logger = logging.getLogger(__name__)


def get_time_index_from_nc(nc_path: str, dt_requested: str) -> Tuple[int, str]:
    """Find the closest time index in NC file for the requested datetime.
    
    Args:
        nc_path: Path to the NetCDF file
        dt_requested: ISO datetime string (e.g., '2026-03-24T23:30:00')
    
    Returns:
        Tuple of (time_index, time_string_for_folder)
        where time_string_for_folder is like '2026-03-24T233000' (no colons)
    
    Raises:
        ValueError: If datetime format is invalid or time dimension not found
    """
    try:
        # Parse requested datetime
        dt_req = datetime.fromisoformat(dt_requested.replace('Z', '+00:00'))
        
        with xr.open_dataset(nc_path) as ds:
            # Find time dimension
            time_dim = None
            for dim in ds.dims:
                if 'time' in dim.lower():
                    time_dim = dim
                    break
            
            if time_dim is None:
                raise ValueError(f"No time dimension found in {nc_path}")
            
            # Get time values and convert to datetime
            times = ds[time_dim].values
            times_dt = pd.to_datetime(times).to_pydatetime()
            
            # Find closest time index
            time_diffs = np.array([abs((t - dt_req).total_seconds()) for t in times_dt])
            closest_idx = int(np.argmin(time_diffs))
            closest_time = times_dt[closest_idx]
            
            # Format time string like process_variable does (YYYY-MM-DDTHHMMSS no colons)
            time_str = closest_time.strftime("%Y-%m-%dT%H%M%S")
            
            logger.info(f"Requested {dt_requested}, found closest time at index {closest_idx}: {closest_time}")
            return closest_idx, time_str
    
    except Exception as e:
        logger.error(f"Error finding time index in {nc_path}: {e}")
        raise


def get_depth_index_from_nc(nc_path: str, depth_value: float) -> int:
    """Find the closest depth index in NC file for the given depth value.
    
    Args:
        nc_path: Path to the NetCDF file
        depth_value: Desired depth value (meters)
    
    Returns:
        Index of the nearest depth coordinate in the NC file
    
    Raises:
        Exception: If NC file cannot be read or no depth coordinate found
    """
    try:
        with xr.open_dataset(nc_path) as ds:
            # Find depth dimension
            depth_coord = None
            for coord_name in ('depth', 'z', 'lev', 'level', 'deptht', 'depthu', 'altitude'):
                if coord_name in ds.coords:
                    depth_coord = ds.coords[coord_name].values
                    break
            
            if depth_coord is None:
                # No depth coordinate found, assume 2D variable
                logger.warning(f"No depth coordinate found in {nc_path}; using index 0")
                return 0
            
            # Find nearest depth index
            idx = int(np.argmin(np.abs(depth_coord - float(depth_value))))
            return idx
    except Exception as e:
        logger.error(f"Error finding depth index in {nc_path}: {e}")
        raise


def get_variable_precision(var_name: str) -> float:
    """Get precision value from fields table for a variable.
    
    Args:
        var_name: Variable name to look up
    
    Returns:
        Precision value from database, or 0.1 as default
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv('PGHOST', 'db'),
            port=int(os.getenv('PGPORT', 5432)),
            database=os.getenv('PGDATABASE', 'oa'),
            user=os.getenv('PGUSER', 'postgres'),
            password=os.getenv('PGPASSWORD', 'postgres')
        )
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("SELECT precision FROM fields WHERE variable=%s LIMIT 1", (var_name,))
            row = cur.fetchone()
            if row:
                precision = float(row['precision']) if row['precision'] else 0.1
                logger.debug(f"Found precision={precision} for {var_name}")
                return precision
            logger.debug(f"No precision found for {var_name}; using default 0.1")
            return 0.1  # default precision
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"Error getting precision for {var_name}: {e}")
        return 0.1


def find_nc_file_for_date(data_dir, variable: str, dt_str: str) -> str:
    """Find NC file for the given date in ISO format. Accepts a single directory or list of directories."""
    from modules.nc_finder import find_nc_file
    result = find_nc_file(data_dir, variable, dt_str)
    if result is None:
        raise FileNotFoundError(f"No NC file found for {variable} on {dt_str}")
    return result


async def generate_png_for_variable(
    var: str, dt: str, depth_value: float,
    data_dir: str, image_root: str,
    png_gen_semaphore,
    executor=None,
) -> str:
    """Generate PNG on-demand for a specific variable/datetime/depth.

    The heavy CPU work (_generate_single_png_task) runs in `executor` when
    provided (a ProcessPoolExecutor), so it never blocks the shared anyio
    threadpool that all other API endpoints depend on.
    """
    from starlette.concurrency import run_in_threadpool

    safe_var = os.path.basename(var)
    safe_depth = str(depth_value).replace('.', 'p')

    try:
        dt_obj = datetime.fromisoformat(dt.replace('Z', '+00:00'))
        dt_folder = dt_obj.strftime("%Y-%m-%dT%H%M%S")
    except Exception as e:
        raise ValueError(f"Invalid datetime format: {dt}") from e

    full_path = os.path.join(image_root, safe_var, dt_folder, f"{safe_depth}.webp")

    if os.path.isfile(full_path):
        logger.info(f"PNG already exists: {full_path}")
        return full_path

    import asyncio
    loop = asyncio.get_event_loop()

    try:
        async with png_gen_semaphore:
            logger.info(f"Starting on-demand PNG generation for {safe_var}/{dt_folder}/{safe_depth}")

            # Fast prep work: stays in shared threadpool (lightweight I/O + DB)
            nc_path = await asyncio.wait_for(
                run_in_threadpool(find_nc_file_for_date, data_dir, safe_var, dt),
                timeout=10.0
            )
            logger.info(f"NC file: {nc_path}")

            time_idx, time_str = await asyncio.wait_for(
                run_in_threadpool(get_time_index_from_nc, nc_path, dt),
                timeout=10.0
            )
            depth_idx = await asyncio.wait_for(
                run_in_threadpool(get_depth_index_from_nc, nc_path, depth_value),
                timeout=10.0
            )
            precision = await asyncio.wait_for(
                run_in_threadpool(get_variable_precision, safe_var),
                timeout=10.0
            )
            logger.info(f"time_idx={time_idx}, depth_idx={depth_idx}, precision={precision}")

            # Heavy CPU work: runs in dedicated ProcessPoolExecutor (isolated from threadpool)
            logger.info("Dispatching PNG generation to dedicated process executor...")
            try:
                result = await asyncio.wait_for(
                    loop.run_in_executor(
                        executor,
                        _generate_single_png_task,
                        nc_path, safe_var, time_idx, time_str,
                        depth_idx, depth_value, precision,
                    ),
                    timeout=120.0
                )
                logger.info(f"PNG generation complete: {result}")
            except asyncio.TimeoutError:
                logger.error(f"PNG generation timed out for {safe_var}/{dt}/{depth_value}")
                raise RuntimeError("PNG generation timed out")

    except Exception as e:
        logger.error(f"PNG generation failed for {safe_var}/{dt}/{depth_value}: {e}", exc_info=True)
        raise

    if not os.path.isfile(full_path):
        raise RuntimeError(f"PNG generation finished but file not found: {full_path}")

    logger.info(f"Successfully generated: {full_path}")
    return full_path


def _generate_single_png_task(
    nc_path: str,
    varname: str, 
    time_idx: int,
    time_str: str,
    depth_idx: int,
    depth_val: float,
    pack_precision: float
) -> Tuple[str, str]:
    """Generate a single PNG for one time × depth combination.
    
    This is a thin wrapper around _process_task that builds the task tuple.
    """
    import os
    
    # Load grid
    lon_src, lat_src = get_grid_from_db('grid')
    
    # Compute mercator bounds
    minx, miny, maxx, maxy = compute_mercator_grid_bounds(lon_src, lat_src)
    xx_merc, yy_merc, w, h = build_target_grid(minx, miny, maxx, maxy, max_dim=2048)
    
    # Get min/max bounds from database (pre-computed, no expensive scan needed)
    vmin = None
    vmax = None
    erddap_min = None
    erddap_max = None
    try:
        conn = psycopg2.connect(
            host=os.getenv('PGHOST', 'db'),
            port=int(os.getenv('PGPORT', 5432)),
            database=os.getenv('PGDATABASE', 'oa'),
            user=os.getenv('PGUSER', 'postgres'),
            password=os.getenv('PGPASSWORD', 'postgres')
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT min, max FROM fields WHERE variable=%s LIMIT 1",
                    (varname,),
                )
                row = cur.fetchone()
                if row:
                    erddap_min, erddap_max = row[0], row[1]
                    # Use database bounds for vmin/vmax (no file scanning needed)
                    vmin, vmax = erddap_min, erddap_max
                    logger.debug(f"Using database bounds for {varname}: vmin={vmin}, vmax={vmax}")
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"Could not fetch bounds from database: {e}")
    
    # Build task tuple (matching _process_task signature)
    task = (
        nc_path,                    # ds_data_path
        varname,                    # varname
        int(time_idx),             # t_idx
        str(time_str),             # t_str
        depth_idx,                 # d_idx
        float(depth_val),          # d_val
        vmin,                      # vmin
        vmax,                      # vmax
        erddap_min,                # erddap_min
        erddap_max,                # erddap_max
        float(minx),               # minx
        float(miny),               # miny
        float(maxx),               # maxx
        float(maxy),               # maxy
        int(w),                    # w
        int(h),                    # h
        "linear",                  # method
        None,                      # clip_percentile
        True,                      # verbose
        float(pack_precision),     # pack_precision
    )
    
    # Call the low-level worker function
    return _process_task(task)
