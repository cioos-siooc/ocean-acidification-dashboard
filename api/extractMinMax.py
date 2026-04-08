#!/usr/bin/env python3
"""extractMinMax.py

Extract min and max values from a NetCDF variable at a specific datetime and depth.

This module finds the appropriate NC file based on the provided datetime, opens it,
and computes min/max statistics for the specified variable at the given depth level.

"""
from __future__ import annotations
import os
from glob import glob
from typing import Optional, Tuple
from datetime import datetime
import logging

import numpy as np
import xarray as xr
import pandas as pd
import psycopg2
from nc_reader import open_nc_uncached, close_nc, _nc_lock as _io_lock

logger = logging.getLogger(__name__)

TIME_CANDIDATES = ("time",)
DEPTH_CANDIDATES = ("depth", "lev", "level", "z", "deptht", "depthu", "depths")


def find_variable(ds: xr.Dataset, name: str) -> xr.DataArray:
    """Find variable by name (case-insensitive)."""
    if name in ds:
        return ds[name]
    low = name.lower()
    for k in ds.data_vars:
        if k.lower() == low:
            return ds[k]
    raise KeyError(f"Variable '{name}' not found in dataset")


def find_dimension(var: xr.DataArray, candidates: tuple) -> Optional[str]:
    """Find first matching dimension from candidates."""
    for dim in var.dims:
        if dim.lower() in candidates:
            return dim
    return None


def find_nc_file_for_date(data_dir, variable: str, dt: datetime) -> Optional[str]:
    """Find NC file for the given date. Accepts a single directory or list of directories."""
    from modules.nc_finder import find_nc_file
    return find_nc_file(data_dir, variable, dt)


def query_grid_points_in_bounds(conn, table: str, north: float, south: float, east: float, west: float) -> Tuple[np.ndarray, np.ndarray]:
    """Query the database for all grid points within lat/lon bounds.
    
    Returns:
        (row_indices, col_indices) arrays of all matching grid points
    """
    # Use PostGIS to find all points within the bounding box
    sql = f"""
        SELECT row_idx, col_idx FROM {table}
        WHERE lat >= %s AND lat <= %s AND lon >= %s AND lon <= %s
        ORDER BY row_idx, col_idx
    """
    with conn.cursor() as cur:
        cur.execute(sql, (south, north, west, east))
        rows = cur.fetchall()
    
    if not rows:
        return np.array([], dtype=int), np.array([], dtype=int)
    
    row_indices = np.array([r[0] for r in rows], dtype=int)
    col_indices = np.array([r[1] for r in rows], dtype=int)
    return row_indices, col_indices


def extract_minmax(
    data_dir: str,
    variable: str,
    dt: datetime,
    depth: Optional[float] = None,
    north: Optional[float] = None,
    south: Optional[float] = None,
    east: Optional[float] = None,
    west: Optional[float] = None,
    db_host: str = "db",
    db_port: int = 5432,
    db_user: str = "postgres",
    db_password: str = "postgres",
    db_name: str = "oa",
    db_table: str = "grid"
) -> Tuple[float, float]:
    """Extract min and max for a variable at a specific datetime and depth.
    
    Args:
        data_dir: Root directory containing variable subdirs (e.g., /opt/data/nc)
        variable: Variable name (e.g., 'temperature', 'ph_total')
        dt: Datetime to extract (used to find appropriate NC file)
        depth: Depth level (optional; if None, uses surface or first available)
        north: Northern latitude bound (optional)
        south: Southern latitude bound (optional)
        east: Eastern longitude bound (optional)
        west: Western longitude bound (optional)
    
    Returns:
        (min_value, max_value) tuple
    
    Raises:
        FileNotFoundError: If no NC file found for the date
        ValueError: If variable not found in dataset
    """
    # Find NC file for the date
    nc_file = find_nc_file_for_date(data_dir, variable, dt)
    if not nc_file:
        raise FileNotFoundError(f"No NC file found for {variable} on {dt.strftime('%Y-%m-%d')}")
    
    logger.debug(f"Loading NC file: {nc_file}")
    logger.debug(f"Bounds: north={north}, south={south}, east={east}, west={west}")
    logger.debug(f"Variable: {variable}, DateTime: {dt}, Depth: {depth}")
    
    # If bounds are provided, query the database for grid points
    row_indices = None
    col_indices = None
    if north is not None and south is not None and east is not None and west is not None:
        try:
            conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password)
            row_indices, col_indices = query_grid_points_in_bounds(conn, db_table, north, south, east, west)
            conn.close()
            logger.info(f"Found {len(row_indices)} grid points within bounds [S:{south:.4f}, N:{north:.4f}, W:{west:.4f}, E:{east:.4f}]")
        except Exception as e:
            logger.warning(f"Could not query database for bounds: {e}")
            row_indices = None
            col_indices = None
    
    # Open dataset (thread-safe)
    with _io_lock:
        ds = open_nc_uncached(nc_file)
    
    try:
        # Find the variable
        var = find_variable(ds, variable)
        
        # Find dimensions
        time_dim = find_dimension(var, TIME_CANDIDATES)
        depth_dim = find_dimension(var, DEPTH_CANDIDATES)
        
        # Select time slice if variable has time dimension
        if time_dim:
            # Find the closest time in the file to the requested datetime
            times = ds[time_dim].values
            
            # Convert xarray times to Python datetime objects using pandas
            try:
                times_pd = pd.to_datetime(times)
                times_dt = times_pd.to_pydatetime()
            except Exception:
                # Fallback: try to use the times directly if they're already datetime-like
                try:
                    times_dt = np.array([np.datetime64(t).astype('datetime64[ns]').astype('O') for t in times], dtype=object)
                except Exception:
                    # If all else fails, use the first time
                    time_idx = 0
                    times_dt = None
            
            if times_dt is not None:
                # Find closest time to the requested datetime
                diffs = np.array([(pd.Timestamp(t) - pd.Timestamp(dt)).total_seconds() for t in times_dt])
                time_idx = int(np.argmin(np.abs(diffs)))
            
            var = var.isel({time_dim: time_idx})
        
        # Select depth slice if variable has depth dimension
        if depth_dim and depth is not None:
            depths = ds[depth_dim].values
            depth_idx = int(np.argmin(np.abs(depths - depth)))
            var = var.isel({depth_dim: depth_idx})
        elif depth_dim:
            # Use first (shallowest) depth level
            var = var.isel({depth_dim: 0})
        
        # Now var should be purely spatial (2D grid with gridY and gridX dimensions)
        # If we have row/col indices from database bounds query, use them
        if row_indices is not None and col_indices is not None and len(row_indices) > 0:
            # Find gridY and gridX dimensions
            grid_y_dim = None
            grid_x_dim = None
            for dim in var.dims:
                if dim.lower() in ('gridy', 'y', 'eta'):
                    grid_y_dim = dim
                elif dim.lower() in ('gridx', 'x', 'xi'):
                    grid_x_dim = dim
            
            if grid_y_dim and grid_x_dim:
                # Keep unique indices and sort them for proper selection
                unique_rows = np.unique(row_indices)
                unique_cols = np.unique(col_indices)
                logger.info(f"Selecting region: {grid_y_dim}[{len(unique_rows)}] x {grid_x_dim}[{len(unique_cols)}]")
                # Select only the grid points that fall within the bounds
                var = var.isel({grid_y_dim: unique_rows, grid_x_dim: unique_cols})
                logger.info(f"Selected region: {var.shape} = {var.size:,} values (was {var.shape[0]*var.shape[1]:,})")
            else:
                logger.warning(f"Could not find gridY/gridX dimensions. Available: {var.dims}")
        else:
            logger.info("No bounds provided or no grid points found in bounds - using full global data")
        
        # Compute min/max across all valid (finite) values
        arr = var.values
        fin = np.isfinite(arr)
        
        if fin.sum() == 0:
            # All NaN data
            min_val = 0.0
            max_val = 1.0
            logger.warning(f"No valid data found in selected region")
        else:
            # For min: exclude zeros to get meaningful minimum
            nonzero_fin = fin & (arr != 0)
            if nonzero_fin.sum() > 0:
                min_val = float(np.min(arr[nonzero_fin]))
            else:
                # All finite values are zero
                min_val = 0.0
            
            max_val = float(np.max(arr[fin]))
        
        logger.info(f"Result: min={min_val:.4f}, max={max_val:.4f} (from {fin.sum()} valid values, {nonzero_fin.sum() if fin.sum() > 0 else 0} non-zero)")
        return min_val, max_val
    
    finally:
        close_nc(nc_file)


if __name__ == "__main__":
    from datetime import datetime
    
    # Example usage
    data_dir = "/opt/data/nc"
    min_val, max_val = extract_minmax(data_dir, "temperature", datetime(2026, 1, 17), depth=0.5)
    print(f"Temperature range: {min_val:.2f} to {max_val:.2f}")
