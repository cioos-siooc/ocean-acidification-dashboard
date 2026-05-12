#!/usr/bin/env python3
"""One-time script to initialize Live Ocean grid table from layer.nc.

Loads lat_rho and lon_rho from a layers.nc file and populates the lo_grid table.
Run this once before the first Live Ocean processing.

Usage:
    python lo_grid_init.py --input layers.nc

This script will:
1. Extract lat_rho and lon_rho from the NetCDF file
2. Connect to the database
3. DROP and recreate the lo_grid table
4. Insert grid points with row_idx and col_idx
"""

from __future__ import annotations

import argparse
import logging
import os
from typing import Tuple

import numpy as np
import xarray as xr
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("lo_grid_init")


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


def extract_grid_from_nc(nc_path: str) -> Tuple[np.ndarray, np.ndarray]:
    """Extract lat_rho and lon_rho from a Live Ocean layer.nc file.
    
    Returns (lon_2d, lat_2d) both shaped (eta_rho, xi_rho).
    """
    logger.info(f"Opening NetCDF file: {nc_path}")
    ds = xr.open_dataset(nc_path)
    
    try:
        # Live Ocean grids use lat_rho and lon_rho
        if "lat_rho" not in ds.coords and "lat_rho" not in ds.data_vars:
            raise ValueError("lat_rho not found in NetCDF file")
        if "lon_rho" not in ds.coords and "lon_rho" not in ds.data_vars:
            raise ValueError("lon_rho not found in NetCDF file")
        
        lat_rho = ds["lat_rho"].values  # Should be 2D
        lon_rho = ds["lon_rho"].values  # Should be 2D
        
        if lat_rho.ndim != 2 or lon_rho.ndim != 2:
            raise ValueError(f"Expected 2D arrays, got lat_rho shape {lat_rho.shape} and lon_rho shape {lon_rho.shape}")
        
        logger.info(f"Grid shape: {lat_rho.shape}")
        return lon_rho, lat_rho
    
    finally:
        ds.close()


def init_lo_grid_table(conn, lon_2d: np.ndarray, lat_2d: np.ndarray) -> None:
    """Initialize or replace the lo_grid table with grid points from 2D lat/lon arrays.
    
    Expects lon_2d and lat_2d both shaped (nrows, ncols).
    """
    nrows, ncols = lat_2d.shape
    logger.info(f"Grid dimensions: {nrows} rows x {ncols} cols = {nrows * ncols} points")
    
    # Warn about replacement
    logger.warning("=" * 60)
    logger.warning("DROPPING and recreating lo_grid table!")
    logger.warning("=" * 60)
    
    try:
        with conn.cursor() as cur:
            # Drop existing table
            cur.execute("DROP TABLE IF EXISTS lo_grid CASCADE")
            logger.info("Dropped existing lo_grid table")
            
            # Create new table
            cur.execute(
                """
                CREATE TABLE lo_grid (
                    id SERIAL PRIMARY KEY,
                    row_idx INTEGER NOT NULL,
                    col_idx INTEGER NOT NULL,
                    lat FLOAT NOT NULL,
                    lon FLOAT NOT NULL,
                    UNIQUE(row_idx, col_idx)
                )
                """
            )
            logger.info("Created new lo_grid table")
            
            # Prepare records
            records = []
            for i in range(nrows):
                for j in range(ncols):
                    lat = float(lat_2d[i, j])
                    lon = float(lon_2d[i, j])
                    records.append((i, j, lat, lon))
            
            # Bulk insert
            cur.executemany(
                "INSERT INTO lo_grid (row_idx, col_idx, lat, lon) VALUES (%s, %s, %s, %s)",
                records,
            )
            logger.info(f"Inserted {len(records)} grid points")
            
            # Create indexes
            cur.execute("CREATE INDEX idx_lo_grid_row_col ON lo_grid(row_idx, col_idx)")
            logger.info("Created indexes")
        
        conn.commit()
        logger.info("lo_grid table initialization complete")
    
    except Exception as e:
        logger.error(f"Failed to initialize lo_grid table: {e}")
        conn.rollback()
        raise


def main(argv=None):
    p = argparse.ArgumentParser(description="Initialize Live Ocean grid table from layer.nc")
    p.add_argument("--input", required=True, help="Path to layer.nc file")
    args = p.parse_args(argv)
    
    # Extract grid
    lon_2d, lat_2d = extract_grid_from_nc(args.input)
    
    # Connect and initialize
    conn = get_db_conn()
    try:
        init_lo_grid_table(conn, lon_2d, lat_2d)
    finally:
        conn.close()
    
    logger.info("Success!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
