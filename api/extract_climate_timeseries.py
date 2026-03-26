#!/usr/bin/env python3
import xarray as xr
import pandas as pd
import numpy as np
import sys
import os
import time
import logging
from datetime import datetime, timedelta
import argparse
# Configure logging
def setup_logging(log_level=logging.INFO):
    """Configure logging with timestamps and formatting."""
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    logger.propagate = False  # Prevent propagation to parent loggers
    
    # Remove existing handlers to avoid duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Only add if we don't already have a handler
    if not logger.handlers:
        # Console handler with detailed format
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)-8s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    
    return logger

# Initialize logger
logger = setup_logging()

# Centralised, thread-safe NetCDF reader (single lock + LRU cache for the whole API)
from nc_reader import open_nc

# Try to import DB helpers from extractProfile (local to api/)
try:
    from extractProfile import connect_db, query_nearest_rowcol
except ImportError:
    # Fallback if run from project root
    sys.path.append(os.path.join(os.path.dirname(__file__), "."))
    from extractProfile import connect_db, query_nearest_rowcol

def get_dataset(file_path):
    """Open and return NetCDF dataset via the centralised nc_reader.
    
    Delegates to nc_reader.open_nc() which handles:
    - Thread-safe HDF5 access (single process-wide lock)
    - LRU caching with automatic eviction
    """
    if not os.path.exists(file_path):
        # Fallback for local testing if /opt/ is not mounted
        local_basename = os.path.basename(file_path)
        local_path = os.path.join(os.getcwd(), local_basename)
        if os.path.exists(local_path):
            logger.info(f"Using local file instead: {local_path}")
            file_path = local_path
        else:
            logger.error(f"File not found at {file_path} or local fallback")
            return None
    
    return open_nc(file_path)

def extract_climate_timeseries(lat, lon, variable, depth, dt, log_level=logging.INFO):
    """
    Extracts a 10-day climatology window (±5 days) around the given datetime.
    
    Args:
        lat: Latitude coordinate
        lon: Longitude coordinate
        variable: Variable name (e.g., 'temperature', 'salinity')
        depth: Depth value as string (e.g., '0p5', '1p0')
        dt: Datetime string in ISO format (e.g., '2026-01-17T05:30:00') or None for current UTC time
        log_level: Logging level (default: INFO)
    
    Returns:
        A list of dictionaries.
    """
    # Set logger level for this execution
    logger.setLevel(log_level)
    
    import time as time_module
    step_start = time_module.time()
    
    logger.info("=" * 70)
    logger.info("CLIMATE TIMESERIES EXTRACTION START")
    logger.info("=" * 70)
    logger.info(f"Input Parameters: lat={lat}, lon={lon}, variable={variable}, depth={depth}, dt={dt}")
    
    # Configuration
    if dt is None:
        target_dt_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        logger.debug(f"Using current UTC: {target_dt_str}")
    else:
        target_dt_str = dt
    file_path = f"/opt/data/SSC/climatology/5d/{variable}/{variable}_{depth}.nc"
    climatology_variables = ['mean', 'median', 'q1', 'q3', 'min', 'max']
    
    # Check if file exists first (fail fast before any heavy operations)
    if not os.path.exists(file_path):
        logger.error(f"Climatology file not found: {file_path}")
        return None
    logger.debug(f"✓ Climatology file exists: {file_path}")
    
    db_config = {
        "host": os.getenv("DB_HOST", "db"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASS", "postgres"),
        "dbname": os.getenv("DB_NAME", "oa"),
        "table": os.getenv("DB_TABLE", "grid")
    }

    # 1. Database Lookup for nearest grid indices
    logger.info("[1/5] Database lookup for nearest grid point")
    logger.debug(f"Database: {db_config['host']}:{db_config['port']}/{db_config['dbname']}")
    db_start = time_module.time()
    try:
        conn = connect_db(None, db_config["host"], db_config["port"], 
                          db_config["user"], db_config["password"], db_config["dbname"])
        # Set a statement timeout to avoid hanging the thread if the DB is slow
        try:
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = 5000") # 5 seconds
        except:
            pass
            
        # query_nearest_rowcol returns (row_idx, col_idx, lat, lon)
        yi, xi, lat_pt, lon_pt = query_nearest_rowcol(conn, db_config["table"], lat, lon)
        conn.close()
        db_elapsed = time_module.time() - db_start
        logger.info(f"✓ Found grid indices: row={yi}, col={xi}")
        logger.debug(f"Nearest point: lat={lat_pt:.4f}, lon={lon_pt:.4f}")
        logger.debug(f"DB lookup time: {db_elapsed:.3f}s")
    except Exception as exc:
        logger.error(f"Database lookup failed: {exc}", exc_info=True)
        return None

    # 2. Parse requested date
    logger.info("[2/5] Parsing datetime")
    parse_start = time_module.time()
    try:
        target_dt = pd.to_datetime(target_dt_str)
        parse_elapsed = time_module.time() - parse_start
        logger.info(f"✓ Parsed datetime: {target_dt}")
        logger.debug(f"Parse time: {parse_elapsed:.3f}s")
    except Exception as exc:
        logger.error(f"Invalid datetime string '{target_dt_str}': {exc}", exc_info=True)
        return None

    # 3. Open Dataset
    logger.info("[3/5] Opening NetCDF dataset")
    logger.debug(f"File: {file_path}")
    file_open_start = time_module.time()
    ds = get_dataset(file_path)
    if ds is None:
        logger.error(f"NetCDF file not found at {file_path}")
        return None
    file_open_elapsed = time_module.time() - file_open_start
    
    logger.info("✓ Dataset opened successfully")
    logger.debug(f"Dimensions: {dict(ds.dims)}")
    logger.debug(f"Data variables: {list(ds.data_vars)}")
    logger.debug(f"Coordinates: {list(ds.coords)}")
    logger.debug(f"File open time: {file_open_elapsed:.3f}s")

    # 4. Extract pixel timeseries for the entire year (optimized by chunking)
    logger.info("[4/5] Extracting pixel timeseries")
    extract_start = time_module.time()
    try:
        # Load one single pixel "tube" into memory (fast with 20x20 spatial chunking)
        # Each request has its own file handle, so no lock needed - concurrent requests run in parallel
        # Check variable availability
        found_vars = [v for v in climatology_variables if v in ds.data_vars]
        logger.debug(f"Available variables: {found_vars}")
        
        # Determine dimension names
        y_dim = 'gridY' if 'gridY' in ds.dims else 'y'
        x_dim = 'gridX' if 'gridX' in ds.dims else 'x'
        logger.debug(f"Using dimensions: {y_dim}, {x_dim}")
        
        logger.info(f"Selecting pixel at [{yi}, {xi}] for all {len(found_vars)} variables")
        ds_pixel = ds[found_vars].isel({y_dim: yi, x_dim: xi}).load()
        
        extract_elapsed = time_module.time() - extract_start
        t_dim = 'virtual_time' if 'virtual_time' in ds_pixel.dims else 'time'
        n_time = len(ds_pixel[t_dim])
        logger.info(f"✓ Pixel data loaded")
        logger.debug(f"Time dimension: {t_dim} ({n_time} timesteps)")
        logger.debug(f"Extraction time: {extract_elapsed:.3f}s")
    except Exception as exc:
        logger.error(f"Extraction from NetCDF failed: {exc}", exc_info=True)
        return None

    # 5. Build the 10-day hourly window (±5 days)
    logger.info("[5/5] Building 10-day hourly window (±5 days)")
    window_start = time_module.time()
    # We map each hour to the year 2020 which is the climatology year
    start_window = target_dt - timedelta(days=5)
    end_window = target_dt + timedelta(days=5)
    
    logger.debug(f"Request date: {target_dt}")
    logger.debug(f"Window: {start_window} to {end_window}")
    
    # Generate all hourly timestamps in the range (use lowercase 'h' for compatibility)
    hourly_range = pd.date_range(start=start_window, end=end_window, freq='h')
    logger.info(f"Generated {len(hourly_range)} hourly timestamps")
    
    results = []
    error_count = 0
    
    for i, dt in enumerate(hourly_range):
        if (i + 1) % 50 == 0 or i == 0:  # Log progress every 50 hours
            progress_pct = (i+1)/len(hourly_range)*100
            logger.debug(f"Processing timestep {i+1}/{len(hourly_range)} ({progress_pct:.1f}%)")
        
        # Preserve month/day/time but map to 2020
        # 2020 is a leap year, so it has Feb 29.
        try:
            mapped_dt = datetime(2020, dt.month, dt.day, 
                                 dt.hour, dt.minute, dt.second)
        except ValueError:
            # Safety for Feb 29 logic: if requested date is Feb 29 in a leap year
            # but mapping fails (though 2020 IS a leap year), use day-of-year.
            # This also handles mapping from non-leap Feb 28 -> Feb 29 if desired, 
            # but usually month/day mapping is preferred for climatology.
            doy = dt.dayofyear
            mapped_dt = datetime(2020, 1, 1) + timedelta(days=doy-1, 
                                                        hours=dt.hour, 
                                                        minutes=dt.minute, 
                                                        seconds=dt.second)

        # 6. Select data for the mapped time
        try:
            point_data = ds_pixel.sel({t_dim: mapped_dt}, method='nearest')
            
            row = {
                'requested_date': dt.strftime("%Y-%m-%dT%H:%M:%S"),
                # 'climatology_date': str(point_data[t_dim].values)[:19]
            }
            for v in found_vars:
                row[v] = float(point_data[v].values)
            
            results.append(row)
        except Exception as e:
            error_count += 1
            if error_count <= 3:  # Log first 3 errors
                logger.warning(f"Error at {dt}: {e}")
    
    window_elapsed = time_module.time() - window_start
    logger.info(f"✓ Extracted {len(results)} timesteps ({error_count} errors)")
    logger.debug(f"Window processing time: {window_elapsed:.3f}s")
    
    total_elapsed = time_module.time() - step_start
    logger.info("=" * 70)
    logger.info(f"Extraction Summary: {len(results)} rows, {len(found_vars)} variables, {total_elapsed:.3f}s total")        
    logger.info("=" * 70)

    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract 10-day climatology window.")
    parser.add_argument("--lat", type=float, required=True)
    parser.add_argument("--lon", type=float, required=True)
    parser.add_argument("--variable", type=str, required=True, help="Variable name (e.g., 'temperature', 'salinity')")
    parser.add_argument("--depth", type=str, required=True, help="Depth value (e.g., '0p5', '1p0')")
    parser.add_argument("--date", type=str, help="ISO format datetime (e.g., 2026-01-17T05:30:00). If not provided, uses current UTC time.")
    parser.add_argument("--log-level", type=str, default="INFO", 
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Logging level (default: INFO)")
    
    args = parser.parse_args()
    
    # Set logger to requested level
    log_level = getattr(logging, args.log_level)
    logger.setLevel(log_level)
    
    logger.info(f"Starting Climate Timeseries Extraction Script")
    logger.debug(f"Arguments: lat={args.lat}, lon={args.lon}, variable={args.variable}, depth={args.depth}, date={args.date}")
    
    start_time = time.perf_counter()
    result = extract_climate_timeseries(args.lat, args.lon, args.variable, args.depth, args.date, log_level=log_level)
    elapsed = time.perf_counter() - start_time
    
    logger.info("=" * 70)
    if result:
        logger.info(f"Status: SUCCESS - {len(result)} records extracted")
        logger.debug(f"First record: {result[0]}")
        if len(result) > 1:
            logger.debug(f"Last record:  {result[-1]}")
    else:
        logger.error(f"Status: FAILED (returned None)")
    logger.info(f"Script execution time: {elapsed:.4f} seconds ({elapsed/60:.2f} minutes)")
    logger.info("=" * 70)
    
    # Also print the result for compatibility
    print(result)
