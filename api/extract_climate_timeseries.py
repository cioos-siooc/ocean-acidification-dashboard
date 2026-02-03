#!/usr/bin/env python3
import xarray as xr
import pandas as pd
import numpy as np
import sys
import os
import time
from datetime import datetime, timedelta
import argparse
import threading
from lock_manager import io_lock as _io_lock

# Try to import DB helpers from extractProfile (local to api/)
try:
    from extractProfile import connect_db, query_nearest_rowcol
except ImportError:
    # Fallback if run from project root
    sys.path.append(os.path.join(os.path.dirname(__file__), "."))
    from extractProfile import connect_db, query_nearest_rowcol

# Global cache for the dataset
_ds_cache = None
_ds_lock = threading.Lock()

def get_dataset(file_path):
    global _ds_cache
    with _ds_lock:
        if _ds_cache is None:
            if not os.path.exists(file_path):
                # Fallback for local testing if /opt/ is not mounted
                local_path = os.path.join(os.getcwd(), "depth_0p5.nc")
                if os.path.exists(local_path):
                    file_path = local_path
                else:
                    return None
            with _io_lock:
                _ds_cache = xr.open_dataset(file_path)
    return _ds_cache

def extract_climate_timeseries(lat, lon, target_dt_str):
    """
    Extracts a 10-day climatology window around target_dt_str.
    Returns a list of dictionaries.
    """
    # Configuration
    file_path = "/opt/data/nc/SalishSeaCast/climate/temperature/depth_0p5.nc"
    variables = ['mean', 'median', 'q1', 'q3', 'min', 'max']
    
    db_config = {
        "host": os.getenv("DB_HOST", "db"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASS", "postgres"),
        "dbname": os.getenv("DB_NAME", "oa"),
        "table": os.getenv("DB_TABLE", "grid")
    }

    # 1. Database Lookup for nearest grid indices
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
    except Exception as exc:
        print(f"Error: Database lookup failed. {exc}", file=sys.stderr)
        return None

    # 2. Parse requested date
    try:
        target_dt = pd.to_datetime(target_dt_str)
    except Exception as exc:
        print(f"Error: Invalid datetime string '{target_dt_str}'. {exc}", file=sys.stderr)
        return None

    # 3. Open Dataset
    ds = get_dataset(file_path)
    if ds is None:
        print(f"Error: NetCDF file not found at {file_path}", file=sys.stderr)
        return None

    # 4. Extract pixel timeseries for the entire year (optimized by chunking)
    try:
        # Load one single pixel "tube" into memory (fast with 20x20 spatial chunking)
        # Use a lock because NetCDF4 backend is NOT thread-safe for simultaneous access to the same dataset object
        with _io_lock:
            # Check variable availability
            found_vars = [v for v in variables if v in ds.data_vars]
            
            # Determine dimension names
            y_dim = 'gridY' if 'gridY' in ds.dims else 'y'
            x_dim = 'gridX' if 'gridX' in ds.dims else 'x'
            
            ds_pixel = ds[found_vars].isel({y_dim: yi, x_dim: xi}).load()
    except Exception as exc:
        print(f"Error: Extraction from NetCDF failed. {exc}", file=sys.stderr)
        return None

    # 5. Build the 10-day hourly window (±5 days)
    # We map each hour to the year 2020 which is the climatology year
    start_window = target_dt - timedelta(days=5)
    end_window = target_dt + timedelta(days=5)
    
    # Generate all hourly timestamps in the range
    hourly_range = pd.date_range(start=start_window, end=end_window, freq='H')
    
    t_dim = 'virtual_time' if 'virtual_time' in ds_pixel.dims else 'time'
    results = []
    
    for dt in hourly_range:
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
        point_data = ds_pixel.sel({t_dim: mapped_dt}, method='nearest')
        
        row = {
            'requested_date': dt.strftime("%Y-%m-%dT%H:%M:%S"),
            # 'climatology_date': str(point_data[t_dim].values)[:19]
        }
        for v in found_vars:
            row[v] = float(point_data[v].values)
        
        results.append(row)

    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract 10-day climatology window.")
    parser.add_argument("--lat", type=float, required=True)
    parser.add_argument("--lon", type=float, required=True)
    parser.add_argument("--date", type=str, required=True, help="ISO format e.g. 2026-01-17T05:30:00")
    
    args = parser.parse_args()
    
    start_time = time.perf_counter()
    df = extract_climate_timeseries(args.lat, args.lon, args.date)
    elapsed = time.perf_counter() - start_time
    
    print(df)
    print(f"\nExtraction completed in {elapsed:.4f} seconds.")
