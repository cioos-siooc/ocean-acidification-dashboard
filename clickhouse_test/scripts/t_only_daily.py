import argparse
import os
import time
import math
from concurrent.futures import ProcessPoolExecutor
import numpy as np
import pandas as pd
import xarray as xr
import clickhouse_connect

# ==============================================================================
# CONFIGURATION
# ==============================================================================
CH_HOST = "localhost"
CH_PORT = 8123
CH_USER = "default"
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "")
CLIENT_CONFIG = {
    'host': CH_HOST,
    'port': CH_PORT,
    'username': CH_USER,
    'password': CH_PASS,
}

VAR_MAP = {
    'time_var': 'time',
    'depth_var': 'depth',
    'lat_var': 'gridY',
    'lon_var': 'gridX'
}

BATCH_SIZE_THRESHOLD = 500000
CORES_TO_USE = 10  # Note: See RAM Warning below regarding core count

# ==============================================================================
# THE MULTIPROCESSING WORKER
# ==============================================================================
def process_time_chunk(args):
    """Runs independently on a CPU core."""
    file_path, dates_chunk, worker_id = args
    
    worker_client = clickhouse_connect.get_client(**CLIENT_CONFIG)
    ds = xr.open_dataset(file_path)
    
    depths = ds[VAR_MAP['depth_var']].values
    lats = ds[VAR_MAP['lat_var']].values
    lons = ds[VAR_MAP['lon_var']].values
    
    d_grid, lat_grid, lon_grid = np.meshgrid(depths, lats, lons, indexing='ij')
    flat_depth = d_grid.flatten().astype(np.float32)
    flat_lat = lat_grid.flatten().astype(np.uint16)
    flat_lon = lon_grid.flatten().astype(np.uint16)
    
    batch_dfs = []
    batch_rows = 0
    total_inserted = 0
    
    print(f"[Worker {worker_id}] Started: {os.path.basename(file_path)} ({len(dates_chunk)} days assigned)")
    
    for local_d_idx, target_date in enumerate(dates_chunk):
        date_str = str(target_date) # Converts to 'YYYY-MM-DD'
        
        # 1. Slice all 24 hours for this specific day natively in xarray
        day_data = ds['temperature'].sel({VAR_MAP['time_var']: date_str})
        
        # 2. Calculate the daily average across the time dimension
        # (skipna=True ensures missing hourly data doesn't ruin the daily mean)
        temp_daily_mean = day_data.mean(dim=VAR_MAP['time_var'], skipna=True).values.flatten()
        
        valid_mask = ~np.isnan(temp_daily_mean)
        if not np.any(valid_mask):
            continue  
            
        df_batch = pd.DataFrame({
            'time': target_date, # Passes the pure Date object
            'depth': flat_depth[valid_mask],
            'gridY': flat_lat[valid_mask],
            'gridX': flat_lon[valid_mask],
            'temperature': temp_daily_mean[valid_mask]
        })
        
        batch_dfs.append(df_batch)
        batch_rows += len(df_batch)
        
        if batch_rows >= BATCH_SIZE_THRESHOLD:
            df_to_insert = pd.concat(batch_dfs, ignore_index=True)
            worker_client.insert('ocean_daily_temperature', df_to_insert, column_names=list(df_to_insert.columns))
            total_inserted += batch_rows
            batch_dfs = []
            batch_rows = 0

        if local_d_idx > 0 and local_d_idx % 10 == 0:
            print(f"[Worker {worker_id}] Progress: {local_d_idx} / {len(dates_chunk)} days completed.", flush=True)

    if batch_dfs:
        df_to_insert = pd.concat(batch_dfs, ignore_index=True)
        worker_client.insert('ocean_daily_temperature', df_to_insert, column_names=list(df_to_insert.columns))
        total_inserted += batch_rows

    ds.close()
    return f"[Worker {worker_id}] Finished {os.path.basename(file_path)}. Inserted {total_inserted} daily rows."

# ==============================================================================
# MAIN EXECUTION (Traffic Controller)
# ==============================================================================
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load massive temperature NetCDF files into ClickHouse (Daily Means)')
    parser.add_argument('files', nargs='+', help='Temperature NetCDF file paths')
    args = parser.parse_args()
    
    total_start_time = time.time()
    
    print("Connecting to ClickHouse (Main Thread)...")
    main_client = clickhouse_connect.get_client(**CLIENT_CONFIG)
    
    print("Creating storage-optimized DAILY temperature table if needed...")
    main_client.command("""
    CREATE TABLE IF NOT EXISTS ocean_daily_temperature (
        time Date CODEC(DoubleDelta, ZSTD(4)),   -- Swapped to Date type!
        depth Float32 CODEC(Gorilla, ZSTD(4)),
        gridX UInt16 CODEC(DoubleDelta, ZSTD(4)),
        gridY UInt16 CODEC(DoubleDelta, ZSTD(4)),
        temperature Float32 CODEC(Gorilla, ZSTD(4))
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(time)
    ORDER BY (gridX, gridY, time, depth)
    """)
    
    for file_path in args.files:
        if not os.path.exists(file_path):
            print(f"Error: {file_path} not found. Skipping.")
            continue
            
        print(f"\n--- Analyzing {os.path.basename(file_path)} ---")
        
        with xr.open_dataset(file_path) as ds_main:
            # Extract unique calendar days from the file
            times = pd.to_datetime(ds_main[VAR_MAP['time_var']].values)
            unique_dates = np.unique(times.date)
            
        days_per_worker = math.ceil(len(unique_dates) / CORES_TO_USE)
        
        worker_tasks = []
        for i in range(CORES_TO_USE):
            start_idx = i * days_per_worker
            end_idx = min((i + 1) * days_per_worker, len(unique_dates))
            chunk_dates = unique_dates[start_idx:end_idx]
            
            if len(chunk_dates) > 0:
                worker_tasks.append((file_path, chunk_dates, i + 1))
                
        print(f"Divided {len(unique_dates)} unique days across {len(worker_tasks)} cores.")
        
        with ProcessPoolExecutor(max_workers=CORES_TO_USE) as executor:
            results = list(executor.map(process_time_chunk, worker_tasks))
            
        for res in results:
            print(res)

    print(f"\nTotal Pipeline complete in {time.time() - total_start_time:.2f} seconds!")