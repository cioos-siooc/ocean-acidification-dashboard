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
CORES_TO_USE = 10  # Adjust based on your machine

# ==============================================================================
# THE MULTIPROCESSING WORKER
# ==============================================================================
def process_time_chunk(args):
    """Runs independently on a CPU core."""
    file_path, start_idx, end_idx, worker_id = args
    
    worker_client = clickhouse_connect.get_client(**CLIENT_CONFIG)
    ds = xr.open_dataset(file_path)
    
    depths = ds[VAR_MAP['depth_var']].values
    lats = ds[VAR_MAP['lat_var']].values
    lons = ds[VAR_MAP['lon_var']].values
    
    d_grid, lat_grid, lon_grid = np.meshgrid(depths, lats, lons, indexing='ij')
    flat_depth = d_grid.flatten().astype(np.float32)
    flat_lat = lat_grid.flatten().astype(np.uint16)
    flat_lon = lon_grid.flatten().astype(np.uint16)
    
    worker_times = ds[VAR_MAP['time_var']].values[start_idx:end_idx]
    
    batch_dfs = []
    batch_rows = 0
    total_inserted = 0
    
    print(f"[Worker {worker_id}] Started: {os.path.basename(file_path)} (Steps {start_idx} to {end_idx - 1})")
    
    for local_t_idx, t_val in enumerate(worker_times):
            global_t_idx = start_idx + local_t_idx
            temp_block = ds['temperature'].isel({VAR_MAP['time_var']: global_t_idx}).values.flatten()
            
            valid_mask = ~np.isnan(temp_block)
            if not np.any(valid_mask):
                continue  
                
            df_batch = pd.DataFrame({
                'time': pd.Timestamp(t_val),
                'depth': flat_depth[valid_mask],
                'gridY': flat_lat[valid_mask],
                'gridX': flat_lon[valid_mask],
                'temperature': temp_block[valid_mask]
            })
            
            batch_dfs.append(df_batch)
            batch_rows += len(df_batch)
            
            if batch_rows >= BATCH_SIZE_THRESHOLD:
                df_to_insert = pd.concat(batch_dfs, ignore_index=True)
                worker_client.insert('ocean_4d_temperature', df_to_insert, column_names=list(df_to_insert.columns))
                total_inserted += batch_rows
                batch_dfs = []
                batch_rows = 0

            # 🔥 NEW: Periodic logging inside the worker loop
            if local_t_idx > 0 and local_t_idx % 200 == 0:
                print(f"[Worker {worker_id}] Progress: {local_t_idx} / {len(worker_times)} steps completed.", flush=True)

    if batch_dfs:
        df_to_insert = pd.concat(batch_dfs, ignore_index=True)
        worker_client.insert('ocean_4d_temperature', df_to_insert, column_names=list(df_to_insert.columns))
        total_inserted += batch_rows

    ds.close()
    return f"[Worker {worker_id}] Finished {os.path.basename(file_path)}. Inserted {total_inserted} rows."

# ==============================================================================
# MAIN EXECUTION (Traffic Controller)
# ==============================================================================
if __name__ == '__main__':
    # 1. Parse Arguments safely in the main thread
    parser = argparse.ArgumentParser(description='Load massive temperature NetCDF files into ClickHouse')
    parser.add_argument('files', nargs='+', help='Temperature NetCDF file paths')
    args = parser.parse_args()
    
    total_start_time = time.time()
    
    # 2. Establish connection and create table once
    print("Connecting to ClickHouse (Main Thread)...")
    main_client = clickhouse_connect.get_client(**CLIENT_CONFIG)
    
    print("Creating storage-optimized temperature table if needed...")
    main_client.command("""
    CREATE TABLE IF NOT EXISTS ocean_4d_temperature (
        time DateTime64(0) CODEC(DoubleDelta, ZSTD(4)),
        depth Float32 CODEC(Gorilla, ZSTD(4)),
        gridX UInt16 CODEC(DoubleDelta, ZSTD(4)),
        gridY UInt16 CODEC(DoubleDelta, ZSTD(4)),
        temperature Float32 CODEC(Gorilla, ZSTD(4))
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(time)
    ORDER BY (gridX, gridY, time, depth)
    """)
    
    # 3. Process each massive file provided via CLI
    for file_path in args.files:
        if not os.path.exists(file_path):
            print(f"Error: {file_path} not found. Skipping.")
            continue
            
        print(f"\n--- Analyzing {os.path.basename(file_path)} ---")
        
        with xr.open_dataset(file_path) as ds_main:
            total_time_steps = len(ds_main[VAR_MAP['time_var']].values)
            
        steps_per_worker = math.ceil(total_time_steps / CORES_TO_USE)
        
        worker_tasks = []
        for i in range(CORES_TO_USE):
            start = i * steps_per_worker
            end = min(start + steps_per_worker, total_time_steps)
            if start < end:
                worker_tasks.append((file_path, start, end, i + 1))
                
        print(f"Divided {total_time_steps} time steps across {len(worker_tasks)} cores.")
        
        with ProcessPoolExecutor(max_workers=CORES_TO_USE) as executor:
            results = list(executor.map(process_time_chunk, worker_tasks))
            
        for res in results:
            print(res)

    print(f"\nTotal Pipeline complete in {time.time() - total_start_time:.2f} seconds!")