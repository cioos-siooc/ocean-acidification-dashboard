import argparse
import os
import time
import math
from concurrent.futures import ProcessPoolExecutor
import numpy as np
import pandas as pd
import xarray as xr
import clickhouse_connect
import gc

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

# Define your 8 variables here. 
# This assumes the file prefix, the NetCDF internal variable name, and 
# the ClickHouse column name are all identical. Edit if they differ!
VARIABLES = [
    'temperature',
    'salinity',
    'total_alkalinity',
    'omega_arag',
    'omega_cal',
    'ph_total',
    'dissolved_oxygen',
    'dissolved_inorganic_carbon'
]

MEASUREMENT_COLS = [
    f'{var}_mean' for var in VARIABLES
] + [
    f'{var}_min' for var in VARIABLES
] + [
    f'{var}_max' for var in VARIABLES
]

# Grid Coordinate Variable Names in your NetCDF files
VAR_MAP = {
    'time_var': 'time',
    'depth_var': 'depth',
    'lat_var': 'gridY',
    'lon_var': 'gridX'
}

BATCH_SIZE_THRESHOLD = 500000
CORES_TO_USE = 2  # Reduced from 10 to safely handle 8 variables in 128GB RAM

# ==============================================================================
# THE MULTIPROCESSING WORKER
# ==============================================================================
def process_time_chunk(args):
    """Runs independently on a CPU core."""
    suffix, dates_chunk, worker_id = args
    
    worker_client = clickhouse_connect.get_client(**CLIENT_CONFIG)
    
    # 1. Open all 8 datasets simultaneously for this suffix
    # Using h5netcdf bypasses the NetCDF4 C-library memory leak
    ds_dict = {}
    for var in VARIABLES:
        file_path = f"/app/data/{var}/{var}_{suffix}.nc"
        ds_dict[var] = xr.open_dataset(file_path, engine='h5netcdf')
    
    # Extract coordinates from the first variable (assuming all share the identical grid)
    base_ds = ds_dict[VARIABLES[0]]
    depths = base_ds[VAR_MAP['depth_var']].values
    lats = base_ds[VAR_MAP['lat_var']].values
    lons = base_ds[VAR_MAP['lon_var']].values
    
    d_grid, lat_grid, lon_grid = np.meshgrid(depths, lats, lons, indexing='ij')
    flat_depth = d_grid.flatten().astype(np.float32)
    flat_lat = lat_grid.flatten().astype(np.uint16)
    flat_lon = lon_grid.flatten().astype(np.uint16)
    
    batch_dfs = []
    batch_rows = 0
    total_inserted = 0
    
    print(f"[Worker {worker_id}] Started: Suffix '{suffix}' ({len(dates_chunk)} days assigned)")
    
    for local_d_idx, target_date in enumerate(dates_chunk):
        date_str = str(target_date)
        
        # We will use the first variable (e.g., temperature) as the "Master Mask"
        # to determine where the ocean is vs. where land is (NaN).
        master_var = VARIABLES[0]
        day_data_master = ds_dict[master_var][master_var].sel({VAR_MAP['time_var']: date_str})
        
        # Calculate Mean, Min, and Max
        master_mean = day_data_master.mean(dim=VAR_MAP['time_var'], skipna=True).values.flatten()
        master_min = day_data_master.min(dim=VAR_MAP['time_var'], skipna=True).values.flatten()
        master_max = day_data_master.max(dim=VAR_MAP['time_var'], skipna=True).values.flatten()
        
        valid_mask = ~np.isnan(master_mean)
        
        if not np.any(valid_mask):
            del day_data_master, master_mean, master_min, master_max, valid_mask
            continue  
            
        # Initialize DataFrame dictionary with the master variable's stats
        df_cols = {
            'time': target_date,
            'depth': flat_depth[valid_mask],
            'gridY': flat_lat[valid_mask],
            'gridX': flat_lon[valid_mask],
            f'{master_var}_mean': master_mean[valid_mask],
            f'{master_var}_min': master_min[valid_mask],
            f'{master_var}_max': master_max[valid_mask]
        }
        
        temp_mem_refs = [day_data_master, master_mean, master_min, master_max] 
        
        # Loop through the remaining 7 variables
        for var in VARIABLES[1:]:
            try:
                day_data_var = ds_dict[var][var].sel({VAR_MAP['time_var']: date_str})
                var_mean = day_data_var.mean(dim=VAR_MAP['time_var'], skipna=True).values.flatten()
                var_min = day_data_var.min(dim=VAR_MAP['time_var'], skipna=True).values.flatten()
                var_max = day_data_var.max(dim=VAR_MAP['time_var'], skipna=True).values.flatten()
            except (ValueError, KeyError, IndexError) as e:
                print(f"[Worker {worker_id}] Warning: {var} missing/empty for {date_str}: {e}", flush=True)
                n_cells = len(flat_depth)
                var_mean = np.full(n_cells, np.nan, dtype=np.float32)
                var_min = np.full(n_cells, np.nan, dtype=np.float32)
                var_max = np.full(n_cells, np.nan, dtype=np.float32)
                day_data_var = None

            df_cols[f'{var}_mean'] = var_mean[valid_mask]
            df_cols[f'{var}_min'] = var_min[valid_mask]
            df_cols[f'{var}_max'] = var_max[valid_mask]

            temp_mem_refs.extend([day_data_var, var_mean, var_min, var_max])
            
        # Build DataFrame and remove rows where all measurement stats are zero or null
        df_batch = pd.DataFrame(df_cols)
        df_batch = df_batch.loc[~(df_batch[MEASUREMENT_COLS].fillna(0) == 0).all(axis=1)]
        if df_batch.empty:
            del df_cols, df_batch, valid_mask
            for ref in temp_mem_refs:
                del ref
            gc.collect()
            continue

        batch_dfs.append(df_batch)
        batch_rows += len(df_batch)
        
        if batch_rows >= BATCH_SIZE_THRESHOLD:
            df_to_insert = pd.concat(batch_dfs, ignore_index=True)
            worker_client.insert('SalishSeaCast_daily', df_to_insert, column_names=list(df_to_insert.columns))
            total_inserted += batch_rows
            batch_dfs = []
            batch_rows = 0
            del df_to_insert

        if local_d_idx > 0 and local_d_idx % 5 == 0:
            print(f"[Worker {worker_id}] Progress: {local_d_idx} / {len(dates_chunk)} days completed.", flush=True)

        # 🔥 AGGRESSIVE MEMORY CLEANUP
        del df_cols, df_batch, valid_mask
        for ref in temp_mem_refs:
            del ref
        gc.collect()

    # Flush remaining batch
    if batch_dfs:
        df_to_insert = pd.concat(batch_dfs, ignore_index=True)
        worker_client.insert('SalishSeaCast_daily', df_to_insert, column_names=list(df_to_insert.columns))
        total_inserted += batch_rows

    # Close all 8 files for this worker
    for var in VARIABLES:
        ds_dict[var].close()
        
    return f"[Worker {worker_id}] Finished Suffix '{suffix}'. Inserted {total_inserted} daily rows."

# ==============================================================================
# MAIN EXECUTION (Traffic Controller)
# ==============================================================================
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load 8 SalishSeaCast variables into ClickHouse')
    parser.add_argument('suffixes', nargs='+', help='File suffixes (e.g., 2013 202601)')
    args = parser.parse_args()
    
    total_start_time = time.time()
    
    print("Connecting to ClickHouse (Main Thread)...")
    main_client = clickhouse_connect.get_client(**CLIENT_CONFIG)
    
# Build the dynamic CREATE TABLE string for Mean, Min, and Max
    column_definitions = []
    for var in VARIABLES:
        column_definitions.append(f"{var}_mean Float32 CODEC(Gorilla, ZSTD(4))")
        column_definitions.append(f"{var}_min Float32 CODEC(Gorilla, ZSTD(4))")
        column_definitions.append(f"{var}_max Float32 CODEC(Gorilla, ZSTD(4))")
        
    cols_sql = ",\n        ".join(column_definitions)
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS SalishSeaCast_daily (
        time Date CODEC(DoubleDelta, ZSTD(4)),
        depth Float32 CODEC(Gorilla, ZSTD(4)),
        gridX UInt16 CODEC(DoubleDelta, ZSTD(4)),
        gridY UInt16 CODEC(DoubleDelta, ZSTD(4)),
        {cols_sql}
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(time)
    ORDER BY (gridX, gridY, time, depth)
    """
    
    print("Creating SalishSeaCast_daily table if needed...")
    main_client.command(create_table_sql)
    
    for suffix in args.suffixes:
        print(f"\n--- Analyzing Suffix: {suffix} ---")
        
        # Verify all 8 files exist before attempting to process
        missing_files = []
        for var in VARIABLES:
            if not os.path.exists(f"/app/data/{var}/{var}_{suffix}.nc"):
                missing_files.append(f"{var}_{suffix}.nc")
                
        if missing_files:
            print(f"Error: Missing files for suffix {suffix}: {missing_files}")
            print("Skipping this suffix entirely.")
            continue
            
        # Open just the first variable file to extract the time axis
        first_file = f"/app/data/{VARIABLES[0]}/{VARIABLES[0]}_{suffix}.nc"
        with xr.open_dataset(first_file, engine='h5netcdf') as ds_main:
            times = pd.to_datetime(ds_main[VAR_MAP['time_var']].values)
            unique_dates = np.unique(times.date)

        # Skip dates already present in ClickHouse
        min_date, max_date = unique_dates.min(), unique_dates.max()
        existing_result = main_client.query(
            "SELECT DISTINCT time FROM SalishSeaCast_daily WHERE time >= %(min_d)s AND time <= %(max_d)s",
            parameters={'min_d': str(min_date), 'max_d': str(max_date)}
        )
        existing_dates = {row[0] for row in existing_result.result_rows}
        if existing_dates:
            total_before = len(unique_dates)
            unique_dates = np.array([d for d in unique_dates if d not in existing_dates])
            print(f"Skipping {total_before - len(unique_dates)} already-inserted dates. {len(unique_dates)} remaining.")

        if len(unique_dates) == 0:
            print(f"All dates for suffix '{suffix}' already in ClickHouse. Skipping.")
            continue

        days_per_worker = math.ceil(len(unique_dates) / CORES_TO_USE)
        
        worker_tasks = []
        for i in range(CORES_TO_USE):
            start_idx = i * days_per_worker
            end_idx = min((i + 1) * days_per_worker, len(unique_dates))
            chunk_dates = unique_dates[start_idx:end_idx]
            
            if len(chunk_dates) > 0:
                worker_tasks.append((suffix, chunk_dates, i + 1))
                
        print(f"Divided {len(unique_dates)} unique days across {len(worker_tasks)} cores.")
        
        with ProcessPoolExecutor(max_workers=CORES_TO_USE) as executor:
            results = list(executor.map(process_time_chunk, worker_tasks))
            
        for res in results:
            print(res)

    print(f"\nTotal Pipeline complete in {time.time() - total_start_time:.2f} seconds!")