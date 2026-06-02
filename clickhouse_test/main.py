import os
import time
import numpy as np
import pandas as pd
import xarray as xr
import clickhouse_connect

# ==============================================================================
# CONFIGURATION
# ==============================================================================
START_DATE = 20260510
END_DATE = 20260519
CH_HOST = "localhost"
CH_PORT = 8123
CH_USER = "default"
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "")

VAR_MAP = {
    'time_var': 'time',
    'depth_var': 'depth',
    'lat_var': 'gridY',
    'lon_var': 'gridX',
    'temp_var': 'temperature',
    'salt_var': 'salinity'
}

# ==============================================================================
# 1. ESTABLISH CLICKHOUSE CONNECTION
# ==============================================================================
print("Connecting to ClickHouse...")
client = clickhouse_connect.get_client(
    host=CH_HOST, 
    port=CH_PORT, 
    username=CH_USER, 
    password=CH_PASS
)

print("Creating storage-optimized table...")
client.command("DROP TABLE IF EXISTS ocean_4d_efficient")
client.command("""
CREATE TABLE ocean_4d_efficient (
    time DateTime64(0) CODEC(DoubleDelta, ZSTD(4)),
    depth Float32 CODEC(Gorilla, ZSTD(4)),
    latitude Float32 CODEC(Gorilla, ZSTD(4)),
    longitude Float32 CODEC(Gorilla, ZSTD(4)),
    temperature Float32 CODEC(Gorilla, ZSTD(4)),
    salinity Float32 CODEC(Gorilla, ZSTD(4))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (time, depth, latitude, longitude)
SETTINGS old_parts_lifetime = 10;
""")

def print_storage_stats(client, phase="CURRENT"):
    stats = client.query("""
        SELECT 
            formatReadableSize(toUInt64(sum(data_compressed_bytes))),
            formatReadableSize(toUInt64(sum(data_uncompressed_bytes))),
            toUInt64(sum(data_compressed_bytes)),
            toUInt64(sum(data_uncompressed_bytes))
        FROM system.parts
        WHERE table = 'ocean_4d_efficient' AND active = 1
    """)
    row = stats.result_rows[0]
    c_bytes = row[2] if row[2] is not None else 0
    r_bytes = row[3] if row[3] is not None else 0
    ratio = round(r_bytes / c_bytes, 2) if c_bytes > 0 else 0.0
    print(f"\n--- CLICKHOUSE STORAGE INSIGHTS ({phase}) ---")
    print(f"Compressed Disk Size:   {row[0] if c_bytes > 0 else '0.00 B'}")
    print(f"Uncompressed Row Size: {row[1] if r_bytes > 0 else '0.00 B'}")
    print(f"Compression Ratio Factor: {ratio}x")

print_storage_stats(client, "BEFORE INGESTION")
total_start_time = time.time()

# ==============================================================================
# 3. STREAM & FLATTEN 4D DATA (OPTIMIZED)
# ==============================================================================
for date_int in range(START_DATE, END_DATE + 1):
    temp_file = f"temperature_{date_int}.nc"
    salt_file = f"salinity_{date_int}.nc"
    
    if not (os.path.exists(temp_file) and os.path.exists(salt_file)):
        print(f"Skipping {date_int}, files not found.")
        continue
        
    print(f"\nOpening NetCDF files for {date_int}: {temp_file} & {salt_file}...")
    ds_temp = xr.open_dataset(temp_file, chunks={VAR_MAP['time_var']: 1})
    ds_salt = xr.open_dataset(salt_file, chunks={VAR_MAP['time_var']: 1})
    
    times = ds_temp[VAR_MAP['time_var']].values
    depths = ds_temp[VAR_MAP['depth_var']].values
    lats = ds_temp[VAR_MAP['lat_var']].values
    lons = ds_temp[VAR_MAP['lon_var']].values
    
    print(f"Dataset Dimensions: Time({len(times)}) x Depth({len(depths)}) x Lat({len(lats)}) x Lon({len(lons)})")
    
    # 🔥 SPEEDUP 1: Compute spatial grid ONCE per file, outside the loop!
    print("Pre-computing spatial grid coordinates...")
    d_grid, lat_grid, lon_grid = np.meshgrid(depths, lats, lons, indexing='ij')
    flat_depth = d_grid.flatten().astype(np.float32)
    flat_lat = lat_grid.flatten().astype(np.float32)
    flat_lon = lon_grid.flatten().astype(np.float32)
    
    start_time = time.time()
    batch_dfs = []
    batch_rows = 0
    BATCH_SIZE_THRESHOLD = 500000  # Flush to DB once we accumulate 500k rows
    
    for t_idx, t_val in enumerate(times):
        p_time = pd.Timestamp(t_val)
        
        # Load 3D slices directly as flat numpy blocks
        temp_block = ds_temp[VAR_MAP['temp_var']].isel({VAR_MAP['time_var']: t_idx}).values.flatten()
        salt_block = ds_salt[VAR_MAP['salt_var']].isel({VAR_MAP['time_var']: t_idx}).values.flatten()
        
        # 🔥 SPEEDUP 2: Mask out NaNs using high-speed NumPy vector operations
        valid_mask = ~np.isnan(temp_block) & ~np.isnan(salt_block)
        if not np.any(valid_mask):
            continue  
            
        # 🔥 SPEEDUP 3: Swap out slow list(zip) with a native Pandas DataFrame block
        # Pandas will automatically broadcast the scalar 'p_time' to match the array lengths
        df_batch = pd.DataFrame({
            'time': p_time,
            'depth': flat_depth[valid_mask],
            'latitude': flat_lat[valid_mask],
            'longitude': flat_lon[valid_mask],
            'temperature': temp_block[valid_mask],
            'salinity': salt_block[valid_mask]
        })
        
        batch_dfs.append(df_batch)
        batch_rows += len(df_batch)
        print(f" -> Prepared Time Step {t_idx + 1}/{len(times)} ({len(df_batch)} rows)...")
        
        if batch_rows >= BATCH_SIZE_THRESHOLD:
            print(f" -> Committing batch of {batch_rows} rows to ClickHouse...")
            df_to_insert = pd.concat(batch_dfs, ignore_index=True)
            client.insert(
                table='ocean_4d_efficient', 
                data=df_to_insert,
                column_names=list(df_to_insert.columns)
            )
            # Clear memory immediately
            batch_dfs = []
            batch_rows = 0

    if batch_dfs:
        print(f" -> Committing final day batch of {batch_rows} rows to ClickHouse...")
        df_to_insert = pd.concat(batch_dfs, ignore_index=True)
        client.insert(
            table='ocean_4d_efficient', 
            data=df_to_insert,
            column_names=list(df_to_insert.columns)
        )
        batch_dfs = []
        batch_rows = 0
    
    print(f"Migration for {date_int} complete in {time.time() - start_time:.2f} seconds!")

print(f"\nTotal Migration complete in {time.time() - total_start_time:.2f} seconds!")

# Optimize table parts to merge data on disk for clean final measurement metrics
print("Running final table optimization pass...")
client.command("OPTIMIZE TABLE ocean_4d_efficient FINAL")

print_storage_stats(client, "AFTER INGESTION AND MERGE")