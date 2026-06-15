import os
import time
import numpy as np
import pandas as pd
import xarray as xr
import clickhouse_connect

# ==============================================================================
# CONFIGURATION
# ==============================================================================
START_DATE = 202601
END_DATE = 202604
CH_HOST = "localhost"
CH_PORT = 8123
CH_USER = "default"
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

VAR_MAP = {
    'time_var': 'time',
    'depth_var': 'depth',
    'lat_var': 'gridY',
    'lon_var': 'gridX'
}

VARNAMES = [
    'temperature',
    'salinity',
    'total_alkalinity',
    'omega_arag',
    'omega_cal',
    'ph_total',
    'dissolved_oxygen',
    'dissolved_inorganic_carbon'
]

BATCH_SIZE_THRESHOLD = 500000
MAX_TIME_SLICE_BATCHES = 24

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

print("Creating storage-optimized table if needed...")
client.command("""
CREATE TABLE IF NOT EXISTS SalishSeaCast_daily (
    time DateTime64(0) CODEC(DoubleDelta, ZSTD(4)),
    depth Float32 CODEC(Gorilla, ZSTD(4)),
    gridX UInt16 CODEC(DoubleDelta, ZSTD(4)),
    gridY UInt16 CODEC(DoubleDelta, ZSTD(4)),
    temperature Float32 CODEC(Gorilla, ZSTD(4)),
    salinity Float32 CODEC(Gorilla, ZSTD(4)),
    total_alkalinity Float32 CODEC(Gorilla, ZSTD(4)),
    omega_arag Float32 CODEC(Gorilla, ZSTD(4)),
    omega_cal Float32 CODEC(Gorilla, ZSTD(4)),
    ph_total Float32 CODEC(Gorilla, ZSTD(4)),
    dissolved_oxygen Float32 CODEC(Gorilla, ZSTD(4)),
    dissolved_inorganic_carbon Float32 CODEC(Gorilla, ZSTD(4))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (time, depth, gridY, gridX)
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
        WHERE table = 'SalishSeaCast_daily' AND active = 1
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
    datasets = {}
    for variable in VARNAMES:
        data_path = os.path.join(DATA_DIR, f"{variable}_{date_int}.nc")
        if not os.path.exists(data_path):
            print(f"Skipping {date_int}, missing {variable} file: {data_path}.")
            datasets = {}
            break
        datasets[variable] = xr.open_dataset(data_path, chunks={VAR_MAP['time_var']: 1})
    if not datasets:
        continue

    print(f"\nOpening NetCDF files for {date_int}: {', '.join(f'{variable}_{date_int}.nc' for variable in VARNAMES)}")
    ds_ref = datasets[VARNAMES[0]]
    times = ds_ref[VAR_MAP['time_var']].values
    depths = ds_ref[VAR_MAP['depth_var']].values
    lats = ds_ref[VAR_MAP['lat_var']].values
    lons = ds_ref[VAR_MAP['lon_var']].values
    
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
    batch_count = 0
    
    def flush_batch():
        nonlocal batch_dfs, batch_rows, batch_count
        if not batch_dfs:
            return
        print(f" -> Flushing batch of {batch_rows} rows to ClickHouse...")
        df_to_insert = pd.concat(batch_dfs, ignore_index=True)
        client.insert(
            table='SalishSeaCast_daily', 
            data=df_to_insert,
            column_names=list(df_to_insert.columns)
        )
        batch_dfs = []
        batch_rows = 0
        batch_count += 1
    
    for t_idx, t_val in enumerate(times):
        p_time = pd.Timestamp(t_val)
        
        # Load 3D slices directly as flat numpy blocks
        blocks = {
            variable: datasets[variable][variable].isel({VAR_MAP['time_var']: t_idx}).values.flatten()
            for variable in VARNAMES
        }
        
        # 🔥 SPEEDUP 2: Mask out NaNs using high-speed NumPy vector operations
        valid_mask = np.ones_like(next(iter(blocks.values())), dtype=bool)
        for block in blocks.values():
            valid_mask &= ~np.isnan(block)
        if not np.any(valid_mask):
            continue  
            
        # 🔥 SPEEDUP 3: Build one DataFrame per time slice and commit periodically
        df_batch = pd.DataFrame({
            'time': p_time,
            'depth': flat_depth[valid_mask],
            'gridY': flat_lat[valid_mask],
            'gridX': flat_lon[valid_mask],
            **{variable: blocks[variable][valid_mask] for variable in VARNAMES}
        })
        
        batch_dfs.append(df_batch)
        batch_rows += len(df_batch)
        print(f" -> Prepared Time Step {t_idx + 1}/{len(times)} ({len(df_batch)} rows)...")

        if batch_rows >= BATCH_SIZE_THRESHOLD or len(batch_dfs) >= MAX_TIME_SLICE_BATCHES:
            flush_batch()

    if batch_dfs:
        flush_batch()
    
    print(f"Migration for {date_int} complete in {time.time() - start_time:.2f} seconds! (flushed {batch_count} time(s))")

print(f"\nTotal Migration complete in {time.time() - total_start_time:.2f} seconds!")

# Final optimization is disabled by default; enable only when a full merge is required.
print("Skipping final ClickHouse OPTIMIZE pass for normal ingestion.")
# client.command("OPTIMIZE TABLE SalishSeaCast_daily FINAL")

print_storage_stats(client, "AFTER INGESTION AND MERGE")