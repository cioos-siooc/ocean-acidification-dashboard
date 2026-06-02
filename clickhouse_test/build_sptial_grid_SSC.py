import os
import numpy as np
import pandas as pd
import xarray as xr
import clickhouse_connect

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# We only need ONE file to build the grid mapping, since the coordinates are static
SOURCE_NC_FILE = "temperature_20260510.nc" 

CH_HOST = "localhost"
CH_PORT = 8123
CH_USER = "default"
CH_PASS = ""

# Define the exact names of dimensions and variables inside your NetCDF file
DIM_X = 'gridX'
DIM_Y = 'gridY'
VAR_LAT = 'latitude'   # Change if your 2D lat variable has a different name
VAR_LON = 'longitude'  # Change if your 2D lon variable has a different name

# ==============================================================================
# 1. CLICKHOUSE INITIALIZATION
# ==============================================================================
print("Connecting to ClickHouse...")
client = clickhouse_connect.get_client(
    host=CH_HOST, 
    port=CH_PORT, 
    username=CH_USER, 
    password=CH_PASS
)

print("Creating grid_SSC mapping table...")
client.command("DROP TABLE IF EXISTS grid_SSC")

# We sort by gridX and gridY because your API lookups will target these coordinates
client.command("""
CREATE TABLE grid_SSC (
time DateTime64(0) CODEC(DoubleDelta, ZSTD(4)),
    
    -- 1 decimal place (Max value allowed: 9,999,999.9)
    depth Decimal32(1) CODEC(T64, ZSTD(4)), 
    
    -- Max below 1000 fits perfectly in UInt16 (0 to 65,535)
    gridX UInt16 CODEC(DoubleDelta, ZSTD(4)),  
    gridY UInt16 CODEC(DoubleDelta, ZSTD(4)),  
    
    -- 4 decimal places (Max value allowed: 99,999.9999)
    temperature Decimal32(4) CODEC(T64, ZSTD(4)),
    salinity Decimal32(4) CODEC(T64, ZSTD(4))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (gridX, gridY, time, depth);
""")

# ==============================================================================
# 2. EXTRACT AND VECTORIZE 2D COORDINATES
# ==============================================================================
if not os.path.exists(SOURCE_NC_FILE):
    raise FileNotFoundError(f"Could not find source NetCDF file: {SOURCE_NC_FILE}")

print(f"Opening {SOURCE_NC_FILE} to extract spatial coordinates...")
ds = xr.open_dataset(SOURCE_NC_FILE)

# Pull 1D dimension axes
gridX_vals = ds[DIM_X].values
gridY_vals = ds[DIM_Y].values

# Pull 2D coordinate matrices
lat_matrix = ds[VAR_LAT].values
lon_matrix = ds[VAR_LON].values

print(f"Dimensions Found: gridX ({len(gridX_vals)}) x gridY ({len(gridY_vals)})")
print(f"Coordinate Matrices Shape: Latitude {lat_matrix.shape} x Longitude {lon_matrix.shape}")

print("Aligning 1D coordinate dimensions with 2D spatial matrices...")
# indexing='xy' generates matrix grids matching the (gridY, gridX) row-column layout of the NC file
X_mesh, Y_mesh = np.meshgrid(gridX_vals, gridY_vals, indexing='xy')

# Flatten all matrices to 1D vectors simultaneously to maintain alignment
flat_X = X_mesh.flatten().astype(np.uint16)
flat_Y = Y_mesh.flatten().astype(np.uint16)
flat_lat = lat_matrix.flatten().astype(np.float32)
flat_lon = lon_matrix.flatten().astype(np.float32)

# Mask out any NaN coordinates if your netcdf file pads boundary edges with nulls
valid_coords = ~np.isnan(flat_lat) & ~np.isnan(flat_lon)

# Build the vectorized dataframe
df_grid = pd.DataFrame({
    'gridX': flat_X[valid_coords],
    'gridY': flat_Y[valid_coords],
    'latitude': flat_lat[valid_coords],
    'longitude': flat_lon[valid_coords]
})

# ==============================================================================
# 3. INSERT AND OPTIMIZE
# ==============================================================================
print(f"Uploading {len(df_grid)} coordinate mapping pairs to grid_SSC...")
client.insert(
    table='grid_SSC',
    data=df_grid,
    column_names=list(df_grid.columns)
)

print("Running final table optimization pass...")
client.command("OPTIMIZE TABLE grid_SSC FINAL")

# Verify size of the mapping table
stats = client.query("SELECT formatReadableSize(sum(data_compressed_bytes)) FROM system.parts WHERE table = 'grid_SSC' AND active = 1")
print(f"Database sync complete! grid_SSC disk size: {stats.result_rows[0][0]}")