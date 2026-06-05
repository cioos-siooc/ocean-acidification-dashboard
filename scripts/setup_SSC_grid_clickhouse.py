import os
import numpy as np
import pandas as pd
import xarray as xr
import clickhouse_connect

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# Replace this with the path to one of your netCDF files containing the grid
SOURCE_NC_FILE = "SSC_grid.nc" 

CH_HOST = "localhost"
CH_PORT = 8123
CH_USER = "default"
CH_PASS = ""

# Define the exact names of your dimensions and 2D variables inside the file
DIM_X = 'gridX'
DIM_Y = 'gridY'
VAR_LAT = 'latitude'   
VAR_LON = 'longitude'  

# ==============================================================================
# 1. CLICKHOUSE CONNECTION & SCHEMA SETUP
# ==============================================================================
print("Connecting to ClickHouse...")
client = clickhouse_connect.get_client(
    host=CH_HOST, 
    port=CH_PORT, 
    username=CH_USER, 
    password=CH_PASS
)

print("Dropping existing grid_SSC table if it exists...")
client.command("DROP TABLE IF EXISTS grid_SSC")

print("Creating storage-optimized grid_SSC table...")
# Using Float64 for coordinates to maintain maximum geometric lookup precision
client.command("""
CREATE TABLE grid_SSC (
    gridX UInt16 CODEC(DoubleDelta, ZSTD(1)),
    gridY UInt16 CODEC(DoubleDelta, ZSTD(1)),
    longitude Float64 CODEC(Gorilla, ZSTD(3)),
    latitude Float64 CODEC(Gorilla, ZSTD(3))
) ENGINE = MergeTree()
ORDER BY (longitude, latitude);
""")

# ==============================================================================
# 2. EXTRACT AND VECTORIZE 2D COORDINATES
# ==============================================================================
if not os.path.exists(SOURCE_NC_FILE):
    raise FileNotFoundError(f"Could not find source NetCDF file: {SOURCE_NC_FILE}. Please check your path configuration.")

print(f"Opening {SOURCE_NC_FILE} to parse coordinate grids...")
ds = xr.open_dataset(SOURCE_NC_FILE)

# Pull 1D dimension coordinate indices
gridX_vals = ds[DIM_X].values
gridY_vals = ds[DIM_Y].values

# Pull 2D coordinate matrices
lat_matrix = ds[VAR_LAT].values
lon_matrix = ds[VAR_LON].values

print(f"Dimensions Found: gridX ({len(gridX_vals)}) x gridY ({len(gridY_vals)})")
print(f"2D Matrices Found: Latitude shape {lat_matrix.shape} x Longitude shape {lon_matrix.shape}")

print("Aligning 1D index axes to match the 2D coordinate space...")
# indexing='xy' paired with meshgrid(X, Y) matches the typical (row=Y, col=X) layout of netCDF files
X_mesh, Y_mesh = np.meshgrid(gridX_vals, gridY_vals, indexing='xy')

# Vector-flatten all elements down to 1D columns simultaneously to preserve index relationships
flat_X = X_mesh.flatten().astype(np.uint16)
flat_Y = Y_mesh.flatten().astype(np.uint16)
flat_lat = lat_matrix.flatten().astype(np.float64)
flat_lon = lon_matrix.flatten().astype(np.float64)

# Mask out any NaN spatial cells (e.g., unmapped background/boundary elements)
valid_coords = ~np.isnan(flat_lat) & ~np.isnan(flat_lon)

print("Constructing high-speed data frame...")
df_grid = pd.DataFrame({
    'gridX': flat_X[valid_coords],
    'gridY': flat_Y[valid_coords],
    'latitude': flat_lat[valid_coords],
    'longitude': flat_lon[valid_coords]
})

# ==============================================================================
# 3. BULK INSERTION AND OPTIMIZATION
# ==============================================================================
print(f"Streaming {len(df_grid)} coordinate nodes into grid_SSC...")
client.insert(
    table='grid_SSC',
    data=df_grid,
    column_names=list(df_grid.columns)
)

print("Forcing final database compression and part optimization pass...")
client.command("OPTIMIZE TABLE grid_SSC FINAL")

# Fetch storage results from system metadata
stats = client.query("SELECT formatReadableSize(sum(data_compressed_bytes)) FROM system.parts WHERE table = 'grid_SSC' AND active = 1")
print(f"\nSuccess! grid_SSC table fully initialized.")
print(f"Final Disk Size: {stats.result_rows[0][0]}")