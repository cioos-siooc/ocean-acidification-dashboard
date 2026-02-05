import xarray as xr
import pandas as pd
import sys
import time

def extract_timeseries(file_path, Y, X, variable_name='temperature'):
    """
    Extracts a time series from a NetCDF file at the nearest grid point 
    to the specified latitude and longitude.
    """
    start_time = time.perf_counter()
    print(f"Opening {file_path}...")
    
    # Open the dataset
    ds = xr.open_dataset(file_path)
    
    # Check for storage/compression information
    var = ds[variable_name]
    print(f"DataArray shape: {var.shape}")
    print(f"DataArray dims: {var.dims}")
    print(f"Format: {ds.data_vars[variable_name].encoding.get('source', 'unknown')}")
    
    # Detailed encoding inspection
    encoding = var.encoding
    print(f"Encoding: {encoding}")
    
    if 'chunksizes' in encoding:
        print(f"File Chunking: {encoding['chunksizes']}")
    else:
        print("File is not chunked in encoding metadata.")
    
    # Identify dimension names
    # Note: Search for common names if they aren't 'latitude'/'longitude'
    # In some of your files, these might be 'gridY'/'gridX' or 'lat'/'lon'
    dims = ds.dims
    print(f"Dataset dimensions: {list(dims.keys())}")

    try:
        # Nearest neighbor interpolation to find the specific pixel
        print(f"Extracting '{variable_name}' at Y: {Y}, X: {X}...")
        
        # This handles common dimension naming conventions
        # Swap 'latitude'/'longitude' for 'gridY'/'gridX' if your file uses project-specific grids
        selection = {
            'gridY': Y,
            'gridX': X
        }

        # Perform the extraction
        ts = ds[variable_name].sel(selection, method='nearest')
        
        # Convert to a Pandas DataFrame for easy viewing/manipulation
        df = ts.to_dataframe().reset_index()
        
        end_time = time.perf_counter()
        elapsed = end_time - start_time
        
        print(f"\nExtraction Successful! Time taken: {elapsed:.4f} seconds")
        print(df.head())
        
        # Optional: Save to CSV
        # output_csv = "extracted_timeseries.csv"
        # df.to_csv(output_csv, index=False)
        # print(f"Saved to {output_csv}")
        
        return df

    except Exception as e:
        print(f"Error during extraction: {e}")
        print("Hint: Check if your NetCDF file uses 'latitude/longitude' or 'gridY/gridX' as dimensions.")
        return None

if __name__ == "__main__":
    # Example usage
    # Update these with your actual file and coordinates
    PATH = sys.argv[1]
    TARGET_Y = 450
    TARGET_X = 250
    VAR = "mean" # In your stats files, variables are named 'mean', 'median', etc.
    
    extract_timeseries(PATH, TARGET_Y, TARGET_X, VAR)
