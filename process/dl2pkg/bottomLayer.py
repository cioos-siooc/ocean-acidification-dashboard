import numpy as np

def extract_bottom_layer(data_3d):
    """
    Extract the deepest non-NaN value for each grid cell (vectorized).
    
    Treats both NaN and 0 as invalid values.
    
    Args:
        data_3d: numpy masked array of shape (depth, lat, lon)
    
    Returns:
        2D array of shape (lat, lon) with the deepest valid value per cell
    """
    # Convert to float and handle masked array
    data = np.ma.filled(data_3d, np.nan).astype(float)
    
    # Convert 0 values to NaN (treat as invalid)
    data = np.where(data == 0, np.nan, data)
    
    n_depth, n_lat, n_lon = data.shape
    
    # Create a mask of valid (non-NaN) values
    valid_mask = ~np.isnan(data)  # (depth, lat, lon)
    
    # Flip along depth axis to find the FIRST valid value (which is the last in original)
    flipped_mask = np.flip(valid_mask, axis=0)
    
    # argmax returns the index of the first True value
    first_valid_flipped = np.argmax(flipped_mask, axis=0)  # (lat, lon)
    
    # Convert back to original depth indices
    last_valid_idx = n_depth - 1 - first_valid_flipped
    
    # Detect cells with all NaN values
    has_valid = np.any(valid_mask, axis=0)
    last_valid_idx = np.where(has_valid, last_valid_idx, 0)
    
    # Extract values using advanced indexing
    lat_idx, lon_idx = np.meshgrid(np.arange(n_lat), np.arange(n_lon), indexing='ij')
    bottom_layer = data[last_valid_idx, lat_idx, lon_idx]
    
    # Set all-NaN cells back to NaN
    bottom_layer = np.where(has_valid, bottom_layer, np.nan)
    
    return bottom_layer