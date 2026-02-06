import xarray as xr
import numpy as np
import sys

def check_quantiles(file_path):
    print(f"Loading {file_path}...")
    ds = xr.open_dataset(file_path)
    
    # List of variables to check against 'max'
    # We check if min <= q1 <= median <= q3 <= max
    check_vars = ['min', 'q1', 'median', 'q3', 'mean']
    
    if 'max' not in ds:
        print("Error: 'max' variable not found in dataset.")
        return

    print("Checking for consistency violations...")
    
    # Check if any variable is greater than 'max'
    for var in check_vars:
        if var in ds:
            # Mask where var > max
            violations = ds[var] > ds['max']
            count = int(violations.sum())
            
            if count > 0:
                print(f"!!! CRITICAL: Found {count} points where {var} > max")
                # Find the first violation for debugging
                if count > 0:
                    violation_idx = np.where(violations.values)
                    # Show the values at the first violation
                    first_v = (violation_idx[0][0], violation_idx[1][0], violation_idx[2][0])
                    val_var = float(ds[var].values[first_v])
                    val_max = float(ds['max'].values[first_v])
                    print(f"    Example at index {first_v}: {var}={val_var:.4f}, max={val_max:.4f}")
            else:
                print(f"[OK] {var} <= max everywhere.")
        else:
            print(f"[Skip] {var} not found in file.")

    # Also check if min is the smallest
    if 'min' in ds:
        for var in ['q1', 'median', 'q3', 'max', 'mean']:
            if var in ds:
                violations = ds[var] < ds['min']
                count = int(violations.sum())
                if count > 0:
                    print(f"!!! CRITICAL: Found {count} points where {var} < min")
                else:
                    print(f"[OK] min <= {var} everywhere.")

    print("\nCheck finished.")

if __name__ == "__main__":
    # Update with your actual file path
    PATH = "/home/taimazb/Projects/OA/data/nc/SalishSeaCast/climate/temperature/depth_0p5.nc"
    if len(sys.argv) > 1:
        PATH = sys.argv[1]
    
    check_quantiles(PATH)
