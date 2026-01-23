import PyCO2SYS as pyco2
import numpy as np
import xarray as xr
import os

# Define file paths
# Ensure these match the files available in the environment
# User asked for: total_alkalinity_20260115T0030_20260115T2330.nc
data_dir = "/opt/data/nc"
date_str = "20260115T0030_20260115T2330"

files = {
    "TA":  os.path.join(data_dir, "total_alkalinity", f"total_alkalinity_{date_str}.nc"),
    "DIC": os.path.join(data_dir, "dissolved_inorganic_carbon", f"dissolved_inorganic_carbon_{date_str}.nc"),
    "Temp": os.path.join(data_dir, "temperature", f"temperature_{date_str}.nc"),
    "Sal": os.path.join(data_dir, "salinity", f"salinity_{date_str}.nc"),
}

# Variable names inside NC files
vars_map = {
    "TA": "total_alkalinity",
    "DIC": "dissolved_inorganic_carbon",
    "Temp": "temperature",
    "Sal": "salinity"
}

print(f"--- Calc pH Single Value Test (from NC inputs) ---")

try:
    datasets = {}
    values = {}
    
    print("Opening datasets...")
    for key, path in files.items():
        if not os.path.exists(path):
            raise FileNotFoundError(f"{key} file not found: {path}")
        ds = xr.open_dataset(path)
        datasets[key] = ds
    
    # Read slices into memory to find a valid point (non-NaN)
    slices = {}
    print("Reading slices...")
    for key in files:
        var = datasets[key][vars_map[key]]
        # Use selector to avoid loading whole array
        if var.ndim == 4: # time, depth, lat, lon
            s = var.isel(time=0, depth=0).values
        elif var.ndim == 3: # time, lat, lon OR depth, lat, lon
            # Try to determine dims from names or just take 0
            # Assuming dim 0 is time or depth
            s = var.isel({var.dims[0]: 0}).values
        else:
            s = var.values
        slices[key] = s
        print(f"  Loaded slice for {key}, shape={s.shape}")
        
    rows, cols = slices["TA"].shape
    print(f"Searching grid ({rows}x{cols}) for valid point...")
    
    found_valid = False
    valid_data = {}
    
    # Vectorized search for valid point (non-NaN and non-zero for Sal/TA/DIC)
    mask = np.ones((rows, cols), dtype=bool)
    for k in files:
        mask &= ~np.isnan(slices[k])
        # Also exclude zeros which likely indicate mask/land if not using NaN
        if k in ['TA', 'DIC', 'Sal']:
            mask &= (slices[k] > 0.1)
        
    valid_indices = np.argwhere(mask)
    if valid_indices.size > 0:
        # Pick a point in the middle of valid indices to avoid edge weirdness
        idx = len(valid_indices) // 2
        r, c = valid_indices[idx]
        found_valid = True
        print(f"Found valid point at row={r}, col={c}")
        valid_data = {k: slices[k][r, c] for k in files}
            
    if not found_valid:
        raise ValueError("No valid point found where all variables are not NaN")
        
    # Extract values
    TA_raw = float(valid_data["TA"])
    DIC_raw = float(valid_data["DIC"])
    Temp_val = float(valid_data["Temp"])
    Sal_val = float(valid_data["Sal"])
    
    # Unit Conversions: TA, DIC mmol/m3 -> umol/kg
    Density = 1025.0 
    TA_umolkg = TA_raw * (1000.0 / Density)
    DIC_umolkg = DIC_raw * (1000.0 / Density)
    Pressure_dbar = 0.0 # Assumed surface
    
    print(f"Extracted Values:")
    print(f"  TA: {TA_raw} mmol/m3 -> {TA_umolkg:.2f} umol/kg")
    print(f"  DIC: {DIC_raw} mmol/m3 -> {DIC_umolkg:.2f} umol/kg")
    print(f"  Temp: {Temp_val} C")
    print(f"  Sal: {Sal_val} PSU")
    
    print("\nCalculating with PyCO2SYS...")
    
    kwargs = {
        'par1': TA_umolkg,
        'par2': DIC_umolkg,
        'par1_type': 1, # 1=TA
        'par2_type': 2, # 2=DIC
        'salinity': Sal_val,
        'temperature': Temp_val,
        'pressure': Pressure_dbar,
        'opt_pH_scale': 1, # 1=Total Scale
        'opt_k_carbonic': 10, # 10=Lueker et al 2000
    }
    
    if callable(getattr(pyco2, 'sys', None)):
        res = pyco2.sys(**kwargs)
        ph = res['pH']
    else:
        from PyCO2SYS.minimal import pH_from_alkalinity_dic
        ph = pH_from_alkalinity_dic(
            alkalinity=TA_umolkg,
            dic=DIC_umolkg,
            salinity=Sal_val,
            temperature=Temp_val,
            pressure=Pressure_dbar,
            opt_pH_scale=1,
            opt_k_carbonic=10
        )
        
    print(f"pH (Total): {ph}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
