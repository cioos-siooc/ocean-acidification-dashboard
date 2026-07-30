import numpy as np
import pandas as pd
import xarray as xr

VARIABLES = [
    'temperature', 'salinity', 'total_alkalinity', 'omega_arag',
    'omega_cal', 'ph_total', 'dissolved_oxygen', 'dissolved_inorganic_carbon'
]
SUFFIX = "2007"
DATA_DIR = "/media/taimaz/CIOOS/data"

ref_var = VARIABLES[0]
with xr.open_dataset(f"{DATA_DIR}/{ref_var}/{ref_var}_{SUFFIX}.nc", engine='h5netcdf') as ds:
    ref_dates = set(pd.to_datetime(ds['time'].values).date)

print(f"Reference ({ref_var}): {len(ref_dates)} dates  [{min(ref_dates)} → {max(ref_dates)}]\n")

for var in VARIABLES[1:]:
    path = f"{DATA_DIR}/{var}/{var}_{SUFFIX}.nc"
    with xr.open_dataset(path, engine='h5netcdf') as ds:
        var_dates = set(pd.to_datetime(ds['time'].values).date)
    #
    missing = sorted(ref_dates - var_dates)
    extra   = sorted(var_dates - ref_dates)
    status  = "OK" if not missing else f"MISSING {len(missing)} date(s)"
    print(f"{var:35s} {len(var_dates)} dates  [{min(var_dates)} → {max(var_dates)}]  {status}")
    if missing:
        print(f"  Missing dates: {missing}")
    if extra:
        print(f"  Extra dates not in temperature: {extra}")
