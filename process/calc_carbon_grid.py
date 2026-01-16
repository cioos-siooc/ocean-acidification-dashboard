#!/usr/bin/env python3
"""
Calculate carbonate system variables (pH, Omega Aragonite, Omega Calcite) for full NetCDF grids.
Reads TA, DIC, Temp, Salinity files, computes derived variables using PyCO2SYS, and writes new NetCDF files.
Processes data in chunks (timesteps/depths) to be memory efficient.
Saves each variable to a separate file with zlib compression.
"""

import os
import argparse
import logging
import numpy as np
import xarray as xr
import PyCO2SYS as pyco2
from glob import glob
import re
import concurrent.futures
import zipfile
import shutil

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("calc_carbon_grid")

# Configuration Constants
DEFAULT_DENSITY = 1025.0  # kg/m3 if sigma not available
DEPTH_TO_PRESSURE_FACTOR = 1.019716  # approx conversion m -> dbar

# Variable Names Mapping
VARS_MAP = {
    "TA": "total_alkalinity",
    "DIC": "dissolved_inorganic_carbon",
    "Temp": "temperature",
    "Sal": "salinity"
}

def worker_compute_slice(args):
    """
    Worker function to compute pH/Omega for a specific slice (time_idx, depth_idx).
    Does NOT read from NetCDF to avoid pickling/concurrency issues with library handles.
    
    args: (ti, di, ta_slice, dic_slice, temp_slice, sal_slice, pressure_slice, sigma_slice)
    All arrays are 2D (lat, lon) slices or scalars broadcasted to that shape.
    """
    ti, di, ta_val, dic_val, temp_val, sal_val, pressure_val, _ = args

    try:
        # Convert mmol/m3 -> umol/kg
        # We assume density is constant 1025 or passed in sigma_slice (ignored for now to match strict logic)
        rho = 1025.0
        
        # Ensure numpy arrays (copy)
        ta_umol = np.asarray(ta_val) * (1000.0 / rho)
        dic_umol = np.asarray(dic_val) * (1000.0 / rho)
        sal = np.asarray(sal_val)
        temp = np.asarray(temp_val)
        p_vec = np.asarray(pressure_val)
        
        # Masking Valid Data
        valid_mask = ~(np.isnan(dic_umol) | np.isnan(ta_umol) | np.isnan(sal) | np.isnan(temp))
        if not np.any(valid_mask):
            return (ti, di, None, None, None)

        # Run PyCO2SYS
        # Flatten for calculation
        results = pyco2.sys(
            par1=ta_umol[valid_mask],
            par2=dic_umol[valid_mask],
            par1_type=1,
            par2_type=2,
            salinity=sal[valid_mask],
            temperature=temp[valid_mask],
            pressure=p_vec[valid_mask] if p_vec.size > 1 else p_vec,
            opt_pH_scale=1,  # Total Scale
            opt_k_carbonic=10 # Lueker et al 2000
        )
        
        # Reconstruct output shapes
        shape = ta_val.shape
        
        s_ph = np.full(shape, np.nan, dtype=np.float32)
        s_ph[valid_mask] = results["pH"]
        
        s_arag = np.full(shape, np.nan, dtype=np.float32)
        s_arag[valid_mask] = results["saturation_aragonite"]
        
        s_cal = np.full(shape, np.nan, dtype=np.float32)
        s_cal[valid_mask] = results["saturation_calcite"]
        
        return (ti, di, s_ph, s_arag, s_cal)

    except Exception as e:
        return (ti, di, e)


def get_matching_files(dic_file, in_dir):
    """
    Given a DIC file, find matching TA, Temp, and Sal files based on datetime pattern.
    """
    base_name = os.path.basename(dic_file)
    # Extract date token
    match = re.search(r"(\d{8}T\d{4}_\d{8}T\d{4})|(\d{8}T\d{4})", base_name)
    if not match:
        logger.warning(f"Could not extract date token from {base_name}")
        return None
    
    token = match.group(0)
    
    def find_file(var_key):
        # Map logical key to likely folder name
        # We assume folders match the variable names in VARS_MAP
        folder_name = VARS_MAP[var_key]
        search_pattern = os.path.join(in_dir, folder_name, f"*{token}*.nc")
        candidates = glob(search_pattern)
        if not candidates:
            return None
        return candidates[0]

    files = {
        "DIC": dic_file,
        "TA": find_file("TA"),
        "Temp": find_file("Temp"),
        "Sal": find_file("Sal")
    }
    
    if any(f is None for f in files.values()):
        missing = [k for k, v in files.items() if v is None]
        # Allow running without explicit full set if we want to add robustness later, 
        # but for pH we need all 4 parameters ideally.
        logger.warning(f"Missing inputs for {base_name}: {missing}")
        return None
        
    return files

def depth_to_pressure(depth_m):
    return depth_m / DEPTH_TO_PRESSURE_FACTOR

def process_file_set(files, out_base_dir, workers=4, overwrite=False):
    """
    Process one set of co-located NetCDF files using parallel workers.
    """
    dic_path = files["DIC"]
    filename_base = os.path.basename(dic_path)
    
    outputs = {
        "ph_total": {
            "name": "pH Total Scale",
            "unit": "1",
            "filename": filename_base.replace("dissolved_inorganic_carbon", "ph_total"),
            "var_name": "ph_total"
        },
        "omega_arag": {
            "name": "Omega Aragonite",
            "unit": "1",
            "filename": filename_base.replace("dissolved_inorganic_carbon", "omega_arag"),
            "var_name": "omega_arag"
        },
        "omega_cal": {
            "name": "Omega Calcite",
            "unit": "1",
            "filename": filename_base.replace("dissolved_inorganic_carbon", "omega_cal"),
            "var_name": "omega_cal"
        }
    }

    # Output paths
    out_paths = {}
    for key, info in outputs.items():
        out_subdir = os.path.join(out_base_dir, key)
        os.makedirs(out_subdir, exist_ok=True)
        out_paths[key] = os.path.join(out_subdir, info["filename"])

    # Check existence
    if all(os.path.exists(p) for p in out_paths.values()) and not overwrite:
        logger.info(f"Skipping set for {filename_base} (outputs exist)")
        return

    logger.info(f"Processing set for {filename_base} with {workers} workers")
    
    ds_map = {}
    try:
        # Load datasets
        for k, p in files.items():
            ds_map[k] = xr.open_dataset(p)
            
        ref_ds = ds_map["DIC"]
        # Determine variable names dynamically
        var_names = {}
        for k, ds in ds_map.items():
            # Heuristic to find the main variable
            for v in ds.data_vars:
                if k == "DIC" and "inorganic_carbon" in v: var_names[k] = v; break
                if k == "TA" and "alkalinity" in v: var_names[k] = v; break
                if k == "Temp" and "temp" in v.lower(): var_names[k] = v; break
                if k == "Sal" and "sal" in v.lower(): var_names[k] = v; break
            if k not in var_names:
                # Fallback to first var
                var_names[k] = list(ds.data_vars)[0]

        dims = ds_map["DIC"][var_names["DIC"]].dims
        shape = ds_map["DIC"][var_names["DIC"]].shape
        coords = ds_map["DIC"].coords
        
        # Identify dims
        time_dim = next((d for d in dims if 'time' in d.lower()), None)
        depth_dim = next((d for d in dims if 'depth' in d.lower()), None)
        
        t_size = ds_map["DIC"].sizes[time_dim] if time_dim else 1
        d_size = ds_map["DIC"].sizes[depth_dim] if depth_dim else 1
        
        depth_vals = ds_map["DIC"][depth_dim].values if depth_dim else np.array([0.0])

        # Prepare result arrays
        res_arrays = {
            "ph_total": np.full(shape, np.nan, dtype=np.float32),
            "omega_arag": np.full(shape, np.nan, dtype=np.float32),
            "omega_cal": np.full(shape, np.nan, dtype=np.float32)
        }

        # Parallel Execution
        # Strategy: Iterate Time, Parallelize Depth
        # This reduces memory overhead compared to parallelizing all T*D at once
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            for t in range(t_size):
                futures = []
                
                # Pre-load time slice for all vars to standard numpy arrays
                # This minimizes disk I/O contention in workers
                def get_t_slice(k):
                    v = var_names[k]
                    da = ds_map[k][v]
                    if time_dim:
                        return da.isel({time_dim: t}).values
                    return da.values

                try:
                    t_dic = get_t_slice("DIC")
                    t_ta = get_t_slice("TA")
                    t_temp = get_t_slice("Temp")
                    t_sal = get_t_slice("Sal")
                except Exception as e:
                    logger.error(f"Error reading slice t={t}: {e}")
                    continue

                for d in range(d_size):
                    # Slice depth
                    # Note: t_arrays do not have Time dim anymore.
                    # Need to check if Depth dim exists in them.
                    
                    # Helper to slice depth from a (Depth, Y, X) or (Y, X) array
                    def get_d_slice(arr, d_idx):
                        # Assuming Depth is the first remaining dimension if it exists in the original
                        # But xarray squeeze might have removed it? No, .values keeps shape sans indexed dim
                        # If original had Depth, ans we haven't indexed it yet, it should be axis 0
                        # But if variable (e.g. Temp) is surface only?
                        # We assume all inputs match structure.
                        if depth_dim and arr.ndim >= 3: # Depth, Lat, Lon likely
                             return arr[d_idx]
                        elif depth_dim and arr.ndim == 1: # Depth profile?
                             return arr[d_idx]
                        return arr # No depth dim

                    s_dic = get_d_slice(t_dic, d)
                    s_ta = get_d_slice(t_ta, d)
                    s_temp = get_d_slice(t_temp, d)
                    s_sal = get_d_slice(t_sal, d)
                    
                    depth_m = float(depth_vals[d]) if depth_dim else 0.0
                    pressure = depth_to_pressure(depth_m)
                    
                    # Submit
                    # We pass numpy arrays
                    fut = executor.submit(worker_compute_slice, (t, d, s_ta, s_dic, s_temp, s_sal, pressure, None))
                    futures.append(fut)
                
                # Collect results
                for f in concurrent.futures.as_completed(futures):
                    ti, di, r_ph, r_arag, r_cal = f.result()
                    if isinstance(r_ph, Exception) or r_ph is None:
                        # Error or empty
                        if isinstance(r_ph, Exception):
                             logger.error(f"Worker Error t={ti} d={di}: {r_ph}")
                        continue
                        
                    # Assign back
                    # Construct index tuple
                    idx = [slice(None)] * len(shape)
                    if time_dim: idx[dims.index(time_dim)] = ti
                    if depth_dim: idx[dims.index(depth_dim)] = di
                    idx = tuple(idx)
                    
                    res_arrays["ph_total"][idx] = r_ph
                    res_arrays["omega_arag"][idx] = r_arag
                    res_arrays["omega_cal"][idx] = r_cal

                if (t + 1) % 5 == 0:
                    logger.info(f"Completed TimeStep {t+1}/{t_size}")

        # Write Files and Zip
        created_files = []
        for key, arr in res_arrays.items():
            info = outputs[key]
            out_fpath = out_paths[key]
            
            da = xr.DataArray(
                arr,
                coords=coords,
                dims=dims,
                name=info["var_name"],
                attrs={
                    "units": info["unit"],
                    "long_name": info["name"],
                    "source": "Calculated from DIC, TA, T, S via PyCO2SYS"
                }
            )
            out_ds = xr.Dataset({info["var_name"]: da}, coords=coords)
            
            if os.path.exists(out_fpath):
                os.remove(out_fpath)
            
            comp = {info["var_name"]: {'zlib': True, 'complevel': 4}}
            out_ds.to_netcdf(out_fpath, encoding=comp)
            created_files.append(out_fpath)
            
        # Zip
        zip_path = os.path.join(out_base_dir, f"carbon_output_{filename_base[:-3]}.zip")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for added_file in created_files:
                # Store relative to base dir or just name? 
                # User said: "separate file in its own dir"
                # Let's keep the structure relative to `out_base_dir`
                rel_path = os.path.relpath(added_file, out_base_dir)
                zf.write(added_file, rel_path)
                
        logger.info(f"Finished set. Zipped to {zip_path}")
            
    except Exception as e:
        logger.exception(f"File set failed: {filename_base}")
    finally:
        for ds in ds_map.values():
            ds.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-dir", default="/opt/data/nc", help="Root dir for nc files")
    parser.add_argument("--date", help="Process only date YYYYMMDD")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    
    # Locate DIC files
    dic_dir = os.path.join(args.base_dir, "dissolved_inorganic_carbon")
    if not os.path.exists(dic_dir):
        logger.error(f"Directory not found: {dic_dir}")
        return

    pattern = os.path.join(dic_dir, "*.nc")
    all_dic = sorted(glob(pattern))
    
    if args.date:
        all_dic = [f for f in all_dic if args.date in os.path.basename(f)]
        
    logger.info(f"Found {len(all_dic)} DIC files. Using {args.workers} workers.")
    
    for f in all_dic:
        match = get_matching_files(f, args.base_dir)
        if match:
            process_file_set(match, args.base_dir, args.workers, args.overwrite)

if __name__ == "__main__":
    main()
