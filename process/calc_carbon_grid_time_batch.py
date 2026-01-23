#!/usr/bin/env python3
"""Vectorized per-time-step implementation for carbonate system computation.

This script is a copy of `calc_carbon_grid.py` but batches work by TIME step.
Each worker receives a full time-slice (depth, lat, lon) and calls PyCO2SYS once per time
(or at most a few times) with large arrays of valid points to significantly reduce task
and IPC overhead.

Usage:
    python process/calc_carbon_grid_time_batch.py --base-dir /opt/data/nc --workers 6 --date 20260105
"""

import os
import argparse
import logging
import time
from glob import glob
import re
from typing import Tuple

import numpy as np
import xarray as xr
import PyCO2SYS as pyco2
import concurrent.futures
import zipfile

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("calc_carbon_grid_time_batch")

# Constants
DEPTH_TO_PRESSURE_FACTOR = 1.019716
DEFAULT_DENSITY = 1025.0

VARS_MAP = {
    "TA": "total_alkalinity",
    "DIC": "dissolved_inorganic_carbon",
    "Temp": "temperature",
    "Sal": "salinity"
}


def depth_to_pressure(depth_m: np.ndarray) -> np.ndarray:
    return depth_m / DEPTH_TO_PRESSURE_FACTOR


def worker_compute_time(args) -> Tuple[int, float, float, int, np.ndarray, np.ndarray, np.ndarray]:
    """
    Compute full time-slice (all depths) in one worker.
    Returns (time_idx, elapsed_total, elapsed_pyco2, n_valid, ph_arr, arag_arr, cal_arr)
    """
    ti, t_ta, t_dic, t_temp, t_sal, depth_vals = args
    start_total = time.time()

    # Normalize shapes: ensure 3D array (depth, y, x)
    def ensure_3d(a):
        a = np.asarray(a)
        if a.ndim == 2:
            return a[np.newaxis, ...]
        return a

    t_ta = ensure_3d(t_ta)
    t_dic = ensure_3d(t_dic)
    t_temp = ensure_3d(t_temp)
    t_sal = ensure_3d(t_sal)

    d, ny, nx = t_dic.shape

    # Build depth grid (d, ny, nx)
    if depth_vals is None:
        depth_vals = np.zeros(d)
    depth_grid = np.repeat(depth_vals[:, None, None], ny, axis=1)
    depth_grid = np.repeat(depth_grid[:, :, None], nx, axis=2)

    p_grid = depth_to_pressure(depth_grid).reshape(-1)

    # Flatten
    ta_flat = (t_ta * (1000.0 / DEFAULT_DENSITY)).reshape(-1)
    dic_flat = (t_dic * (1000.0 / DEFAULT_DENSITY)).reshape(-1)
    temp_flat = t_temp.reshape(-1)
    sal_flat = t_sal.reshape(-1)

    valid_mask = ~(np.isnan(ta_flat) | np.isnan(dic_flat) | np.isnan(temp_flat) | np.isnan(sal_flat) | (sal_flat <= 0))
    n_valid = int(valid_mask.sum())

    # Pre-allocate outputs
    ph_flat = np.full_like(ta_flat, np.nan, dtype=np.float32)
    arag_flat = np.full_like(ta_flat, np.nan, dtype=np.float32)
    cal_flat = np.full_like(ta_flat, np.nan, dtype=np.float32)

    elapsed_pyco = 0.0

    if n_valid > 0:
        t0 = time.time()
        try:
            res = pyco2.sys(
                par1=ta_flat[valid_mask],
                par2=dic_flat[valid_mask],
                par1_type=1,
                par2_type=2,
                salinity=sal_flat[valid_mask],
                temperature=temp_flat[valid_mask],
                pressure=p_grid[valid_mask],
                opt_pH_scale=1,
                opt_k_carbonic=10
            )
            t1 = time.time()
            elapsed_pyco = t1 - t0

            # Assign
            ph_flat[valid_mask] = res.get("pH")
            arag_flat[valid_mask] = res.get("saturation_aragonite")
            cal_flat[valid_mask] = res.get("saturation_calcite")
        except Exception as e:
            logger.exception(f"PyCO2SYS error in worker time={ti}: {e}")

    # Reshape back to (d, ny, nx)
    ph_arr = ph_flat.reshape((d, ny, nx)).astype(np.float32)
    arag_arr = arag_flat.reshape((d, ny, nx)).astype(np.float32)
    cal_arr = cal_flat.reshape((d, ny, nx)).astype(np.float32)

    elapsed_total = time.time() - start_total
    return (ti, elapsed_total, elapsed_pyco, n_valid, ph_arr, arag_arr, cal_arr)


def get_matching_files(dic_file: str, in_dir: str):
    base_name = os.path.basename(dic_file)
    match = re.search(r"(\d{8}T\d{4}_\d{8}T\d{4})|(\d{8}T\d{4})", base_name)
    if not match:
        logger.warning(f"Could not extract date token from {base_name}")
        return None
    token = match.group(0)

    def find_file(var_key):
        folder_name = VARS_MAP[var_key]
        candidates = glob(os.path.join(in_dir, folder_name, f"*{token}*.nc"))
        return candidates[0] if candidates else None

    files = {"DIC": dic_file, "TA": find_file("TA"), "Temp": find_file("Temp"), "Sal": find_file("Sal")}
    if any(f is None for f in files.values()):
        missing = [k for k, v in files.items() if v is None]
        logger.warning(f"Missing inputs for {base_name}: {missing}")
        return None
    return files


def process_file_set_time_batched(files, out_base_dir, workers=4, overwrite=False):
    dic_path = files["DIC"]
    filename_base = os.path.basename(dic_path)

    outputs = {
        "ph_total": {"name": "pH Total Scale", "unit": "1", "filename": filename_base.replace("dissolved_inorganic_carbon", "ph_total"), "var_name": "ph_total"},
        "omega_arag": {"name": "Omega Aragonite", "unit": "1", "filename": filename_base.replace("dissolved_inorganic_carbon", "omega_arag"), "var_name": "omega_arag"},
        "omega_cal": {"name": "Omega Calcite", "unit": "1", "filename": filename_base.replace("dissolved_inorganic_carbon", "omega_cal"), "var_name": "omega_cal"}
    }

    out_paths = {}
    for key, info in outputs.items():
        out_subdir = os.path.join(out_base_dir, key)
        os.makedirs(out_subdir, exist_ok=True)
        out_paths[key] = os.path.join(out_subdir, info["filename"])

    if all(os.path.exists(p) for p in out_paths.values()) and not overwrite:
        logger.info(f"Skipping set for {filename_base} (outputs exist)")
        return

    logger.info(f"Processing set for {filename_base} in time-batched mode with {workers} workers")

    ds_map = {}
    try:
        for k, p in files.items():
            ds_map[k] = xr.open_dataset(p)

        # Find variable names
        var_names = {}
        for k, ds in ds_map.items():
            for v in ds.data_vars:
                if k == "DIC" and "inorganic_carbon" in v: var_names[k] = v; break
                if k == "TA" and "alkalinity" in v: var_names[k] = v; break
                if k == "Temp" and "temp" in v.lower(): var_names[k] = v; break
                if k == "Sal" and "sal" in v.lower(): var_names[k] = v; break
            if k not in var_names:
                var_names[k] = list(ds.data_vars)[0]

        dims = ds_map["DIC"][var_names["DIC"]].dims
        time_dim = next((d for d in dims if 'time' in d.lower()), None)
        depth_dim = next((d for d in dims if 'depth' in d.lower()), None)

        t_size = ds_map["DIC"].sizes[time_dim] if time_dim else 1
        d_size = ds_map["DIC"].sizes[depth_dim] if depth_dim else 1

        coords = ds_map["DIC"].coords
        depth_vals = ds_map["DIC"][depth_dim].values if depth_dim else None

        # Full result arrays
        shape = ds_map["DIC"][var_names["DIC"]].shape
        res_arrays = {"ph_total": np.full(shape, np.nan, dtype=np.float32), "omega_arag": np.full(shape, np.nan, dtype=np.float32), "omega_cal": np.full(shape, np.nan, dtype=np.float32)}

        # Prepare and submit time tasks
        tasks = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            for t in range(t_size):
                # Read time slice arrays
                def get_slice(k):
                    v = var_names[k]
                    da = ds_map[k][v]
                    if time_dim:
                        return da.isel({time_dim: t}).values
                    return da.values

                try:
                    t_dic = get_slice("DIC")
                    t_ta = get_slice("TA")
                    t_temp = get_slice("Temp")
                    t_sal = get_slice("Sal")
                except Exception as e:
                    logger.error(f"Error reading time slice t={t}: {e}")
                    continue

                time_value = None
                if time_dim and time_dim in coords:
                    try:
                        time_value = coords[time_dim].values[t]
                    except Exception:
                        time_value = None

                logger.info(f"Submitting time {t+1}/{t_size} ({time_value})")
                fut = executor.submit(worker_compute_time, (t, t_ta, t_dic, t_temp, t_sal, depth_vals))
                tasks.append(fut)

            # Collect
            for fut in concurrent.futures.as_completed(tasks):
                try:
                    ti, elapsed_total, elapsed_pyco, n_valid, ph_arr, arag_arr, cal_arr = fut.result()
                except Exception as e:
                    logger.exception(f"Worker raised: {e}")
                    continue

                # Assign back
                # Build full index
                idx = [slice(None)] * len(shape)
                if time_dim: idx[dims.index(time_dim)] = ti
                idx = tuple(idx)

                res_arrays["ph_total"][idx] = ph_arr
                res_arrays["omega_arag"][idx] = arag_arr
                res_arrays["omega_cal"][idx] = cal_arr

                logger.info(f"Finished time {ti+1}/{t_size}: elapsed_total={elapsed_total:.2f}s, pyco2={elapsed_pyco:.2f}s, valid_points={n_valid}")

        # Write results and zip (same as original)
        created_files = []
        for key, arr in res_arrays.items():
            info = outputs[key]
            out_fpath = out_paths[key]

            da = xr.DataArray(arr, coords=coords, dims=dims, name=info["var_name"], attrs={"units": info["unit"], "long_name": info["name"], "source": "Calculated from DIC, TA, T, S via PyCO2SYS"})
            out_ds = xr.Dataset({info["var_name"]: da}, coords=coords)

            if os.path.exists(out_fpath):
                os.remove(out_fpath)
            comp = {info["var_name"]: {'zlib': True, 'complevel': 4}}
            out_ds.to_netcdf(out_fpath, encoding=comp)
            created_files.append(out_fpath)

        zip_path = os.path.join(out_base_dir, f"carbon_output_{filename_base[:-3]}.zip")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for added_file in created_files:
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
            process_file_set_time_batched(match, args.base_dir, args.workers, args.overwrite)


if __name__ == "__main__":
    main()
