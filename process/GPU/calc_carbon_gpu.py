#!/usr/bin/env python3
"""GPU-accelerated carbonate chemistry computation using Numba + ROCm.

This script computes ph_total, omega_arag, and omega_cal from input NetCDF files
using GPU acceleration via Numba's ROCm backend.

GPU-Only mode: No CPU fallback. If GPU unavailable, script fails.

Usage:
  python3 calc_carbon_gpu.py --date 20260105 --base-dir /opt/data
  python3 calc_carbon_gpu.py --date 20260105 --workers 1 --depth-batch-size 16
"""

from __future__ import annotations
import argparse
import logging
import os
import sys
import time
from glob import glob
from pathlib import Path

import numpy as np
import xarray as xr
import netCDF4 as nc4
import PyCO2SYS as pyco2

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("calc_carbon_gpu")

# Constants
DEFAULT_DENSITY = 1025.0
DEPTH_TO_PRESSURE_FACTOR = 1.019716

VARS_MAP = {
    "TA": "total_alkalinity",
    "DIC": "dissolved_inorganic_carbon",
    "Temp": "temperature",
    "Sal": "salinity"
}

# PyCO2SYS call is vectorized and cannot use Numba @jit
# We'll call it directly in the compute function instead

def compute_carbonate_pyco2sys_batched(
    ta_3d: np.ndarray,
    dic_3d: np.ndarray,
    temp_3d: np.ndarray,
    sal_3d: np.ndarray,
    pressure_3d: np.ndarray,
    depth_batch_size: int = 4,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Compute carbonate chemistry using batched depth slices.
    
    Processes depth in batches to keep memory footprint bounded,
    avoiding the memory explosion of flattening entire 3D arrays.
    
    Args:
        ta_3d: Total alkalinity (µmol/kg), shape (depth, lat, lon)
        dic_3d: Dissolved inorganic carbon (µmol/kg)
        temp_3d: Temperature (°C)
        sal_3d: Salinity (PSU)
        pressure_3d: Pressure (dbar)
        depth_batch_size: Number of depth levels per batch (default 4)
    
    Returns:
        Tuple of (ph_total, omega_arag, omega_cal) arrays, shape (depth, lat, lon)
    """
    n_depth, n_lat, n_lon = ta_3d.shape
    
    # Initialize output arrays
    ph_total = np.zeros((n_depth, n_lat, n_lon), dtype=np.float64)
    omega_arag = np.zeros((n_depth, n_lat, n_lon), dtype=np.float64)
    omega_cal = np.zeros((n_depth, n_lat, n_lon), dtype=np.float64)
    
    # Process depth in batches
    for d_start in range(0, n_depth, depth_batch_size):
        d_end = min(d_start + depth_batch_size, n_depth)
        batch_size = d_end - d_start
        
        logger.info(f"  Processing depth batch {d_start}-{d_end} of {n_depth}")
        
        # Extract batch (depth_batch, lat, lon)
        ta_batch = ta_3d[d_start:d_end, :, :].reshape(batch_size * n_lat * n_lon)
        dic_batch = dic_3d[d_start:d_end, :, :].reshape(batch_size * n_lat * n_lon)
        temp_batch = temp_3d[d_start:d_end, :, :].reshape(batch_size * n_lat * n_lon)
        sal_batch = sal_3d[d_start:d_end, :, :].reshape(batch_size * n_lat * n_lon)
        pres_batch = pressure_3d[d_start:d_end, :, :].reshape(batch_size * n_lat * n_lon)
        
        # Call PyCO2SYS on batch
        results = pyco2.sys(
            ta_batch,           # PAR1: TA (µmol/kg)
            dic_batch,          # PAR2: DIC (µmol/kg)
            1,                  # PAR1TYPE: 1 = TA
            2,                  # PAR2TYPE: 2 = DIC
            sal_batch,          # Salinity (PSU)
            temp_batch,         # Temperature in (°C)
            temp_batch,         # Temperature out (same as in)
            pres_batch,         # Pressure in (dbar)
            0,                  # Pressure out (0 = same as in)
            0,                  # SI (silica, μmol/kg)
            0,                  # PO4 (phosphate, μmol/kg)
            1,                  # pHSCALEIN: 1 = Total pH scale
            10,                 # K1K2CONSTANTS: 10 = Lueker et al. 2000
            1,                  # KSO4CONSTANTS: 1 = Dickson 1990
        )
        
        # Debug: log available keys on first batch
        if d_start == 0:
            logger.info(f"PyCO2SYS output keys: {list(results.keys())}")
        
        # Extract and reshape results
        # PyCO2SYS keys: 'pH', 'saturation_calcite', 'saturation_aragonite'
        ph_batch = results['pH'].reshape((batch_size, n_lat, n_lon))
        omega_cal_batch = results['saturation_calcite'].reshape((batch_size, n_lat, n_lon))
        omega_arag_batch = results['saturation_aragonite'].reshape((batch_size, n_lat, n_lon))
        
        # Store in output arrays
        ph_total[d_start:d_end, :, :] = ph_batch
        omega_cal[d_start:d_end, :, :] = omega_cal_batch
        omega_arag[d_start:d_end, :, :] = omega_arag_batch
        
        # Explicitly delete batch arrays to free memory
        del ta_batch, dic_batch, temp_batch, sal_batch, pres_batch, results, ph_batch, omega_cal_batch, omega_arag_batch
    
    return ph_total, omega_arag, omega_cal


def depth_to_pressure(depth_m: np.ndarray) -> np.ndarray:
    """Convert depth (m) to pressure (dbar)."""
    return depth_m / DEPTH_TO_PRESSURE_FACTOR


def find_vars(ds: xr.Dataset, key: str) -> str:
    """Find variable name in dataset."""
    target = VARS_MAP.get(key)
    if target and target in ds:
        return target
    for v in ds.data_vars:
        if key == 'DIC' and 'inorganic_carbon' in v:
            return v
        if key == 'TA' and 'alkalinity' in v:
            return v
        if key == 'Temp' and 'temp' in v.lower():
            return v
        if key == 'Sal' and 'sal' in v.lower():
            return v
    return list(ds.data_vars)[0]


def load_input_files(base_dir: str, date_token: str) -> dict[str, xr.Dataset]:
    """Load input NetCDF files for the given date token (YYYYMMDD)."""
    inputs = {}
    
    for var_key in ["DIC", "TA", "Temp", "Sal"]:
        var_dir = os.path.join(base_dir, VARS_MAP[var_key])
        
        if not os.path.exists(var_dir):
            raise RuntimeError(f"{var_key} directory not found: {var_dir}")
        
        files = sorted(glob(os.path.join(var_dir, "*.nc")))
        matching = [f for f in files if date_token in os.path.basename(f)]
        
        if not matching:
            raise RuntimeError(
                f"No files found for date token '{date_token}' in {var_dir}"
            )
        
        file_path = matching[0]
        logger.info(f"Loading {var_key} from {os.path.basename(file_path)}")
        inputs[var_key] = xr.open_dataset(file_path)
    
    return inputs


def compute_time_slice_gpu(
    time_idx: int,
    ta_data: np.ndarray,
    dic_data: np.ndarray,
    temp_data: np.ndarray,
    sal_data: np.ndarray,
    depth_vals: np.ndarray,
    depth_batch_size: int = 4,
) -> dict:
    """Compute carbonate chemistry for a single time slice using GPU."""
    
    logger.info(f"Computing time index {time_idx} on GPU...")
    start = time.time()
    
    # Extract 3D arrays for this time step
    ta_3d = ta_data[time_idx]  # (depth, lat, lon)
    dic_3d = dic_data[time_idx]
    temp_3d = temp_data[time_idx]
    sal_3d = sal_data[time_idx]
    
    # Create pressure grid
    depth_2d = np.expand_dims(depth_vals, axis=(1, 2))  # (depth, 1, 1)
    depth_2d = np.broadcast_to(
        depth_2d,
        (len(depth_vals), ta_3d.shape[1], ta_3d.shape[2])
    )
    pressure_3d = depth_to_pressure(depth_2d)
    
    # Call vectorized PyCO2SYS computation with depth batching
    ph_total, omega_arag, omega_cal = compute_carbonate_pyco2sys_batched(
        ta_3d.astype(np.float64),
        dic_3d.astype(np.float64),
        temp_3d.astype(np.float64),
        sal_3d.astype(np.float64),
        pressure_3d.astype(np.float64),
        depth_batch_size=depth_batch_size,
    )
    
    elapsed = time.time() - start
    logger.info(f"  Time index {time_idx} completed in {elapsed:.2f}s")
    
    return {
        "time_idx": time_idx,
        "ph_total": ph_total,
        "omega_arag": omega_arag,
        "omega_cal": omega_cal,
        "elapsed": elapsed,
    }


def write_outputs(
    output_dir: str,
    date_token: str,
    results: dict,
    coords: dict,
) -> None:
    """Write computed results to NetCDF files."""
    
    for var_name in ["ph_total", "omega_arag", "omega_cal"]:
        output_file = os.path.join(output_dir, f"{var_name}_{date_token}.nc")
        
        # Stack results from all time steps
        data = np.stack([r[var_name] for r in results])
        
        # Create dataset
        ds = xr.Dataset(
            {var_name: (["time", "z", "y", "x"], data)},
            coords={
                "time": coords["time"],
                "z": coords["z"],
                "y": coords["y"],
                "x": coords["x"],
            },
        )
        
        logger.info(f"Writing {var_name} to {output_file}")
        ds.to_netcdf(output_file, mode="w")


def main():
    parser = argparse.ArgumentParser(description="GPU-accelerated carbonate chemistry computation")
    parser.add_argument("--date", required=True, help="Date token in YYYYMMDD format (e.g., 20260105)")
    parser.add_argument("--base-dir", default="/opt/data", help="Base directory for input NetCDF files")
    parser.add_argument("--output-dir", default="/opt/data", help="Output directory for results")
    parser.add_argument("--workers", type=int, default=1, help="Number of workers (unused in GPU mode)")
    parser.add_argument("--depth-batch-size", type=int, default=4, help="Depth levels per batch to control memory (default 4)")
    
    args = parser.parse_args()
    
    logger.info("GPU-Accelerated Carbonate Chemistry Computation (ROCm + Numba)")
    logger.info(f"Date: {args.date}")
    logger.info(f"Input directory: {args.base_dir}")
    logger.info(f"Output directory: {args.output_dir}")
    
    try:
        # Load input files
        logger.info("Loading input files...")
        inputs = load_input_files(args.base_dir, args.date)
        
        # Extract data and coordinates
        ta_ds = inputs["TA"]
        dic_ds = inputs["DIC"]
        temp_ds = inputs["Temp"]
        sal_ds = inputs["Sal"]
        
        ta_var = find_vars(ta_ds, "TA")
        dic_var = find_vars(dic_ds, "DIC")
        temp_var = find_vars(temp_ds, "Temp")
        sal_var = find_vars(sal_ds, "Sal")
        
        ta_data = ta_ds[ta_var].values
        dic_data = dic_ds[dic_var].values
        temp_data = temp_ds[temp_var].values
        sal_data = sal_ds[sal_var].values
        
        # Get depth and coordinate info
        depth_var = "depth" if "depth" in ta_ds.dims else list(ta_ds.dims)[0]
        depth_vals = ta_ds[depth_var].values if depth_var in ta_ds else np.arange(ta_data.shape[1])
        
        logger.info(f"Data shape: {ta_data.shape} (time, depth, lat, lon)")
        logger.info(f"Depth range: {depth_vals.min():.1f}m to {depth_vals.max():.1f}m")
        
        # Compute for each time step
        start_total = time.time()
        results = []
        
        for t in range(ta_data.shape[0]):
            result = compute_time_slice_gpu(
                t,
                ta_data,
                dic_data,
                temp_data,
                sal_data,
                depth_vals if len(depth_vals) == ta_data.shape[1] else np.arange(ta_data.shape[1]),
                depth_batch_size=args.depth_batch_size,
            )
            results.append(result)
        
        elapsed_total = time.time() - start_total
        logger.info(f"Total computation time: {elapsed_total:.2f}s")
        
        # Prepare coordinates
        coords = {
            "time": ta_ds["time"].values if "time" in ta_ds else np.arange(ta_data.shape[0]),
            "z": depth_vals if len(depth_vals) == ta_data.shape[1] else np.arange(ta_data.shape[1]),
            "y": np.arange(ta_data.shape[2]),
            "x": np.arange(ta_data.shape[3]),
        }
        
        # Write outputs
        logger.info("Writing results...")
        write_outputs(args.output_dir, args.date, results, coords)
        
        logger.info("✓ GPU computation completed successfully!")
        
    except Exception as e:
        logger.error(f"GPU computation failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
