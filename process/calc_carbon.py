#!/usr/bin/env python3
"""Calculate carbonate system fields (pH, omega_arag, omega_cal) from DIC/TA/T/S files.

Usage:
  calc_carbon.py --date YYYY-MM-DD [--dataset DATASET_ID] [--in-dir /opt/data/nc] [--out-dir /opt/data/nc] [--use-sigma]

Notes:
- TA and DIC in input files are expected to be in mmol/m3. They are converted to umol/kg using density (sigma_theta) if available, otherwise a default density of 1025 kg/m3 is used.
- Depth-to-pressure conversion uses p (dbar) = depth_m / 1.019716 by default.
- Outputs: NetCDF files for `ph_total`, `omega_arag`, and `omega_cal` with the same spatial/time/depth coordinates. Files will be written under `/opt/data/nc/<var>/<datetime>/...` similar to other outputs. Default naming: `<var>_<start>_<end>.nc` (keeps same time coverage as inputs).
- PyCO2SYS is required (added to pyproject). We use common defaults for dissociation constants and pH scale (total scale). See README or code comments for details.
"""

from __future__ import annotations
import argparse
import os
import logging
from glob import glob
from datetime import datetime
from typing import Optional

import numpy as np
import xarray as xr

# Try to import PyCO2SYS; we assume PyCO2SYS>=3.0.0 is installed
try:
    import PyCO2SYS as pyco2
except Exception as e:
    pyco2 = None

logger = logging.getLogger("calc_carbon")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Variable names (directory names) used by our pipeline
DIC_VAR = "dissolved_inorganic_carbon"
TA_VAR = "total_alkalinity"
TEMP_CANDIDATES = ["temp", "temperature", "sea_water_temperature", "water_temperature"]
SAL_CANDIDATES = ["salinity", "salt", "sea_water_salinity"]
SIGMA_CANDIDATES = ["sigma_theta", "potential_density", "density", "sigma"]

DEFAULT_DENSITY = 1025.0  # kg/m3 default when sigma is not available
DEPTH_TO_PRESSURE_FACTOR = 1.019716  # depth (m) -> dbar: p = depth / factor


def find_var(ds: xr.Dataset, candidates):
    for c in candidates:
        if c in ds:
            return c
        # fallback: case-insensitive search
        for k in ds.data_vars:
            if k.lower() == c.lower():
                return k
    return None


def convert_mmolm3_to_umolkg(arr_mmolm3: np.ndarray, density_kgm3: np.ndarray):
    """Convert from mmol/m3 to umol/kg.

    Steps:
    - 1 mmol/m3 == 1 umol/L (see analysis: 1 mmol/m3 = 1e-3 mol/m3 = 1e-6 mol/L = 1 umol/L)
    - umol/kg = umol/L * (1000 / density_kgm3)
    """
    umol_per_l = arr_mmolm3  # mmol/m3 -> umol/L
    factor = 1000.0 / density_kgm3
    return umol_per_l * factor


def depth_to_pressure(depth_m):
    return depth_m / DEPTH_TO_PRESSURE_FACTOR


def compute_for_file(dic_fp: str, ta_fp: str, temp_fp: str, sal_fp: str, sigma_fp: Optional[str], out_dir: str, overwrite: bool = False):
    # Open datasets
    with xr.open_dataset(dic_fp) as ds_dic:
        with xr.open_dataset(ta_fp) as ds_ta:
            with xr.open_dataset(temp_fp) as ds_temp:
                with xr.open_dataset(sal_fp) as ds_sal:
                    # find variable names inside datasets
                    dic_name = None
                    for k in ds_dic.data_vars:
                        dic_name = k; break
                    ta_name = None
                    for k in ds_ta.data_vars:
                        ta_name = k; break

                    temp_name = find_var(ds_temp, TEMP_CANDIDATES)
                    sal_name = find_var(ds_sal, SAL_CANDIDATES)

                    if temp_name is None or sal_name is None:
                        raise RuntimeError("Could not find temperature or salinity variable in files")

                    # Determine density
                    if sigma_fp:
                        try:
                            with xr.open_dataset(sigma_fp) as ds_sig:
                                sig_name = find_var(ds_sig, SIGMA_CANDIDATES)
                                if sig_name is None:
                                    logger.warning("Sigma file provided but sigma variable not found; using default density")
                                    density = DEFAULT_DENSITY
                                else:
                                    density = ds_sig[sig_name].values  # kg/m3
                        except Exception as e:
                            logger.warning("Failed to open sigma file %s: %s; using default density", sigma_fp, e)
                            density = DEFAULT_DENSITY
                    else:
                        density = DEFAULT_DENSITY

                    # Broadcast density to shape of data (time, depth, y, x)
                    # Process data in low-memory chunks (time/depth slices) to avoid OOM
                    coords = ds_dic.coords
                    dims = ds_dic[dic_name].dims

                    # depth coordinate
                    depth_coord = None
                    for d in dims:
                        if d.lower() not in ("time", "x", "lon", "y", "lat"):
                            depth_coord = d
                            break

                    if depth_coord is None:
                        depth_vals = np.array([0.0])
                    else:
                        depth_vals = ds_dic[depth_coord].values

                    # time coordinate if present
                    time_coord = 'time' if 'time' in dims else None
                    time_len = ds_dic.dims[time_coord] if time_coord else 1
                    depth_len = ds_dic.dims[depth_coord] if depth_coord else 1

                    # Prepare output file path
                    base = os.path.basename(dic_fp)
                    out_name = base.replace(DIC_VAR, "ph_omega")
                    out_path = os.path.join(out_dir, "ph_omega")
                    os.makedirs(out_path, exist_ok=True)
                    out_fp = os.path.join(out_path, out_name)
                    if os.path.exists(out_fp) and not overwrite:
                        logger.info("Skipping existing output %s", out_fp)
                        return

                    # Ensure PyCO2SYS is available
                    if pyco2 is None:
                        raise RuntimeError("PyCO2SYS not installed; please add it to the environment")

                    # Create output netCDF file and variables using netCDF4 for incremental writes
                    try:
                        import netCDF4 as nc
                    except Exception:
                        raise RuntimeError("netCDF4 is required to write output incrementally")

                    logger.info("Creating output %s (time=%s depth=%s)", out_fp, time_len, depth_len)
                    nc_out = nc.Dataset(out_fp, 'w', format='NETCDF4')

                    # create dimensions
                    for dname, dlen in ds_dic.dims.items():
                        nc_out.createDimension(dname, None if dname == 'time' else dlen)

                    # copy coordinate vars
                    for cname in ds_dic.coords:
                        coord = ds_dic.coords[cname]
                        # handle datetime coords specially (store as seconds since epoch)
                        if np.issubdtype(coord.dtype, np.datetime64):
                            var = nc_out.createVariable(cname, 'f8', coord.dims)
                            times = coord.values.astype('datetime64[s]').astype('int64')
                            var[:] = times
                            var.units = 'seconds since 1970-01-01 00:00:00'
                        else:
                            var = nc_out.createVariable(cname, coord.dtype, coord.dims)
                            try:
                                var[:] = coord.values
                            except Exception:
                                try:
                                    var.assignValue(coord.values)
                                except Exception:
                                    # fallback to bytes
                                    var[:] = np.asarray(coord.values, dtype='S')

                    # create data variables
                    out_vars = {}
                    for vname in ('ph_total', 'omega_arag', 'omega_cal'):
                        out_vars[vname] = nc_out.createVariable(vname, 'f4', dims, zlib=True, fill_value=np.nan)

                    # iterate over time/depth and compute per-slice
                    for ti in range(time_len):
                        for di in range(depth_len):
                            # build indexer for xarray isel
                            indexer = {}
                            if time_coord:
                                indexer[time_coord] = ti
                            if depth_coord:
                                indexer[depth_coord] = di

                            try:
                                dic_slice = ds_dic[dic_name].isel(indexer).values.astype(float)
                                ta_slice = ds_ta[ta_name].isel(indexer).values.astype(float)
                                temp_slice = ds_temp[temp_name].isel(indexer).values.astype(float)
                                sal_slice = ds_sal[sal_name].isel(indexer).values.astype(float)
                            except Exception as e:
                                logger.exception("Failed to read slices for t=%s d=%s: %s", ti, di, e)
                                continue

                            # determine density for this slice
                            if sigma_fp:
                                try:
                                    with xr.open_dataset(sigma_fp) as ds_sig:
                                        sig_name = find_var(ds_sig, SIGMA_CANDIDATES)
                                        if sig_name:
                                            # may need same indexer
                                            try:
                                                density_slice = ds_sig[sig_name].isel(indexer).values.astype(float)
                                            except Exception:
                                                density_slice = DEFAULT_DENSITY
                                        else:
                                            density_slice = DEFAULT_DENSITY
                                except Exception:
                                    density_slice = DEFAULT_DENSITY
                            else:
                                density_slice = DEFAULT_DENSITY

                            # convert units
                            ta_umolkg = convert_mmolm3_to_umolkg(ta_slice, density_slice)
                            dic_umolkg = convert_mmolm3_to_umolkg(dic_slice, density_slice)

                            # compute pressure for this depth/time slice
                            if depth_coord is None:
                                pressure_slice = np.zeros_like(temp_slice)
                            else:
                                p = depth_to_pressure(float(depth_vals[di]))
                                pressure_slice = np.full_like(temp_slice, p, dtype=float)

                            flat_temp = temp_slice.ravel()
                            flat_sal = sal_slice.ravel()
                            flat_pressure = pressure_slice.ravel()
                            flat_ta = ta_umolkg.ravel()
                            flat_dic = dic_umolkg.ravel()

                            # compute via PyCO2SYS
                            # We use the 'sys' function which wraps the main logic.
                            # It accepts arrays matching the shapes of inputs.
                            
                            # Parameters for pyco2.sys (based on simple_ph_test success)
                            # par1=TA, par2=DIC
                            kwargs = {
                                'par1': flat_ta,
                                'par2': flat_dic,
                                'par1_type': 1, # 1=TA
                                'par2_type': 2, # 2=DIC
                                'salinity': flat_sal,
                                'temperature': flat_temp,
                                'pressure': flat_pressure,
                                'opt_pH_scale': 1, # 1=Total Scale
                                'opt_k_carbonic': 10, # 10=Lueker et al 2000
                            }

                            try:
                                # Try standard function name 'sys' or 'system' depending on version
                                func = getattr(pyco2, 'sys', None)
                                if not func:
                                    func = getattr(pyco2, 'system', None)
                                
                                if func:
                                    res = func(**kwargs)
                                    # Output keys might differ slightly in some versions but usually 'pH', 'saturation_aragonite'
                                    ph_vals = res.get('pH', res.get('pH_total'))
                                    omega_vals_arag = res.get('saturation_aragonite', res.get('Omega_aragonite'))
                                    omega_vals_cal = res.get('saturation_calcite', res.get('Omega_calcite'))
                                    
                                    ph_slice = np.asarray(ph_vals).reshape(temp_slice.shape)
                                    omega_arag_slice = np.asarray(omega_vals_arag).reshape(temp_slice.shape)
                                    omega_cal_slice = np.asarray(omega_vals_cal).reshape(temp_slice.shape)
                                else:
                                    # Fallback to manual minimal/solubility if sys wrapper missing
                                    raise RuntimeError("PyCO2SYS 'sys' function not found")

                            except Exception as e:
                                logger.exception("PyCO2SYS failed for t=%s d=%s: %s", ti, di, e)
                                continue

                            # Build index tuple for netCDF assignment
                            idx = []
                            for d in dims:
                                if d == time_coord:
                                    idx.append(ti)
                                elif d == depth_coord:
                                    idx.append(di)
                                else:
                                    idx.append(slice(None))
                            idx = tuple(idx)

                            # assign to file
                            try:
                                out_vars['ph_total'][idx] = ph_slice
                                out_vars['omega_arag'][idx] = omega_arag_slice
                                out_vars['omega_cal'][idx] = omega_cal_slice
                            except Exception as e:
                                logger.exception("Failed to write slice t=%s d=%s: %s", ti, di, e)

                    # close output
                    nc_out.close()
                    logger.info("Wrote %s", out_fp)


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--date", help="YYYY-MM-DD to process (optional, otherwise all files)")
    p.add_argument("--dataset", help="Optional dataset text id to restrict processing")
    p.add_argument("--in-dir", default=os.environ.get("DATA_DIR", "/opt/data/nc"))
    p.add_argument("--out-dir", default=os.environ.get("DATA_DIR", "/opt/data/nc"))
    p.add_argument("--use-sigma", action="store_true", help="Prefer using sigma_theta for density conversion if available")
    p.add_argument("--overwrite", action="store_true")
    args = p.parse_args(argv)

    # Find DIC files to process
    dic_dir = os.path.join(args.in_dir, DIC_VAR)
    if not os.path.isdir(dic_dir):
        logger.error("DIC directory not found: %s", dic_dir)
        return 1

    files = sorted(glob(os.path.join(dic_dir, "*.nc")))
    # Normalize date filter to support both YYYY-MM-DD and YYYYMMDD tokens in filenames
    date_token = None
    if args.date:
        # accept both formats: user may pass 2026-01-15 or 20260115
        date_token = args.date.replace('-', '')
        logger.info("Filtering files with date token %s", date_token)

    for fp in files:
        try:
            # optional date filtering
            if date_token:
                if date_token not in fp:
                    continue
            base = os.path.basename(fp)
            # Derive equivalent filenames for TA, temp, sal, and optional sigma using base suffix
            ta_fp = os.path.join(args.in_dir, TA_VAR, base.replace(DIC_VAR, TA_VAR))
            temp_fp = None
            sal_fp = None
            sigma_fp = None
            # try to find matching temp/sal files by scanning directories for a file containing the same datetime substring
            # extract date token from base (first datetime-like token)
            # simplistic approach: find first 15-char token like 20260105T0030
            import re
            m = re.search(r"\d{8}T\d{4}", base)
            token = m.group(0) if m else None
            if token:
                # find temp and sal files containing token
                temp_cands = glob(os.path.join(args.in_dir, "*", f"*{token}*.nc"))
                for c in temp_cands:
                    if DIC_VAR in c or TA_VAR in c:
                        continue
                    # check that file likely contains temp or sal
                    try:
                        with xr.open_dataset(c) as ds:
                            tname = find_var(ds, TEMP_CANDIDATES)
                            sname = find_var(ds, SAL_CANDIDATES)
                            signame = find_var(ds, SIGMA_CANDIDATES)
                            if tname and temp_fp is None:
                                temp_fp = c
                            if sname and sal_fp is None:
                                sal_fp = c
                            if args.use_sigma and signame and sigma_fp is None:
                                sigma_fp = c
                    except Exception:
                        continue
            if not (os.path.exists(ta_fp) and temp_fp and sal_fp):
                logger.warning("Skipping %s because matching TA/temp/sal files not found (TA:%s temp:%s sal:%s)", fp, ta_fp, temp_fp, sal_fp)
                continue

            compute_for_file(fp, ta_fp, temp_fp, sal_fp, sigma_fp if args.use_sigma else None, args.out_dir, overwrite=args.overwrite)
        except Exception as e:
            logger.exception("Failed processing %s: %s", fp, e)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
