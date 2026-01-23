Carbonate computations (pH, Omega_arag, Omega_cal)

Overview
--------
This module computes carbonate-system fields from existing downloaded variables (temperature, salinity, dissolved_inorganic_carbon, total_alkalinity).

Assumptions & Units
- Input TA and DIC are expected in **mmol/m3** (project convention). The script converts these to **µmol/kg** for PyCO2SYS:
  - 1 mmol/m3 == 1 µmol/L
  - µmol/kg = (µmol/L) * (1000 / density_kg_m3)
- If `sigma_theta` (potential density, kg m-3) is available for the same time/depth/location, it is used to compute density for conversion. If not, a default density of 1025 kg/m3 is used.
- Depth to pressure conversion uses: p (dbar) = depth (m) / 1.019716

PyCO2SYS choices
- We use PyCO2SYS (>=3.0.0) to compute the carbonate system.
- Defaults: pH returned on the **total scale**; dissociation constants and other constants use PyCO2SYS defaults.
- Outputs: `ph_total`, `omega_arag`, `omega_cal`.

Files produced
- Output files follow the same daily/hourly convention and naming pattern as the input NetCDF files. They are stored under `/opt/data/nc/ph_omega/` by default, with the same time coverage filename.

CLI
- `python process/calc_carbon.py --date 2026-01-05 [--use-sigma]`
- If `--use-sigma` is passed, script tries to locate sigma_theta files and use them for density conversion; otherwise uses 1025 kg/m3.

Integration
- `process/dl2.py compute` calls the calc script (per-dataset or globally) and can be called from the pipeline. 

Notes
- PyCO2SYS API may have several call patterns; the script tries common interfaces and will fail clearly if the installed version uses an incompatible API.
- If you want more conservative density handling (e.g., compute density from T and S), we can add a seawater/gsw dependency and derive density directly instead of relying on sigma_theta.

