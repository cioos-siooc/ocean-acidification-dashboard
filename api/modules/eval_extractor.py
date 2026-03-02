"""eval_extractor.py

Extract evaluation data from a 1D netCDF file containing sensor and model comparisons.

The netCDF file structure is expected to be:
  - time dimension with multiple timesteps
  - Variables named like: {variable}_sensor, {variable}_SSC, {variable}_LiveOcean
  - E.g., temperature_sensor, temperature_SSC, temperature_LiveOcean

Returns a JSON-serializable dict with time and data arrays (NaN converted to null).
"""

import xarray as xr
import numpy as np
from typing import Dict, List, Optional, Any
import logging
from nc_reader import open_nc_uncached, close_nc

logger = logging.getLogger(__name__)


def extract_eval_data(
    nc_path: str,
    variable: str,
    model: str
) -> Dict[str, Any]:
    """
    Extract evaluation data from a netCDF file.
    
    Args:
        nc_path: Path to the netCDF file
        variable: Variable name (e.g., 'temperature', 'salinity')
        sensor_id: Sensor ID (placeholder for future use)
    
    Returns:
        Dict with keys: time, sensor, model
        Each value is a list (time as ISO strings, others as floats or null)
    
    Raises:
        FileNotFoundError: If nc_path does not exist
        KeyError: If expected variables are not found in the file
        ValueError: If no valid time dimension found
    """
    
    if not nc_path or not nc_path.endswith('.nc'):
        raise ValueError(f"Invalid netCDF path: {nc_path}")
    
    # Load the netCDF file via centralised thread-safe reader
    ds = open_nc_uncached(nc_path)
    if ds is None:
        raise FileNotFoundError(f"NetCDF file not found or could not be opened: {nc_path}")
    
    # Build expected variable names
    var_sensor = f"{variable}_sensor"
    var_model = f"{variable}_{model}"
    
    # Check if all variables exist
    missing_vars = []
    for var_name in [var_sensor, var_model]:
        if var_name not in ds.data_vars:
            missing_vars.append(var_name)
    
    if missing_vars:
        available = list(ds.data_vars.keys())
        raise KeyError(
            f"Missing expected variables: {missing_vars}. "
            f"Available variables: {available}"
        )
    
    # Find and extract time dimension
    time_var = None
    time_candidates = ["time_UTC", "time", "datetime"]
    
    for cand in time_candidates:
        if cand in ds:
            time_var = cand
            break
    
    if time_var is None:
        raise ValueError(
            f"No time dimension found. Checked for: {time_candidates}. "
            f"Available: {list(ds.dims.keys())}"
        )
    
    # Extract time and convert to ISO format strings
    time_data = ds[time_var].values
    time_list = []
    print(time_data)
    # time_data is array of numpy.float64 of timestamps. Convert to ISO strings.
    for t in time_data:
        try:
            # Convert to numpy datetime64 and then to ISO string
            t_iso = np.datetime_as_string(np.datetime64(int(t), 's'), unit='s')
            time_list.append(t_iso)
        except Exception as e:
            logger.warning(f"Failed to convert time value {t} to ISO string: {str(e)}")
            time_list.append(None)
    
    # Extract data and convert NaN to None (null in JSON)
    def extract_array(var_name: str) -> List[Optional[float]]:
        data = ds[var_name].values.flatten()  # Ensure 1D
        result = []
        for val in data:
            try:
                val_float = float(val)
                # Convert NaN to None
                if np.isnan(val_float):
                    result.append(None)
                else:
                    result.append(val_float)
            except (ValueError, TypeError):
                result.append(None)
        return result
    
    sensor_data = extract_array(var_sensor)
    model_data = extract_array(var_model)
    
    # Close the dataset
    close_nc(ds)
    
    return {
        "time": time_list,
        "sensor": sensor_data,
        "model": model_data
    }
