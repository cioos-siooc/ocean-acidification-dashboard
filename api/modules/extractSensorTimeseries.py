"""extractSensorTimeseries.py

Read a sensor timeseries from a compressed netCDF4 file.

Files are stored at:
    {SENSORS_ROOT}/{sensor_id}/{variable}.nc

The variable inside may be:
  - 1D: (time,)
  - 2D: (time, depth)

Returns a dict ready to serialize as a JSON response.
"""

import os
import sys
import numpy as np
import netCDF4
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Sensor NC files live in a separate directory from model NC files and are
# opened with raw netCDF4 (not xarray).  Use a dedicated lock so sensor reads
# do not compete with the global xarray _nc_lock used by timeseries/climatology.
_sensor_nc_lock = threading.RLock()

SENSORS_ROOT = os.getenv("SENSORS_ROOT", "/opt/data/sensors")

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _epoch_to_iso(seconds: float) -> str:
    return (_EPOCH + timedelta(seconds=float(seconds))).strftime("%Y-%m-%dT%H:%M:%S")


def extract_sensor_timeseries(
    sensor_id: int,
    variable: str,
    from_date: str,
    to_date: str,
    depth: Optional[float] = None,
) -> dict:
    """
    Read sensor timeseries from an NC file.

    Parameters
    ----------
    sensor_id : integer sensor ID
    variable  : sensorCategoryCode — also the filename stem and the variable
                name stored inside the NC file (e.g. "DOXY", "PSAL")
    from_date : ISO-8601 string (start of range, inclusive)
    to_date   : ISO-8601 string (end of range, inclusive)
    depth     : optional depth (m); if None and a depth dimension exists,
                data for all depths is returned

    Returns
    -------
    dict with keys:
      "time"  — list of ISO-8601 strings
      "value" — list of float | None
      "depth" — list of float  (present only when the NC file has a depth dim)

    Raises
    ------
    FileNotFoundError  — NC file not found for this sensor / variable
    KeyError           — required variable not found inside the NC file
    ValueError         — unparseable date strings
    """
    nc_path = Path(SENSORS_ROOT) / str(sensor_id) / f"{variable}.nc"
    if not nc_path.exists():
        raise FileNotFoundError(
            f"No NC file for sensor {sensor_id}, variable '{variable}' "
            f"(expected path: {nc_path})"
        )

    def _parse(s: str) -> datetime:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    from_dt = _parse(from_date)
    to_dt = _parse(to_date)
    from_epoch = (from_dt - _EPOCH).total_seconds()
    to_epoch = (to_dt - _EPOCH).total_seconds()

    # Hold the lock only for the raw HDF5 reads; release before any numpy/Python processing.
    with _sensor_nc_lock:
        with netCDF4.Dataset(nc_path, "r") as ds:
            if "time" not in ds.variables:
                raise KeyError(f"NC file '{nc_path}' has no 'time' variable")
            if variable not in ds.variables:
                raise KeyError(
                    f"Variable '{variable}' not found in NC file '{nc_path}'. "
                    f"Available variables: {list(ds.variables.keys())}"
                )

            dims = ds.variables[variable].dimensions
            has_depth_dim = (
                len(dims) == 2
                and "depth" in dims
                and "depth" in ds.variables
            )

            # Read all needed data into plain numpy arrays, then release the lock.
            times_epoch_raw = np.array(ds.variables["time"][:], dtype=np.float64)
            depths_raw = np.array(ds.variables["depth"][:], dtype=np.float64) if has_depth_dim else None
            time_mask = (times_epoch_raw >= from_epoch) & (times_epoch_raw <= to_epoch)

            if not has_depth_dim:
                values_raw = np.array(ds.variables[variable][time_mask], dtype=np.float64)
                data_2d_raw = None
            elif depth is not None:
                depths_list = depths_raw.tolist()
                depth_idx = int(np.argmin(np.abs(depths_raw - depth)))
                values_raw = np.array(ds.variables[variable][time_mask, depth_idx], dtype=np.float64)
                data_2d_raw = None
            else:
                values_raw = None
                data_2d_raw = np.array(ds.variables[variable][time_mask, :], dtype=np.float64)
                depths_list = depths_raw.tolist()

    # Lock is released — all processing below is pure numpy/Python.
    filtered_epochs = times_epoch_raw[time_mask]
    time_iso = [_epoch_to_iso(t) for t in filtered_epochs.tolist()]

    if not has_depth_dim:
        return {
            "time": time_iso,
            "value": [
                None if np.isnan(v) else float(v)
                for v in values_raw.tolist()
            ],
        }

    if depth is not None:
        return {
            "time": time_iso,
            "depth": float(depths_list[depth_idx]),
            "value": [
                None if np.isnan(v) else float(v)
                for v in values_raw.tolist()
            ],
        }

    # All depths — flatten to parallel arrays
    times_out: list = []
    depths_out: list = []
    values_out: list = []
    for ti, t_iso in enumerate(time_iso):
        for di, d in enumerate(depths_list):
            v = data_2d_raw[ti, di]
            times_out.append(t_iso)
            depths_out.append(float(d))
            values_out.append(None if np.isnan(v) else float(v))

    return {"time": times_out, "depth": depths_out, "value": values_out}
