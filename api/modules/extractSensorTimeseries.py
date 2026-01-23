"""Small helper to extract a variable at a given time from a NetCDF file.

Functions
- extract_variable_at_time: return values at given time as a flat list (optionally with coordinates)

Usage examples:
>>> extract_variable_at_time('/opt/data/nc/dissolved_inorganic_carbon/…nc', 'dissolved_inorganic_carbon', '2026-01-15T00:30:00Z')
[0.123, 0.234, ...]

>>> extract_variable_at_time('/path/to.nc', 'ph_total', '2026-01-15T00:30:00Z', with_coords=True)
[{'lat': -12.3, 'lon': 145.2, 'value': 7.82}, ...]
"""
from __future__ import annotations

from datetime import datetime
from typing import List, Dict, Optional

import json
import numpy as np
import xarray as xr
import pandas as pd


def extract_sensor_timeseries(var: str, nc_path: str, max_samples: Optional[int] = None) -> List[Dict]:
    """Extract the time series for `var` from `nc_path`.

    Special handling for sensor-style files:
    - `time` is stored as a variable (not a coordinate) and often shares the same
      index dimension named `row` with the data variables.
    - No spatial interpolation is performed (single coordinate file).

    If `max_samples` is provided and the series is longer, the function will sample
    up to `max_samples` indices directly from the file (via `isel`) to avoid loading
    the full arrays into memory — this is much faster for large files.

    Returns a list of dicts: [{'time': '2026-01-15T00:30:00Z', 'value': 1.23}, ...]

    Raises KeyError if the variable or time variable cannot be found.
    """
    ds = xr.open_dataset(nc_path)
    try:
        if var not in ds:
            raise KeyError(f"Variable not found in dataset: {var}")

        # Determine time container. Keep as xarray objects so we can isel later
        if 'time' in ds:
            full_times = ds['time']
        elif 'time' in ds.coords:
            full_times = ds.coords['time']
        else:
            da_tmp = ds[var]
            tdim = next((d for d in da_tmp.dims if d.lower() == 'time'), None)
            if tdim and tdim in ds.coords:
                full_times = ds.coords[tdim]
            else:
                raise KeyError("Could not find a 'time' variable or coordinate in dataset")

        da = ds[var]

        # Decide the primary index dimension that corresponds to time positions
        if 'row' in da.dims:
            primary_dim = 'row'
        else:
            primary_dim = next((d for d in da.dims if d.lower() == 'time'), None)

        # Sampling path: compute indices and isel to avoid loading full series
        if max_samples is not None and primary_dim is not None:
            total_len = int(full_times.size)
            if total_len > max_samples:
                idxs = np.linspace(0, total_len - 1, num=max_samples, dtype=int)
                da_sel = da.isel({primary_dim: idxs}).squeeze(drop=True)
                times_sel = full_times.isel({primary_dim: idxs}).values
                vals = da_sel.values.flatten()
                times_arr = pd.to_datetime(times_sel)
            else:
                da_sel = da.squeeze(drop=True)
                vals = da_sel.values.flatten()
                times_arr = pd.to_datetime(full_times.values)
        else:
            # No sampling requested — load full series
            da_sel = da.squeeze(drop=True)
            vals = da_sel.values.flatten()
            times_arr = pd.to_datetime(full_times.values)

        # Ensure lengths match; truncate if necessary
        if len(vals) != len(times_arr):
            minlen = min(len(vals), len(times_arr))
            vals = vals[:minlen]
            times_arr = times_arr[:minlen]

        out = []
        for t, v in zip(times_arr, vals):
            ts = pd.Timestamp(t)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            else:
                ts = ts.tz_convert("UTC")
            iso = ts.isoformat().replace("+00:00", "Z")
            if isinstance(v, float) and pd.isna(v):
                value = None
            else:
                try:
                    value = float(v)
                except Exception:
                    value = v
            out.append({"time": iso, "value": value})

        return out
    finally:
        try:
            ds.close()
        except Exception:
            pass


def extract_sensor_timeseries_from_json(json_path: str = '/opt/data/data.json', time_index: int = 0, temp_index: int = 1) -> List[Dict]:
    """Read a JSON file where each entry is an array of values and return
    [{'time': <ISO Z string>, 'value': <float|null>}, ...] using columns at
    `time_index` and `temp_index`.

    The JSON is expected to be an array of arrays. Rows that are not lists or
    don't have the expected indices are skipped.
    """
    try:
        with open(json_path, 'r') as fh:
            data = json.load(fh)
    except Exception as exc:
        raise RuntimeError(f"Could not open/parse JSON file {json_path}: {exc}") from exc

    if not isinstance(data, list):
        raise RuntimeError("JSON file must contain an array of rows")

    out: List[Dict] = []
    for row in data:
        if not isinstance(row, (list, tuple)):
            continue
        if len(row) <= max(time_index, temp_index):
            continue
        t = row[time_index]
        v = row[temp_index]
        # parse time
        try:
            ts = pd.to_datetime(t, unit='ms')
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            else:
                ts = ts.tz_convert("UTC")
            iso = ts.isoformat().replace("+00:00", "Z")
        except Exception:
            iso = str(t)
        # parse value
        try:
            if isinstance(v, float) and pd.isna(v):
                value = None
            else:
                value = float(v)
        except Exception:
            value = v
        out.append({"time": iso, "value": value})

    return out


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extract a sensor time series from a NetCDF file or JSON file")
    parser.add_argument("nc_path", help="Path to NetCDF file")
    parser.add_argument("variable", help="Variable name to extract")
    parser.add_argument("--json", action='store_true', help="Read JSON at /opt/data/data.json using time/temp indices instead of NetCDF")
    args = parser.parse_args()

    if args.json:
        res = extract_sensor_timeseries_from_json()
    else:
        res = extract_sensor_timeseries(args.variable, args.nc_path)
    print(json.dumps(res))
