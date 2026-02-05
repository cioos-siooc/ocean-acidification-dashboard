#!/usr/bin/env python3
"""Download Live Ocean layers.nc and split into per-variable daily files.

Workflow:
- Download layers.nc from a URL (default points to daily Live Ocean S3 path)
- Group variables like temp_10/temp_20/... into one variable with a new depth dimension
- Split into daily NetCDF files with naming: <var>_<start>_<end>.nc
"""

from __future__ import annotations

import argparse
import os
import re
from typing import Dict, Iterable, List, Tuple

import numpy as np
import xarray as xr
import requests

DEFAULT_URL = "https://s3.kopah.uw.edu/liveocean-share/f2026.02.04/layers.nc"
DEFAULT_INPUT = "/opt/data/nc/liveOcean/layers.nc"
DEFAULT_OUT = "/opt/data/nc"

BASE_NAME_MAP = {
    "temp": "temperature",
    "sal": "salinity",
}


def download_file(url: str, dest: str) -> None:
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def find_time_dim(ds: xr.Dataset) -> str:
    # prefer a coord named time-like with datetime dtype
    for name, coord in ds.coords.items():
        name_str = str(name)
        if "time" in name_str.lower() and np.issubdtype(coord.dtype, np.datetime64):
            return name_str
    # fallback: any datetime coord
    for name, coord in ds.coords.items():
        if np.issubdtype(coord.dtype, np.datetime64):
            return str(name)
    raise RuntimeError("No datetime coordinate found for time dimension")


def parse_depth_var(name: str) -> Tuple[str, str] | None:
    m = re.match(r"^(?P<base>[A-Za-z]+?)_(?P<suffix>.+)$", name)
    if not m:
        return None
    base = m.group("base").lower()
    suffix = m.group("suffix")
    return base, suffix


def group_depth_vars(ds: xr.Dataset) -> Dict[str, List[Tuple[str, str]]]:
    groups: Dict[str, List[Tuple[str, str]]] = {}
    for var in ds.data_vars:
        var_name = str(var)
        parsed = parse_depth_var(var_name)
        if parsed:
            base, suffix = parsed
            base_name = BASE_NAME_MAP.get(base, base)
            groups.setdefault(base_name, []).append((suffix, var_name))
    return groups


def normalize_depth_meta(depths_meta: Iterable[float | str]) -> Tuple[dict[str, int], dict[str, float]]:
    order: dict[str, int] = {}
    raw_values: dict[str, float | str] = {}
    numeric_values: list[float] = []
    for idx, entry in enumerate(depths_meta):
        key = str(entry)
        order[key] = idx
        if isinstance(entry, (int, float)):
            value = float(entry)
            raw_values[key] = value
            numeric_values.append(value)
        else:
            raw_values[key] = str(entry)

    values: dict[str, float] = {}
    max_numeric = max(numeric_values) if numeric_values else 0.0
    for key, raw in raw_values.items():
        if isinstance(raw, (int, float)):
            values[key] = float(raw)
        else:
            low = raw.lower()
            if low == "surface":
                values[key] = 0.0
            elif low == "bottom":
                values[key] = max_numeric
            else:
                try:
                    values[key] = float(raw)
                except ValueError:
                    values[key] = float("nan")
    return order, values


def format_time_token(t: np.datetime64) -> str:
    s = np.datetime_as_string(t, unit="m")  # YYYY-MM-DDTHH:MM
    return s.replace("-", "").replace(":", "")


def split_times_by_day(times: np.ndarray) -> Dict[str, np.ndarray]:
    day_map: Dict[str, List[np.datetime64]] = {}
    for t in times:
        day = np.datetime_as_string(t, unit="D")
        day_map.setdefault(day, []).append(t)
    return {k: np.array(v) for k, v in day_map.items()}


def build_depth_merged(
    ds: xr.Dataset,
    time_dim: str,
    base_name: str,
    items: List[Tuple[str, str]],
    depth_order: dict[str, int] | None = None,
    depth_values: dict[str, float] | None = None,
) -> xr.Dataset:

    def sort_key(pair: Tuple[str, str]):
        suffix = pair[0]
        if depth_order and suffix in depth_order:
            return (depth_order[suffix], 0.0)
        try:
            return (len(depth_order or {}), float(suffix))
        except ValueError:
            return (len(depth_order or {}), float("nan"))

    items_sorted = sorted(items, key=sort_key)
    depths: List[float] = []
    arrays = []
    for suffix, var in items_sorted:
        da = ds[var]
        depth_val = None
        if depth_values and suffix in depth_values:
            depth_val = depth_values[suffix]
        if depth_val is None:
            try:
                depth_val = float(suffix)
            except ValueError:
                depth_val = float(len(depths))
        depths.append(depth_val)
        arrays.append(da.expand_dims({"depth": [depth_val]}))
    merged = xr.concat(arrays, dim="depth")
    merged = merged.assign_coords(depth=("depth", depths))
    merged.name = base_name
    return xr.Dataset({base_name: merged})


def write_daily_outputs(
    ds: xr.Dataset,
    out_root: str,
    depth_order_meta: Iterable[float | str] | None = None,
) -> List[dict]:
    time_dim = find_time_dim(ds)
    times = ds[time_dim].values
    day_map = split_times_by_day(times)

    groups = group_depth_vars(ds)
    if not groups:
        raise RuntimeError("No depth-suffixed variables found (e.g., temp_10)")

    depth_order = None
    depth_values = None
    if depth_order_meta:
        depth_order, depth_values = normalize_depth_meta(depth_order_meta)

    outputs: List[dict] = []

    for base_name, items in groups.items():
        merged = build_depth_merged(
            ds,
            time_dim,
            base_name,
            items,
            depth_order=depth_order,
            depth_values=depth_values,
        )
        out_dir = os.path.join(out_root, base_name)
        os.makedirs(out_dir, exist_ok=True)

        for day, day_times in day_map.items():
            day_times_sorted = np.sort(day_times)
            start = day_times_sorted[0]
            end = day_times_sorted[-1]
            token_start = format_time_token(start)
            token_end = format_time_token(end)
            out_fn = f"{base_name}_{token_start}_{token_end}.nc"
            out_path = os.path.join(out_dir, out_fn)
            subset = merged.sel({time_dim: day_times_sorted})
            subset.to_netcdf(out_path)
            outputs.append(
                {
                    "variable": base_name,
                    "start_time": start,
                    "end_time": end,
                    "path": out_path,
                }
            )

    return outputs


def process_live_ocean(
    url: str,
    input_path: str,
    out_dir: str,
    skip_download: bool = False,
    depth_order_meta: Iterable[float | str] | None = None,
) -> List[dict]:
    if not skip_download:
        download_file(url, input_path)

    if not os.path.exists(input_path):
        raise RuntimeError(f"Input file not found: {input_path}")

    ds = xr.open_dataset(input_path)
    try:
        outputs = write_daily_outputs(ds, out_dir, depth_order_meta=depth_order_meta)
    finally:
        ds.close()

    return outputs


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Live Ocean: download and split layers.nc")
    p.add_argument("--url", default=DEFAULT_URL, help="Download URL for layers.nc")
    p.add_argument("--input", default=DEFAULT_INPUT, help="Path to layers.nc")
    p.add_argument("--out-dir", default=DEFAULT_OUT, help="Output root directory for per-variable files")
    p.add_argument("--skip-download", action="store_true", help="Skip download and use existing input file")
    args = p.parse_args(argv)

    process_live_ocean(
        url=args.url,
        input_path=args.input,
        out_dir=args.out_dir,
        skip_download=args.skip_download,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
