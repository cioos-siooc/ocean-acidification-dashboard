#!/usr/bin/env python3
"""Live Ocean pipeline with multi-stage processing and parallel workers.

Stages:
- download: Fetch layers.nc from URL
- extract: Extract variables into daily NetCDF files
- image: Generate WebP tiles
- all: Run all stages (default)

Each stage has its own status tracking in the database for resumability.
"""

from __future__ import annotations

import argparse
import gc
import json
import logging
import os
import re
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np
import requests
import xarray as xr

from db import (
    get_db_conn,
    init_schema,
    extract_source_date_from_url,
    get_last_processed_date,
    update_file_status,
    insert_processed_dates,
    get_extracted_files_for_source_date,
)
from lo_imaging import image_live_ocean, image_live_ocean_parallel

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("liveocean")

# Fixed URLs and paths (override via environment variables)
URL_BASE = "https://s3.kopah.uw.edu/liveocean-share"
NC_OUTPUT_DIR = os.getenv("LO_NC_DIR", "/opt/data/LO/nc")
IMAGE_OUTPUT_DIR = os.getenv("LO_IMAGE_DIR", "/opt/data/LO/images")
VARS_JSON_PATH = os.path.join(os.path.dirname(__file__), "lo_vars.json")
INPUT_FILE_TEMP = "/tmp/layers.nc"

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
    # Check for direct 'ocean_time' first as per ncdump
    if "ocean_time" in ds.dims or "ocean_time" in ds.coords:
        return "ocean_time"
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
    vars_config: dict[str, Any],
) -> List[dict]:
    time_dim = find_time_dim(ds)
    times = ds[time_dim].values
    day_map = split_times_by_day(times)

    outputs: List[dict] = []

    for field_name, cfg in vars_config.items():
        nc_base = cfg.get("ncVariable", field_name)
        depth_list = cfg.get("depths", [])
        
        out_dir = os.path.join(out_root, field_name)
        os.makedirs(out_dir, exist_ok=True)

        # Process day by day to minimize memory footprint
        for day, day_times in day_map.items():
            day_times_sorted = np.sort(day_times)
            token = day.replace("-", "") # YYYYMMDD
            out_fn = f"{field_name}_{token}.nc"
            out_path = os.path.join(out_dir, out_fn)
            
            arrays = []
            actual_depths = []
            
            for d in depth_list:
                # Convert depth to suffix and numeric value
                if isinstance(d, str):
                    suffix = d
                    if d.lower() == "surface":
                        depth_val = 0.0
                    elif d.lower() == "bottom":
                        depth_val = 99999.0
                    else:
                        try:
                            depth_val = float(d)
                        except ValueError:
                            print(f"Warning: Cannot convert depth '{d}' to numeric. Skipping.")
                            continue
                else:
                    # Numeric depth
                    suffix = "surface" if d == 0 else str(int(d))
                    depth_val = float(d)
                
                var_name = f"{nc_base}_{suffix}"
                
                if var_name not in ds.data_vars:
                    # Try capitalization if needed (e.g. PH_surface)
                    if f"{nc_base.upper()}_{suffix}" in ds.data_vars:
                        var_name = f"{nc_base.upper()}_{suffix}"
                    else:
                        print(f"Warning: {var_name} not found in dataset. Skipping depth {d}.")
                        continue
                
                # Select only this day's data for this variable
                da = ds[var_name].sel({time_dim: day_times_sorted})
                # Rename ocean_time to time
                da = da.rename({time_dim: "time"})
                # Expand depth dimension
                da = da.expand_dims({"depth": [depth_val]})
                arrays.append(da)
                actual_depths.append(depth_val)
            
            if not arrays:
                print(f"Warning: No data found for field {field_name} on {day}. Skipping.")
                continue

            # Merge all depths into one DataArray
            merged_da = xr.concat(arrays, dim="depth")
            merged_da.name = field_name
            
            # Build dataset for this day (variables only, no spatial coords)
            day_ds = xr.Dataset({field_name: merged_da})
            
            # Transpose to have time as first dimension: (time, depth, eta_rho, xi_rho)
            day_ds[field_name] = day_ds[field_name].transpose("time", "depth", "eta_rho", "xi_rho")
            
            # Use compression
            encoding = {field_name: {"zlib": True, "complevel": 4}}
            day_ds.to_netcdf(out_path, encoding=encoding)
            
            outputs.append({
                "variable": field_name,
                "date": token,
                "path": out_path,
            })
            
            # Explicit cleanup
            del arrays, actual_depths, merged_da, day_ds
            import gc
            gc.collect()

    return outputs


def get_next_source_date_from_db(conn) -> str:
    """Get the next date to process from database.
    
    Finds the latest source_date in live_ocean_files table,
    adds 1 day, and returns it as YYYY-MM-DD string.
    If no records exist, returns today's date.
    
    Args:
        conn: Database connection
    
    Returns:
        Date string in YYYY-MM-DD format
    """
    from datetime import datetime, timedelta
    
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(source_date) FROM live_ocean_files")
        result = cur.fetchone()
        last_date = result[0] if result and result[0] else None
    
    if last_date:
        next_date = last_date + timedelta(days=1)
    else:
        next_date = datetime.now().date()
    
    return next_date.strftime("%Y-%m-%d")


def date_to_url(date_str: str) -> str:
    """Convert date string (YYYY-MM-DD) to Live Ocean S3 URL.
    
    Example: "2026-02-04" -> "https://s3.kopah.uw.edu/liveocean-share/f2026.02.04/layers.nc"
    """
    year, month, day = date_str.split('-')
    return f"{URL_BASE}/f{year}.{month}.{day}/layers.nc"


def stage_download(url: str, input_path: str) -> bool:
    """Download stage. Returns True on success."""
    try:
        logger.info(f"Downloading from: {url}")
        download_file(url, input_path)
        logger.info("Download complete")
        return True
    except Exception as e:
        logger.error(f"Download failed: {e}")
        raise


def stage_extract(input_path: str, out_dir: str, vars_json: str, source_date: str, conn) -> List[dict]:
    """Extract stage. Returns list of output file dicts."""
    try:
        logger.info(f"Starting extraction for source_date: {source_date}...")
        
        if not os.path.exists(input_path):
            raise RuntimeError(f"Input file not found: {input_path}")
        
        with open(vars_json, "r") as f:
            vars_config = json.load(f)
        
        ds = xr.open_dataset(input_path)
        try:
            outputs = write_daily_outputs(ds, out_dir, vars_config=vars_config)
        finally:
            # Aggressive xarray cleanup
            ds.close()
            
            # Explicitly close backend store to release HDF5 file handle
            if hasattr(ds, '_file_obj') and ds._file_obj is not None:
                try:
                    ds._file_obj.close()
                except Exception:
                    pass
            
            del ds
            gc.collect()
        
        logger.info(f"Extraction complete: {len(outputs)} output files")
        
        # Insert into database (creates records with status='extracted')
        insert_processed_dates(conn, source_date, outputs)
        
        # Aggressive cleanup: delete input file and force garbage collection
        if os.path.exists(input_path):
            os.remove(input_path)
            logger.info(f"Deleted input file: {input_path}")
        
        # Make a copy of outputs to return, then clear the reference
        result = outputs[:]
        del outputs
        gc.collect()
        logger.info("Memory cleanup completed")
        
        return result
    
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise


def stage_image(outputs: List[dict], workers: int, image_root: str, source_date: str, conn) -> List[str]:
    """Image generation stage. Returns list of generated WebP paths."""
    try:
        logger.info(f"Starting image generation with {workers} worker(s)...")
        
        if workers > 1:
            image_paths = image_live_ocean_parallel(outputs, workers=workers, image_root=image_root, source_date=source_date)
        else:
            image_paths = image_live_ocean(outputs, image_root=image_root)
        
        logger.info(f"Image generation complete: {len(image_paths)} WebP tiles")
        
        # NOTE: Files are updated to 'success' as each completes (in parallel worker).
        # No need to batch-update here.
        
        # Cleanup outputs reference
        del outputs
        gc.collect()
        logger.info("Post-imaging memory cleanup completed")
        
        return image_paths
    
    except Exception as e:
        logger.error(f"Image generation failed: {e}")
        raise


def run_pipeline(
    stage: str = "all",
    workers: int = 4,
    date: str | None = None,
) -> None:
    """Run the Live Ocean pipeline with fixed paths and automatic date progression.
    
    Args:
        stage: Which stage to run ('download', 'extract', 'image', or 'all')
        workers: Number of parallel workers for imaging (default: 4)
        date: Specific date to process (YYYY-MM-DD). Required for 'extract' and 'image' stages.
              If None with 'download' or 'all', uses day after last processed date in DB.
    """
    conn = None
    outputs = []
    
    try:
        # Initialize database
        conn = get_db_conn()
        init_schema(conn)
        
        # Validate date requirement
        if stage in ("extract", "image") and not date:
            raise ValueError(f"--date is required for stage '{stage}'. Specify YYYY-MM-DD format.")
        
        # Get date to process
        if date:
            source_date = date
            logger.info(f"Using provided date: {source_date}")
        else:
            source_date = get_next_source_date_from_db(conn)
            logger.info(f"Using next date from DB: {source_date}")
        
        url = date_to_url(source_date)
        logger.info(f"Processing Live Ocean for source date: {source_date}, stage: {stage}")
        logger.info(f"URL: {url}")
        
        # Auto-determine skip_download: skip if stage is extract or image
        skip_download = stage in ("extract", "image")
        
        # Run requested stages
        if stage in ("download", "all"):
            if not skip_download:
                stage_download(url, INPUT_FILE_TEMP)
            else:
                logger.info("Skipping download stage")
        
        if stage in ("extract", "all"):
            # Ensure file exists
            if not os.path.exists(INPUT_FILE_TEMP):
                if stage == "extract":
                    raise RuntimeError(f"Input file not found: {INPUT_FILE_TEMP}. Run download stage first.")
                # For all, download will have happened above
            outputs = stage_extract(INPUT_FILE_TEMP, NC_OUTPUT_DIR, VARS_JSON_PATH, source_date, conn)
            logger.info(f"After extraction: {len(outputs)} files to process")
            gc.collect()
        
        if stage in ("image", "all"):
            # Load outputs from database if we didn't just extract
            if not outputs:
                logger.info("Querying database for extracted files...")
                outputs = get_extracted_files_for_source_date(conn, source_date)
                # Convert file_date to date format expected by imaging
                for out in outputs:
                    out["date"] = out["file_date"].replace("-", "")
                logger.info(f"Loaded from DB: {len(outputs)} files to process")
            
            # Aggressive cleanup before imaging
            gc.collect()
            logger.info("Pre-imaging garbage collection completed")
            
            if outputs:
                stage_image(outputs, workers, IMAGE_OUTPUT_DIR, source_date, conn)
            else:
                logger.warning("No extracted files to image. Check that extraction completed successfully.")
        
        logger.info(f"Pipeline complete for source_date {source_date}")
    
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        raise
    
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Live Ocean: multi-stage pipeline with parallel workers")
    p.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date to process in YYYY-MM-DD format. Required for 'extract' and 'image' stages. If not provided with 'download' or 'all', uses day after last processed date in DB."
    )
    p.add_argument(
        "--stage",
        choices=["download", "extract", "image", "all"],
        default="all",
        help="Which stage(s) to run: 'download', 'extract', 'image', or 'all' (default)"
    )
    p.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers for imaging (default: 4)"
    )
    args = p.parse_args(argv)

    run_pipeline(
        stage=args.stage,
        workers=args.workers,
        date=args.date,
    )

    return 0



if __name__ == "__main__":
    raise SystemExit(main())
