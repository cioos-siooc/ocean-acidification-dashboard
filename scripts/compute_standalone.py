#!/usr/bin/env python3
"""Standalone compute script - no database required.

Mirrors the logic of compute.py / compute_for_group(), but accepts a date
directly from the user instead of pulling pending jobs from the DB.

Invokes calc_carbon_grid_shm_memmap.py (the same script the process service uses)
to compute ph_total, omega_arag, and omega_cal from downloaded NetCDF files.

Usage:
  python compute_standalone.py --date 2025-01-01
  python compute_standalone.py --date 2025-01-01 --mode sharedmem --workers 4
  python compute_standalone.py --date 2025-01-01 --base-dir /opt/data/nc --mode memmap --workers 2
"""

from __future__ import annotations
import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from glob import glob

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("compute_standalone")

COMPUTE_VARS = ["ph_total", "omega_arag", "omega_cal"]
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CALC_SCRIPT = os.path.join(SCRIPT_DIR, "calc_carbon_grid_shm_memmap.py")


def find_input_files(base_dir: str, date_token: str) -> dict[str, str]:
    """Find the four input NetCDF files matching the given date token (YYYYMMDD).

    Returns a dict with keys DIC, TA, Temp, Sal, or raises RuntimeError if not found.
    """
    dic_dir = os.path.join(base_dir, "dissolved_inorganic_carbon")
    if not os.path.exists(dic_dir):
        raise RuntimeError(f"DIC directory not found: {dic_dir}")

    dic_files = sorted(glob(os.path.join(dic_dir, "*.nc")))
    matching_dic = [f for f in dic_files if date_token in os.path.basename(f)]

    if not matching_dic:
        raise RuntimeError(
            f"No DIC files found for date token '{date_token}' in {dic_dir}"
        )

    # Use the first matching DIC file
    dic_file = matching_dic[0]
    base = os.path.basename(dic_file)
    logger.info(f"Found DIC file: {base}")

    # Find matching TA/Temp/Sal files using the same date token
    candidates = glob(os.path.join(base_dir, "*", f"*{date_token}*.nc"))
    found: dict[str, str] = {}
    for c in candidates:
        if "dissolved_inorganic_carbon" in c:
            found["DIC"] = c
        elif "total_alkalinity" in c:
            found["TA"] = c
        elif "temperature" in c.lower() or "temp" in c.lower():
            found["Temp"] = c
        elif "salinity" in c.lower() or "salt" in c.lower():
            found["Sal"] = c

    missing = [k for k in ("DIC", "TA", "Temp", "Sal") if k not in found]
    if missing:
        raise RuntimeError(
            f"Could not find input files for: {missing}. "
            f"Available files matching '{date_token}':\n  "
            + "\n  ".join(candidates)
        )

    for k, v in found.items():
        logger.info(f"  {k}: {os.path.basename(v)}")

    return found


def verify_outputs(base_dir: str, date_token: str) -> list[str]:
    """Verify that output files exist. Returns list of output paths."""
    out_paths = []
    for var in COMPUTE_VARS:
        out_dir = os.path.join(base_dir, var)
        matches = glob(os.path.join(out_dir, f"*{date_token}*.nc"))
        if not matches:
            raise RuntimeError(
                f"Expected output file not found for variable '{var}' "
                f"matching date '{date_token}' in {out_dir}"
            )
        out_paths.append(matches[0])
        logger.info(f"Output verified: {os.path.basename(matches[0])}")
    return out_paths


def compute(
    date_str: str,
    base_dir: str,
    mode: str = "memmap",
    workers: int = 2,
    depth_batch_size: int = 8,
    overwrite: bool = False,
    worker_timeout: int = 1800,
    python_exec: str = sys.executable,
) -> bool:
    """Run carbonate computation for the given date.

    Args:
        date_str: ISO date string, e.g. '2025-01-01'
        base_dir: Base directory containing variable subdirectories
        mode: 'sharedmem' or 'memmap'
        workers: Number of parallel workers
        depth_batch_size: Depth batch size for workers
        overwrite: Overwrite existing output files
        python_exec: Python executable to use (defaults to current interpreter)
    """
    # Parse and reformat date: accept 2025-01-01 or 20250101
    try:
        if "-" in date_str:
            dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        else:
            dt = datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
    except ValueError as e:
        raise ValueError(
            f"Invalid date format '{date_str}'. Use YYYY-MM-DD or YYYYMMDD."
        ) from e

    date_token = dt.strftime("%Y%m%d")
    logger.info(f"Computing for date: {dt.strftime('%Y-%m-%d')} (token: {date_token})")
    logger.info(f"Base dir: {base_dir}")
    logger.info(f"Mode: {mode}, Workers: {workers}, Depth batch: {depth_batch_size}")

    # Verify inputs exist before launching subprocess
    find_input_files(base_dir, date_token)

    # Build command - same as compute.py does
    cmd = [
        python_exec,
        CALC_SCRIPT,
        "--date", date_token,
        "--base-dir", base_dir,
        "--mode", mode,
        "--workers", str(workers),
        "--depth-batch-size", str(depth_batch_size),
        "--worker-timeout", str(worker_timeout),
    ]
    if overwrite:
        cmd.append("--overwrite")

    logger.info(f"Running: {' '.join(cmd)}")

    res = subprocess.run(cmd, check=False)
    if res.returncode != 0:
        logger.error(f"Compute subprocess failed with return code {res.returncode}")
        return False

    # Verify outputs
    try:
        out_paths = verify_outputs(base_dir, date_token)
        logger.info(f"Success! {len(out_paths)} output files written:")
        for p in out_paths:
            size_mb = os.path.getsize(p) / (1024 ** 2)
            logger.info(f"  {p}  ({size_mb:.1f} MB)")
    except RuntimeError as e:
        logger.error(f"Output verification failed: {e}")
        return False

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Standalone carbonate compute - computes pH, omega_arag, omega_cal from downloaded NetCDF files."
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Date to process, e.g. 2025-01-01 or 20250101",
    )
    parser.add_argument(
        "--base-dir",
        default=os.getenv("DATA_DIR", "/opt/data/nc"),
        help="Base directory containing variable subdirectories (default: $DATA_DIR or /opt/data/nc)",
    )
    parser.add_argument(
        "--mode",
        choices=["sharedmem", "memmap"],
        default="memmap",
        help="Data sharing mode (default: memmap)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=2,
        help="Number of parallel workers (default: 2)",
    )
    parser.add_argument(
        "--depth-batch-size",
        type=int,
        default=8,
        help="Depth batch size for workers (default: 8)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing output files",
    )
    parser.add_argument(
        "--worker-timeout",
        type=int,
        default=1800,
        help="Seconds to wait for a single timestep worker (default: 1800)",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python executable to use (default: current interpreter)",
    )
    args = parser.parse_args()

    if not os.path.exists(CALC_SCRIPT):
        logger.error(f"Compute script not found: {CALC_SCRIPT}")
        sys.exit(1)

    success = compute(
        date_str=args.date,
        base_dir=args.base_dir,
        mode=args.mode,
        workers=args.workers,
        depth_batch_size=args.depth_batch_size,
        overwrite=args.overwrite,
        worker_timeout=args.worker_timeout,
        python_exec=args.python,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
