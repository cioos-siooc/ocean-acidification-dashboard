#!/usr/bin/env python3
"""Download a NetCDF file from an ERDDAP/griddap URL with retries and progress.

Usage examples:

# Use the default URL (embedded in the script)
python process/main.py

# Specify URL and output path
python process/main.py -u "https://.../ubcSSg3DChemistryFields1hV21-11.nc?dissolved_inorganic_carbon[...]" -o /tmp/my.nc

The script:
- streams the response to disk to avoid high memory usage
- supports retries for transient HTTP errors
- shows a progress bar (if `tqdm` is installed)
- writes to a temporary `.part` file and atomically renames on success
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Optional

import requests
from requests.adapters import HTTPAdapter, Retry

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover - optional dependency
    tqdm = None


def _derive_filename_from_url(url: str) -> str:
    # get path portion before query
    base = url.split("?")[0]
    return os.path.basename(base) or "download.nc"


def download(
    variable: str,
    from_date: str,
    to_date: str,
    out_path: str,
    chunk_size: int = 1024 * 32,
    max_retries: int = 5,
    backoff_factor: float = 0.5,
    timeout: int = 60,
) -> str:
    """Download `url` to `out_path` and return the path on success.

    The function streams to a temporary .part file and moves it into place when done.
    It uses requests + Retry to automatically retry transient errors.
    """

    url = (
        "https://salishsea.eos.ubc.ca/erddap/griddap/ubcSSg3DChemistryFields1hV21-11.nc"
        f"?{variable}%5B({from_date}):1:({to_date})%5D%5B(0.5000003):1:(441.4661)%5D%5B(0.0):1:(897.0)%5D%5B(0.0):1:(397.0)%5D"
    )

    session = requests.Session()
    retries = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))

    tmp_path = out_path + ".part"

    with session.get(url, stream=True, timeout=timeout) as resp:
        resp.raise_for_status()
        total = resp.headers.get("Content-Length")
        total = int(total) if total and total.isdigit() else None

        # Ensure parent dir exists
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

        with open(tmp_path, "wb") as f:
            if tqdm and total:
                pbar = tqdm(
                    total=total,
                    unit="B",
                    unit_scale=True,
                    desc=os.path.basename(out_path),
                )
            elif tqdm:
                pbar = tqdm(unit="B", unit_scale=True, desc=os.path.basename(out_path))
            else:
                pbar = None

            try:
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    f.write(chunk)
                    if pbar:
                        pbar.update(len(chunk))
            finally:
                if pbar:
                    pbar.close()

    # move into final path atomically
    os.replace(tmp_path, out_path)
    return out_path


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Download a NetCDF file from an ERDDAP/griddap URL"
    )
    p.add_argument(
        "--variable", help="Variable name to download", required=True, dest="variable"
    )
    p.add_argument(
        "--from", help="Start date (ISO format)", required=True, dest="from_date"
    )
    p.add_argument(
        "--to", help="End date (ISO format)", required=True, dest="to_date"
    )
    p.add_argument(
        "-f", "--force", action="store_true", help="Overwrite file if it exists"
    )
    p.add_argument(
        "--retries",
        type=int,
        default=5,
        help="Maximum number of retries for transient errors",
    )
    p.add_argument("--timeout", type=int, default=60, help="Request timeout (seconds)")
    p.add_argument(
        "--chunk-size", type=int, default=1024 * 32, help="Download chunk size in bytes"
    )
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    out = f"/opt/data/nc/{args.variable}_{args.from_date}_to_{args.to_date}.nc"

    if os.path.exists(out) and not args.force:
        print(f"File exists: {out} (use --force to overwrite)")
        return 0

    try:
        print(f"Downloading:\n  From: {args.from_date}\n  To: {args.to_date}\n  -> {out}")
        path = download(
            args.variable,
            args.from_date,
            args.to_date,
            out,
            chunk_size=args.chunk_size,
            max_retries=args.retries,
            timeout=args.timeout,
        )
        print(f"Downloaded to: {path}")
        return 0
    except KeyboardInterrupt:
        print("Download cancelled by user", file=sys.stderr)
        try:
            if os.path.exists(out + ".part"):
                os.remove(out + ".part")
        except Exception:
            pass
        return 2
    except Exception as exc:  # noqa: BLE001 - top-level script handler
        print(f"Error downloading file: {exc}", file=sys.stderr)
        # try to remove partial
        try:
            if os.path.exists(out + ".part"):
                os.remove(out + ".part")
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
