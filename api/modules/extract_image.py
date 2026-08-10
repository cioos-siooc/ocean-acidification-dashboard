#!/usr/bin/env python3
"""extract_image.py

On-demand WebP tile generation for GET /png, straight from ClickHouse — for
dates/bin-modes the process/SSC pipeline hasn't (or, for daily/monthly, never
will) pre-rendered. See shared/nc2tile.py's render_tile_from_db for the
actual rendering; this module is the HTTP-layer glue: parsing the route's
`dt`/`depth` path segments and picking the right ClickHouse query shape.

Examples:
  python modules/extract_image.py --source SalishSeaCast --var temperature \
      --dt 2024-01-15 --depth 0.5
"""
from __future__ import annotations

import argparse
from datetime import datetime
import re
from typing import Optional

import shared.nc2tile as nc2tile

from modules.extractTimeseries import ALLOWED_VARIABLES

# Dash-separated, matching the pipeline's own hourly folder convention
# (2024-01-15T003000) so all three bin modes read as the same date format at
# a glance instead of daily/monthly looking like a different scheme.
_DAILY_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
_MONTHLY_RE = re.compile(r'^\d{4}-\d{2}$')


def resolve_bin_mode(dt: str) -> str:
    """Infer bin_mode from the shape of the `dt` path segment.

    `2024-01-15` (YYYY-MM-DD) -> daily; `2024-01` (YYYY-MM) -> monthly;
    anything else (the existing `2024-01-15T003000`-style hourly folder name)
    -> hourly. Keeps the URL self-describing — no separate bin_mode param
    that could drift out of sync with it.
    """
    if _DAILY_RE.match(dt):
        return 'daily'
    if _MONTHLY_RE.match(dt):
        return 'monthly'
    return 'hourly'


def parse_depth_label(depth: str) -> Optional[float]:
    """Parse the `depth` path segment (e.g. "18.0") into a float.

    Returns None for "bottom" (the per-cell deepest-valid-layer pseudo-depth
    — not a fixed value, so not supported for on-demand generation) or any
    other unparseable string; callers should treat that as "can't generate,
    fall through to the usual 404".
    """
    try:
        return float(depth)
    except (TypeError, ValueError):
        return None


def _time_param_for(bin_mode: str, dt: str):
    if bin_mode == 'daily':
        return datetime.strptime(dt, '%Y-%m-%d').date()
    if bin_mode == 'monthly':
        return datetime.strptime(dt, '%Y-%m').date()
    return datetime.strptime(dt, '%Y-%m-%dT%H%M%S')


def generate_image(
    source: str,
    var: str,
    dt: str,
    depth: str,
    image_root: Optional[str] = None,
) -> Optional[str]:
    """Render (and cache to disk) the WebP tile for source/var/dt/depth.

    Returns the written path on success, or None if the request can't be
    resolved to a generatable tile (unknown var, unparseable depth/dt, or no
    ClickHouse rows for that variable/date/depth) — callers should treat
    None the same as a cache miss that stays a 404, not a server error.
    """
    if source != 'SalishSeaCast' or var not in ALLOWED_VARIABLES:
        return None

    depth_val = parse_depth_label(depth)
    if depth_val is None:
        return None

    bin_mode = resolve_bin_mode(dt)
    try:
        time_param = _time_param_for(bin_mode, dt)
    except ValueError:
        return None

    return nc2tile.render_tile_from_db(
        variable=var,
        bin_mode=bin_mode,
        time_param=time_param,
        depth=depth_val,
        depth_label=depth,
        dt_folder=dt,
        image_root=image_root,
    )


def main(argv: Optional[list] = None) -> int:
    p = argparse.ArgumentParser(description="Generate a WebP tile on demand from ClickHouse")
    p.add_argument("--source", default="SalishSeaCast")
    p.add_argument("--var", "-v", required=True, choices=sorted(ALLOWED_VARIABLES))
    p.add_argument("--dt", required=True, help="2024-01-15T003000 (hourly), 2024-01-15 (daily), or 2024-01 (monthly)")
    p.add_argument("--depth", required=True, help="e.g. 18.0")
    args = p.parse_args(argv)

    out_path = generate_image(args.source, args.var, args.dt, args.depth)
    if out_path is None:
        print("No data available for this source/var/dt/depth combination.")
        return 1
    print(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
