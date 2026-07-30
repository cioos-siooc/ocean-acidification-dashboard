#!/usr/bin/env python3
"""extract_profile.py

Extract a vertical profile (depth vs. value) for an ocean variable at a
coordinate and datetime, from ClickHouse. Mirrors extractTimeseries.py's
ClickHouse pattern but fixes time and returns all depth levels instead of
fixing depth and returning all times.

Examples:
  python modules/extract_profile.py --source SalishSeaCast --var temperature \
      --lat 49.2 --lon -123.5 --dt 2023-01-15T12:00:00
"""
from __future__ import annotations

import argparse
from typing import List, Optional

import pandas as pd

from modules.clickhouse_helpers import get_ch_client
from modules.extractTimeseries import (
    ALLOWED_VARIABLES,
    DATA_TABLE_BY_SOURCE,
    GRID_TABLE_BY_SOURCE,
    _find_nearest_grid_point,
)

# Requested `dt` must resolve to an available hourly row within this many
# seconds, else we treat it as "no data at that time" rather than silently
# snapping to a far-off hour.
TIME_MATCH_TOLERANCE_S = 3600


def _find_nearest_time(client, table: str, grid_x: int, grid_y: int, target: pd.Timestamp) -> pd.Timestamp:
    """Return the available `time` at (grid_x, grid_y) nearest to `target`.

    Raises RuntimeError if the cell has no rows at all, or if the nearest
    available time is farther than TIME_MATCH_TOLERANCE_S away.
    """
    query = f"""
        SELECT time, abs(dateDiff('second', time, toDateTime(%(target)s))) AS diff_s
        FROM {table}
        WHERE gridX = %(gx)s AND gridY = %(gy)s
        ORDER BY diff_s ASC
        LIMIT 1
    """
    result = client.query(query, parameters={"target": target, "gx": grid_x, "gy": grid_y})
    if not result.result_rows:
        raise RuntimeError(
            f"No data available for grid point (gridX={grid_x}, gridY={grid_y}) at any time."
        )
    nearest_time, diff_s = result.result_rows[0]
    if diff_s > TIME_MATCH_TOLERANCE_S:
        raise RuntimeError(
            f"Requested datetime {target} has no matching data within "
            f"{TIME_MATCH_TOLERANCE_S}s at this location; nearest available time is "
            f"{nearest_time} ({diff_s}s away)."
        )
    return pd.Timestamp(nearest_time)


def extract_profile(
    *,
    source: str,
    var: str,
    lat: float,
    lon: float,
    dt: str,
    verbose: bool = False,
) -> List[dict]:
    """Extract a vertical profile for `var` at (lat, lon, dt) from ClickHouse.

    Returns a list of {"depth": float, "value": float} dicts, sorted
    ascending by depth. Raises RuntimeError/ValueError on bad input or
    out-of-domain coordinates/time (caller maps these to HTTP 400/500).
    """
    if source not in DATA_TABLE_BY_SOURCE:
        raise RuntimeError(
            f"Source '{source}' is not yet available via ClickHouse. "
            f"Supported sources: {sorted(DATA_TABLE_BY_SOURCE)}"
        )
    if var not in ALLOWED_VARIABLES:
        raise ValueError(f"Unknown variable '{var}'. Supported variables: {sorted(ALLOWED_VARIABLES)}")

    try:
        target_dt = pd.to_datetime(dt)
    except Exception as exc:
        raise ValueError(f"Could not parse datetime '{dt}': {exc}")

    table = DATA_TABLE_BY_SOURCE[source]
    grid_table = GRID_TABLE_BY_SOURCE[source]

    client = get_ch_client()
    try:
        grid_x, grid_y, grid_lat, grid_lon = _find_nearest_grid_point(client, grid_table, lat, lon)
        if verbose:
            print(f"Nearest grid point: gridX={grid_x} gridY={grid_y} lat={grid_lat} lon={grid_lon}", flush=True)

        resolved_time = _find_nearest_time(client, table, grid_x, grid_y, target_dt)
        if verbose:
            print(f"Resolved time: requested={target_dt} -> nearest={resolved_time}", flush=True)

        query = f"""
            SELECT depth, {var} AS value
            FROM {table}
            WHERE gridX = %(gx)s AND gridY = %(gy)s AND time = %(t)s
            ORDER BY depth ASC
        """
        result = client.query(query, parameters={"gx": grid_x, "gy": grid_y, "t": resolved_time})
        # ingest.py already filters NaN/0/land rows out of the table, so no
        # client-side value filtering is needed here (unlike the old NetCDF path).
        return [{"depth": float(d), "value": float(v)} for d, v in result.result_rows]
    finally:
        client.close()


def main(argv: Optional[list] = None) -> int:
    p = argparse.ArgumentParser(description="Extract a vertical profile for an ocean variable at a coordinate/datetime from ClickHouse")
    p.add_argument("--source", default="SalishSeaCast", choices=sorted(DATA_TABLE_BY_SOURCE), help="Data source")
    p.add_argument("--var", "-v", required=True, choices=sorted(ALLOWED_VARIABLES), help="Variable name to extract")
    p.add_argument("--lat", "-a", type=float, required=True, help="Latitude (required)")
    p.add_argument("--lon", "-o", type=float, required=True, help="Longitude (required)")
    p.add_argument("--dt", required=True, help="Target datetime (ISO-8601-ish string)")
    p.add_argument("--verbose", "-V", action="store_true", help="Verbose output")

    args = p.parse_args(argv)

    profile = extract_profile(
        source=args.source,
        var=args.var,
        lat=args.lat,
        lon=args.lon,
        dt=args.dt,
        verbose=args.verbose,
    )
    print("depth,value")
    for row in profile:
        print(f"{row['depth']},{row['value']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
