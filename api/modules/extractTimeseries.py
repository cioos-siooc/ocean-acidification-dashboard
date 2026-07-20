#!/usr/bin/env python3
"""extractTimeseries.py

Extract a time series for an ocean variable at a coordinate from ClickHouse.

For each supported `source`, the nearest grid cell to (lat, lon) is looked up
in that source's grid table (e.g. `grid_SSC`), then the requested variable is
read from the source's 4D data table (e.g. `SalishSeaCast_hourly`) for that
grid cell over the requested time range.

When `depth` is given, the available depth level nearest to it is selected
(use `depth=-1` to select the deepest/bottom level) and a single (time,
value) series is returned. When `depth` is omitted, every depth level is
returned as a DataFrame with `time`, `depth`, `value` columns.

Each row in the hourly table already holds one raw value per hour, so no
mean/min/max aggregation is involved.

Examples:
  python modules/extractTimeseries.py --source SalishSeaCast --var temperature \
      --lat 49.2 --lon -123.5 --depth 5 \
      --from-date 2023-01-01T00:00:00 --to-date 2023-01-31T23:59:59
"""
from __future__ import annotations

import argparse
from typing import List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from modules.clickhouse_helpers import get_ch_client

# ClickHouse table holding the 4D (time, depth, gridX, gridY, <variables>) data per source
DATA_TABLE_BY_SOURCE = {
    "SalishSeaCast": "SalishSeaCast_hourly",
}

# ClickHouse table mapping gridX/gridY to longitude/latitude per source
GRID_TABLE_BY_SOURCE = {
    "SalishSeaCast": "grid_SSC",
}

# Columns in the data table that hold variable values; also the allowed `var` values
ALLOWED_VARIABLES = {
    "temperature",
    "salinity",
    "total_alkalinity",
    "omega_arag",
    "omega_cal",
    "ph_total",
    "dissolved_oxygen",
    "dissolved_inorganic_carbon",
}

MAX_GRID_DIST_KM = 25.0

# Maximum allowed gap (meters) between a requested depth and the nearest
# depth actually present at a grid cell. Native sigma levels are discrete and
# bathymetry-truncated per cell, so without this a request for e.g. 400m at a
# shallow coastal cell would silently return that cell's shallowest level
# instead of signalling "no data here".
DEPTH_MATCH_TOLERANCE_M = 0.1

# Depth levels are uniform across the entire SSC sigma-coordinate grid — every
# cell has the same discrete set. Cached per table per process to avoid a
# per-request full-table scan.
_CACHED_DEPTHS: dict[str, list[float]] = {}


def _get_depth_levels(client, table: str) -> list[float]:
    if table not in _CACHED_DEPTHS:
        result = client.query(f"SELECT DISTINCT depth FROM {table} ORDER BY depth")
        _CACHED_DEPTHS[table] = [float(r[0]) for r in result.result_rows]
    return _CACHED_DEPTHS[table]


def _find_nearest_grid_point(client, grid_table: str, lat: float, lon: float, max_dist_km: float = MAX_GRID_DIST_KM) -> Tuple[int, int, float, float]:
    """Find the (gridX, gridY) cell nearest to (lat, lon) in `grid_table`.

    Raises RuntimeError if the grid table is empty or the nearest cell is
    farther than `max_dist_km` away.
    """
    # 0.5-degree bounding box (~55 km at Salish Sea latitudes) pre-filters the
    # full-table scan before the geoDistance sort. Safe: max_dist_km (25 km) is
    # well inside 55 km, so the true nearest cell is always inside the box.
    query = f"""
        SELECT gridX, gridY, longitude, latitude,
               geoDistance(longitude, latitude, %(lon)s, %(lat)s) AS dist_m
        FROM {grid_table}
        WHERE latitude BETWEEN %(lat_min)s AND %(lat_max)s
          AND longitude BETWEEN %(lon_min)s AND %(lon_max)s
        ORDER BY dist_m ASC
        LIMIT 1
    """
    result = client.query(query, parameters={
        "lon": lon, "lat": lat,
        "lat_min": lat - 0.5, "lat_max": lat + 0.5,
        "lon_min": lon - 0.5, "lon_max": lon + 0.5,
    })
    if not result.result_rows:
        raise RuntimeError(f"Grid table '{grid_table}' is empty or not found")

    grid_x, grid_y, grid_lon, grid_lat, dist_m = result.result_rows[0]
    dist_km = dist_m / 1000.0
    if dist_km > max_dist_km:
        raise RuntimeError(
            f"Requested coordinate ({lat}, {lon}) is {dist_km:.1f} km from the nearest grid point. "
            f"Maximum allowed distance is {max_dist_km} km. "
            f"Please verify your coordinates are within the model domain (Salish Sea / BC coast region)."
        )
    return int(grid_x), int(grid_y), float(grid_lat), float(grid_lon)


def _find_grid_points_in_polygon(client, grid_table: str, polygon: List[Tuple[float, float]]) -> List[Tuple[int, int]]:
    """Find all (gridX, gridY) cells inside `polygon` (a [(lon, lat), ...] ring) in `grid_table`.

    Mirrors modules/ocean_analysis.py's lookup_grid_cells_for_polygon. Coordinates are cast to
    float before being interpolated into the query since ClickHouse doesn't support a parameter
    type for arrays of tuples — Pydantic has already validated them as floats by this point.
    """
    lons = [float(p[0]) for p in polygon]
    lats = [float(p[1]) for p in polygon]
    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)
    polygon_sql = "[" + ", ".join(f"({lon}, {lat})" for lon, lat in zip(lons, lats)) + "]"

    query = f"""
        SELECT gridX, gridY
        FROM {grid_table}
        WHERE longitude BETWEEN {min_lon} AND {max_lon}
          AND latitude BETWEEN {min_lat} AND {max_lat}
          AND pointInPolygon((longitude, latitude), {polygon_sql})
    """
    result = client.query(query)
    return [(int(r[0]), int(r[1])) for r in result.result_rows]


def _resolve_depth(client, table: str, grid_x: int, grid_y: int, depth: float) -> Optional[float]:
    """Return the available depth level nearest to `depth`.

    `depth == -1` selects the deepest (bottom) level, whatever it is.
    Otherwise, the nearest available level must be within
    `DEPTH_MATCH_TOLERANCE_M` of `depth` or this returns None — a depth
    deeper than the local water column should mean "no data", not silently
    fall back to the cell's shallowest/deepest level.
    """
    depths = _get_depth_levels(client, table)
    if not depths:
        return None
    if float(depth) == -1.0:
        return max(depths)
    closest = min(depths, key=lambda d: abs(d - depth))
    if abs(closest - depth) > DEPTH_MATCH_TOLERANCE_M:
        return None
    return closest


def _nearest_level_index(depth: float, levels: list[float]) -> int:
    """Index into `levels` (ascending) of the level nearest `depth`."""
    best_i, best_dist = 0, float("inf")
    for i, lvl in enumerate(levels):
        dist = abs(lvl - depth)
        if dist < best_dist:
            best_dist, best_i = dist, i
    return best_i


def extract_timeseries(
    *,
    source: str,
    var: str,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    polygon: Optional[List[Tuple[float, float]]] = None,
    depth: Optional[float] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    verbose: bool = False,
) -> Union[Tuple[pd.Series, pd.Series], pd.DataFrame]:
    """Extract the hourly time series for `var` from ClickHouse.

    Accepts either:
      - lat + lon (point mode): the single nearest grid cell, or
      - polygon (area mode): a [(lon, lat), ...] ring; every grid cell inside it
        is averaged together per timestamp.

    When `depth` is provided, returns `(times, values)` as a tuple of
    pd.Series. When `depth` is None, returns a `pd.DataFrame` with columns
    `time`, `depth`, `value` covering every available depth level.

    Exceptions are raised on errors; caller should catch and handle them.
    """
    has_polygon = polygon is not None and len(polygon) >= 3
    if not has_polygon and (lat is None or lon is None):
        raise ValueError("Provide either a polygon (area mode) or lat + lon (point mode)")

    if verbose:
        print("########### Extracting timeseries with parameters: ###########", flush=True)
        print(f"Source: {source}", flush=True)
        print(f"Variable: {var}", flush=True)
        if has_polygon:
            print(f"Polygon: {polygon}", flush=True)
        else:
            print(f"Latitude: {lat}", flush=True)
            print(f"Longitude: {lon}", flush=True)
        print(f"Depth: {depth}", flush=True)
        print(f"From date: {from_date}", flush=True)
        print(f"To date: {to_date}", flush=True)

    if source not in DATA_TABLE_BY_SOURCE:
        raise RuntimeError(
            f"Source '{source}' is not yet available via ClickHouse. "
            f"Supported sources: {sorted(DATA_TABLE_BY_SOURCE)}"
        )
    if var not in ALLOWED_VARIABLES:
        raise ValueError(f"Unknown variable '{var}'. Supported variables: {sorted(ALLOWED_VARIABLES)}")

    table = DATA_TABLE_BY_SOURCE[source]
    grid_table = GRID_TABLE_BY_SOURCE[source]

    client = get_ch_client()
    try:
        if has_polygon:
            assert polygon is not None
            grid_points = _find_grid_points_in_polygon(client, grid_table, polygon)
            if not grid_points:
                raise RuntimeError("The selected polygon area does not cover any active marine grid cells.")
            if verbose:
                print(f"Grid points in polygon: {len(grid_points)}", flush=True)
        else:
            assert lat is not None and lon is not None
            grid_x, grid_y, grid_lat, grid_lon = _find_nearest_grid_point(client, grid_table, lat, lon)
            if verbose:
                print(f"Nearest grid point in ClickHouse: gridX={grid_x} gridY={grid_y} lat={grid_lat} lon={grid_lon}", flush=True)
            grid_points = [(grid_x, grid_y)]

        where = ["(gridX, gridY) IN %(grid_points)s"]
        params: dict = {"grid_points": grid_points}
        if from_date is not None:
            where.append("time >= %(from_time)s")
            params["from_time"] = pd.to_datetime(from_date)
        if to_date is not None:
            where.append("time <= %(to_time)s")
            params["to_time"] = pd.to_datetime(to_date)

        if depth is None:
            # avg() is a no-op for point mode (one grid cell per group) and averages
            # across cells for area mode — one query path covers both.
            query = (
                f"SELECT time, depth, avg({var}) AS value FROM {table} "
                f"WHERE {' AND '.join(where)} GROUP BY time, depth ORDER BY time, depth"
            )
            result = client.query(query, parameters=params)
            if not result.result_rows:
                raise RuntimeError("No data found in ClickHouse for the requested time range")

            df = pd.DataFrame(result.result_rows, columns=["time", "depth", "value"])
            df["time"] = pd.to_datetime(df["time"])
            df["depth"] = df["depth"].astype(float)
            df["value"] = df["value"].astype(float)
            df = (
                df.drop_duplicates(subset=["time", "depth"])
                .sort_values(by=["time", "depth"])
                .reset_index(drop=True)
            )
            return df

        # Depth levels are uniform across the SSC grid, so any grid point in the
        # selection can resolve the nearest available level (same assumption used
        # by ocean_analysis.py's query_region_timeseries for polygon aggregation).
        gx0, gy0 = grid_points[0]
        depth_sel = _resolve_depth(client, table, gx0, gy0, depth)
        if depth_sel is None:
            raise RuntimeError(
                f"No data available at depth {depth}m for grid point (gridX={gx0}, gridY={gy0}). "
                f"The water column at this location may not extend to that depth."
            )
        if verbose:
            print(f"Selecting depth {depth_sel} nearest to requested depth {depth}", flush=True)

        where.append("depth = %(depth)s")
        params["depth"] = depth_sel
        query = f"SELECT time, avg({var}) AS value FROM {table} WHERE {' AND '.join(where)} GROUP BY time ORDER BY time"
        result = client.query(query, parameters=params)
        if not result.result_rows:
            raise RuntimeError("No data found in ClickHouse for the requested time range")

        df = pd.DataFrame(result.result_rows, columns=["time", "value"])
        df["time"] = pd.to_datetime(df["time"])
        df["value"] = df["value"].astype(float)
        df = df.drop_duplicates(subset="time").sort_values(by="time").reset_index(drop=True)
        return df.time, df.value
    finally:
        client.close()


def main(argv: Optional[list] = None) -> int:
    p = argparse.ArgumentParser(description="Extract a time series for an ocean variable at a coordinate from ClickHouse")
    p.add_argument("--source", default="SalishSeaCast", choices=sorted(DATA_TABLE_BY_SOURCE), help="Data source")
    p.add_argument("--var", "-v", required=True, choices=sorted(ALLOWED_VARIABLES), help="Variable name to extract")
    p.add_argument("--lat", "-a", type=float, required=True, help="Latitude (required)")
    p.add_argument("--lon", "-o", type=float, required=True, help="Longitude (required)")
    p.add_argument("--depth", type=float, default=None,
                   help="Depth value to select (-1 for the deepest/bottom level); omit to extract all depth levels")
    p.add_argument("--from-date", required=False, default=None, help="Start datetime (ISO-8601); omit to extract all times")
    p.add_argument("--to-date", required=False, default=None, help="End datetime (ISO-8601); omit to extract all times")
    p.add_argument("--output", "-O", default=None, help="CSV output file")
    p.add_argument("--verbose", "-V", action="store_true", help="Verbose output")

    args = p.parse_args(argv)

    result = extract_timeseries(
        source=args.source,
        var=args.var,
        lat=args.lat,
        lon=args.lon,
        depth=args.depth,
        from_date=args.from_date,
        to_date=args.to_date,
        verbose=args.verbose,
    )

    if isinstance(result, pd.DataFrame):
        # All-depths result
        if args.output:
            result.to_csv(args.output, index=False)
            if args.verbose:
                print(f"Wrote {len(result)} rows to {args.output}")
        else:
            print("time,depth,value")
            for _, row in result.iterrows():
                v = float(row["value"])
                t = pd.Timestamp(row["time"])
                print(f"{t.isoformat()},{row['depth']},{'' if np.isnan(v) else v}")
    else:
        times, values = result
        if args.output:
            pd.DataFrame({"time": times, "value": values}).to_csv(args.output, index=False)
            if args.verbose:
                print(f"Wrote {len(times)} rows to {args.output}")
        else:
            print("time,value")
            for t, v in zip(times, values):
                print(f"{t.isoformat()},{'' if np.isnan(v) else v}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
