#!/usr/bin/env python3
"""extractTimeseries.py

Extract a time series for an ocean variable at a coordinate from ClickHouse.

For each supported `source`, the nearest grid cell to (lat, lon) is looked up
in that source's grid table (e.g. `grid_SSC`), then the requested variable is
read from the source's 4D data table (e.g. `SalishSeaCast_daily`) for that
grid cell over the requested time range.

When `depth` is given, the available depth level nearest to it is selected
(use `depth=-1` to select the deepest/bottom level) and a single (time,
value) series is returned. When `depth` is omitted, every depth level is
returned as a DataFrame with `time`, `depth`, `value` columns.

Examples:
  python modules/extractTimeseries.py --source SalishSeaCast --var temperature --stat mean \
      --lat 49.2 --lon -123.5 --depth 5 \
      --from-date 2023-01-01T00:00:00 --to-date 2023-01-31T23:59:59
"""
from __future__ import annotations

import argparse
from typing import Optional, Tuple, Union

import numpy as np
import pandas as pd

from modules.clickhouse_helpers import get_ch_client

# ClickHouse table holding the 4D (time, depth, gridX, gridY, <variables>) data per source
DATA_TABLE_BY_SOURCE = {
    "SalishSeaCast": "SalishSeaCast_daily",
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

# Suffix appended to `var` to form the data table column name, e.g. "temperature_mean"
ALLOWED_STATS = {"mean", "min", "max"}

MAX_GRID_DIST_KM = 25.0


def _find_nearest_grid_point(client, grid_table: str, lat: float, lon: float, max_dist_km: float = MAX_GRID_DIST_KM) -> Tuple[int, int, float, float]:
    """Find the (gridX, gridY) cell nearest to (lat, lon) in `grid_table`.

    Raises RuntimeError if the grid table is empty or the nearest cell is
    farther than `max_dist_km` away.
    """
    query = f"""
        SELECT gridX, gridY, longitude, latitude,
               geoDistance(longitude, latitude, %(lon)s, %(lat)s) AS dist_m
        FROM {grid_table}
        ORDER BY dist_m ASC
        LIMIT 1
    """
    result = client.query(query, parameters={"lon": lon, "lat": lat})
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


def _resolve_depth(client, table: str, grid_x: int, grid_y: int, depth: float) -> Optional[float]:
    """Return the available depth level nearest to `depth` for (grid_x, grid_y).

    `depth == -1` selects the deepest (bottom) level. Returns None if no
    depth levels are available for this grid cell.
    """
    query = f"""
        SELECT DISTINCT depth FROM {table}
        WHERE gridX = %(gx)s AND gridY = %(gy)s
    """
    result = client.query(query, parameters={"gx": grid_x, "gy": grid_y})
    depths = [float(row[0]) for row in result.result_rows]
    if not depths:
        return None
    if float(depth) == -1.0:
        return max(depths)
    return min(depths, key=lambda d: abs(d - depth))


def extract_timeseries(
    *,
    source: str,
    var: str,
    stat: str,
    lat: float,
    lon: float,
    depth: Optional[float] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    verbose: bool = False,
) -> Union[Tuple[pd.Series, pd.Series], pd.DataFrame]:
    """Extract a time series for `var`'s `stat` (mean/min/max) at (lat, lon) from ClickHouse.

    When `depth` is provided, returns `(times, values)` as a tuple of
    pd.Series. When `depth` is None, returns a `pd.DataFrame` with columns
    `time`, `depth`, `value` covering every available depth level.

    Exceptions are raised on errors; caller should catch and handle them.
    """
    if verbose:
        print("########### Extracting timeseries with parameters: ###########", flush=True)
        print(f"Source: {source}", flush=True)
        print(f"Variable: {var}", flush=True)
        print(f"Stat: {stat}", flush=True)
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
    if stat not in ALLOWED_STATS:
        raise ValueError(f"Unknown stat '{stat}'. Supported stats: {sorted(ALLOWED_STATS)}")

    table = DATA_TABLE_BY_SOURCE[source]
    grid_table = GRID_TABLE_BY_SOURCE[source]

    client = get_ch_client()
    try:
        grid_x, grid_y, grid_lat, grid_lon = _find_nearest_grid_point(client, grid_table, lat, lon)
        if verbose:
            print(f"Nearest grid point in ClickHouse: gridX={grid_x} gridY={grid_y} lat={grid_lat} lon={grid_lon}", flush=True)

        where = ["gridX = %(gx)s", "gridY = %(gy)s"]
        params: dict = {"gx": grid_x, "gy": grid_y}
        if from_date is not None:
            where.append("time >= %(from_time)s")
            params["from_time"] = pd.to_datetime(from_date).date()
        if to_date is not None:
            where.append("time <= %(to_time)s")
            params["to_time"] = pd.to_datetime(to_date).date()

        if depth is None:
            query = (
                f"SELECT time, depth, {var}_{stat} AS value FROM {table} "
                f"WHERE {' AND '.join(where)} ORDER BY time, depth"
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

        depth_sel = _resolve_depth(client, table, grid_x, grid_y, depth)
        if depth_sel is None:
            raise RuntimeError(f"No data found for grid point (gridX={grid_x}, gridY={grid_y})")
        if verbose:
            print(f"Selecting depth {depth_sel} nearest to requested depth {depth}", flush=True)

        where.append("depth = %(depth)s")
        params["depth"] = depth_sel
        query = f"SELECT time, {var}_{stat} AS value FROM {table} WHERE {' AND '.join(where)} ORDER BY time"
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
    p.add_argument("--stat", required=True, choices=sorted(ALLOWED_STATS), help="Statistic to extract (mean, min, or max)")
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
        stat=args.stat,
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
