"""
Ocean Analysis Module for the Analysis Builder.

Provides query functions that read from ClickHouse SalishSeaCast_daily /
grid_SSC tables for the Analysis Builder frontend. The frontend resolves a
polygon to a flat regionally-averaged daily timeseries via these helpers,
then computes climatology, trend, threshold, streak, and extreme-value
analytics client-side.
"""

import os
import logging
from typing import List, Tuple
import clickhouse_connect

logger = logging.getLogger(__name__)


def _get_ch_client():
    host = os.getenv("CH_HOST", "localhost")
    port = int(os.getenv("CH_PORT", "8123"))
    user = os.getenv("CH_USER", "default")
    password = os.getenv("CH_PASSWORD", os.getenv("CLICKHOUSE_PASSWORD", ""))
    return clickhouse_connect.get_client(host=host, port=port, username=user, password=password)


def lookup_nearest_grid_cell(lat: float, lon: float) -> List[Tuple[int, int]]:
    """
    Returns the single (gridX, gridY) tuple whose coordinate is closest
    to the given (lat, lon), using great-circle distance via ClickHouse's
    geoDistance function.
    """
    client = _get_ch_client()
    query = f"""
        SELECT gridX, gridY
        FROM grid_SSC
        ORDER BY geoDistance({lon}, {lat}, longitude, latitude)
        LIMIT 1
    """
    result = client.query(query)
    return result.result_rows


def lookup_grid_cells_for_polygon(polygon_coords: List[Tuple[float, float]]) -> List[Tuple[int, int]]:
    """
    Takes a GeoJSON polygon array [(lon1, lat1), (lon2, lat2), ...]
    Filters the grid_SSC table using primary index bounding boxes,
    runs pointInPolygon, and returns a list of matching (gridX, gridY) tuples.
    """
    if not polygon_coords:
        return []

    # 1. Calculate the bounding box of the polygon for primary index pruning
    longitudes = [pt[0] for pt in polygon_coords]
    latitudes = [pt[1] for pt in polygon_coords]

    min_lon, max_lon = min(longitudes), max(longitudes)
    min_lat, max_lat = min(latitudes), max(latitudes)

    # 2. Format the polygon points into ClickHouse SQL array-of-tuples syntax
    # e.g., "[(lon1, lat1), (lon2, lat2), ...]"
    polygon_sql_format = "[" + ", ".join(f"({lon}, {lat})" for lon, lat in polygon_coords) + "]"

    # 3. Connect to ClickHouse
    client = _get_ch_client()

    # 4. Execute the optimized coordinate lookup
    query = f"""
        SELECT gridX, gridY
        FROM grid_SSC
        WHERE longitude BETWEEN {min_lon} AND {max_lon}
          AND latitude BETWEEN {min_lat} AND {max_lat}
          AND pointInPolygon((longitude, latitude), {polygon_sql_format})
    """

    result = client.query(query)

    # 5. Return the rows as a clean list of python tuples [(gridX, gridY), ...]
    return result.result_rows


def query_region_timeseries(
    grid_points: List[Tuple[int, int]],
    depth_range: Tuple[float, float],
    variable: str,
    stat: str,
    year_range: List[int],
) -> dict:
    """
    Returns the regionally-averaged daily timeseries for `{variable}_{stat}`
    across `grid_points`, averaged over `depth_range` and restricted to
    `year_range`. No seasonal filtering or per-year split is applied here —
    the frontend derives climatology, trend, threshold counts, streaks, and
    extremes from this flat series.

    Returns: {"data": [{"time": "YYYY-MM-DD", "value": ...}, ...]}
    """
    column_name = f"{variable}_{stat}"

    params = {
        "grid_points": grid_points,
        "min_depth": depth_range[0],
        "max_depth": depth_range[1],
        "min_year": year_range[0],
        "max_year": year_range[1],
    }

    query = f"""
        SELECT time, avg({column_name}) AS val
        FROM SalishSeaCast_daily
        WHERE (gridX, gridY) IN %(grid_points)s
          AND depth BETWEEN %(min_depth)s AND %(max_depth)s
          AND toYear(time) BETWEEN %(min_year)s AND %(max_year)s
        GROUP BY time
        ORDER BY time
    """

    client = _get_ch_client()
    result = client.query(query, parameters=params)

    return {
        "data": [
            {"time": str(t), "value": round(float(v), 6) if v is not None else None}
            for t, v in result.result_rows
        ]
    }
