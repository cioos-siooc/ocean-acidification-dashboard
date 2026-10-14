"""
Ocean Analysis Module for the Analysis Builder.

Provides query functions that read from a source's ClickHouse daily / grid
tables (e.g. SalishSeaCast_daily / grid_SSC) for the Analysis Builder frontend. The frontend resolves a
polygon to a flat regionally-averaged daily timeseries via these helpers,
then computes climatology, trend, threshold, streak, and extreme-value
analytics client-side.
"""

import math
import logging
from typing import List, Tuple

from modules.clickhouse_helpers import get_ch_client as _get_ch_client
from modules.sources import DAILY_TABLE_BY_SOURCE, GRID_TABLE_BY_SOURCE, check_variable, has_bottom_rows

logger = logging.getLogger(__name__)


def lookup_nearest_grid_cell(lat: float, lon: float, source: str = "SalishSeaCast") -> List[Tuple[int, int]]:
    """
    Returns the single (gridX, gridY) tuple whose coordinate is closest
    to the given (lat, lon), using great-circle distance via ClickHouse's
    geoDistance function.
    """
    client = _get_ch_client()
    # 0.5-degree bounding box pre-filters the full-table scan to ~1% of rows
    # before the geoDistance sort. Safe because the Salish Sea grid spacing is
    # well under 0.5°, so the true nearest cell is always inside the box.
    query = f"""
        SELECT gridX, gridY
        FROM {GRID_TABLE_BY_SOURCE[source]}
        WHERE latitude BETWEEN {lat - 0.5} AND {lat + 0.5}
          AND longitude BETWEEN {lon - 0.5} AND {lon + 0.5}
        ORDER BY geoDistance({lon}, {lat}, longitude, latitude)
        LIMIT 1
    """
    result = client.query(query)
    return result.result_rows


def lookup_grid_cells_for_polygon(polygon_coords: List[Tuple[float, float]],
                                  source: str = "SalishSeaCast") -> List[Tuple[int, int]]:
    """
    Takes a GeoJSON polygon array [(lon1, lat1), (lon2, lat2), ...]
    Filters the source's grid table using primary index bounding boxes,
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
        FROM {GRID_TABLE_BY_SOURCE[source]}
        WHERE longitude BETWEEN {min_lon} AND {max_lon}
          AND latitude BETWEEN {min_lat} AND {max_lat}
          AND pointInPolygon((longitude, latitude), {polygon_sql_format})
    """

    result = client.query(query)

    # 5. Return the rows as a clean list of python tuples [(gridX, gridY), ...]
    return result.result_rows


# Same tolerance and rationale as extractTimeseries.py's DEPTH_MATCH_TOLERANCE_M:
# native sigma levels are discrete and bathymetry-truncated per cell, so a
# depth deeper than the local water column should resolve to "no data", not
# silently snap to whatever level happens to be closest.
DEPTH_MATCH_TOLERANCE_M = 0.1

# Depth levels are uniform across a source's whole grid — every cell has the
# same discrete set. Cached on first use (per process, per table) to avoid a
# per-request full-table scan. Real depths only (see extractTimeseries.py's
# _get_depth_levels for LiveOcean's -1 bottom rows).
_CACHED_DEPTH_LEVELS: dict[str, list[float]] = {}


def _get_depth_levels(client, table: str) -> list[float]:
    if table not in _CACHED_DEPTH_LEVELS:
        result = client.query(
            f"SELECT DISTINCT depth FROM {table} WHERE depth >= 0 ORDER BY depth"
        )
        _CACHED_DEPTH_LEVELS[table] = [float(r[0]) for r in result.result_rows]
    return _CACHED_DEPTH_LEVELS[table]


def _resolve_nearest_depth(client, grid_x: int, grid_y: int, depth: float,
                           source: str = "SalishSeaCast") -> float | None:
    """Return the available depth level nearest to `depth`.

    `depth == -1` selects the bottom: the stored -1 rows for a source that
    has them, else the deepest level, whatever it is.
    Otherwise, the nearest available level must be within
    `DEPTH_MATCH_TOLERANCE_M` of `depth` or this returns None.
    """
    if float(depth) == -1.0 and has_bottom_rows(source):
        return -1.0
    depths = _get_depth_levels(client, DAILY_TABLE_BY_SOURCE[source])
    if not depths:
        return None
    if float(depth) == -1.0:
        return max(depths)
    closest = min(depths, key=lambda d: abs(d - depth))
    if abs(closest - depth) > DEPTH_MATCH_TOLERANCE_M:
        return None
    return closest


def query_region_timeseries(
    grid_points: List[Tuple[int, int]],
    depth: float,
    variable: str,
    stat: str,
    year_range: List[int],
    source: str = "SalishSeaCast",
) -> dict:
    """
    Returns the regionally-averaged daily timeseries for `{variable}_{stat}`
    across `grid_points`, at the depth level nearest to `depth` and
    restricted to `year_range`. No seasonal filtering or per-year split is
    applied here — the frontend derives climatology, trend, threshold
    counts, streaks, and extremes from this flat series.

    Returns: {"data": [{"time": "YYYY-MM-DD", "value": ...}, ...]}
    """
    check_variable(source, variable)
    if stat not in ("mean", "min", "max"):
        raise ValueError(f"Unsupported stat '{stat}'. Supported: mean, min, max.")
    column_name = f"{variable}_{stat}"

    client = _get_ch_client()

    # Depth levels are uniform across the SSC grid, so any grid point in the
    # selection can resolve the nearest available level (same approach as
    # extractTimeseries.py's _resolve_depth, used for single-point lookups).
    grid_x, grid_y = grid_points[0]
    depth_sel = _resolve_nearest_depth(client, grid_x, grid_y, depth, source)
    if depth_sel is None:
        return {"data": []}

    params = {
        "grid_points": grid_points,
        "depth": depth_sel,
        "min_year": year_range[0],
        "max_year": year_range[1],
    }

    query = f"""
        SELECT time, avg({column_name}) AS val
        FROM {DAILY_TABLE_BY_SOURCE[source]}
        WHERE (gridX, gridY) IN %(grid_points)s
          AND depth = %(depth)s
          AND toYear(time) BETWEEN %(min_year)s AND %(max_year)s
        GROUP BY time
        ORDER BY time
    """

    result = client.query(query, parameters=params)

    return {
        "data": [
            {"time": str(t), "value": (None if math.isnan(fv := float(v)) else round(fv, 6)) if v is not None else None}
            for t, v in result.result_rows
        ]
    }
