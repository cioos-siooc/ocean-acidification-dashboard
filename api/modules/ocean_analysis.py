"""
Ocean Analysis Module for the Analysis Builder.

Provides query functions for 5 analysis modes that read from ClickHouse
SalishSeaCast_daily table and return data for the VuiOverlayComponent frontend.
"""

import os
import logging
from typing import List, Tuple
import clickhouse_connect

logger = logging.getLogger(__name__)


def _get_ch_client():
    host = os.getenv("CH_HOST", "192.168.18.33")
    port = int(os.getenv("CH_PORT", "8123"))
    user = os.getenv("CH_USER", "default")
    password = os.getenv("CH_PASSWORD", os.getenv("CLICKHOUSE_PASSWORD", ""))
    return clickhouse_connect.get_client(host=host, port=port, username=user, password=password)


SEASON_MONTHS = {
    "full_year": list(range(1, 13)), "jja": [6, 7, 8], "mam": [3, 4, 5],
    "son": [9, 10, 11], "djf": [12, 1, 2],
}


def _build_base_where(grid_x_range, grid_y_range, depth_range, year_range, season):
    gx_min, gx_max = grid_x_range
    gy_min, gy_max = grid_y_range
    d_min, d_max = depth_range
    months = SEASON_MONTHS.get(season, list(range(1, 13)))
    params = {
        "gx_min": int(gx_min), "gx_max": int(gx_max), "gy_min": int(gy_min), "gy_max": int(gy_max),
        "d_min": float(d_min), "d_max": float(d_max),
        "year_min": int(year_range[0]), "year_max": int(year_range[1]), "months": months,
    }
    where = (
        "gridX BETWEEN %(gx_min)s AND %(gx_max)s "
        "AND gridY BETWEEN %(gy_min)s AND %(gy_max)s "
        "AND depth BETWEEN %(d_min)s AND %(d_max)s "
        "AND toYear(time) BETWEEN %(year_min)s AND %(year_max)s "
        "AND toMonth(time) IN %(months)s"
    )
    return where, params


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
    # (In production, pull this client from your app state/pool)
    client = clickhouse_connect.get_client(host="localhost", port=8123)

    # 4. Execute the optimized coordinate lookup
    query = f"""
        SELECT gridX, gridY
        FROM grid_SSC
        WHERE longitude BETWEEN {min_lon} AND {max_lon}  -- 🚀 Hits Primary Index!
          AND latitude BETWEEN {min_lat} AND {max_lat}   -- 🚀 Hits Primary Index!
          AND pointInPolygon((longitude, latitude), {polygon_sql_format})
    """
    
    result = client.query(query)
    
    # 5. Return the rows as a clean list of python tuples [(gridX, gridY), ...]
    return result.result_rows


def query_overlay_timeseries(grid_x_range, grid_y_range, depth_range, variable, stat, year_range, season):
    """Return per-year timeseries for overlay plotting.
    Returns: {"series": [{"year": 2015, "data": [{"time": "...", "value": ...}, ...]}]}
    """
    col = f"{variable}_{stat}"
    where, params = _build_base_where(grid_x_range, grid_y_range, depth_range, year_range, season)
    query = (
        f"SELECT toYear(time) AS year, time, avg({col}) AS val "
        f"FROM SalishSeaCast_daily WHERE {where} "
        f"GROUP BY year, time ORDER BY year, time"
    )
    client = _get_ch_client()
    result = client.query(query, parameters=params)
    series_map = {}
    for row in result.result_rows:
        year, time_val, val = row
        year = int(year)
        if year not in series_map:
            series_map[year] = []
        series_map[year].append({
            "time": str(time_val),
            "value": round(float(val), 6) if val is not None else None,
        })
    return {"series": [{"year": y, "data": series_map[y]} for y in sorted(series_map)]}


def query_climatology(grid_x_range, grid_y_range, depth_range, variable, stat, year_range):
    """Return averaged monthly climatology over the full year range.
    Returns: {"data": [{"time": "01", "value": ...}, ...]}
    """
    col = f"{variable}_{stat}"
    gx_min, gx_max = grid_x_range
    gy_min, gy_max = grid_y_range
    d_min, d_max = depth_range
    params = {"gx_min": int(gx_min), "gx_max": int(gx_max), "gy_min": int(gy_min), "gy_max": int(gy_max),
              "d_min": float(d_min), "d_max": float(d_max), "year_min": int(year_range[0]), "year_max": int(year_range[1])}
    where = "gridX BETWEEN %(gx_min)s AND %(gx_max)s AND gridY BETWEEN %(gy_min)s AND %(gy_max)s AND depth BETWEEN %(d_min)s AND %(d_max)s AND toYear(time) BETWEEN %(year_min)s AND %(year_max)s"
    query = f"SELECT toMonth(time) AS month, avg({col}) AS val FROM SalishSeaCast_daily WHERE {where} GROUP BY month ORDER BY month"
    client = _get_ch_client()
    result = client.query(query, parameters=params)
    return {"data": [{"time": f"{int(m):02d}", "value": round(float(v), 6) if v is not None else None} for m, v in result.result_rows]}


def query_threshold_count(grid_x_range, grid_y_range, depth_range, variable, stat, year_range, season, threshold_value, threshold_direction):
    """Return per-year count of days matching the threshold condition.
    Returns: {"data": [{"time": "2015", "value": 12}, ...]}
    """
    col = f"{variable}_{stat}"
    where, params = _build_base_where(grid_x_range, grid_y_range, depth_range, year_range, season)
    params["thresh"] = float(threshold_value)
    query = f"SELECT toYear(time) AS year, count() AS day_count FROM (SELECT time FROM SalishSeaCast_daily WHERE {where} GROUP BY time HAVING avg({col}) {threshold_direction} %(thresh)s) GROUP BY year ORDER BY year"
    client = _get_ch_client()
    result = client.query(query, parameters=params)
    return {"data": [{"time": str(int(y)), "value": int(c)} for y, c in result.result_rows]}


def query_trend(grid_x_range, grid_y_range, depth_range, variable, stat, year_range, season):
    """Return per-year annual mean values for trend analysis.
    Returns: {"data": [{"time": "2015", "value": 12.3}, ...]}
    """
    col = f"{variable}_{stat}"
    where, params = _build_base_where(grid_x_range, grid_y_range, depth_range, year_range, season)
    query = f"SELECT toYear(time) AS year, avg({col}) AS val FROM SalishSeaCast_daily WHERE {where} GROUP BY year ORDER BY year"
    client = _get_ch_client()
    result = client.query(query, parameters=params)
    return {"data": [{"time": str(int(y)), "value": round(float(v), 6) if v is not None else None} for y, v in result.result_rows]}


def query_correlation(grid_x_range, grid_y_range, depth_range, primary_variable, primary_stat, second_variable, second_stat, year_range, season):
    """Return paired per-timestep values for two variables.
    Returns: {"data": [{"time": "...", "primary_value": ..., "second_value": ...}, ...]}
    """
    col1 = f"{primary_variable}_{primary_stat}"
    col2 = f"{second_variable}_{second_stat}"
    where, params = _build_base_where(grid_x_range, grid_y_range, depth_range, year_range, season)
    query = f"SELECT time, avg({col1}) AS val1, avg({col2}) AS val2 FROM SalishSeaCast_daily WHERE {where} GROUP BY time ORDER BY time"
    client = _get_ch_client()
    result = client.query(query, parameters=params)
    return {"data": [{"time": str(t), "primary_value": round(float(v1), 6) if v1 is not None else None, "second_value": round(float(v2), 6) if v2 is not None else None} for t, v1, v2 in result.result_rows]}