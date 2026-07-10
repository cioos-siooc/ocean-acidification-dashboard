#!/usr/bin/env python3
"""
Climatology timeseries extraction from ClickHouse SalishSeaCast_daily.

For a given lat/lon/variable/depth and a date window, aggregates all available
historical years to produce daily climatological mean, min, and max — one entry
per calendar day in the window mapped back onto the requested dates.
"""
import math
import os
import logging
import pandas as pd
import clickhouse_connect

logger = logging.getLogger(__name__)


def _get_ch_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CH_HOST", "localhost"),
        port=int(os.getenv("CH_PORT", "8123")),
        username=os.getenv("CH_USER", "default"),
        password=os.getenv("CH_PASSWORD", os.getenv("CLICKHOUSE_PASSWORD", "")),
    )


def _clean(v):
    if v is None:
        return None
    f = float(v)
    return None if (math.isnan(f) or math.isinf(f)) else round(f, 6)


def extract_climate_timeseries(lat, lon, variable, depth, from_date, to_date):
    """
    Returns [{requested_date, mean, min, max}, ...] — one entry per calendar
    day in [from_date, to_date], with climatological stats aggregated across
    all available years in SalishSeaCast_daily.

    requested_date is set to noon UTC on each day so it plots centred within
    the day on the hourly-resolution model chart.
    """
    from modules.ocean_analysis import lookup_nearest_grid_cell

    # Nearest grid cell
    grid_points = lookup_nearest_grid_cell(lat, lon)
    if not grid_points:
        logger.error("No grid cell found near lat=%s lon=%s", lat, lon)
        return None
    grid_x, grid_y = grid_points[0]

    client = _get_ch_client()

    # Resolve nearest available depth
    depth_float = float(depth)
    if depth_float == -1.0:
        r = client.query("SELECT max(depth) FROM SalishSeaCast_daily")
        depth_sel = float(r.result_rows[0][0])
    else:
        r = client.query(
            f"SELECT depth FROM SalishSeaCast_daily "
            f"WHERE abs(depth - {depth_float}) < 0.1 LIMIT 1"
        )
        if not r.result_rows:
            logger.warning("No depth near %s found in SalishSeaCast_daily", depth_float)
            return []
        depth_sel = float(r.result_rows[0][0])

    # Generate all calendar days in the window
    start_dt = pd.to_datetime(from_date).normalize()
    end_dt = pd.to_datetime(to_date).normalize()
    days = pd.date_range(start=start_dt, end=end_dt, freq='D')
    if len(days) == 0:
        return []

    # Build (month, day) IN list — deduped so year-spanning windows don't repeat
    month_day_pairs = sorted({(int(d.month), int(d.day)) for d in days})
    md_sql = ", ".join(f"({mo}, {dy})" for mo, dy in month_day_pairs)

    col_mean = f"{variable}_mean"
    col_min  = f"{variable}_min"
    col_max  = f"{variable}_max"

    query = f"""
        SELECT
            toMonth(time)      AS mo,
            toDayOfMonth(time) AS dy,
            avg({col_mean}),
            min({col_min}),
            max({col_max})
        FROM SalishSeaCast_daily
        WHERE gridX = {grid_x}
          AND gridY = {grid_y}
          AND depth = {depth_sel}
          AND toYear(time) <= 2025
          AND (toMonth(time), toDayOfMonth(time)) IN ({md_sql})
        GROUP BY mo, dy
        ORDER BY mo, dy
    """

    result = client.query(query)
    lookup = {
        (int(row[0]), int(row[1])): (_clean(row[2]), _clean(row[3]), _clean(row[4]))
        for row in result.result_rows
    }

    output = []
    for d in days:
        key = (d.month, d.day)
        if key in lookup:
            mean_v, min_v, max_v = lookup[key]
            output.append({
                'requested_date': d.strftime('%Y-%m-%dT12:00:00'),
                'mean': mean_v,
                'min':  min_v,
                'max':  max_v,
            })

    logger.info(
        "Climatology: %d days returned for %s at depth=%.2f (grid %d,%d)",
        len(output), variable, depth_sel, grid_x, grid_y,
    )
    return output
