"""extract_depth_profile.py

Bin a variable-depth ("profiler") sensor's raw casts onto the model's own
discrete depth levels and hourly time buckets, alongside the model's own
values at those same cells — the data behind the Comparison tab's Depth
Profile (Hovmöller) view.

Unlike extractSensorTimeseries.py's nearest-depth mode (one fixed depth over
a sensor's full history), this is for sensors with `sensors.depth == -1`:
casts continuously through the water column, so there is no single depth to
query against. Binning happens here, per request, over the raw
(time, depth, value) rows already stored in ClickHouse sensor_timeseries —
not at ingestion (which would be a lossy, irreversible transform) and not
client-side (which would ship the full-resolution cast stream to the
browser).
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

from modules.clickhouse_helpers import get_ch_client
from modules.extractTimeseries import (
    ALLOWED_VARIABLES,
    DATA_TABLE_BY_SOURCE,
    GRID_TABLE_BY_SOURCE,
    _find_nearest_grid_point,
    _get_depth_levels,
)


def _fmt(dt_str: str) -> str:
    """Normalise an ISO-8601 string to the format ClickHouse DateTime accepts."""
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _floor_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def _is_bad(value) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def _nearest_level_index(depth: float, levels: list[float]) -> int:
    """Index into `levels` (ascending) of the level nearest `depth`."""
    best_i, best_dist = 0, float("inf")
    for i, lvl in enumerate(levels):
        dist = abs(lvl - depth)
        if dist < best_dist:
            best_dist, best_i = dist, i
    return best_i


def extract_depth_profile(
    *,
    source: str,
    var: str,
    sensor_id: str,
    lat: float,
    lon: float,
    from_date: str,
    to_date: str,
) -> dict:
    """
    Bin a profiler sensor's raw casts onto the model's depth levels and
    hourly time buckets, and return the model's own values at the same grid.

    Parameters
    ----------
    source, var  : model source/variable, as in extractTimeseries.extract_timeseries
    sensor_id    : sensor UUID (matches sensor_timeseries.sensor_id)
    lat, lon     : sensor coordinate, used to look up the nearest model grid cell
    from_date, to_date : ISO-8601 window bounds (inclusive)

    Returns
    -------
    dict with keys:
      "time"   — list of ISO-8601 hourly bin-start strings, ascending
      "depths" — list of model depth levels (metres), ascending
      "model"  — 2D list [depth][time] of float, always populated where the
                 model has data at that cell
      "sensor" — 2D list [depth][time] of float | None — None where no cast
                 landed in that (depth level, hour) bin; never interpolated

    Raises
    ------
    ValueError   — unknown source/var
    RuntimeError — no grid cell near (lat, lon), or no model data in the window
    """
    if source not in DATA_TABLE_BY_SOURCE:
        raise ValueError(f"Source '{source}' is not yet available via ClickHouse.")
    if var not in ALLOWED_VARIABLES:
        raise ValueError(f"Unknown variable '{var}'. Supported variables: {sorted(ALLOWED_VARIABLES)}")

    table = DATA_TABLE_BY_SOURCE[source]
    grid_table = GRID_TABLE_BY_SOURCE[source]

    client = get_ch_client()
    try:
        grid_x, grid_y, _, _ = _find_nearest_grid_point(client, grid_table, lat, lon)

        levels = _get_depth_levels(client, table)
        if not levels:
            raise RuntimeError(f"No depth levels found for source '{source}'.")

        from_str, to_str = _fmt(from_date), _fmt(to_date)

        # ── MODEL: every level at this grid cell, hourly, for the window ──────────
        model_rows = client.query(
            f"SELECT time, depth, {var} AS value FROM {table} "
            f"WHERE gridX = %(gx)s AND gridY = %(gy)s "
            f"  AND time >= toDateTime(%(from)s) AND time <= toDateTime(%(to)s) "
            f"ORDER BY time, depth",
            parameters={"gx": grid_x, "gy": grid_y, "from": from_str, "to": to_str},
        ).result_rows
        if not model_rows:
            raise RuntimeError("No model data found in ClickHouse for the requested window.")

        # Hourly bin keys are the shared x-axis for both grids. SalishSeaCast_hourly
        # rows already sit on the hour, so this is just collecting the set — the
        # flooring matters for the sensor's continuous-timestamp casts below.
        hour_keys = sorted({_floor_hour(r[0]) for r in model_rows})
        hour_index = {h: i for i, h in enumerate(hour_keys)}
        level_index = {lvl: i for i, lvl in enumerate(levels)}

        model_grid: list[list[Optional[float]]] = [[None] * len(hour_keys) for _ in levels]
        for t, depth, value in model_rows:
            if _is_bad(value):
                continue
            li = level_index.get(float(depth))
            hi = hour_index.get(_floor_hour(t))
            if li is None or hi is None:
                continue
            model_grid[li][hi] = float(value)

        # ── SENSOR: raw casts, snapped to nearest model level + hour bucket ───────
        safe_var = var.replace("'", "''")
        safe_id = str(sensor_id).replace("'", "")
        sensor_rows = client.query(
            f"SELECT time, depth, value FROM sensor_timeseries FINAL "
            f"WHERE sensor_id = '{safe_id}' AND variable = '{safe_var}' "
            f"  AND time >= toDateTime('{from_str}') AND time <= toDateTime('{to_str}') "
            f"ORDER BY time"
        ).result_rows

        # Group raw casts landing in the same (level, hour) cell so a burst of
        # readings from one pass doesn't get flattened by simple overwrite.
        buckets: dict[tuple[int, int], list[float]] = defaultdict(list)
        for t, depth, value in sensor_rows:
            if _is_bad(value):
                continue
            hi = hour_index.get(_floor_hour(t))
            if hi is None:
                continue  # cast falls outside the model's hourly grid for this window
            li = _nearest_level_index(float(depth), levels)
            buckets[(li, hi)].append(float(value))

        sensor_grid: list[list[Optional[float]]] = [[None] * len(hour_keys) for _ in levels]
        for (li, hi), values in buckets.items():
            values.sort()
            sensor_grid[li][hi] = values[len(values) // 2]  # median — robust to a stray spike mid-cast

        return {
            "time": [h.strftime("%Y-%m-%dT%H:%M:%S") for h in hour_keys],
            "depths": levels,
            "model": model_grid,
            "sensor": sensor_grid,
        }
    finally:
        client.close()
