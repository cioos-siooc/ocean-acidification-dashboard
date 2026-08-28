"""extract_depth_profile.py

Build a time-depth (Hovmöller) grid for one model grid cell — the model's own
values at every depth level across time buckets of a requested resolution
(hourly, daily, or monthly) — and, optionally, a variable-depth ("profiler")
sensor's raw casts binned onto that same grid.

Two callers, one query path:
  * Comparison tab's Depth Profile — passes `sensor_id`, and gets both grids
    back so the model and the sensor can be differenced cell by cell.
  * Depth tab's model time-depth view — omits `sensor_id`, and gets the model
    grid alone (`"sensor": None`) for an arbitrary map point, no sensor
    required.

The sensor half exists because sensors with `sensors.depth == -1` cast
continuously through the water column, so unlike extractSensorTimeseries.py's
nearest-depth mode (one fixed depth over a sensor's full history) there is no
single depth to query against. Binning happens here, per request, over the raw
(time, depth, value) rows already stored in ClickHouse sensor_timeseries —
not at ingestion (which would be a lossy, irreversible transform) and not
client-side (which would ship the full-resolution cast stream to the
browser).
"""

from __future__ import annotations

import calendar
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
    _nearest_level_index,
)


# Bin resolutions. "monthly" has no fixed hour width (see _floor_bin) and is
# absent from this map deliberately — anything reading it must special-case
# months first.
BIN_HOURS_BY_MODE = {"hourly": 1, "daily": 24}

# Legacy `bin_hours` ints, still accepted on the wire so an older deployed
# frontend keeps working against a newer API (the two deploy independently).
MODE_BY_BIN_HOURS = {1: "hourly", 24: "daily"}


def _fmt(dt_str: str) -> str:
    """Normalise an ISO-8601 string to the format ClickHouse DateTime accepts."""
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _floor_bin(dt: datetime, mode: str) -> datetime:
    """Floor `dt` to the start of its bucket for `mode`, returned naive.

    Months are calendar-based and so get their own branch: the epoch-modulo
    trick below cannot express them (months aren't a fixed number of seconds,
    and no fixed-width bin lands on the 1st of every month).

    For the fixed-width modes, uses `utctimetuple()`/`calendar.timegm` rather
    than `dt.timestamp()` — the latter interprets a naive datetime as *local*
    server time, whereas ClickHouse rows here are UTC wall-clock values (naive
    or not), matching how `_fmt` and the rest of this module already treat
    timestamps. Epoch-modulo works cleanly for 1/6/24-hour bins because the
    Unix epoch (1970-01-01T00:00:00Z) already sits on all three boundaries —
    no special day/week alignment logic needed.
    """
    if mode == "monthly":
        return datetime(dt.year, dt.month, 1)
    bin_seconds = BIN_HOURS_BY_MODE[mode] * 3600
    epoch_seconds = calendar.timegm(dt.utctimetuple())
    floored = epoch_seconds - (epoch_seconds % bin_seconds)
    return datetime.fromtimestamp(floored, tz=timezone.utc).replace(tzinfo=None)


def _is_bad(value) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def extract_depth_profile(
    *,
    source: str,
    var: str,
    lat: float,
    lon: float,
    from_date: str,
    to_date: str,
    sensor_id: Optional[str] = None,
    bin_hours: int = 1,
    bin_mode: Optional[str] = None,
) -> dict:
    """
    Build the model's time-depth grid at the cell nearest (lat, lon), and —
    when `sensor_id` is given — a profiler sensor's raw casts binned onto that
    same grid.

    Parameters
    ----------
    source, var  : model source/variable, as in extractTimeseries.extract_timeseries
    lat, lon     : coordinate, used to look up the nearest model grid cell
    from_date, to_date : ISO-8601 window bounds (inclusive)
    sensor_id    : sensor UUID (matches sensor_timeseries.sensor_id), or None
                   for a model-only grid
    bin_hours    : legacy bucket width in hours — 1 or 24. Ignored when
                   `bin_mode` is set.
    bin_mode     : "hourly", "daily", or "monthly". Preferred over `bin_hours`.

    Returns
    -------
    dict with keys:
      "time"   — list of ISO-8601 bin-start strings at the requested
                 resolution, ascending
      "depths" — list of model depth levels (metres), ascending
      "model"  — 2D list [depth][time] of float, always populated where the
                 model has data at that cell
      "sensor" — 2D list [depth][time] of float | None — None where no cast
                 landed in that (depth level, time bin) cell; never
                 interpolated. None (not a grid) when `sensor_id` is omitted.
      "grid"   — {"lat", "lon", "distanceKm"} for the model cell that answered,
                 so callers can show how far the point snapped

    Raises
    ------
    ValueError   — unknown source/var, or unsupported bin_hours/bin_mode
    RuntimeError — no model data in the window
    OutsideDomainError — (a RuntimeError) no grid cell within MAX_GRID_DIST_KM
                 of (lat, lon)
    """
    if source not in DATA_TABLE_BY_SOURCE:
        raise ValueError(f"Source '{source}' is not yet available via ClickHouse.")
    if var not in ALLOWED_VARIABLES:
        raise ValueError(f"Unknown variable '{var}'. Supported variables: {sorted(ALLOWED_VARIABLES)}")

    if bin_mode is None:
        if bin_hours not in MODE_BY_BIN_HOURS:
            raise ValueError(f"Unsupported bin_hours '{bin_hours}'. Supported: {sorted(MODE_BY_BIN_HOURS)}.")
        mode = MODE_BY_BIN_HOURS[bin_hours]
    elif bin_mode not in ("hourly", "daily", "monthly"):
        raise ValueError(f"Unsupported bin_mode '{bin_mode}'. Supported: hourly, daily, monthly.")
    else:
        mode = bin_mode

    table = DATA_TABLE_BY_SOURCE[source]
    grid_table = GRID_TABLE_BY_SOURCE[source]

    client = get_ch_client()
    try:
        grid_x, grid_y, cell_lat, cell_lon, cell_dist_km = _find_nearest_grid_point(
            client, grid_table, lat, lon, source=source
        )

        levels = _get_depth_levels(client, table)
        if not levels:
            raise RuntimeError(f"No depth levels found for source '{source}'.")

        from_str, to_str = _fmt(from_date), _fmt(to_date)

        # How far this cell's record actually reaches, for the table this mode
        # reads. Hourly-vs-daily coverage differs by nearly two decades, so the
        # frontend cannot derive paging bounds from a single global constant —
        # and hardcoding the eras there would silently rot as backfill lands.
        # Both tables lead their sort key with (gridX, gridY), so this is a
        # primary-key range scan rather than a table scan.
        coverage_table = table if mode == "hourly" else "SalishSeaCast_daily"
        cov_rows = client.query(
            f"SELECT min(time), max(time) FROM {coverage_table} "
            f"WHERE gridX = %(gx)s AND gridY = %(gy)s",
            parameters={"gx": grid_x, "gy": grid_y},
        ).result_rows
        cov_from, cov_to = cov_rows[0] if cov_rows else (None, None)
        coverage = {
            "from": cov_from.isoformat() if cov_from else None,
            "to": cov_to.isoformat() if cov_to else None,
        }

        if mode == "daily":
            # SalishSeaCast_daily already stores one pre-aggregated row per
            # (day, depth, grid cell) — a plain filtered select, no GROUP BY
            # needed. It also covers 2007-present, vs. SalishSeaCast_hourly's
            # 2026-present, so daily mode isn't silently truncated to whatever
            # short window the hourly table happens to cover.
            daily_rows = client.query(
                f"SELECT time, depth, {var}_mean AS value FROM SalishSeaCast_daily "
                f"WHERE gridX = %(gx)s AND gridY = %(gy)s "
                f"  AND time >= toDate(%(from)s) AND time <= toDate(%(to)s) "
                f"ORDER BY time, depth",
                parameters={"gx": grid_x, "gy": grid_y, "from": from_str[:10], "to": to_str[:10]},
            ).result_rows
            model_rows = [(datetime(d.year, d.month, d.day), depth, value) for d, depth, value in daily_rows]
        elif mode == "monthly":
            # Also off the daily table (for its full 2007-present reach), rolled
            # up a second time. Daily bins over that whole span would be ~6.8k
            # buckets x ~40 depths — far past what the frontend renders in one
            # pass — where monthly keeps a two-decade section at ~230 buckets.
            monthly_rows = client.query(
                f"SELECT toStartOfMonth(time) AS bucket, depth, avg({var}_mean) AS value "
                f"FROM SalishSeaCast_daily "
                f"WHERE gridX = %(gx)s AND gridY = %(gy)s "
                f"  AND time >= toDate(%(from)s) AND time <= toDate(%(to)s) "
                f"GROUP BY bucket, depth "
                f"ORDER BY bucket, depth",
                parameters={"gx": grid_x, "gy": grid_y, "from": from_str[:10], "to": to_str[:10]},
            ).result_rows
            model_rows = [(datetime(d.year, d.month, d.day), depth, value) for d, depth, value in monthly_rows]
        else:
            # hourly: every hourly row at this grid cell, for the window.
            model_rows = client.query(
                f"SELECT toStartOfInterval(time, INTERVAL 1 HOUR) AS bucket, depth, avg({var}) AS value "
                f"FROM {table} "
                f"WHERE gridX = %(gx)s AND gridY = %(gy)s "
                f"  AND time >= toDateTime(%(from)s) AND time <= toDateTime(%(to)s) "
                f"GROUP BY bucket, depth "
                f"ORDER BY bucket, depth",
                parameters={"gx": grid_x, "gy": grid_y, "from": from_str, "to": to_str},
            ).result_rows
        if not model_rows:
            raise RuntimeError("No model data found in ClickHouse for the requested window.")

        # Bin keys are the shared x-axis for both grids, normalised to naive
        # datetimes so they compare equal to _floor_bin's output below
        # regardless of whether the ClickHouse client returns naive or
        # tz-aware datetime objects for DateTime columns.
        bin_keys = sorted({r[0].replace(tzinfo=None) for r in model_rows})
        bin_index = {b: i for i, b in enumerate(bin_keys)}
        level_index = {lvl: i for i, lvl in enumerate(levels)}

        model_grid: list[list[Optional[float]]] = [[None] * len(bin_keys) for _ in levels]
        for t, depth, value in model_rows:
            if _is_bad(value):
                continue
            li = level_index.get(float(depth))
            hi = bin_index.get(t.replace(tzinfo=None))
            if li is None or hi is None:
                continue
            model_grid[li][hi] = float(value)

        base = {
            "time": [b.strftime("%Y-%m-%dT%H:%M:%S") for b in bin_keys],
            "depths": levels,
            "model": model_grid,
            "coverage": coverage,
            # Which cell actually answered, and how far it is from the point the
            # user clicked. A click near the edge of the domain can snap several
            # km without failing, and the chart has no other way to say so.
            "grid": {
                "lat": cell_lat,
                "lon": cell_lon,
                "distanceKm": round(cell_dist_km, 2),
            },
        }
        if sensor_id is None:
            # Model-only caller (Depth tab): no second grid to build.
            return {**base, "sensor": None}

        # ── SENSOR: raw casts, snapped to nearest model level + time bucket ───────
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
            hi = bin_index.get(_floor_bin(t, mode))
            if hi is None:
                continue  # cast falls outside the model's bin grid for this window
            li = _nearest_level_index(float(depth), levels)
            buckets[(li, hi)].append(float(value))

        sensor_grid: list[list[Optional[float]]] = [[None] * len(bin_keys) for _ in levels]
        for (li, hi), values in buckets.items():
            values.sort()
            sensor_grid[li][hi] = values[len(values) // 2]  # median — robust to a stray spike mid-cast

        return {**base, "sensor": sensor_grid}
    finally:
        client.close()
