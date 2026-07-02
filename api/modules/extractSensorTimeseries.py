"""extractSensorTimeseries.py

Read a sensor time series from ClickHouse sensor_timeseries.

The canonical variable name is stored directly as the `variable` column, so
no code-resolution step is needed — just query by (sensor_id, variable, time
range).  Depth selection (nearest) is handled in CH to avoid pulling all
depths over the wire.

Returns a dict ready to serialize as a JSON response:
  {"time": [...], "value": [...]}
  {"time": [...], "depth": [...], "value": [...]}   — when depth data exists
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Optional

from modules.clickhouse_helpers import get_ch_client


def _fmt(dt_str: str) -> str:
    """Normalise an ISO-8601 string to the format ClickHouse DateTime accepts."""
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def extract_sensor_timeseries(
    sensor_id: int,
    variable: str,
    from_date: str,
    to_date: str,
    depth: Optional[float] = None,
) -> dict:
    """
    Read sensor time series from ClickHouse.

    Parameters
    ----------
    sensor_id : integer sensor ID
    variable  : canonical variable name (e.g. "dissolved_oxygen")
    from_date : ISO-8601 string (start, inclusive)
    to_date   : ISO-8601 string (end, inclusive)
    depth     : optional depth in metres; if None, returns all depth levels

    Returns
    -------
    dict with keys:
      "time"  — list of ISO-8601 strings
      "value" — list of float | None
      "depth" — list of float  (present only when the data has a depth dimension)

    Raises
    ------
    FileNotFoundError — no data found for this sensor / variable
    """
    client = get_ch_client()

    from_str = _fmt(from_date)
    to_str   = _fmt(to_date)

    # Escape variable name for use in query string (it comes from the API
    # request but is already validated by the caller against the sensors table).
    safe_var = variable.replace("'", "''")
    safe_id  = int(sensor_id)

    if depth is not None:
        # Find the single stored depth closest to the requested value, then
        # fetch only rows at that depth.  Two queries, both tiny.
        nearest_rows = client.query(
            f"SELECT depth FROM sensor_timeseries "
            f"WHERE sensor_id = {safe_id} AND variable = '{safe_var}' "
            f"ORDER BY abs(depth - {float(depth)}) LIMIT 1"
        ).result_rows
        if not nearest_rows:
            raise FileNotFoundError(
                f"No data found for sensor {sensor_id}, variable '{variable}'"
            )
        target_depth = float(nearest_rows[0][0])

        result = client.query(
            f"SELECT time, value FROM sensor_timeseries FINAL "
            f"WHERE sensor_id = {safe_id} AND variable = '{safe_var}' "
            f"  AND depth = {target_depth} "
            f"  AND time >= toDateTime('{from_str}') "
            f"  AND time <= toDateTime('{to_str}') "
            f"ORDER BY time"
        )
        rows = result.result_rows
        if not rows:
            return {"time": [], "value": [], "depth": target_depth}

        times_out = [r[0].strftime("%Y-%m-%dT%H:%M:%S") for r in rows]
        values_out = [None if math.isnan(r[1]) else float(r[1]) for r in rows]
        return {"time": times_out, "depth": target_depth, "value": values_out}

    # No depth filter — return all depth levels.
    result = client.query(
        f"SELECT time, depth, value FROM sensor_timeseries FINAL "
        f"WHERE sensor_id = {safe_id} AND variable = '{safe_var}' "
        f"  AND time >= toDateTime('{from_str}') "
        f"  AND time <= toDateTime('{to_str}') "
        f"ORDER BY time, depth"
    )
    rows = result.result_rows
    if not rows:
        raise FileNotFoundError(
            f"No data found for sensor {sensor_id}, variable '{variable}'"
        )

    # Check whether all rows share the same depth (fixed-depth sensor) or not.
    depths = [float(r[1]) for r in rows]
    all_same_depth = len(set(depths)) == 1

    times_out  = [r[0].strftime("%Y-%m-%dT%H:%M:%S") for r in rows]
    values_out = [None if math.isnan(float(r[2])) else float(r[2]) for r in rows]

    if all_same_depth:
        # Fixed-depth sensor — omit depth from response to match old NC behaviour.
        return {"time": times_out, "value": values_out}

    return {"time": times_out, "depth": depths, "value": values_out}
