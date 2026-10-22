#!/usr/bin/env python3
"""point_snapshot.py

Every variable at one point, for the frontend's point snapshot: the model's
eight variables at the map's depth, plus the selected sensor's own variables
at the sensor's depth, each as a short trailing window ending at `to`.

The model side is a single query — all variables are columns of the same
SalishSeaCast_hourly row, so one read at the nearest cell and level returns
them together. The sensor side reuses extract_sensor_timeseries per variable
(so instruments merge by the same priority as everywhere else) and falls back
to the last reading on record when the window is empty, so a stale sensor
still says when it last reported.

Values are in canonical units (variable_config.yml `unit`); the frontend
converts for display.
"""
from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

from modules.clickhouse_helpers import get_ch_client
from modules.extractTimeseries import (
    ALLOWED_VARIABLES,
    DATA_TABLE_BY_SOURCE,
    GRID_TABLE_BY_SOURCE,
    OutsideDomainError,
    _find_nearest_grid_point,
    _resolve_depth,
)
from modules.extractSensorTimeseries import extract_sensor_timeseries

# Fixed order so the response (and the frontend's cards) read the same way
# every time; matches variable_config.yml.
SNAPSHOT_VARIABLES = [
    "temperature",
    "salinity",
    "ph_total",
    "dissolved_oxygen",
    "omega_arag",
    "omega_cal",
    "dissolved_inorganic_carbon",
    "total_alkalinity",
]
assert set(SNAPSHOT_VARIABLES) == ALLOWED_VARIABLES

DEFAULT_WINDOW_HOURS = 72


def _clean(v) -> Optional[float]:
    if v is None:
        return None
    f = float(v)
    return None if math.isnan(f) else f


def _iso(t) -> str:
    return pd.Timestamp(t).strftime("%Y-%m-%dT%H:%M:%S")


def _model_snapshot(client, source: str, lat: float, lon: float, depth: float,
                    t_from: datetime, t_to: datetime) -> dict:
    table = DATA_TABLE_BY_SOURCE[source]
    grid_x, grid_y, grid_lat, grid_lon, dist_km = _find_nearest_grid_point(
        client, GRID_TABLE_BY_SOURCE[source], lat, lon, source=source)
    depth_sel = _resolve_depth(client, table, grid_x, grid_y, depth)
    grid = {"lat": grid_lat, "lon": grid_lon, "distanceKm": round(dist_km, 2)}
    if depth_sel is None:
        # Deeper than this cell's water column: an empty series, not an error.
        return {"depth": None, "grid": grid, "time": [], "values": {v: [] for v in SNAPSHOT_VARIABLES}}

    cols = ", ".join(SNAPSHOT_VARIABLES)
    rows = client.query(
        f"SELECT time, {cols} FROM {table} "
        f"WHERE gridX = %(gx)s AND gridY = %(gy)s AND depth = %(depth)s "
        f"AND time >= %(t_from)s AND time <= %(t_to)s ORDER BY time",
        parameters={"gx": grid_x, "gy": grid_y, "depth": depth_sel, "t_from": t_from, "t_to": t_to},
    ).result_rows
    # Duplicated hours can exist after re-ingestion; keep the last per time.
    by_time: dict = {}
    for r in rows:
        by_time[r[0]] = r[1:]
    times = sorted(by_time)
    return {
        "depth": float(depth_sel),
        "grid": grid,
        "time": [_iso(t) for t in times],
        "values": {
            var: [_clean(by_time[t][i]) for t in times]
            for i, var in enumerate(SNAPSHOT_VARIABLES)
        },
    }


def _sensor_variables(client, sensor_id: str) -> list[str]:
    """The snapshot variables this sensor measures, in snapshot order
    (keys of `sensors.variables`, which are canonical variable ids)."""
    safe_id = str(sensor_id).replace("'", "")
    rows = client.query(
        f"SELECT variables FROM sensors FINAL WHERE id = '{safe_id}' LIMIT 1"
    ).result_rows
    have = set(json.loads(rows[0][0]).keys()) if rows and rows[0][0] else set()
    return [v for v in SNAPSHOT_VARIABLES if v in have]


def _last_reading(client, sensor_id: str, variable: str, near_depth: Optional[float]) -> Optional[dict]:
    """The most recent value on record (any instrument). `near_depth` limits a
    profiler to casts near the level being read; a fixed sensor has one depth,
    so it is read unfiltered (its stored depth needn't match the catalog's)."""
    safe_id = str(sensor_id).replace("'", "")
    safe_var = variable.replace("'", "''")
    depth_where = f" AND abs(depth - {float(near_depth)}) < 5" if near_depth is not None else ""
    rows = client.query(
        f"SELECT max(time), argMax(value, time) FROM sensor_timeseries "
        f"WHERE sensor_id = '{safe_id}' AND variable = '{safe_var}'{depth_where} AND NOT isNaN(value)"
    ).result_rows
    if not rows or rows[0][0] is None or pd.Timestamp(rows[0][0]).year < 1971:
        return None
    return {"time": _iso(rows[0][0]), "value": _clean(rows[0][1])}


def _sensor_snapshot(client, source: str, sensor_id: str, sensor_depth: Optional[float],
                     model_depth: float, t_from: datetime, t_to: datetime) -> dict:
    # A profiler (depth -1 or unknown) is read at the model's depth, as the
    # Explore chart does; a fixed sensor at its own depth. The reported depth
    # is the catalog's (fixed) or the model level the casts were binned to
    # (profiler) — stored row depths aren't reliable for fixed sensors.
    is_profiler = sensor_depth is None or sensor_depth < 0
    query_depth = model_depth if is_profiler else sensor_depth
    out: dict = {"id": sensor_id, "depth": None if is_profiler else float(sensor_depth), "variables": {}}
    for var in _sensor_variables(client, sensor_id):
        try:
            series = extract_sensor_timeseries(
                sensor_id, var, t_from.isoformat(), t_to.isoformat(),
                depth=query_depth, source=source)
        except FileNotFoundError:
            series = {"time": [], "value": [], "depth": None}
        if is_profiler and out["depth"] is None and series.get("depth") is not None:
            out["depth"] = float(series["depth"])
        last = None
        for t, v in zip(reversed(series["time"]), reversed(series["value"])):
            if v is not None:
                last = {"time": t, "value": v}
                break
        if last is None:
            last = _last_reading(client, sensor_id, var, query_depth if is_profiler else None)
        out["variables"][var] = {"time": series["time"], "value": series["value"], "last": last}
    return out


def extract_point_snapshot(
    *,
    source: str,
    lat: float,
    lon: float,
    depth: float,
    to: Optional[str] = None,
    hours: int = DEFAULT_WINDOW_HOURS,
    sensor_id: Optional[str] = None,
    sensor_depth: Optional[float] = None,
) -> dict:
    """Snapshot of every variable at (lat, lon).

    Returns ``{"to", "hours", "model", "modelError", "sensor"}``. ``model`` is
    None (with ``modelError`` set to OutsideDomainError's payload) when the
    point is outside the model domain — the sensor half still answers.
    """
    if source not in DATA_TABLE_BY_SOURCE:
        raise ValueError(f"Unknown source '{source}'")
    hours = max(1, min(int(hours), 24 * 31))
    t_to = (datetime.fromisoformat(to.replace("Z", "+00:00")) if to else datetime.now(timezone.utc))
    t_to = t_to.astimezone(timezone.utc).replace(tzinfo=None)
    t_from = t_to - timedelta(hours=hours)

    client = get_ch_client()
    try:
        model, model_error = None, None
        try:
            model = _model_snapshot(client, source, lat, lon, depth, t_from, t_to)
        except OutsideDomainError as exc:
            model_error = exc.to_payload()
        sensor = (_sensor_snapshot(client, source, sensor_id, sensor_depth, depth, t_from, t_to)
                  if sensor_id else None)
        return {"to": _iso(t_to), "hours": hours, "model": model, "modelError": model_error, "sensor": sensor}
    finally:
        client.close()
