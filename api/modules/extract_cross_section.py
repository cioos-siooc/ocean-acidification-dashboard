"""extract_cross_section.py

Build a depth-vs-distance grid for the model along an arbitrary multi-vertex
polyline drawn on the map, at one time snapshot — the spatial counterpart to
extract_depth_profile.py's single-point *time*-depth grid.

The polyline is resampled into evenly-spaced points by arc length, each
snapped to its nearest grid_SSC cell via shared/grid_lookup.py's cached
KD-tree (one vectorized nearest-neighbor query for every sample point, no
per-point ClickHouse round trip). The distinct cells actually hit are then
read in a single query and the resulting depth columns are broadcast back
out to every sample point that snapped to them.
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

import shared.grid_lookup as grid_lookup
from modules.clickhouse_helpers import get_ch_client
from modules.extractTimeseries import ALLOWED_VARIABLES, DATA_TABLE_BY_SOURCE

EARTH_RADIUS_KM = 6371.0088

# Caps the resampled point count (and so the distinct-cell IN-list size) at a
# few hundred — comfortably inside ClickHouse's max_query_size and the cell
# count TimeDepthHeatmap's custom series is already tuned to render in one
# pass (see its `progressive: false` comment).
DEFAULT_MAX_SAMPLES = 300
DEFAULT_TARGET_SPACING_KM = 1.0
# Floor so a short line still gets several samples instead of just its endpoints.
MIN_TARGET_SPACING_KM = 0.05


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def _is_bad(value) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def _fmt_hourly(dt_str: str) -> str:
    """Normalise an ISO-8601 string to the format ClickHouse DateTime accepts."""
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def resample_polyline(
    vertices: list[tuple[float, float]],
    target_spacing_km: float = DEFAULT_TARGET_SPACING_KM,
    max_samples: int = DEFAULT_MAX_SAMPLES,
) -> tuple[list[tuple[float, float]], list[float], list[float]]:
    """Resample a (lat, lon) polyline into evenly-spaced points by arc length.

    Linear lat/lon interpolation within each segment (not great-circle slerp)
    — fine at this app's regional scale, same flat-coordinate treatment
    nc2tile.py's own interpolators already use for the curvilinear grid.

    Returns (points, distances_km, vertex_distances_km):
      points               — resampled (lat, lon) points; first == vertices[0],
                              last == vertices[-1]
      distances_km         — cumulative distance along the line at each
                              resampled point, ascending from 0
      vertex_distances_km  — cumulative distance at each *original* vertex,
                              for marking segment boundaries client-side

    Raises ValueError for fewer than 2 vertices or a zero-length line.
    """
    if len(vertices) < 2:
        raise ValueError("A cross-section line needs at least 2 vertices.")

    seg_lengths = [
        _haversine_km(vertices[i][0], vertices[i][1], vertices[i + 1][0], vertices[i + 1][1])
        for i in range(len(vertices) - 1)
    ]
    total_km = sum(seg_lengths)
    if total_km <= 0:
        raise ValueError("Cross-section line has zero length.")

    vertex_distances_km = [0.0]
    for length in seg_lengths:
        vertex_distances_km.append(vertex_distances_km[-1] + length)

    spacing = max(target_spacing_km, total_km / max_samples, MIN_TARGET_SPACING_KM)
    sample_count = max(2, min(max_samples, math.ceil(total_km / spacing) + 1))

    distances_km = [total_km * i / (sample_count - 1) for i in range(sample_count)]

    points: list[tuple[float, float]] = []
    seg_idx = 0
    for d in distances_km:
        while seg_idx < len(seg_lengths) - 1 and d > vertex_distances_km[seg_idx + 1]:
            seg_idx += 1
        seg_start_d = vertex_distances_km[seg_idx]
        seg_len = seg_lengths[seg_idx]
        frac = 0.0 if seg_len == 0 else (d - seg_start_d) / seg_len
        frac = min(1.0, max(0.0, frac))
        lat1, lon1 = vertices[seg_idx]
        lat2, lon2 = vertices[seg_idx + 1]
        points.append((lat1 + frac * (lat2 - lat1), lon1 + frac * (lon2 - lon1)))

    return points, distances_km, vertex_distances_km


def extract_cross_section(
    *,
    source: str,
    var: str,
    vertices: list[tuple[float, float]],
    dt: str,
    bin_mode: str,
) -> dict:
    """
    Build the model's depth-vs-distance grid along an arbitrary polyline, at
    one time snapshot.

    Parameters
    ----------
    source, var : model source/variable, as in extractTimeseries.extract_timeseries
    vertices    : polyline vertices as [(lat, lon), ...], >= 2 points
    dt          : a full timestamp for "hourly", "YYYY-MM-DD" for "daily", or
                  "YYYY-MM" for "monthly" — matching `bin_mode`
    bin_mode    : "hourly", "daily", or "monthly"

    Returns
    -------
    dict with keys:
      "distances_km"        — cumulative distance (km) at each resampled
                               sample point along the line, ascending
      "depths"               — model depth levels (metres) actually present
                                in the result, ascending
      "model"                — 2D list [depthIdx][sampleIdx] of float | None
      "vertex_distances_km"  — cumulative distance (km) at each original
                                drawn vertex, for segment-boundary markers
      "points"                — [lat, lon] of each resampled sample point,
                                 same order/length as "distances_km" — lets a
                                 client map a clicked column back to a map
                                 coordinate (e.g. the vertical-profile drawer)

    Raises
    ------
    ValueError   — unknown source/var/bin_mode, or an invalid line (see
                   resample_polyline)
    RuntimeError — no data found for the requested line/time
    """
    if source not in DATA_TABLE_BY_SOURCE:
        raise ValueError(f"Source '{source}' is not yet available via ClickHouse.")
    if var not in ALLOWED_VARIABLES:
        raise ValueError(f"Unknown variable '{var}'. Supported variables: {sorted(ALLOWED_VARIABLES)}")
    if bin_mode not in ("hourly", "daily", "monthly"):
        raise ValueError(f"Unsupported bin_mode '{bin_mode}'. Supported: hourly, daily, monthly.")

    table = DATA_TABLE_BY_SOURCE[source]

    points, distances_km, vertex_distances_km = resample_polyline(vertices)

    lats = [p[0] for p in points]
    lons = [p[1] for p in points]
    grid_x, grid_y = grid_lookup.nearest_grid_cells(lons, lats)
    cell_for_sample = [(int(gx), int(gy)) for gx, gy in zip(grid_x, grid_y)]

    # Distinct cells actually hit, in first-occurrence order — bounds the
    # query's IN-list size at resample_polyline's max_samples regardless of
    # how many sample points collapse onto the same cell (typical whenever
    # the line is much longer than the grid's native cell spacing).
    distinct_cells = list(dict.fromkeys(cell_for_sample))

    client = get_ch_client()
    try:
        if bin_mode == "daily":
            rows = client.query(
                f"SELECT gridX, gridY, depth, {var}_mean AS value FROM SalishSeaCast_daily "
                f"WHERE (gridX, gridY) IN %(cells)s AND time = toDate(%(day)s)",
                parameters={"cells": distinct_cells, "day": dt[:10]},
            ).result_rows
        elif bin_mode == "monthly":
            rows = client.query(
                f"SELECT gridX, gridY, depth, avg({var}_mean) AS value FROM SalishSeaCast_daily "
                f"WHERE (gridX, gridY) IN %(cells)s AND toStartOfMonth(time) = toDate(%(month)s) "
                f"GROUP BY gridX, gridY, depth",
                parameters={"cells": distinct_cells, "month": f"{dt[:7]}-01"},
            ).result_rows
        else:
            rows = client.query(
                f"SELECT gridX, gridY, depth, {var} AS value FROM {table} "
                f"WHERE (gridX, gridY) IN %(cells)s AND time = %(t)s",
                parameters={"cells": distinct_cells, "t": _fmt_hourly(dt)},
            ).result_rows
        if not rows:
            raise RuntimeError("No model data found in ClickHouse for the requested line/time.")

        levels = sorted({float(depth) for _, _, depth, value in rows if not _is_bad(value)})
        if not levels:
            raise RuntimeError("No model data found in ClickHouse for the requested line/time.")
        level_index = {lvl: i for i, lvl in enumerate(levels)}

        cell_values: dict[tuple[int, int], dict[float, float]] = defaultdict(dict)
        for gx, gy, depth, value in rows:
            if _is_bad(value):
                continue
            cell_values[(int(gx), int(gy))][float(depth)] = float(value)

        model_grid: list[list[Optional[float]]] = [[None] * len(points) for _ in levels]
        for sample_idx, cell in enumerate(cell_for_sample):
            for depth, value in cell_values.get(cell, {}).items():
                li = level_index.get(depth)
                if li is not None:
                    model_grid[li][sample_idx] = value

        return {
            "distances_km": distances_km,
            "depths": levels,
            "model": model_grid,
            "vertex_distances_km": vertex_distances_km,
            "points": [[lat, lon] for lat, lon in points],
        }
    finally:
        client.close()
