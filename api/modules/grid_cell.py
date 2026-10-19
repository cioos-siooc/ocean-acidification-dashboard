"""The model cell nearest a point, with its water depth.

Lets the frontend place a sensor relative to the model's seabed: for a source
with a bottom layer (LiveOcean), a near-bottom mooring belongs on "bottom",
not on whichever fixed level is numerically closest (often below the seabed).
"""
from __future__ import annotations

from modules.clickhouse_helpers import get_ch_client
from modules.extractTimeseries import _find_nearest_grid_point
from modules.sources import GRID_TABLE_BY_SOURCE, has_bottom_rows, require_source


def grid_cell(source: str, lat: float, lon: float) -> dict:
    require_source(source)
    grid_table = GRID_TABLE_BY_SOURCE[source]
    client = get_ch_client()
    try:
        gx, gy, cell_lat, cell_lon, dist_km = _find_nearest_grid_point(client, grid_table, lat, lon, source=source)
        h = None
        # Only grids of sources with a bottom layer carry `h` (grid_LO).
        if has_bottom_rows(source):
            rows = client.query(
                f"SELECT h FROM {grid_table} WHERE gridX = %(gx)s AND gridY = %(gy)s LIMIT 1",
                parameters={"gx": gx, "gy": gy},
            ).result_rows
            h = float(rows[0][0]) if rows else None
    finally:
        client.close()
    return {"lat": cell_lat, "lon": cell_lon, "distanceKm": round(dist_km, 2), "h": h}
