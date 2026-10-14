"""grid_lookup.py

Snap many arbitrary (lon, lat) points to their nearest model grid cell in one
vectorized pass, for callers that need to sample a curve across the grid
(e.g. a cross-section polyline) without a ClickHouse round trip per point.

Reuses nc2tile.py's cached grid load (same module/on-disk cache, no
duplicate fetch) but only needs nearest-neighbor snapping, not the
Delaunay/linear interpolation machinery nc2tile's own interpolator classes
carry for tile regridding.
"""

from __future__ import annotations

from typing import Tuple

import numpy as np
from scipy.spatial import cKDTree

import shared.nc2tile as nc2tile

# Module-level cache, per grid table: (kdtree, gridX_flat, gridY_flat) for
# every valid grid point, aligned so kdtree's point i corresponds to
# (gridX_flat[i], gridY_flat[i]). Built once per process per table.
_KDTREE_CACHE: dict[str, Tuple[cKDTree, np.ndarray, np.ndarray]] = {}


def _build_kdtree(grid_table: str) -> Tuple[cKDTree, np.ndarray, np.ndarray]:
    if grid_table in _KDTREE_CACHE:
        return _KDTREE_CACHE[grid_table]

    lon, lat = nc2tile.get_grid_from_db(grid_table)
    row_pos, col_pos, nrows, ncols = nc2tile.get_grid_index_maps(grid_table)

    gridx_by_row = np.empty(nrows, dtype=np.int64)
    for gx, i in row_pos.items():
        gridx_by_row[i] = gx
    gridy_by_col = np.empty(ncols, dtype=np.int64)
    for gy, j in col_pos.items():
        gridy_by_col[j] = gy

    gx_grid, gy_grid = np.meshgrid(gridx_by_row, gridy_by_col, indexing="ij")

    lon_flat, lat_flat = lon.ravel(), lat.ravel()
    valid = np.isfinite(lon_flat) & np.isfinite(lat_flat)
    pts = np.column_stack((lon_flat[valid], lat_flat[valid]))

    tree = cKDTree(pts)
    _KDTREE_CACHE[grid_table] = (tree, gx_grid.ravel()[valid], gy_grid.ravel()[valid])
    return _KDTREE_CACHE[grid_table]


def nearest_grid_cells(
    lons: np.ndarray, lats: np.ndarray, grid_table: str = 'grid_SSC',
) -> Tuple[np.ndarray, np.ndarray]:
    """Return (gridX, gridY) arrays, one pair per input (lon, lat) point.

    A single vectorized cKDTree query handles arbitrarily many points at
    once — the tree itself is built once per process and reused across
    requests, so repeated calls only pay for the query, not the grid load.
    """
    tree, gx, gy = _build_kdtree(grid_table)
    lons = np.asarray(lons, dtype=float)
    lats = np.asarray(lats, dtype=float)
    _, idx = tree.query(np.column_stack((lons, lats)), k=1)
    return gx[idx], gy[idx]
