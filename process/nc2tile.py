#!/usr/bin/env python3
"""Re-export nc2tile from shared library for backwards compatibility."""

import sys
import os

# Add parent directory to path so we can import shared
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import and re-export everything from shared
from shared.nc2tile import (
    GRID_CACHE,
    GRID_CACHE_PATH,
    INTERP_CACHE,
    logger,
    get_db_conn,
    _load_grid_cache,
    _write_grid_cache,
    get_grid_from_db,
    _PrecomputedLinearInterpolator,
    _PrecomputedNearestInterpolator,
    _get_interpolator,
    compute_mercator_grid_bounds,
    build_target_grid,
    _process_task,
    scale_to_uint8,
    cap_to_range,
    write_png_packed,
    compute_global_minmax_exclude_zero,
    compute_global_minmax,
    process_variable,
    parse_args,
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
