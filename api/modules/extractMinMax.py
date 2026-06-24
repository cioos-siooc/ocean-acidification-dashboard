#!/usr/bin/env python3
"""extractMinMax.py

Compute the min/max of a variable over the visible map area from ClickHouse,
for the "auto-range colorbar" feature — restricted to a lat/lon bounding box,
a single time, and a single depth level.

Examples:
  python modules/extractMinMax.py --var temperature --dt 2026-01-17T00:30:00 \
      --depth 0.5 --north 50 --south 48 --east -122 --west -124
"""
from __future__ import annotations

import argparse
from datetime import datetime
from typing import Optional, Tuple

from modules.clickhouse_helpers import get_ch_client
from modules.extractTimeseries import (
    ALLOWED_VARIABLES,
    DATA_TABLE_BY_SOURCE,
    DEPTH_MATCH_TOLERANCE_M,
    GRID_TABLE_BY_SOURCE,
)


def extract_minmax(
    *,
    source: str,
    var: str,
    dt: datetime,
    depth: Optional[float] = None,
    north: Optional[float] = None,
    south: Optional[float] = None,
    east: Optional[float] = None,
    west: Optional[float] = None,
) -> Tuple[float, float]:
    """Return (min, max) for `var` at `dt`, restricted to the lat/lon bbox if given.

    `depth=None` uses each grid cell's shallowest available level; `depth=-1`
    uses each grid cell's deepest (bottom) level — both vary per cell due to
    bathymetry, so they're resolved per-cell with argMin/argMax rather than a
    fixed depth filter. Any other depth value is expected to be one of the
    canonical native sigma levels (as returned by /variables) and is matched
    within DEPTH_MATCH_TOLERANCE_M — native levels are stored as Float32, so
    a value nominally "14.6" round-trips as 14.600000381469727, which would
    never satisfy a strict equality match against the Float64 literal from
    the request (see extractTimeseries.py's _resolve_depth, same issue). A
    depth that doesn't exist at any cell in the area still means no data.

    Raises RuntimeError if no data matches.
    """
    if source not in DATA_TABLE_BY_SOURCE:
        raise RuntimeError(
            f"Source '{source}' is not yet available via ClickHouse. "
            f"Supported sources: {sorted(DATA_TABLE_BY_SOURCE)}"
        )
    if var not in ALLOWED_VARIABLES:
        raise ValueError(f"Unknown variable '{var}'. Supported variables: {sorted(ALLOWED_VARIABLES)}")

    table = DATA_TABLE_BY_SOURCE[source]
    grid_table = GRID_TABLE_BY_SOURCE[source]
    has_bounds = None not in (north, south, east, west)

    client = get_ch_client()
    try:
        params: dict = {"dt": dt}
        where = ["time = %(dt)s"]

        if has_bounds:
            # Filter via a subquery against the (small) grid table rather than
            # pulling matching (gridX, gridY) pairs into Python and re-sending
            # them as a literal IN-list — for a wide visible-area bbox that
            # list can be tens of thousands of tuples, large enough to blow
            # past ClickHouse's max_query_size (256KB) once inlined as SQL text.
            where.append(f"""(gridX, gridY) IN (
                SELECT gridX, gridY FROM {grid_table}
                WHERE latitude BETWEEN %(south)s AND %(north)s
                  AND longitude BETWEEN %(west)s AND %(east)s
            )""")
            params.update({"south": south, "north": north, "west": west, "east": east})

        if depth is None or float(depth) == -1.0:
            agg = "argMin" if depth is None else "argMax"
            query = f"""
                SELECT min(value) AS min_val, max(value) AS max_val, count() AS cnt
                FROM (
                    SELECT {agg}({var}, depth) AS value
                    FROM {table}
                    WHERE {' AND '.join(where)}
                    GROUP BY gridX, gridY
                )
            """
        else:
            params["depth"] = float(depth)
            params["depth_tol"] = DEPTH_MATCH_TOLERANCE_M
            where.append("abs(depth - %(depth)s) <= %(depth_tol)s")
            query = f"SELECT min({var}) AS min_val, max({var}) AS max_val, count() AS cnt FROM {table} WHERE {' AND '.join(where)}"

        result = client.query(query, parameters=params)
        min_val, max_val, cnt = result.result_rows[0]
        # ClickHouse's min()/max() return the column type's default (0 for
        # Float32, not NULL) when zero rows match — count() is the only
        # reliable way to tell "no data" apart from a genuine value of 0.
        if cnt == 0:
            raise RuntimeError(
                f"No data available for {var} at {dt.isoformat()}"
                + (f", depth {depth}m" if depth is not None else "")
                + " in the visible area."
            )
        return float(min_val), float(max_val)
    finally:
        client.close()


def main(argv: Optional[list] = None) -> int:
    p = argparse.ArgumentParser(description="Compute min/max for a variable over an area from ClickHouse")
    p.add_argument("--source", default="SalishSeaCast", choices=sorted(DATA_TABLE_BY_SOURCE), help="Data source")
    p.add_argument("--var", "-v", required=True, choices=sorted(ALLOWED_VARIABLES), help="Variable name")
    p.add_argument("--dt", required=True, help="Datetime (ISO-8601)")
    p.add_argument("--depth", type=float, default=None, help="Depth value (-1 for bottom); omit for shallowest")
    p.add_argument("--north", type=float, default=None)
    p.add_argument("--south", type=float, default=None)
    p.add_argument("--east", type=float, default=None)
    p.add_argument("--west", type=float, default=None)

    args = p.parse_args(argv)

    min_val, max_val = extract_minmax(
        source=args.source,
        var=args.var,
        dt=datetime.fromisoformat(args.dt),
        depth=args.depth,
        north=args.north,
        south=args.south,
        east=args.east,
        west=args.west,
    )
    print(f"min={min_val}, max={max_val}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
