#!/usr/bin/env python3
import argparse
import json
import os
import sys
from datetime import datetime

import clickhouse_connect

DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = 8123
DEFAULT_DB_USER = "default"
DEFAULT_DB_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
DEFAULT_DB_NAME = "default"

VARIABLE_COLUMN_MAP = {
    "temperature": "temperature",
    "salinity": "salinity",
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Query a time series from the ClickHouse ocean_4d_efficient table",
        epilog="Example: python query_timeseries.py --variable temperature --depth 5 --gridX -123.45 --gridY 48.5",
    )
    parser.add_argument("--variable", required=True, choices=VARIABLE_COLUMN_MAP.keys(), help="Variable to extract: temperature or salinity")
    parser.add_argument("--depth", required=True, type=float, help="Depth value to query")
    parser.add_argument("--gridX", required=True, type=float, help="Longitude coordinate")
    parser.add_argument("--gridY", required=True, type=float, help="Latitude coordinate")
    parser.add_argument("--from", dest="from_time", default=None, help="Start time filter (ISO 8601)")
    parser.add_argument("--to", dest="to_time", default=None, help="End time filter (ISO 8601)")
    parser.add_argument("--db-host", default=DEFAULT_DB_HOST, help="ClickHouse host")
    parser.add_argument("--db-port", type=int, default=DEFAULT_DB_PORT, help="ClickHouse HTTP port")
    parser.add_argument("--db-user", default=DEFAULT_DB_USER, help="ClickHouse user")
    parser.add_argument("--db-password", default=DEFAULT_DB_PASSWORD, help="ClickHouse password")
    parser.add_argument("--db-name", default=DEFAULT_DB_NAME, help="ClickHouse database name")
    parser.add_argument("--tolerance", type=float, default=1e-6, help="Float tolerance for matching coordinates and depth")
    parser.add_argument("--limit", type=int, default=10000, help="Maximum number of rows to return")
    return parser.parse_args(argv)


def format_iso(dt):
    if isinstance(dt, datetime):
        return dt.isoformat()
    return dt


def build_query(column: str, depth: float, lat: float, lon: float, from_time: str | None, to_time: str | None, limit: int, tolerance: float) -> tuple[str, list]:
    sql = [f"SELECT time, {column} AS value FROM ocean_4d_efficient"]
    sql.append("WHERE abs(depth - %s) <= %s AND abs(latitude - %s) <= %s AND abs(longitude - %s) <= %s")
    params = [depth, tolerance, lat, tolerance, lon, tolerance]
    if from_time is not None:
        sql.append("AND time >= toDateTime(%s)")
        params.append(from_time)
    if to_time is not None:
        sql.append("AND time <= toDateTime(%s)")
        params.append(to_time)
    sql.append("ORDER BY time ASC")
    sql.append("LIMIT %s")
    params.append(limit)
    return " ".join(sql), params


def main(argv=None):
    args = parse_args(argv)
    column = VARIABLE_COLUMN_MAP[args.variable]

    client = clickhouse_connect.get_client(
        host=args.db_host,
        port=args.db_port,
        username=args.db_user,
        password=args.db_password,
        database=args.db_name,
    )

    query, params = build_query(
        column=column,
        depth=args.depth,
        lat=args.gridY,
        lon=args.gridX,
        from_time=args.from_time,
        to_time=args.to_time,
        limit=args.limit,
        tolerance=args.tolerance,
    )

    try:
        result = client.query(query, parameters=params)
    except Exception as exc:
        print(f"✗ Query failed: {exc}", file=sys.stderr)
        return 1

    rows = []
    for row in result.result_rows:
        # row values are [time, value]
        rows.append({
            "time": format_iso(row[0]),
            "value": None if row[1] is None else float(row[1]),
        })

    print(json.dumps({
        "variable": args.variable,
        "depth": args.depth,
        "gridX": args.gridX,
        "gridY": args.gridY,
        "count": len(rows),
        "series": rows,
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
