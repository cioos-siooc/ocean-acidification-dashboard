"""PostGIS database helper functions for extracting ocean data.

Provides common utilities for connecting to PostGIS databases and querying
grid coordinates with distance validation.
"""
from __future__ import annotations
from typing import Optional, Tuple

try:
    import psycopg2
    import psycopg2.extras
    psycopg2_import_error = False
except Exception:
    psycopg2 = None
    psycopg2_import_error = True


def connect_db(dsn: Optional[str], host: str, port: int, user: str, password: str, dbname: str):
    """Connect to a PostGIS database.
    
    Args:
        dsn: Optional libpq connection string (overrides host/user/password/dbname if provided)
        host: Database host
        port: Database port
        user: Database user
        password: Database password
        dbname: Database name
    
    Returns:
        psycopg2 connection object
    
    Raises:
        RuntimeError: If psycopg2 is not installed
    """
    if psycopg2 is None:
        raise RuntimeError(
            "psycopg2 is not available. For quick development install the binary wheel:\n"
            "  pip install psycopg2-binary\n"
            "or in uv: uv add --active psycopg2-binary\n"
        )
    if dsn:
        return psycopg2.connect(dsn)
    return psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)


def query_nearest_rowcol(conn, table: str, lat: float, lon: float, max_dist_km: float = 25.0) -> Tuple[int, int, float, float]:
    """Find the nearest grid point to (lat, lon) in the PostGIS table.
    
    Uses PostGIS KNN (<->) operator for efficient spatial queries. Validates that
    the nearest point is within max_dist_km to prevent silent mis-matches for
    out-of-domain coordinates.
    
    Args:
        conn: PostgreSQL connection object
        table: Name of the grid table with columns: row_idx, col_idx, lat, lon, geom (SRID=4326)
        lat: Latitude in degrees
        lon: Longitude in degrees
        max_dist_km: Maximum allowed distance in kilometers. Raises error if nearest point is farther. Default 25 km.
    
    Returns:
        Tuple of (row_idx, col_idx, lat, lon) of the nearest grid point
    
    Raises:
        RuntimeError: If grid table is empty or if nearest point is > max_dist_km away
    """
    sql = f"SELECT row_idx, col_idx, lat, lon, ST_DistanceSphere(geom, ST_SetSRID(ST_MakePoint(%s,%s),4326)) as dist_m FROM {table} ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s,%s),4326) LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(sql, (lon, lat, lon, lat))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("Grid table is empty or not found")
        row_idx, col_idx, grid_lat, grid_lon, dist_meters = row
        
        # Check if the nearest point is within the allowed distance
        dist_km = dist_meters / 1000.0
        if dist_km > max_dist_km:
            raise RuntimeError(
                f"Requested coordinate ({lat}, {lon}) is {dist_km:.1f} km from the nearest grid point. "
                f"Maximum allowed distance is {max_dist_km} km. "
                f"Please verify your coordinates are within the model domain (Salish Sea / BC coast region)."
            )
        
        return int(row_idx), int(col_idx), float(grid_lat), float(grid_lon)


def query_neighbors(conn, table: str, row: int, col: int, radius: int = 1) -> list:
    """Return list of neighbor records with (row_idx, col_idx, lat, lon) within radius (inclusive).
    
    Args:
        conn: PostgreSQL connection object
        table: Name of the grid table
        row: Center row index
        col: Center column index
        radius: Search radius in grid cells (default 1)
    
    Returns:
        List of tuples: (row_idx, col_idx, lat, lon)
    """
    r0 = row - radius
    r1 = row + radius
    c0 = col - radius
    c1 = col + radius
    sql = f"SELECT row_idx, col_idx, lat, lon FROM {table} WHERE row_idx BETWEEN %s AND %s AND col_idx BETWEEN %s AND %s"
    with conn.cursor() as cur:
        cur.execute(sql, (r0, r1, c0, c1))
        return [(int(r), int(c), float(latv), float(lonv)) for r, c, latv, lonv in cur.fetchall()]


def get_grid_shape_from_db(conn, table: str) -> Tuple[int, int]:
    """Get the dimensions (rows, cols) of the grid from the PostGIS table.
    
    Args:
        conn: PostgreSQL connection object
        table: Name of the grid table
    
    Returns:
        Tuple of (num_rows, num_cols)
    """
    sql = f"SELECT COALESCE(MAX(row_idx),0), COALESCE(MAX(col_idx),0) FROM {table}"
    with conn.cursor() as cur:
        cur.execute(sql)
        r, c = cur.fetchone()
        return int(r) + 1, int(c) + 1
