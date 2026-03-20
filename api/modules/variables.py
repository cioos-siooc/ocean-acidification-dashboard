"""Database-backed helpers for the `/variables` API endpoint.

This module contains a single helper `get_variables` which executes the
query and returns a list of dicts: {"var": ..., "from_dt": ..., "to_dt": ...}.
"""
import os
from typing import List, Dict


def get_variables(db_host: str, db_port: int, db_name: str, db_user: str, db_password: str) -> List[Dict]:
    # Include `colormap`, `bounds`, and `source` columns so clients have full metadata
    query = """
        SELECT 
            f.variable, 
            f.available_datetimes, 
            f.min, 
            f.max, 
            f.depths_image, 
            f.precision, 
            f.colormap,
            d.bounds,
            d.source
        FROM fields f
        LEFT JOIN datasets d ON f.dataset_id = d.id;
    """
    
    try:
        import psycopg2
        import psycopg2.extras
    except Exception as exc:
        raise RuntimeError("psycopg2 is required for /variables endpoint") from exc

    conn = None
    try:
        conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        
        variables = []
        for row in rows:
            variable = row.get("variable")
            print(variable)
            available_datetimes = row.get("available_datetimes")
            colormap_min = row.get("min")
            colormap_max = row.get("max")
            depths = row.get("depths_image")
            precision = row.get("precision")
            # if available_datetimes and isinstance(available_datetimes, list) and len(available_datetimes) > 0:
            variables.append({
                "var": variable,
                "dts": available_datetimes,
                "colormapMin": colormap_min,
                "colormapMax": colormap_max,
                "depths": depths,
                "precision": precision,
                "colormap": row.get("colormap"),
                "bounds": row.get("bounds"),
                "source": row.get("source")
            })
        return variables
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
