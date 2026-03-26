"""Database-backed helpers for the `/variables` API endpoint.

This module contains a single helper `get_variables` which executes the
query and returns a list of dicts: {"var": ..., "from_dt": ..., "to_dt": ...}.
"""
from typing import List, Dict
import datetime


def get_variables(db_host: str, db_port: int, db_name: str, db_user: str, db_password: str) -> List[Dict]:
    # Get metadata from fields table and unique datetimes from nc_jobs where status='success_image'
    query = """
        SELECT 
            f.variable, 
            f.min, 
            f.max,  
            f.precision, 
            f.colormap,
            f.unit,
            d.bounds,
            d.depths,
            d.source,
            ARRAY_AGG(DISTINCT nj.start_time ORDER BY nj.start_time) as available_datetimes
        FROM fields f
        LEFT JOIN datasets d ON f.dataset_id = d.id
        LEFT JOIN nc_jobs nj ON f.id = nj.variable_id AND nj.status = 'success_image'
        GROUP BY f.id, f.variable, d.id, d.source;
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
            # Expand dates to hourly datetimes at half-hour marks (00:30 to 23:30)
            available_datetimes = row.get("available_datetimes")
            if available_datetimes is not None:
                try:
                    expanded_datetimes = []
                    for dt in available_datetimes:
                        # Convert to date if it's a timestamp
                        date = dt.date() if hasattr(dt, 'date') else dt
                        # Create 24 hourly datetimes at half-hour marks
                        for hour in range(24):
                            hourly_dt = datetime.datetime.combine(
                                date,
                                datetime.time(hour=hour, minute=30)
                            )
                            expanded_datetimes.append(hourly_dt)
                    available_datetimes = expanded_datetimes
                except Exception as e:
                    print(f"Error processing datetimes for variable {variable}: {e}")
                    available_datetimes = None
            colormap_min = row.get("min")
            colormap_max = row.get("max")
            depths = row.get("depths")
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
                "source": row.get("source"),
                "unit": row.get("unit"),
            })
        return variables
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
