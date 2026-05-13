"""Database-backed helpers for the `/variables` API endpoint.

This module contains a single helper `get_variables` which executes the
query and returns a list of dicts for both SSC and LiveOcean models.
"""
from typing import List, Dict
import datetime


def get_variables(db_host: str, db_port: int, db_name: str, db_user: str, db_password: str) -> List[Dict]:
    # Get metadata from fields table and available dates from the unified nc_jobs table
    query = """
        SELECT
            f.variable,
            f.id as field_id,
            f.min,
            f.max,
            f.precision,
            f.colormap,
            f.unit,
            d.bounds,
            d.depths,
            d.source,
            f.dataset_id,
            ARRAY_AGG(DISTINCT pd.start_time ORDER BY pd.start_time) as available_dates
        FROM fields f
        LEFT JOIN datasets d ON f.dataset_id = d.id
        LEFT JOIN nc_jobs pd ON f.id = pd.variable_id AND pd.status = 'success_image'
        GROUP BY f.id, f.variable, f.dataset_id, d.id, d.source;
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
            dataset_id = row.get("dataset_id")  # 4 = LiveOcean, 1-3 = SSC

            # Expand dates to appropriate resolution
            available_dates = row.get("available_dates")
            if available_dates is not None:
                try:
                    expanded_datetimes = []
                    for dt in available_dates:
                        # Convert to date if it's a timestamp
                        date = dt.date() if hasattr(dt, 'date') else dt

                        if dataset_id != 4:
                            # SSC: hourly datetimes at half-hour marks (00:30 to 23:30)
                            for hour in range(24):
                                hourly_dt = datetime.datetime.combine(
                                    date,
                                    datetime.time(hour=hour, minute=30)
                                )
                                expanded_datetimes.append(hourly_dt)
                        else:
                            # LiveOcean: 4-hourly datetimes (00:00, 04:00, 08:00, 12:00, 16:00, 20:00)
                            for hour in [0, 4, 8, 12, 16, 20]:
                                hourly_dt = datetime.datetime.combine(
                                    date,
                                    datetime.time(hour=hour, minute=0)
                                )
                                expanded_datetimes.append(hourly_dt)
                    available_dates = expanded_datetimes
                except Exception as e:
                    print(f"Error processing datetimes for variable {variable}: {e}")
                    available_dates = None
            
            colormap_min = row.get("min")
            colormap_max = row.get("max")
            depths = row.get("depths")
            precision = row.get("precision")
            
            variables.append({
                "var": variable,
                "dts": available_dates,
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
