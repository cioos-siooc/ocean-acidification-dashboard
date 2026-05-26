"""Database-backed helpers for the `/variables` API endpoint.

This module contains a single helper `get_variables` which executes the
query and returns a list of dicts for both SSC and LiveOcean models.
"""
from typing import List, Dict
import datetime


def get_variables(db_host: str, db_port: int, db_name: str, db_user: str, db_password: str) -> List[Dict]:
    # Get metadata from fields table and actual time ranges from nc_jobs
    # Query actual start/end times instead of synthesizing them
    query = """
        WITH time_ranges AS (
            SELECT DISTINCT ON (variable_id, start_time, end_time)
                variable_id,
                start_time,
                end_time
            FROM nc_jobs
            WHERE status = 'success_image'
        )
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
            ARRAY_AGG(jsonb_build_object('start', tr.start_time, 'end', tr.end_time) ORDER BY tr.start_time) as time_ranges
        FROM fields f
        LEFT JOIN datasets d ON f.dataset_id = d.id
        LEFT JOIN time_ranges tr ON f.id = tr.variable_id
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
            source = row.get("source")

            # Generate actual datetimes from time ranges
            time_ranges = row.get("time_ranges")
            if time_ranges is not None:
                try:
                    expanded_datetimes = []
                    
                    for time_range in time_ranges:
                        start_dt = time_range.get("start")
                        end_dt = time_range.get("end")
                        
                        if start_dt is None or end_dt is None:
                            continue
                        
                        # Convert to datetime if needed
                        if isinstance(start_dt, str):
                            start_dt = datetime.datetime.fromisoformat(start_dt)
                        if isinstance(end_dt, str):
                            end_dt = datetime.datetime.fromisoformat(end_dt)
                        
                        if source != 'Live Ocean':
                            # SSC: hourly datetimes at half-hour marks
                            # Generate all hourly times from start to end
                            current = start_dt.replace(minute=30, second=0, microsecond=0)
                            while current <= end_dt:
                                expanded_datetimes.append(current)
                                current += datetime.timedelta(hours=1)
                        else:
                            # LiveOcean: 4-hourly datetimes
                            # Generate all 4-hourly times from start to end
                            current = start_dt.replace(minute=0, second=0, microsecond=0)
                            while current <= end_dt:
                                expanded_datetimes.append(current)
                                current += datetime.timedelta(hours=4)
                    
                    available_datetimes = sorted(set(expanded_datetimes))  # Remove duplicates and sort
                except Exception as e:
                    print(f"Error processing time ranges for variable {variable}: {e}")
                    available_datetimes = None
            else:
                available_datetimes = None
            
            colormap_min = row.get("min")
            colormap_max = row.get("max")
            depths = row.get("depths")
            precision = row.get("precision")
            
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
