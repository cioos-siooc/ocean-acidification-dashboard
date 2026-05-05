"""
nc_utils.py

Shared utilities for netCDF4 sensor data files.
"""

import json
import netCDF4
import psycopg2
from datetime import datetime, timedelta
from pathlib import Path

EPOCH = datetime(1970, 1, 1)


def get_datetime_range_from_nc_file(
    nc_path: Path,
) -> tuple[datetime | None, datetime | None]:
    """
    Extract min and max datetime from a single NC file.
    Returns (min_datetime, max_datetime) or (None, None) if no valid data found.
    """
    if not nc_path.exists():
        return None, None

    try:
        with netCDF4.Dataset(nc_path, "r") as ds:
            if "time" not in ds.variables or len(ds.variables["time"]) == 0:
                return None, None
            
            times_epoch = ds.variables["time"][:]
            if len(times_epoch) > 0:
                min_epoch = float(times_epoch.min())
                max_epoch = float(times_epoch.max())
                min_dt = EPOCH + timedelta(seconds=min_epoch)
                max_dt = EPOCH + timedelta(seconds=max_epoch)
                return min_dt, max_dt
    except Exception as e:
        print(f"    Warning: could not read {nc_path}: {e}")
        return None, None

    return None, None


def update_variable_datetime_range(
    conn,
    sensor_id: int,
    variable_name: str,
    from_dt: datetime | None,
    to_dt: datetime | None,
):
    """
    Update from_datetime and to_datetime in the variables JSON for a specific variable.
    
    Args:
        conn: Database connection
        sensor_id: The sensor ID
        variable_name: The canonical variable name (key in variables JSON)
        from_dt: Start datetime or None
        to_dt: End datetime or None
    """
    try:
        with conn.cursor() as cur:
            # Get current variables JSON
            cur.execute("SELECT variables FROM sensors WHERE id = %s", (sensor_id,))
            result = cur.fetchone()
            if not result or not result[0]:
                print(f"    Warning: sensor {sensor_id} has no variables column")
                return

            variables_data = result[0]
            if isinstance(variables_data, str):
                variables_dict = json.loads(variables_data)
            else:
                variables_dict = variables_data or {}

            # Update the specific variable with datetime range
            if variable_name in variables_dict:
                if variables_dict[variable_name] is None:
                    variables_dict[variable_name] = {}
                if not isinstance(variables_dict[variable_name], dict):
                    # If it's not a dict, convert it to one
                    variables_dict[variable_name] = {"name": variable_name}
                
                # Add/update datetime fields
                if from_dt is not None:
                    variables_dict[variable_name]["from_datetime"] = from_dt.isoformat()
                if to_dt is not None:
                    variables_dict[variable_name]["to_datetime"] = to_dt.isoformat()

                # Update the database
                updated_json = json.dumps(variables_dict)
                cur.execute(
                    "UPDATE sensors SET variables = %s WHERE id = %s",
                    (updated_json, sensor_id),
                )
                conn.commit()
                print(f"    Updated {variable_name}: {from_dt} → {to_dt}")
            else:
                print(f"    Warning: variable '{variable_name}' not found in sensor {sensor_id} variables")

    except json.JSONDecodeError as e:
        print(f"    ERROR: Could not parse variables JSON for sensor {sensor_id}: {e}")
    except Exception as e:
        print(f"    ERROR updating variable datetime range: {e}")
