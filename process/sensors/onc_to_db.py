import json
import os
import psycopg2
from psycopg2.extras import Json, execute_values
from onc import ONC
from datetime import datetime, timedelta
import numpy as np

# Configuration
TOKEN = "7d291a6a-b57e-49cd-acb1-83f59010d32b"
onc = ONC(TOKEN)

# DB Connection info from environment
DB_HOST = os.getenv("PGHOST", "db")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "oa")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "postgres")

# Default range if not specified
# Typically we want to pull "recent" data, e.g. last 3 days
# dateFrom = "2026-01-20T00:00:00.000Z"

def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )

def get_sensor_var_mapping(conn, sensor_id: int) -> dict:
    """
    Fetch the variables mapping for a sensor, which contains column names and conversion factors.
    Returns: {canonical_var: {"name": sensor_column, "unit": unit_str, "conversion_factor": factor}}
    """
    with conn.cursor() as cur:
        cur.execute("SELECT variables FROM sensors WHERE id = %s", (sensor_id,))
        result = cur.fetchone()
        if not result or not result[0]:
            return {}
        
        variables_data = result[0]
        if isinstance(variables_data, str):
            return json.loads(variables_data)
        return variables_data or {}

def apply_conversion(value: float, canonical_var: str, sensor_var_mapping: dict) -> float:
    """
    Apply unit conversion if defined in the mapping.
    Returns the converted value, or the original value if no conversion is defined.
    """
    if value is None or not isinstance(value, (int, float)):
        return value
    if canonical_var not in sensor_var_mapping:
        return value
    
    var_info = sensor_var_mapping[canonical_var]
    if isinstance(var_info, dict):
        conversion_factor = var_info.get("conversion_factor", 1.0)
        return value * conversion_factor
    return value

def ensure_schema(conn):
    with conn.cursor() as cur:
        # 1. Ensure sensors table exists with device_config
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sensors (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                depths DOUBLE PRECISION[],
                variables TEXT[],
                device_config JSONB DEFAULT '{}'::jsonb,
                active BOOLEAN DEFAULT TRUE
            );
        """)
        
        # Add columns if they were created without them previously
        cur.execute("ALTER TABLE sensors ADD COLUMN IF NOT EXISTS device_config JSONB DEFAULT '{}'::jsonb;")
        cur.execute("ALTER TABLE sensors ADD COLUMN IF NOT EXISTS active BOOLEAN DEFAULT TRUE;")
        
        # 2. Ensure sensors_data table exists with JSONB
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sensors_data (
                time TIMESTAMPTZ NOT NULL,
                sensor_id INTEGER REFERENCES sensors(id) ON DELETE CASCADE,
                measurements JSONB NOT NULL DEFAULT '{}'::jsonb
            );
        """)
        
        # Add PRIMARY KEY if it doesn't exist (table may have been created without it)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.table_constraints
                    WHERE constraint_type = 'PRIMARY KEY'
                    AND table_name = 'sensors_data'
                ) THEN
                    ALTER TABLE sensors_data ADD PRIMARY KEY (sensor_id, time);
                END IF;
            END $$;
        """)
        
        # 3. Create Functional Indexes for performance
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sensor_data_time ON sensors_data (time DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sensor_data_sensor_time ON sensors_data (sensor_id, time DESC);")
        conn.commit()

def fetch_and_store():
    conn = get_db_conn()
    ensure_schema(conn)
    
    # 1. Fetch all sensors that have ONC configuration
    # If device_config is empty, we'll try to use the 'name' as a fallback deviceCode.
    sensors = []
    with conn.cursor() as cur:
        cur.execute("SELECT id, name, device_config FROM sensors WHERE source->>'type' = 'ONC'")
        sensors = cur.fetchall()

    if not sensors:
        print("No sensors found in database.")
        conn.close()
        return

    dateTo = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")

    for sensor_id, sensor_name, device_config in sensors:
        print(f"Processing Sensor: {sensor_name} (ID: {sensor_id})")
        
        if device_config and isinstance(device_config, dict) and len(device_config) > 0:
            # for d_code, v_list in device_config.items():
            #     # v_list could be ["temperature", "salinity"] or a comma-sep string
            #     cat_codes = ",".join(v_list) if isinstance(v_list, list) else v_list
            #     configs_to_fetch.append((d_code, cat_codes))
            locationCode = device_config.get('locationCode')
            codeRows = device_config.get('codes', [])
        else:
            # Skip sensors without valid device_config
            print(f"  No valid device_config for sensor {sensor_name}, skipping.")
            continue

        for codeRow in codeRows:
            deviceCategoryCode = codeRow.get('deviceCategoryCode')
            sensorCategoryCodes = codeRow.get('sensorCategoryCodes')
            #
            # Find max time in DB where measurements includes this sensorCategoryCode for this sensor_id
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT MAX(time) FROM sensors_data
                    WHERE sensor_id = %s AND measurements ? %s
                """, (sensor_id, sensorCategoryCodes))
                result = cur.fetchone()
                # -1 day and format date only.  ONC API expects this format.
                dateFrom = (result[0] - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z") if result and result[0] else None
            #
            print(f"  Fetching data for locationCode={locationCode}, deviceCategoryCode={deviceCategoryCode}, sensorCategoryCodes={sensorCategoryCodes}")
            try:
                data = onc.getScalardataByLocation(
                    {
                        "locationCode": locationCode,
                        "deviceCategoryCode": deviceCategoryCode,
                        "getLatest": True,
                        "resamplePeriod": 3600,
                        "resampleType": "avg",
                        "qualityControl": "clean",
                        "sensorCategoryCodes": sensorCategoryCodes,
                        "dateFrom": dateFrom,
                        "dateTo": dateTo
                    }
                )
                
                if 'sensorData' not in data:
                    print(f"    No sensorData found for {deviceCategoryCode}")
                    continue
                    
                # Get sensor variable mapping for conversion
                sensor_var_mapping = get_sensor_var_mapping(conn, sensor_id)
                
                # Build reverse mapping: sensor column name -> canonical name (for conversion lookup)
                col_to_canonical = {}
                for canonical_var, var_info in sensor_var_mapping.items():
                    if isinstance(var_info, dict):
                        col_name = var_info.get("name")
                        if col_name:
                            col_to_canonical[col_name] = canonical_var
                
                # Pivot data: time -> { var: value }
                pivoted = {}
                for s in data['sensorData']:
                    sensorCategoryCode = s.get('sensorCategoryCode')
                    
                    vals = s.get('data', {}).get('values', [])
                    times = s.get('data', {}).get('sampleTimes', [])
                    
                    for t, v in zip(times, vals):
                        if t not in pivoted:
                            pivoted[t] = {}
                        if v is not None and not (isinstance(v, (float, int)) and np.isnan(v)):
                            # Look up canonical name to apply correct conversion factor
                            canonical_var = col_to_canonical.get(sensorCategoryCode, sensorCategoryCode)
                            # Apply conversion to canonical units
                            converted_v = apply_conversion(float(v), canonical_var, sensor_var_mapping)
                            # Store under sensor-specific column name
                            pivoted[t][sensorCategoryCode] = converted_v

                # Upsert into DB
                records = []
                for t, measurements in pivoted.items():
                    if not measurements: continue
                    records.append((t, sensor_id, Json(measurements)))
                
                if records:
                    with conn.cursor() as cur:
                        execute_values(cur, """
                            INSERT INTO sensors_data (time, sensor_id, measurements)
                            VALUES %s
                            ON CONFLICT (sensor_id, time) DO UPDATE SET
                            measurements = sensors_data.measurements || EXCLUDED.measurements
                        """, records)
                        conn.commit()
                    print(f"    Stored {len(records)} points.")
                else:
                    print(f"    No numeric data found.")
                    
            except Exception as e:
                print(f"    Error: {e}")

    conn.close()

if __name__ == "__main__":
    fetch_and_store()
