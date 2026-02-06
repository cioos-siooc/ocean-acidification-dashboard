import json
import os
import sys
import psycopg2
from psycopg2.extras import Json, execute_values
from onc import ONC
from datetime import datetime
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
dateFrom = "2026-01-20T00:00:00.000Z"

def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )

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
                measurements JSONB NOT NULL DEFAULT '{}'::jsonb,
                PRIMARY KEY (sensor_id, time)
            );
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
        cur.execute("SELECT id, name, device_config FROM sensors")
        sensors = cur.fetchall()

    if not sensors:
        print("No sensors found in database.")
        conn.close()
        return

    dateTo = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")

    for sensor_id, sensor_name, device_config in sensors:
        print(f"Processing Sensor: {sensor_name} (ID: {sensor_id})")
        
        # Build a list of (deviceCode, categoryCodes)
        configs_to_fetch = []
        if device_config and isinstance(device_config, dict) and len(device_config) > 0:
            for d_code, v_list in device_config.items():
                # v_list could be ["temperature", "salinity"] or a comma-sep string
                cat_codes = ",".join(v_list) if isinstance(v_list, list) else v_list
                configs_to_fetch.append((d_code, cat_codes))
        else:
            # Skip sensors without valid device_config
            print(f"  No valid device_config for sensor {sensor_name}, skipping.")
            continue

        for deviceCode, sensorCategoryCodes in configs_to_fetch:
            print(f"  Fetching Device {deviceCode} (Vars: {sensorCategoryCodes})...")
            try:
                data = onc.getScalardataByDevice({
                    "deviceCode": deviceCode, 
                    "dateFrom": dateFrom, 
                    "dateTo": dateTo, 
                    "getLatest": True, 
                    "resamplePeriod": 3600, 
                    "sensorCategoryCodes": sensorCategoryCodes
                })
                
                if 'sensorData' not in data:
                    print(f"    No sensorData found for {deviceCode}")
                    continue
                    
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
                            pivoted[t][sensorCategoryCode] = float(v)

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
