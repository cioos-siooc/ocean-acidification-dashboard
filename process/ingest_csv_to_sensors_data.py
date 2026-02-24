#!/usr/bin/env python3
"""Simple CSV to sensors_data ingestion.

Takes a CSV file and sensor_id, extracts the variable name from the header,
and upserts measurements into the sensors_data JSONB dict.

Usage:
  python process/ingest_csv_to_sensors_data.py --csv-file <path> --sensor-id <id>
  python process/ingest_csv_to_sensors_data.py --csv-file cleaned.csv --sensor-id 1
"""

import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import psycopg2


DB_HOST = os.getenv("PGHOST", "db")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "oa")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "postgres")


def get_db_conn():
    """Create database connection."""
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )


def extract_variable_name_from_header(header_line):
    """
    Extract variable name from CSV header.
    
    Example: "Time UTC (yyyy-mm-ddThh:mm:ss.fffZ)", "Temperature (C)", "Temperature QC Flag", "Temperature Count"
    Returns: "temperature" (from "Temperature (C)")
    """
    # Split by comma and get columns
    columns = [col.strip().strip('"').strip() for col in header_line.split(',')]
    
    if len(columns) < 2:
        raise ValueError(f"Header has fewer than 2 columns: {header_line}")
    
    # The second column should be the variable (first is Time, second is the measurement)
    var_col = columns[1]
    
    # Extract the variable name before any parentheses
    # E.g., "Temperature (C)" -> "Temperature"
    match = re.match(r'(\w+)', var_col)
    if match:
        var_name = match.group(1).lower()
        return var_name
    
    raise ValueError(f"Could not extract variable name from header column: {var_col}")


def parse_timestamp(ts_str):
    """Parse various timestamp formats."""
    ts_str = ts_str.strip()
    if not ts_str:
        return None
    
    # Remove timezone info for parsing
    ts_clean = ts_str.split('+')[0].split('Z')[0].split('.')[0]
    
    for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
        try:
            dt = datetime.strptime(ts_clean, fmt)
            return dt.isoformat()
        except ValueError:
            continue
    
    raise ValueError(f"Could not parse timestamp: {ts_str}")


def read_csv_data(csv_file):
    """
    Read CSV and return (header_line, data_rows).
    
    First line is the header, all subsequent lines are data rows.
    """
    with open(csv_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    if len(lines) < 2:
        raise ValueError("CSV file must have at least a header and one data row")
    
    # First line is the header
    header_line = lines[0].strip()
    
    # Parse the header to get column names
    reader = csv.reader([header_line])
    columns = next(reader)
    
    # Read all data rows (skip header)
    data_rows = []
    for line in lines[1:]:
        line = line.strip()
        if line:  # Skip empty lines
            reader = csv.reader([line])
            row = next(reader)
            if len(row) >= 2:  # Need at least time and value
                data_rows.append(row)
    
    return header_line, columns, data_rows


def ingest_csv(csv_file, sensor_id, verbose=False):
    """Main ingestion function."""
    
    # Validate file exists
    if not Path(csv_file).exists():
        print(f"ERROR: CSV file not found: {csv_file}")
        sys.exit(1)
    
    # Read CSV
    print(f"Reading CSV: {csv_file}")
    try:
        header_line, columns, data_rows = read_csv_data(csv_file)
    except Exception as e:
        print(f"ERROR reading CSV: {e}")
        sys.exit(1)
    
    # Extract variable name from header
    try:
        variable_name = extract_variable_name_from_header(header_line)
    except Exception as e:
        print(f"ERROR extracting variable name: {e}")
        sys.exit(1)
    
    # Show to user and confirm
    print(f"\nCSV Header: {header_line}")
    print(f"Detected variable: '{variable_name}'")
    print(f"Sensor ID: {sensor_id}")
    print(f"Data rows: {len(data_rows)}")
    
    confirm = input("\nProceed with ingestion? (yes/no): ").strip().lower()
    if confirm != 'yes':
        print("Cancelled.")
        sys.exit(0)
    
    # Connect to database
    conn = get_db_conn()
    
    # Process data
    inserted = 0
    updated = 0
    skipped = 0
    
    with conn.cursor() as cur:
        for row_idx, row in enumerate(data_rows):
            try:
                # Parse time (first column) and value (second column)
                ts_str = row[0].strip()
                value_str = row[1].strip()
                
                # Skip NaN values
                if value_str.upper() == 'NAN':
                    skipped += 1
                    continue
                
                # Parse timestamp
                try:
                    ts = parse_timestamp(ts_str)
                except ValueError as e:
                    if verbose:
                        print(f"Row {row_idx}: {e}, skipping")
                    skipped += 1
                    continue
                
                # Parse value as float
                try:
                    value = float(value_str)
                except ValueError:
                    if verbose:
                        print(f"Row {row_idx}: Could not parse value '{value_str}', skipping")
                    skipped += 1
                    continue
                
                # Build the measurement dict
                measurement = {variable_name: value}
                
                # Check if row exists
                cur.execute(
                    "SELECT measurements FROM sensors_data WHERE sensor_id = %s AND time = %s",
                    (sensor_id, ts)
                )
                existing = cur.fetchone()
                
                if existing:
                    # Update: merge the measurement into existing dict
                    existing_measurements = existing[0] or {}
                    existing_measurements.update(measurement)
                    
                    cur.execute(
                        "UPDATE sensors_data SET measurements = %s WHERE sensor_id = %s AND time = %s",
                        (json.dumps(existing_measurements), sensor_id, ts)
                    )
                    updated += 1
                else:
                    # Insert: new row
                    cur.execute(
                        "INSERT INTO sensors_data (time, sensor_id, measurements) VALUES (%s, %s, %s)",
                        (ts, sensor_id, json.dumps(measurement))
                    )
                    inserted += 1
                
                # Commit every 100 rows for performance
                if (row_idx + 1) % 100 == 0:
                    conn.commit()
                    if verbose:
                        print(f"  Committed {row_idx + 1} rows...")
            
            except Exception as e:
                print(f"Row {row_idx}: ERROR - {e}")
                skipped += 1
                continue
    
    # Final commit
    conn.commit()
    conn.close()
    
    # Summary
    print(f"\n✓ Ingestion complete:")
    print(f"  Inserted: {inserted} new rows")
    print(f"  Updated: {updated} existing rows")
    print(f"  Skipped: {skipped} rows")
    print(f"  Total: {inserted + updated + skipped} rows processed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest CSV sensor data into sensors_data table"
    )
    parser.add_argument("--csv-file", required=True, help="Path to CSV file")
    parser.add_argument("--sensor-id", type=int, required=True, help="Sensor ID (numeric)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    ingest_csv(
        csv_file=args.csv_file,
        sensor_id=args.sensor_id,
        verbose=args.verbose
    )
