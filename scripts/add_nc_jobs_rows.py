#!/usr/bin/env python3
"""Add rows to nc_jobs table with automatic dataset_id and variable_id lookup.

Usage:
    python add_nc_jobs_rows.py --dataset SalishSeaCast --variable temperature \
                               --start 2026-01-01 --end 2026-01-01
    
    Or with full timestamps and optional nc_path:
    python add_nc_jobs_rows.py --dataset SalishSeaCast --variable temperature \
                               --start "2026-01-01 06:00:00" --end "2026-01-01 18:00:00" \
                               --nc-path /opt/data/SalishSeaCast/temperature_2026-01-01.nc \
                               --status success_image
"""

import argparse
import sys
from datetime import datetime
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor


def get_db_conn(host: str = 'db', port: int = 5432, db: str = 'oa', 
                user: str = 'postgres', password: str = 'postgres'):
    """Create a database connection."""
    return psycopg2.connect(
        host=host,
        port=port,
        database=db,
        user=user,
        password=password,
    )


def get_dataset_id(conn, source: str) -> str:
    """Resolve dataset_id from dataset source name.
    
    Args:
        conn: Database connection
        source: Dataset source name (e.g., 'SalishSeaCast', 'LiveOcean')
    
    Returns:
        The dataset UUID
        
    Raises:
        ValueError: If dataset not found
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT id FROM datasets WHERE source = %s LIMIT 1",
            (source,)
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Dataset source '{source}' not found in datasets table")
    return row['id']


def format_datetime(dt_str: str, is_end_time: bool = False) -> str:
    """Format datetime string, auto-appending time if only date provided.
    
    Args:
        dt_str: DateTime string (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
        is_end_time: If True, append 23:59:59 to dates; else append 00:00:00
    
    Returns:
        Formatted datetime string (YYYY-MM-DD HH:MM:SS)
    """
    dt_str = dt_str.strip()
    # If only date provided, append time
    if len(dt_str) == 10 and dt_str.count('-') == 2:
        time_part = "23:59:59" if is_end_time else "00:00:00"
        return f"{dt_str} {time_part}"
    return dt_str


def add_nc_job_row(
    conn,
    dataset_source: str,
    variable_name: str,
    start_time: str,
    end_time: str,
    nc_path: Optional[str] = None,
    status: str = 'success_image',
) -> str:
    """
    Add a row to nc_jobs table.
    
    Args:
        conn: Database connection
        dataset_source: Dataset source name (e.g., 'SalishSeaCast', 'LiveOcean')
        variable_name: Variable name (e.g., 'temperature', 'salinity')
        start_time: Start time as string (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
        end_time: End time as string (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
        nc_path: Optional path to NetCDF file
        status: Status (default 'success_image')
    
    Returns:
        The generated UUID of the inserted row
        
    Raises:
        ValueError: If dataset or variable not found
        psycopg2.Error: If database operation fails
    """
    # Format datetimes
    start_formatted = format_datetime(start_time, is_end_time=False)
    end_formatted = format_datetime(end_time, is_end_time=True)
    
    # Resolve dataset_id
    dataset_id = get_dataset_id(conn, dataset_source)
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get variable_id from fields table
        cur.execute(
            "SELECT id FROM fields WHERE dataset_id = %s AND variable = %s LIMIT 1",
            (dataset_id, variable_name)
        )
        variable_row = cur.fetchone()
        if not variable_row:
            raise ValueError(
                f"Variable '{variable_name}' not found for dataset source '{dataset_source}'"
            )
        variable_id = variable_row['id']
        
        # Insert into nc_jobs (explicitly generate UUID for id)
        cur.execute(
            """
            INSERT INTO nc_jobs 
            (id, dataset_id, variable_id, start_time, end_time, status, nc_path, 
             attempts, error_message, created_at, updated_at, misc)
            VALUES (gen_random_uuid(), %s, %s, %s, %s, %s::nc_job_status, %s, 0, NULL, NOW(), NOW(), NULL)
            RETURNING id
            """,
            (dataset_id, variable_id, start_formatted, end_formatted, status, nc_path)
        )
        result = cur.fetchone()
        row_id = result['id']
    
    conn.commit()
    return row_id


def main(argv=None):
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Add rows to nc_jobs table with automatic dataset and variable lookup',
        epilog='Examples:\n'
               '  python add_nc_jobs_rows.py --dataset SalishSeaCast --variable temperature --start 2026-01-01 --end 2026-01-01\n'
               '  python add_nc_jobs_rows.py --dataset LiveOcean --variable salinity --start 2026-01-15 --end 2026-01-20 --nc-path /path/to/file.nc',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--dataset', required=True, help='Dataset source name (e.g., SalishSeaCast, LiveOcean)')
    parser.add_argument('--variable', required=True, help='Variable name')
    parser.add_argument('--start', required=True, help='Start date/time (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--end', required=True, help='End date/time (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--nc-path', default=None, help='Path to NetCDF file')
    parser.add_argument('--status', default='success_image', help='Status (default: success_image)')
    parser.add_argument('--db-host', default='db', help='Database host')
    parser.add_argument('--db-port', type=int, default=9012, help='Database port')
    parser.add_argument('--db-name', default='oa', help='Database name')
    parser.add_argument('--db-user', default='postgres', help='Database user')
    parser.add_argument('--db-password', default='postgres', help='Database password')
    
    args = parser.parse_args(argv)
    
    try:
        conn = get_db_conn(
            host=args.db_host,
            port=args.db_port,
            db=args.db_name,
            user=args.db_user,
            password=args.db_password,
        )
        
        row_id = add_nc_job_row(
            conn,
            dataset_source=args.dataset,
            variable_name=args.variable,
            start_time=args.start,
            end_time=args.end,
            nc_path=args.nc_path,
            status=args.status,
        )
        
        start_formatted = format_datetime(args.start, is_end_time=False)
        end_formatted = format_datetime(args.end, is_end_time=True)
        
        print(f"✓ Successfully added nc_jobs row")
        print(f"  ID: {row_id}")
        print(f"  Dataset: {args.dataset}")
        print(f"  Variable: {args.variable}")
        print(f"  Start: {start_formatted}")
        print(f"  End: {end_formatted}")
        print(f"  Status: {args.status}")
        if args.nc_path:
            print(f"  NC Path: {args.nc_path}")
        
        conn.close()
        return 0
        
    except ValueError as e:
        print(f"✗ Error: {e}", file=sys.stderr)
        return 1
    except psycopg2.Error as e:
        print(f"✗ Database error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"✗ Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
