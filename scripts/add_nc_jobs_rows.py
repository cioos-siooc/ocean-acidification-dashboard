#!/usr/bin/env python3
"""Add rows to nc_jobs table with automatic variable_id lookup.

Usage:
    python add_nc_jobs_rows.py --dataset-id <uuid> --variable <var_name> \
                               --start <YYYY-MM-DD HH:MM:SS> --end <YYYY-MM-DD HH:MM:SS>
    
    Or programmatically:
    from add_nc_jobs_rows import add_nc_job_row
    add_nc_job_row(conn, '123e4567-e89b-12d3-a456-426614174000', 'temperature', '2026-05-20 00:00:00', '2026-05-20 23:59:59')
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


def add_nc_job_row(
    conn,
    dataset_id: str,
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
        dataset_id: Dataset UUID (e.g., '123e4567-e89b-12d3-a456-426614174000')
        variable_name: Variable name (e.g., 'temperature', 'salinity')
        start_time: Start time as string (YYYY-MM-DD HH:MM:SS or ISO format)
        end_time: End time as string (YYYY-MM-DD HH:MM:SS or ISO format)
        nc_path: Optional path to NetCDF file
        status: Status (default 'success_image')
    
    Returns:
        The generated UUID of the inserted row
        
    Raises:
        ValueError: If variable not found
        psycopg2.Error: If database operation fails
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get variable_id from fields table
        cur.execute(
            "SELECT id FROM fields WHERE dataset_id = %s AND variable = %s LIMIT 1",
            (dataset_id, variable_name)
        )
        variable_row = cur.fetchone()
        if not variable_row:
            raise ValueError(
                f"Variable '{variable_name}' not found for dataset_id '{dataset_id}'"
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
            (dataset_id, variable_id, start_time, end_time, status, nc_path)
        )
        result = cur.fetchone()
        row_id = result['id']
    
    conn.commit()
    return row_id


def main(argv=None):
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Add rows to nc_jobs table with automatic variable lookup'
    )
    parser.add_argument('--dataset-id', required=True, help='Dataset UUID')
    parser.add_argument('--variable', required=True, help='Variable name')
    parser.add_argument('--start', required=True, help='Start time (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--end', required=True, help='End time (YYYY-MM-DD HH:MM:SS)')
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
            dataset_id=args.dataset_id,
            variable_name=args.variable,
            start_time=args.start,
            end_time=args.end,
            nc_path=args.nc_path,
            status=args.status,
        )
        
        print(f"✓ Successfully added nc_jobs row")
        print(f"  ID: {row_id}")
        print(f"  Dataset ID: {args.dataset_id}")
        print(f"  Variable: {args.variable}")
        print(f"  Start: {args.start}")
        print(f"  End: {args.end}")
        print(f"  Status: {args.status}")
        
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
