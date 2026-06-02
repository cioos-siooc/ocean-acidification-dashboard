#!/usr/bin/env python3
"""Add a row to nc_jobs table by dataset source and variable name.

Usage:
    python add_nc_job_by_source.py --source SalishSeaCast --variable temperature --date 2026-01-15
    python add_nc_job_by_source.py --source LiveOcean --variable salinity --start 2026-01-15 --end 2026-01-20
    python add_nc_job_by_source.py --source SalishSeaCast --variable temperature \
                                   --start 2026-01-15 --end 2026-01-15 --status pending_download
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


def get_dataset_id_by_source(conn, source: str) -> str:
    """Look up dataset UUID by source name.
    
    Args:
        conn: Database connection
        source: Source name (e.g., 'SalishSeaCast', 'LiveOcean', 'ONC')
    
    Returns:
        Dataset UUID
        
    Raises:
        ValueError: If source not found
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


def get_variable_id(conn, dataset_id: str, variable_name: str) -> str:
    """Look up variable UUID by name and dataset.
    
    Args:
        conn: Database connection
        dataset_id: Dataset UUID
        variable_name: Variable name (e.g., 'temperature', 'salinity')
    
    Returns:
        Variable UUID
        
    Raises:
        ValueError: If variable not found
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT id FROM fields WHERE dataset_id = %s AND variable = %s LIMIT 1",
            (dataset_id, variable_name)
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(
                f"Variable '{variable_name}' not found for dataset '{dataset_id}'"
            )
        return row['id']


def add_nc_job(
    conn,
    source: str,
    variable_name: str,
    start_date: str,
    end_date: str,
    nc_path: Optional[str] = None,
    status: str = 'success_image',
) -> dict:
    """
    Add a row to nc_jobs table.
    
    Args:
        conn: Database connection
        source: Dataset source name (e.g., 'SalishSeaCast')
        variable_name: Variable name (e.g., 'temperature')
        start_date: Start date as YYYY-MM-DD (converted to YYYY-MM-DD 00:00:00)
        end_date: End date as YYYY-MM-DD (converted to YYYY-MM-DD 23:59:59)
        nc_path: Optional path to NetCDF file
        status: Status (default 'success_image')
    
    Returns:
        Dictionary with job details
        
    Raises:
        ValueError: If source or variable not found
        psycopg2.Error: If database operation fails
    """
    # Convert dates to full timestamps
    start_time = f"{start_date} 00:00:00"
    end_time = f"{end_date} 23:59:59"
    
    # Look up IDs
    dataset_id = get_dataset_id_by_source(conn, source)
    variable_id = get_variable_id(conn, dataset_id, variable_name)
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Insert into nc_jobs
        cur.execute(
            """
            INSERT INTO nc_jobs 
            (dataset_id, variable_id, start_time, end_time, status, nc_path, attempts)
            VALUES (%s, %s, %s, %s, %s::nc_job_status, %s, 0)
            RETURNING id, dataset_id, variable_id, start_time, end_time, status
            """,
            (dataset_id, variable_id, start_time, end_time, status, nc_path)
        )
        result = cur.fetchone()
    
    conn.commit()
    return dict(result)


def main(argv=None):
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Add a row to nc_jobs table by dataset source and variable name',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single day
  python add_nc_job_by_source.py --source SalishSeaCast --variable temperature --date 2026-01-15
  
  # Date range
  python add_nc_job_by_source.py --source SalishSeaCast --variable temperature \\
                                 --start 2026-01-15 --end 2026-01-20
  
  # With custom status
  python add_nc_job_by_source.py --source LiveOcean --variable salinity --date 2026-01-15 \\
                                 --status pending_download
        """
    )
    parser.add_argument('--source', required=True, help='Dataset source (e.g., SalishSeaCast, LiveOcean)')
    parser.add_argument('--variable', required=True, help='Variable name (e.g., temperature, salinity)')
    parser.add_argument('--date', default=None, help='Single date (YYYY-MM-DD); mutually exclusive with --start/--end')
    parser.add_argument('--start', default=None, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', default=None, help='End date (YYYY-MM-DD)')
    parser.add_argument('--nc-path', default=None, help='Path to NetCDF file')
    parser.add_argument('--status', default='success_image', help='Status (default: success_image)')
    parser.add_argument('--db-host', default='db', help='Database host')
    parser.add_argument('--db-port', type=int, default=5432, help='Database port')
    parser.add_argument('--db-name', default='oa', help='Database name')
    parser.add_argument('--db-user', default='postgres', help='Database user')
    parser.add_argument('--db-password', default='postgres', help='Database password')
    
    args = parser.parse_args(argv)
    
    # Validate date arguments
    if args.date:
        if args.start or args.end:
            print("✗ Error: --date is mutually exclusive with --start/--end", file=sys.stderr)
            return 1
        start_date = args.date
        end_date = args.date
    elif args.start and args.end:
        start_date = args.start
        end_date = args.end
    else:
        print("✗ Error: Either --date or both --start and --end are required", file=sys.stderr)
        return 1
    
    try:
        conn = get_db_conn(
            host=args.db_host,
            port=args.db_port,
            db=args.db_name,
            user=args.db_user,
            password=args.db_password,
        )
        
        result = add_nc_job(
            conn,
            source=args.source,
            variable_name=args.variable,
            start_date=start_date,
            end_date=end_date,
            nc_path=args.nc_path,
            status=args.status,
        )
        
        print(f"✓ Successfully added nc_jobs row")
        print(f"  ID: {result['id']}")
        print(f"  Source: {args.source}")
        print(f"  Variable: {args.variable}")
        print(f"  Start: {result['start_time']}")
        print(f"  End: {result['end_time']}")
        print(f"  Status: {result['status']}")
        
        conn.close()
        return 0
        
    except ValueError as e:
        print(f"✗ Error: {e}", file=sys.stderr)
        return 1
    except psycopg2.IntegrityError as e:
        print(f"✗ Database integrity error (unique constraint violation?): {e}", file=sys.stderr)
        return 1
    except psycopg2.Error as e:
        print(f"✗ Database error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"✗ Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
