"""Database operations for Live Ocean workflow."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Optional, List
import uuid
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger("liveocean.db")


def get_db_conn(db_host: str = None, db_user: str = None, db_password: str = None, db_name: str = None):
    """Create a database connection, using environment variables if args not provided."""
    host = db_host or os.getenv("DB_HOST", "db")
    user = db_user or os.getenv("DB_USER", "postgres")
    password = db_password or os.getenv("DB_PASSWORD", "postgres")
    database = db_name or os.getenv("DB_NAME", "oa")
    
    return psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=database,
    )


def extract_source_date_from_url(url: str) -> str:
    """Extract source_date (YYYY-MM-DD) from URL like f2026.02.04."""
    # URL format: https://s3.kopah.uw.edu/liveocean-share/f2026.02.04/layers.nc
    import re
    match = re.search(r"f(\d{4})\.(\d{2})\.(\d{2})", url)
    if match:
        year, month, day = match.groups()
        return f"{year}-{month}-{day}"
    raise ValueError(f"Could not extract date from URL: {url}")


# Cache for Live Ocean dataset UUID
_live_ocean_dataset_id_cache = None

def get_live_ocean_dataset_id(conn):
    """Get the Live Ocean dataset UUID from the datasets table.
    
    Caches the result after first query for efficiency.
    
    Args:
        conn: Database connection
    
    Returns:
        The Live Ocean dataset UUID
        
    Raises:
        ValueError: If Live Ocean dataset not found
    """
    global _live_ocean_dataset_id_cache
    
    if _live_ocean_dataset_id_cache is not None:
        return _live_ocean_dataset_id_cache
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM datasets WHERE source = %s LIMIT 1",
                ("Live Ocean",)
            )
            result = cur.fetchone()
            if not result:
                raise ValueError("Live Ocean dataset not found in datasets table")
            _live_ocean_dataset_id_cache = result[0]
            logger.info(f"Found Live Ocean dataset_id: {_live_ocean_dataset_id_cache}")
            return _live_ocean_dataset_id_cache
    except Exception as e:
        logger.error(f"Failed to get Live Ocean dataset_id: {e}")
        raise


def get_last_processed_date(conn, db_name: str = "oa") -> Optional[str]:
    """Get the last successfully processed source_date (YYYY-MM-DD)."""
    try:
        dataset_id = get_live_ocean_dataset_id(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT misc->>'source_date' AS source_date FROM nc_jobs
                WHERE status = 'success_image' AND dataset_id = %s
                  AND misc->>'source_date' IS NOT NULL
                ORDER BY misc->>'source_date' DESC
                LIMIT 1
                """,
                (dataset_id,),
            )
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as e:
        logger.warning(f"Could not fetch last processed date: {e}")
        return None


def update_file_status(conn, source_date: str, variable_id: int, start_time: str, status: str, error_message: str = None):
    """Update the status of a single nc_jobs record (Live Ocean).

    Args:
        conn: Database connection
        source_date: Source date (YYYY-MM-DD)
        variable_id: Field ID (foreign key to fields table)
        start_time: Start time (ISO 8601 timestamp)
        status: New status
        error_message: Error message if applicable
    """
    try:
        dataset_id = get_live_ocean_dataset_id(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE nc_jobs
                SET status = %s, error_message = %s, updated_at = NOW()
                WHERE misc->>'source_date' = %s AND variable_id = %s AND start_time = %s AND dataset_id = %s
                """,
                (status, error_message, source_date, variable_id, start_time, dataset_id),
            )
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to update file status for source_date={source_date}, variable_id={variable_id}, start_time={start_time}: {e}")
        raise


def insert_processed_dates(conn, source_date: str, outputs: List[dict]) -> List[dict]:
    """Insert extracted daily files into nc_jobs.
    
    Args:
        conn: Database connection
        source_date: Source date (YYYY-MM-DD)
        outputs: List of output dicts from write_daily_outputs with keys: variable (name), path, start_time, end_time
    
    Returns:
        Updated outputs list with variable_id added to each dict
    """
    try:
        dataset_id = get_live_ocean_dataset_id(conn)
        records = []
        updated_outputs = []
        for output in outputs:
            variable_name = output["variable"]
            
            # Look up field_id by variable name and dataset_id
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id FROM fields
                    WHERE variable = %s AND dataset_id = %s
                    LIMIT 1
                    """,
                    (variable_name, dataset_id),
                )
                result = cur.fetchone()
                if not result:
                    logger.warning(f"Could not find field_id for variable '{variable_name}' in dataset {dataset_id}, skipping")
                    continue
                variable_id = result[0]
            
            # Extract start_time and end_time from output (set by write_daily_outputs)
            start_time = output.get("start_time")
            end_time = output.get("end_time")
            if not start_time or not end_time:
                logger.warning(f"Output missing start_time or end_time for {variable_name}, skipping")
                continue
            
            import json
            records.append(
                (
                    str(uuid.uuid4()),  # id (UUID as string)
                    dataset_id,  # dataset_id
                    variable_id,
                    json.dumps({"source_date": source_date}),  # misc
                    start_time,
                    end_time,
                    output["path"], # nc_path
                    "extracted",    # status
                )
            )

            # Add variable_id to output dict for use in imaging stage
            output_with_id = output.copy()
            output_with_id["variable_id"] = variable_id
            updated_outputs.append(output_with_id)

        if records:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO nc_jobs
                    (id, dataset_id, variable_id, misc, start_time, end_time, nc_path, status)
                    VALUES %s
                    ON CONFLICT ON CONSTRAINT ux_nc_jobs_dataset_variable_time DO UPDATE SET
                        misc = EXCLUDED.misc,
                        nc_path = EXCLUDED.nc_path,
                        status = 'extracted',
                        updated_at = NOW()
                    """,
                    records,
                )
            conn.commit()
            logger.info(f"Inserted or updated {len(records)} files for source_date {source_date}")

        return updated_outputs
    except Exception as e:
        logger.error(f"Failed to insert processed dates for source_date {source_date}: {e}")
        raise


def get_extracted_files_for_source_date(conn, source_date: str) -> List[dict]:
    """Get all extracted files for a source_date.
    
    Args:
        conn: Database connection
        source_date: Source date (YYYY-MM-DD)
    
    Returns:
        List of dicts with keys: variable, variable_id, start_time, end_time, file_path
    """
    try:
        dataset_id = get_live_ocean_dataset_id(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT nj.variable_id, f.variable, nj.start_time, nj.end_time, nj.nc_path
                FROM nc_jobs nj
                JOIN fields f ON nj.variable_id = f.id
                WHERE nj.misc->>'source_date' = %s AND nj.dataset_id = %s
                  AND nj.status IN ('extracted', 'pending_image', 'imaging_failed', 'success_image')
                ORDER BY f.variable, nj.start_time
                """,
                (source_date, dataset_id),
            )
            rows = cur.fetchall()
            return [{"variable_id": r[0], "variable": r[1], "start_time": r[2].isoformat(), "end_time": r[3].isoformat(), "path": r[4]} for r in rows]
    except Exception as e:
        logger.error(f"Failed to get extracted files for source_date {source_date}: {e}")
        return []


def get_available_dates_for_variable(conn, variable: str) -> List[str]:
    """Get list of available start_times for a variable (ISO 8601 format)."""
    try:
        with conn.cursor() as cur:
            # First get the Live Ocean dataset_id
            dataset_id = get_live_ocean_dataset_id(conn)
            
            # Get field_id for this variable in Live Ocean dataset
            cur.execute(
                """
                SELECT id FROM fields
                WHERE variable = %s AND dataset_id = %s
                LIMIT 1
                """,
                (variable, dataset_id),
            )
            result = cur.fetchone()
            if not result:
                logger.warning(f"Variable '{variable}' not found in Live Ocean dataset")
                return []
            variable_id = result[0]
            
            # Get distinct start_times for this variable
            cur.execute(
                """
                SELECT DISTINCT start_time FROM nc_jobs
                WHERE variable_id = %s AND dataset_id = %s
                ORDER BY start_time DESC
                """,
                (variable_id, dataset_id),
            )
            return [row[0].isoformat() for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Failed to fetch available dates for {variable}: {e}")
        return []


def init_schema(conn):
    """Ensure nc_jobs has the columns and enum values needed for Live Ocean data.

    Idempotent: safe to call on an already-migrated database.
    """
    try:
        with conn.cursor() as cur:
            # Add LO-specific columns to nc_jobs if not already present
            for col, defn in [
                ("error_message", "TEXT"),
                ("created_at", "TIMESTAMPTZ DEFAULT NOW()"),
                ("updated_at", "TIMESTAMPTZ DEFAULT NOW()"),
                ("misc", "JSONB"),
            ]:
                cur.execute(
                    f"ALTER TABLE nc_jobs ADD COLUMN IF NOT EXISTS {col} {defn}"
                )

            # Ensure 'extracted' and 'imaging_failed' enum values exist
            for enum_name in ('nc_file_status', 'nc_job_status'):
                cur.execute("SELECT 1 FROM pg_type WHERE typname = %s", (enum_name,))
                if cur.fetchone():
                    for val in ('extracted', 'imaging_failed'):
                        cur.execute(
                            f"ALTER TYPE {enum_name} ADD VALUE IF NOT EXISTS %s", (val,)
                        )

            # GIN index on misc for efficient JSONB lookups
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_nc_jobs_misc ON nc_jobs USING GIN (misc)"
            )
            
            # Drop legacy source_date column if it exists from a previous migration
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name='nc_jobs' AND column_name='source_date'"
            )
            if cur.fetchone():
                cur.execute("ALTER TABLE nc_jobs DROP COLUMN source_date")

        conn.commit()
        
        # Handle constraint creation in separate transaction to avoid transaction abortion
        with conn.cursor() as cur:
            # Drop constraint first (which will also drop the associated index)
            cur.execute("ALTER TABLE nc_jobs DROP CONSTRAINT IF EXISTS ux_nc_jobs_dataset_variable_time")
            
            # Drop old indexes if they exist
            cur.execute("DROP INDEX IF EXISTS ux_nc_jobs_dataset_variable_time CASCADE")
            cur.execute("DROP INDEX IF EXISTS ux_nc_jobs_null_dataset_variable_time CASCADE")
            
            # Create the UNIQUE constraint
            cur.execute(
                "ALTER TABLE nc_jobs ADD CONSTRAINT ux_nc_jobs_dataset_variable_time "
                "UNIQUE (dataset_id, variable_id, start_time, end_time)"
            )
            logger.info("Created UNIQUE CONSTRAINT ux_nc_jobs_dataset_variable_time on nc_jobs")
        
        conn.commit()
        logger.info("nc_jobs schema updated for Live Ocean")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to update schema: {e}")
        raise
