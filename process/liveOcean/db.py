"""Database operations for Live Ocean workflow."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Optional, List
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


def get_last_processed_date(conn, db_name: str = "oa") -> Optional[str]:
    """Get the last successfully processed source_date (YYYY-MM-DD)."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT source_date FROM live_ocean_data
                WHERE status = 'success'
                ORDER BY source_date DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            return row[0].isoformat() if row else None
    except Exception as e:
        logger.warning(f"Could not fetch last processed date: {e}")
        return None


def update_file_status(conn, source_date: str, variable: str, file_date: str, status: str, error_message: str = None):
    """Update the status of a single live_ocean_data record.
    
    Args:
        conn: Database connection
        source_date: Source date (YYYY-MM-DD)
        variable: Variable name
        file_date: File date (YYYY-MM-DD)
        status: New status
        error_message: Error message if applicable
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE live_ocean_data
                SET status = %s, error_message = %s, updated_at = NOW()
                WHERE source_date = %s AND variable = %s AND file_date = %s
                """,
                (status, error_message, source_date, variable, file_date),
            )
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to update file status for {source_date}/{variable}/{file_date}: {e}")
        raise


def insert_processed_dates(conn, source_date: str, outputs: List[dict]):
    """Insert extracted daily files into live_ocean_data.
    
    Args:
        conn: Database connection
        source_date: Source date (YYYY-MM-DD)
        outputs: List of output dicts from write_daily_outputs with keys: variable, date (YYYYMMDD), path
    """
    try:
        records = []
        for output in outputs:
            # Convert YYYYMMDD to YYYY-MM-DD
            date_str = output["date"]
            file_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            records.append(
                (
                    source_date,
                    output["variable"],
                    file_date,
                    output["path"],
                    "extracted",
                )
            )

        if records:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO live_ocean_data
                    (source_date, variable, file_date, file_path, status)
                    VALUES %s
                    ON CONFLICT (source_date, variable, file_date) DO UPDATE SET
                        file_path = EXCLUDED.file_path,
                        status = 'extracted',
                        updated_at = NOW()
                    """,
                    records,
                )
            conn.commit()
            logger.info(f"Inserted or updated {len(records)} files for source_date {source_date}")
    except Exception as e:
        logger.error(f"Failed to insert processed dates for source_date {source_date}: {e}")
        raise


def get_extracted_files_for_source_date(conn, source_date: str) -> List[dict]:
    """Get all extracted files for a source_date.
    
    Args:
        conn: Database connection
        source_date: Source date (YYYY-MM-DD)
    
    Returns:
        List of dicts with keys: variable, file_date, file_path
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT variable, file_date, file_path FROM live_ocean_data
                WHERE source_date = %s AND status IN ('extracted', 'pending_image', 'imaging_failed', 'success')
                ORDER BY variable, file_date
                """,
                (source_date,),
            )
            rows = cur.fetchall()
            return [{"variable": r[0], "file_date": r[1].isoformat(), "path": r[2]} for r in rows]
    except Exception as e:
        logger.error(f"Failed to get extracted files for source_date {source_date}: {e}")
        return []


def get_available_dates_for_variable(conn, variable: str) -> List[str]:
    """Get list of available file_dates for a variable (YYYY-MM-DD format)."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT file_date FROM live_ocean_data
                WHERE variable = %s
                ORDER BY file_date DESC
                """,
                (variable,),
            )
            return [row[0].isoformat() for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Failed to fetch available dates for {variable}: {e}")
        return []


def init_schema(conn):
    """Create unified live_ocean_data table for tracking daily files through their lifecycle."""
    try:
        with conn.cursor() as cur:
            # Drop old tables if they exist (clean start)
            # Uncomment these if you want automatic migration:
            # cur.execute("DROP TABLE IF EXISTS live_ocean_processed_dates CASCADE")
            # cur.execute("DROP TABLE IF EXISTS live_ocean_files CASCADE")
            
            # Create unified live_ocean_data table
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS live_ocean_data (
                    id SERIAL PRIMARY KEY,
                    source_date DATE NOT NULL,
                    variable VARCHAR(50) NOT NULL,
                    file_date DATE NOT NULL,
                    file_path TEXT NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'extracted',
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE (source_date, variable, file_date)
                )
                """
            )
            
            # Create indexes for performance
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_live_ocean_data_source_date ON live_ocean_data(source_date)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_live_ocean_data_status ON live_ocean_data(status)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_live_ocean_data_variable ON live_ocean_data(variable)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_live_ocean_data_file_date ON live_ocean_data(file_date)"
            )
            
        conn.commit()
        logger.info("Database schema initialized successfully")
    except psycopg2.Error as e:
        if "already exists" in str(e):
            logger.info("Schema already initialized")
            try:
                conn.commit()
            except:
                pass
        else:
            logger.error(f"Failed to initialize schema: {e}")
            raise
    except Exception as e:
        logger.error(f"Failed to initialize schema: {e}")
        raise
