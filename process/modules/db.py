"""Database helpers for dl2.

Provides get_db_conn() and ensure_schema(conn), kept minimal and idempotent so it
can be used at import-time safely.
"""
from typing import Optional
import os
import logging
import psycopg2
from psycopg2.extras import Json

logger = logging.getLogger("dl2.db")


def get_db_conn():
    host = os.getenv("PGHOST", "db")
    port = int(os.getenv("PGPORT", 5432))
    db = os.getenv("PGDATABASE", "oa")
    user = os.getenv("PGUSER", "postgres")
    pw = os.getenv("PGPASSWORD", "postgres")
    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pw)
    return conn


def ensure_schema(conn):
    with conn.cursor() as cur:
        # Determine which enum name(s) are in use (db may have been created with either/both names)
        cur.execute("SELECT typname FROM pg_type WHERE typname IN ('nc_file_status', 'nc_job_status')")
        existing_enums = [r[0] for r in cur.fetchall()]

        if not existing_enums:
            # Neither exists — create the canonical name
            cur.execute("CREATE TYPE nc_file_status AS ENUM ('pending_download','downloading','failed_download','success_download','pending_compute','computing','failed_compute','success_compute','pending_image','imaging','failed_image','success_image')")
            existing_enums = ['nc_file_status']

        # Add new values to all matching enum names (handles both nc_file_status and nc_job_status)
        for enum_name in existing_enums:
            for val in ('downloading', 'computing', 'imaging',
                        'pending_bottom', 'bottoming', 'success_bottom', 'failed_bottom',
                        'extracted', 'imaging_failed'):
                cur.execute(f"ALTER TYPE {enum_name} ADD VALUE IF NOT EXISTS %s", (val,))

        # Live Ocean status enum
        cur.execute("SELECT 1 FROM pg_type WHERE typname = 'live_ocean_status'")
        if not cur.fetchone():
            cur.execute("CREATE TYPE live_ocean_status AS ENUM ('downloading','pending_process','processing','success','failed_download','failed_process')")

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS datasets (
            id SERIAL PRIMARY KEY,
            base_url TEXT UNIQUE NOT NULL,
            title TEXT,
            last_checked_at TIMESTAMPTZ,
            last_remote_time TIMESTAMPTZ,
            last_downloaded_at TIMESTAMPTZ,
            depths JSONB DEFAULT NULL,
            meta JSONB
        );
        CREATE TABLE IF NOT EXISTS fields (
            id SERIAL PRIMARY KEY,
            dataset_id INT REFERENCES datasets(id) ON DELETE SET NULL,
            variable TEXT NOT NULL,
            last_downloaded_at TIMESTAMPTZ,
            meta JSONB,
            UNIQUE(dataset_id, variable)
        );
        -- New jobs table for pipeline processing. This table will be used instead of the legacy
        -- `nc_files` table going forward. We intentionally create the table without
        -- auto-migration of old rows (admin must decide when to migrate/drop the legacy table).
        -- enum type ensured above

        CREATE TABLE IF NOT EXISTS nc_jobs (
            id SERIAL PRIMARY KEY,
            dataset_id INT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
            variable_id INT NOT NULL REFERENCES fields(id) ON DELETE CASCADE,
            start_time TIMESTAMPTZ NOT NULL,
            end_time TIMESTAMPTZ NOT NULL,
            status nc_file_status DEFAULT 'pending_download',
            nc_path TEXT,
            attempts INT DEFAULT 0,
            last_attempt TIMESTAMPTZ,
            error_message TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            misc JSONB
        );

        CREATE TABLE IF NOT EXISTS live_ocean_runs (
            id SERIAL PRIMARY KEY,
            run_date DATE NOT NULL,
            status live_ocean_status DEFAULT 'downloading',
            input_path TEXT,
            out_dir TEXT,
            attempts INT DEFAULT 0,
            last_attempt TIMESTAMPTZ,
            meta JSONB,
            UNIQUE(run_date)
        );

        CREATE INDEX IF NOT EXISTS idx_nc_jobs_var_start ON nc_jobs(variable_id, start_time);
        CREATE INDEX IF NOT EXISTS idx_nc_jobs_status ON nc_jobs(status);
            """)
        # Add new columns to existing nc_jobs tables (idempotent for already-running DBs)
        for col, defn in [
            ("error_message", "TEXT"),
            ("created_at", "TIMESTAMPTZ DEFAULT NOW()"),
            ("updated_at", "TIMESTAMPTZ DEFAULT NOW()"),
            ("misc", "JSONB"),
        ]:
            cur.execute(f"ALTER TABLE nc_jobs ADD COLUMN IF NOT EXISTS {col} {defn}")
        # GIN index on misc for efficient JSONB key lookups
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nc_jobs_misc ON nc_jobs USING GIN (misc)")
        # Drop old source_date column if it exists (replaced by misc)
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='nc_jobs' AND column_name='source_date'"
        )
        if cur.fetchone():
            cur.execute("ALTER TABLE nc_jobs DROP COLUMN source_date")
        # Replace 4-column unique index (dataset_id, variable_id, start_time, end_time)
        # with 3-column (dataset_id, variable_id, start_time) to support unified SSC+LO table.
        try:
            cur.execute("SELECT indexdef FROM pg_indexes WHERE indexname = 'ux_nc_jobs_dataset_variable_time'")
            idx_row = cur.fetchone()
            if idx_row and 'end_time' in idx_row[0]:
                # Old 4-column index — drop and recreate as 3-column
                cur.execute("DROP INDEX IF EXISTS ux_nc_jobs_dataset_variable_time")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_nc_jobs_dataset_variable_time ON nc_jobs (dataset_id, variable_id, start_time)")
        except Exception:
            conn.rollback()
            logger.warning("Could not update unique index ux_nc_jobs_dataset_variable_time; continuing without it")
        # Remove old partial-null index (superseded by the 3-column constraint above)
        try:
            cur.execute("DROP INDEX IF EXISTS ux_nc_jobs_null_dataset_variable_time")
        except Exception:
            conn.rollback()

        # If older schema used dataset_id column name, rename to base_url
        cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='datasets' AND column_name='dataset_id'"
        )
        has_dataset_id = cur.fetchone() is not None
        cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='datasets' AND column_name='base_url'"
        )
        has_base_url = cur.fetchone() is not None
        if has_dataset_id and not has_base_url:
            cur.execute("ALTER TABLE datasets RENAME COLUMN dataset_id TO base_url")

        # Ensure fields has a column to store available datetimes per variable.
        # Prefer a native timestamptz[] column for efficient storage and querying.
        # If an existing JSONB column is present, try to convert it safely.
        cur.execute("SELECT data_type, udt_name FROM information_schema.columns WHERE table_name='fields' AND column_name='available_datetimes'")
        col = cur.fetchone()
        if not col:
            cur.execute("ALTER TABLE fields ADD COLUMN available_datetimes timestamptz[] DEFAULT '{}'::timestamptz[]")
        else:
            data_type, udt_name = col
            if data_type.lower() == 'jsonb':                # ensure 'type' column exists to indicate whether a variable is 'download' or 'compute'
                cur.execute("ALTER TABLE fields ADD COLUMN IF NOT EXISTS type TEXT DEFAULT 'download'")
            else:
                # ensure 'type' column exists regardless
                cur.execute("ALTER TABLE fields ADD COLUMN IF NOT EXISTS type TEXT DEFAULT 'download'")
            if data_type.lower() == 'jsonb':                # Convert JSONB array of ISO strings to timestamptz[] in a new temporary column,
                # then replace the original column to avoid data loss.
                cur.execute("ALTER TABLE fields ADD COLUMN IF NOT EXISTS available_datetimes_ts timestamptz[] DEFAULT '{}'::timestamptz[]")
                # Use jsonb_array_elements_text to extract strings and cast to timestamptz, aggregate distinct ordered list
                cur.execute(
                    "UPDATE fields SET available_datetimes_ts = (SELECT array_agg(DISTINCT (x::timestamptz) ORDER BY (x::timestamptz)) FROM jsonb_array_elements_text(available_datetimes) AS x WHERE available_datetimes IS NOT NULL)"
                )
                cur.execute("ALTER TABLE fields DROP COLUMN available_datetimes")
                cur.execute("ALTER TABLE fields RENAME COLUMN available_datetimes_ts TO available_datetimes")
            elif data_type.lower() == 'array':
                # already an array type; nothing to do
                pass
            else:
                # If it's some other type, try to change to timestamptz[] (may fail if incompatible values exist)
                try:
                    cur.execute("ALTER TABLE fields ALTER COLUMN available_datetimes TYPE timestamptz[] USING available_datetimes::timestamptz[]")
                except Exception:
                    # leave as-is; downstream code will handle JSONB as fallback
                    logger.exception('Could not convert available_datetimes column to timestamptz[]; leaving existing type')
        
        # Ensure fields has a precision column for PNG packing (defaults to 0.1)
        cur.execute("SELECT 1 FROM information_schema.columns WHERE table_name='fields' AND column_name='precision'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE fields ADD COLUMN precision FLOAT DEFAULT 0.1")

        # Ensure datasets has a depths JSONB column (array of {value, hasImage} objects)
        cur.execute("ALTER TABLE datasets ADD COLUMN IF NOT EXISTS depths JSONB DEFAULT NULL")

        # No automatic migration of `nc_files` rows will be performed.
        # We create the `nc_jobs` table for new pipeline usage and leave the legacy
        # `nc_files` table untouched; administrators may migrate/drop it manually when ready.
        pass
    conn.commit()


def upsert_dataset(conn, base_url, title=None, last_remote_time=None, meta=None):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, last_remote_time FROM datasets WHERE base_url = %s",
            (base_url,),
        )
        row = cur.fetchone()
        if row:
            ds_id, prev_remote = row
            if last_remote_time and (not prev_remote or last_remote_time > prev_remote):
                cur.execute(
                    "UPDATE datasets SET last_remote_time = %s, last_checked_at = NOW(), title = COALESCE(%s, title), meta = COALESCE(%s, meta) WHERE id = %s",
                    (
                        last_remote_time,
                        title,
                        Json(meta) if meta is not None else None,
                        ds_id,
                    ),
                )
            else:
                cur.execute(
                    "UPDATE datasets SET last_checked_at = NOW(), title = COALESCE(%s, title) WHERE id = %s",
                    (title, ds_id),
                )
        else:
            cur.execute(
                "INSERT INTO datasets (base_url, title, last_checked_at, last_remote_time, meta) VALUES (%s,%s,NOW(),%s,%s) RETURNING id",
                (
                    base_url,
                    title,
                    last_remote_time,
                    Json(meta) if meta is not None else None,
                ),
            )
            ds_id = cur.fetchone()[0]
    conn.commit()
    return ds_id


def ensure_variable(conn, ds_id, variable):
    # Use NULL-safe comparison so callers can pass ds_id=None to create/query
    # variables not attached to a dataset. PostgreSQL's "IS NOT DISTINCT FROM"
    # treats NULLs as equal for comparison purposes.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM fields WHERE dataset_id IS NOT DISTINCT FROM %s AND variable=%s",
            (ds_id, variable),
        )
        r = cur.fetchone()
        if r:
            return r[0]
        cur.execute(
            "INSERT INTO fields (dataset_id, variable, meta) VALUES (%s,%s,%s) RETURNING id",
            (ds_id, variable, Json({})),
        )
        conn.commit()
        return cur.fetchone()[0]
 

def get_dataset_meta(conn, id):
    with conn.cursor() as cur:
        cur.execute("SELECT meta FROM datasets WHERE id=%s", (id,))
        row = cur.fetchone()
        return row[0] if row else {}
