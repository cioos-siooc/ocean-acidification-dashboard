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
        # Ensure enum type nc_file_status exists; some Postgres setups don't support "CREATE TYPE IF NOT EXISTS"
        cur.execute("SELECT 1 FROM pg_type WHERE typname = 'nc_file_status'")
        if not cur.fetchone():
            cur.execute("CREATE TYPE nc_file_status AS ENUM ('pending_download','downloading','failed_download','success_download','pending_compute','computing','failed_compute','success_compute','pending_image','imaging','failed_image','success_image')")
        else:
            # Ensure new in-flight status values exist (idempotent)
            cur.execute("ALTER TYPE nc_file_status ADD VALUE IF NOT EXISTS 'downloading'")
            cur.execute("ALTER TYPE nc_file_status ADD VALUE IF NOT EXISTS 'computing'")
            cur.execute("ALTER TYPE nc_file_status ADD VALUE IF NOT EXISTS 'imaging'")

        # Live Ocean status enum
        cur.execute("SELECT 1 FROM pg_type WHERE typname = 'live_ocean_status'")
        if not cur.fetchone():
            cur.execute("CREATE TYPE live_ocean_status AS ENUM ('downloading','pending_process','processing','success','failed_download','failed_process')")

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS erddap_datasets (
            id SERIAL PRIMARY KEY,
            base_url TEXT UNIQUE NOT NULL,
            title TEXT,
            last_checked_at TIMESTAMPTZ,
            last_remote_time TIMESTAMPTZ,
            last_downloaded_at TIMESTAMPTZ,
            meta JSONB
        );
        CREATE TABLE IF NOT EXISTS erddap_variables (
            id SERIAL PRIMARY KEY,
            dataset_id INT REFERENCES erddap_datasets(id) ON DELETE SET NULL,
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
            dataset_id INT NOT NULL REFERENCES erddap_datasets(id) ON DELETE CASCADE,
            variable_id INT NOT NULL REFERENCES erddap_variables(id) ON DELETE CASCADE,
            start_time TIMESTAMPTZ NOT NULL,
            end_time TIMESTAMPTZ NOT NULL,
            status nc_file_status DEFAULT 'pending_download',
            nc_path TEXT,
            checksum TEXT,
            attempts INT DEFAULT 0,
            last_attempt TIMESTAMPTZ
        );

        CREATE TABLE IF NOT EXISTS live_ocean_runs (
            id SERIAL PRIMARY KEY,
            run_date DATE NOT NULL,
            status live_ocean_status DEFAULT 'downloading',
            input_path TEXT,
            out_dir TEXT,
            checksum TEXT,
            attempts INT DEFAULT 0,
            last_attempt TIMESTAMPTZ,
            meta JSONB,
            UNIQUE(run_date)
        );

        CREATE INDEX IF NOT EXISTS idx_nc_jobs_var_start ON nc_jobs(variable_id, start_time);
        -- create index on unified status column
        CREATE INDEX IF NOT EXISTS idx_nc_jobs_status ON nc_jobs(status);
        -- Prevent duplicate rows for NULL dataset_id: add a partial unique index for rows where dataset_id IS NULL
        -- Partial unique index for rows where dataset_id IS NULL will be created below
            -- (created separately to avoid multi-statement parsing issues)
            """)
        # Create the full and partial unique indexes (created separately to avoid parser issues)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_nc_jobs_dataset_variable_time ON nc_jobs (dataset_id, variable_id, start_time, end_time)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_nc_jobs_null_dataset_variable_time ON nc_jobs (variable_id, start_time, end_time) WHERE dataset_id IS NULL")

        # If older schema used dataset_id column name, rename to base_url
        cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='erddap_datasets' AND column_name='dataset_id'"
        )
        has_dataset_id = cur.fetchone() is not None
        cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='erddap_datasets' AND column_name='base_url'"
        )
        has_base_url = cur.fetchone() is not None
        if has_dataset_id and not has_base_url:
            cur.execute("ALTER TABLE erddap_datasets RENAME COLUMN dataset_id TO base_url")

        # Ensure erddap_variables has a column to store available datetimes per variable.
        # Prefer a native timestamptz[] column for efficient storage and querying.
        # If an existing JSONB column is present, try to convert it safely.
        cur.execute("SELECT data_type, udt_name FROM information_schema.columns WHERE table_name='erddap_variables' AND column_name='available_datetimes'")
        col = cur.fetchone()
        if not col:
            cur.execute("ALTER TABLE erddap_variables ADD COLUMN available_datetimes timestamptz[] DEFAULT '{}'::timestamptz[]")
        else:
            data_type, udt_name = col
            if data_type.lower() == 'jsonb':                # ensure 'type' column exists to indicate whether a variable is 'download' or 'compute'
                cur.execute("ALTER TABLE erddap_variables ADD COLUMN IF NOT EXISTS type TEXT DEFAULT 'download'")
            else:
                # ensure 'type' column exists regardless
                cur.execute("ALTER TABLE erddap_variables ADD COLUMN IF NOT EXISTS type TEXT DEFAULT 'download'")
            if data_type.lower() == 'jsonb':                # Convert JSONB array of ISO strings to timestamptz[] in a new temporary column,
                # then replace the original column to avoid data loss.
                cur.execute("ALTER TABLE erddap_variables ADD COLUMN IF NOT EXISTS available_datetimes_ts timestamptz[] DEFAULT '{}'::timestamptz[]")
                # Use jsonb_array_elements_text to extract strings and cast to timestamptz, aggregate distinct ordered list
                cur.execute(
                    "UPDATE erddap_variables SET available_datetimes_ts = (SELECT array_agg(DISTINCT (x::timestamptz) ORDER BY (x::timestamptz)) FROM jsonb_array_elements_text(available_datetimes) AS x WHERE available_datetimes IS NOT NULL)"
                )
                cur.execute("ALTER TABLE erddap_variables DROP COLUMN available_datetimes")
                cur.execute("ALTER TABLE erddap_variables RENAME COLUMN available_datetimes_ts TO available_datetimes")
            elif data_type.lower() == 'array':
                # already an array type; nothing to do
                pass
            else:
                # If it's some other type, try to change to timestamptz[] (may fail if incompatible values exist)
                try:
                    cur.execute("ALTER TABLE erddap_variables ALTER COLUMN available_datetimes TYPE timestamptz[] USING available_datetimes::timestamptz[]")
                except Exception:
                    # leave as-is; downstream code will handle JSONB as fallback
                    logger.exception('Could not convert available_datetimes column to timestamptz[]; leaving existing type')
        
        # Ensure erddap_variables has a precision column for PNG packing (defaults to 0.1)
        cur.execute("SELECT 1 FROM information_schema.columns WHERE table_name='erddap_variables' AND column_name='precision'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE erddap_variables ADD COLUMN precision FLOAT DEFAULT 0.1")

        # No automatic migration of `nc_files` rows will be performed.
        # We create the `nc_jobs` table for new pipeline usage and leave the legacy
        # `nc_files` table untouched; administrators may migrate/drop it manually when ready.
        pass
    conn.commit()


def upsert_dataset(conn, base_url, title=None, last_remote_time=None, meta=None):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, last_remote_time FROM erddap_datasets WHERE base_url = %s",
            (base_url,),
        )
        row = cur.fetchone()
        if row:
            ds_id, prev_remote = row
            if last_remote_time and (not prev_remote or last_remote_time > prev_remote):
                cur.execute(
                    "UPDATE erddap_datasets SET last_remote_time = %s, last_checked_at = NOW(), title = COALESCE(%s, title), meta = COALESCE(%s, meta) WHERE id = %s",
                    (
                        last_remote_time,
                        title,
                        Json(meta) if meta is not None else None,
                        ds_id,
                    ),
                )
            else:
                cur.execute(
                    "UPDATE erddap_datasets SET last_checked_at = NOW(), title = COALESCE(%s, title) WHERE id = %s",
                    (title, ds_id),
                )
        else:
            cur.execute(
                "INSERT INTO erddap_datasets (base_url, title, last_checked_at, last_remote_time, meta) VALUES (%s,%s,NOW(),%s,%s) RETURNING id",
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
            "SELECT id FROM erddap_variables WHERE dataset_id IS NOT DISTINCT FROM %s AND variable=%s",
            (ds_id, variable),
        )
        r = cur.fetchone()
        if r:
            return r[0]
        cur.execute(
            "INSERT INTO erddap_variables (dataset_id, variable, meta) VALUES (%s,%s,%s) RETURNING id",
            (ds_id, variable, Json({})),
        )
        conn.commit()
        return cur.fetchone()[0]
 

def get_dataset_meta(conn, id):
    with conn.cursor() as cur:
        cur.execute("SELECT meta FROM erddap_datasets WHERE id=%s", (id,))
        row = cur.fetchone()
        return row[0] if row else {}
