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
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS erddap_datasets (
            id SERIAL PRIMARY KEY,
            dataset_id TEXT UNIQUE NOT NULL,
            title TEXT,
            last_checked_at TIMESTAMPTZ,
            last_remote_time TIMESTAMPTZ,
            last_downloaded_at TIMESTAMPTZ,
            meta JSONB
        );
        CREATE TABLE IF NOT EXISTS erddap_variables (
            id SERIAL PRIMARY KEY,
            dataset_id INT NOT NULL REFERENCES erddap_datasets(id) ON DELETE CASCADE,
            variable TEXT NOT NULL,
            last_downloaded_at TIMESTAMPTZ,
            meta JSONB,
            UNIQUE(dataset_id, variable)
        );
        CREATE TABLE IF NOT EXISTS nc_files (
            id SERIAL PRIMARY KEY,
            dataset_id INT NOT NULL REFERENCES erddap_datasets(id) ON DELETE CASCADE,
            variable TEXT NOT NULL,
            start_time TIMESTAMPTZ NOT NULL,
            end_time TIMESTAMPTZ NOT NULL,
            filename TEXT,
            file_path TEXT,
            file_size BIGINT,
            checksum TEXT,
            downloaded_at TIMESTAMPTZ,
            attempts INT DEFAULT 0,
            last_attempt TIMESTAMPTZ,
            last_error TEXT,
            meta JSONB,
            status_dl TEXT DEFAULT 'pending',
            status_sublevel TEXT DEFAULT NULL,
            status_png TEXT DEFAULT NULL,
            filename_sublevel TEXT,
            file_path_sublevel TEXT,
            file_size_sublevel BIGINT,
            checksum_sublevel TEXT,
            attempts_sublevel INT DEFAULT 0,
            last_attempt_sublevel TIMESTAMPTZ,
            last_error_sublevel TEXT,
            attempts_png INT DEFAULT 0,
            last_attempt_png TIMESTAMPTZ,
            last_error_png TEXT,
            png_meta JSONB,
            UNIQUE(dataset_id, variable, start_time, end_time)
        );
        CREATE INDEX IF NOT EXISTS idx_nc_files_var_start ON nc_files(variable, start_time);
        CREATE INDEX IF NOT EXISTS idx_nc_files_status_dl ON nc_files(status_dl);
        """)
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
    conn.commit()


def upsert_dataset(conn, dataset_id, title=None, last_remote_time=None, meta=None):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, last_remote_time FROM erddap_datasets WHERE dataset_id = %s",
            (dataset_id,),
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
                "INSERT INTO erddap_datasets (dataset_id, title, last_checked_at, last_remote_time, meta) VALUES (%s,%s,NOW(),%s,%s) RETURNING id",
                (
                    dataset_id,
                    title,
                    last_remote_time,
                    Json(meta) if meta is not None else None,
                ),
            )
            ds_id = cur.fetchone()[0]
    conn.commit()
    return ds_id


def ensure_variable(conn, ds_id, variable):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM erddap_variables WHERE dataset_id=%s AND variable=%s",
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
