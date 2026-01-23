#!/usr/bin/env python3
"""Backfill nc_files.file_path from file_path_sublevel and schedule PNG generation.

Run this one-time script if you are decommissioning sublevels and want PNG worker to process
rows that previously only had sublevel files recorded.

It will update rows where file_path IS NULL and file_path_sublevel IS NOT NULL and set:
  file_path = file_path_sublevel,
  filename = filename_sublevel,
  file_size = file_size_sublevel,
  checksum = checksum_sublevel,
  status_png = 'pending'

Use cautiously and consider taking a DB dump first.
"""
from dl2pkg.db import get_db_conn

def main():
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # Check whether the unified 'status' column exists; if not, fall back to updating the old 'status_png' column for compatibility.
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='nc_files' AND column_name='status'")
            has_status = cur.fetchone() is not None
            if has_status:
                cur.execute(
                    """
                    UPDATE nc_files
                    SET file_path = file_path_sublevel,
                        filename = filename_sublevel,
                        file_size = file_size_sublevel,
                        checksum = checksum_sublevel,
                        status = 'pending_image'
                    WHERE file_path IS NULL AND file_path_sublevel IS NOT NULL
                    RETURNING id, file_path, filename
                    """
                )
            else:
                cur.execute(
                    """
                    UPDATE nc_files
                    SET file_path = file_path_sublevel,
                        filename = filename_sublevel,
                        file_size = file_size_sublevel,
                        checksum = checksum_sublevel,
                        status_png = 'pending'
                    WHERE file_path IS NULL AND file_path_sublevel IS NOT NULL
                    RETURNING id, file_path, filename
                    """
                )
            rows = cur.fetchall()
            conn.commit()
            for r in rows:
                print(f"Backfilled id={r[0]} file={r[1]} ({r[2]})")
    finally:
        conn.close()

if __name__ == '__main__':
    main()
