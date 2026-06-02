#!/usr/bin/env python3
"""Insert nc_jobs rows for all variables for a given date.

For each (dataset_id, variable) in the fields table, inserts a row into
nc_jobs with status 'pending_download' for the given date (UTC midnight to
next midnight). Skips rows that already exist.

Usage
-----
    python insert_nc_jobs.py --date 2026-03-30
    python insert_nc_jobs.py --date 2026-03-30 --status pending_compute
    python insert_nc_jobs.py --date 2026-03-30 --dataset SalishSeaCast
    python insert_nc_jobs.py --date 2026-03-30 --dry-run
"""

import argparse
import os
import sys
from datetime import datetime, timezone

import psycopg2


def get_conn(host, port, dbname, user, password):
    return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)


def main():
    p = argparse.ArgumentParser(description="Insert nc_jobs rows for all variables for a given date.")
    p.add_argument("--date", required=True, help="Date in YYYY-MM-DD format")
    p.add_argument(
        "--status",
        default="pending_download",
        help="Initial status for new rows (default: pending_download)",
    )
    p.add_argument(
        "--dataset",
        default=None,
        help="Comma-separated dataset source name(s) to restrict (default: all datasets)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be inserted without writing to DB",
    )
    p.add_argument("--pghost", default=os.getenv("PGHOST", "localhost"))
    p.add_argument("--pgport", type=int, default=int(os.getenv("PGPORT", 5432)))
    p.add_argument("--pgdatabase", default=os.getenv("PGDATABASE", "oa"))
    p.add_argument("--pguser", default=os.getenv("PGUSER", "postgres"))
    p.add_argument("--pgpassword", default=os.getenv("PGPASSWORD", "postgres"))
    args = p.parse_args()

    try:
        date = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        sys.exit(f"Invalid date format: {args.date!r}. Expected YYYY-MM-DD.")

    start_time = date.replace(hour=0, minute=30, second=0)
    end_time = date.replace(hour=23, minute=30, second=0)

    dataset_ids = None

    conn = get_conn(args.pghost, args.pgport, args.pgdatabase, args.pguser, args.pgpassword)

    try:
        # Resolve dataset sources to IDs if restricted
        if args.dataset is not None:
            dataset_sources = [x.strip() for x in args.dataset.split(",") if x.strip()]
            if not dataset_sources:
                sys.exit("Invalid --dataset value. Expected comma-separated dataset source name(s).")
            
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, source FROM datasets WHERE source = ANY(%s)",
                    (dataset_sources,),
                )
                rows = cur.fetchall()
                found_sources = {row[1]: row[0] for row in rows}
                missing = [s for s in dataset_sources if s not in found_sources]
                if missing:
                    sys.exit(f"Dataset(s) not found in datasets table: {', '.join(missing)}")
                dataset_ids = list(found_sources.values())

        # Reset sequence to 1 if table is empty
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM nc_jobs")
            count = cur.fetchone()[0]
            if count == 0:
                try:
                    cur.execute("ALTER SEQUENCE nc_jobs_id_seq RESTART WITH 1")
                    conn.commit()
                    print("Reset nc_jobs_id_seq to 1 (table is empty).")
                except psycopg2.Error:
                    conn.rollback()

        with conn.cursor() as cur:
            if dataset_ids is not None:
                cur.execute(
                    "SELECT id, dataset_id, variable FROM fields WHERE dataset_id = ANY(%s::uuid[]) ORDER BY dataset_id, variable",
                    (dataset_ids,),
                )
            else:
                cur.execute(
                    "SELECT id, dataset_id, variable FROM fields ORDER BY dataset_id, variable"
                )
            variables = cur.fetchall()

        if not variables:
            print("No variables found in fields table.")
            return

        print(f"Date range : {start_time.date()} → {end_time.date()}")
        print(f"Status     : {args.status}")
        print(f"Variables  : {len(variables)}")
        if args.dry_run:
            print("(dry run — nothing will be written)\n")

        inserted = 0
        skipped = 0

        with conn.cursor() as cur:
            for var_id, ds_id, variable in variables:
                if args.dry_run:
                    # Check if row already exists
                    cur.execute(
                        """
                        SELECT id FROM nc_jobs
                        WHERE dataset_id IS NOT DISTINCT FROM %s
                          AND variable_id = %s
                          AND start_time = %s
                          AND end_time = %s
                        """,
                        (ds_id, var_id, start_time, end_time),
                    )
                    exists = cur.fetchone() is not None
                    status_label = "(exists — would skip)" if exists else "(would insert)"
                    print(f"  dataset_id={ds_id}  variable={variable}  {status_label}")
                    if exists:
                        skipped += 1
                    else:
                        inserted += 1
                    continue

                cur.execute(
                    """
                    SELECT 1 FROM nc_jobs
                    WHERE dataset_id IS NOT DISTINCT FROM %s
                      AND variable_id = %s
                      AND start_time = %s
                      AND end_time = %s
                    """,
                    (ds_id, var_id, start_time, end_time),
                )
                if cur.fetchone() is not None:
                    skipped += 1
                    print(f"  Skipped   dataset_id={ds_id}  variable={variable}  (already exists)")
                    continue

                cur.execute(
                    """
                    INSERT INTO nc_jobs (dataset_id, variable_id, start_time, end_time, status, attempts)
                    VALUES (%s, %s, %s, %s, %s, 0)
                    """,
                    (ds_id, var_id, start_time, end_time, args.status),
                )
                inserted += 1
                print(f"  Inserted  dataset_id={ds_id}  variable={variable}")

        if not args.dry_run:
            conn.commit()

        print(f"\nDone: {inserted} inserted, {skipped} skipped.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
