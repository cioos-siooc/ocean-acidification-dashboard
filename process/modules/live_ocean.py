"""Live Ocean workflow using live_ocean_runs and nc_jobs."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import List

import requests
from psycopg2.extras import Json  # type: ignore[import]

from .db import ensure_variable, get_dataset_meta, upsert_dataset
from dl_LO.main import process_live_ocean

logger = logging.getLogger("dl2.live_ocean")


def _download_layers(url: str, input_path: str) -> str:
    os.makedirs(os.path.dirname(input_path), exist_ok=True)
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(input_path, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    fh.write(chunk)
    return input_path


def create_live_ocean_run(conn, run_date: str, url: str, input_path: str, out_dir: str):
    """Insert a live_ocean_runs row with status=downloading (idempotent by run_date)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO live_ocean_runs (run_date, status, input_path, out_dir, meta)
            VALUES (%s, 'downloading', %s, %s, %s)
            ON CONFLICT (run_date)
            DO UPDATE SET status='downloading', input_path=EXCLUDED.input_path, out_dir=EXCLUDED.out_dir, meta=EXCLUDED.meta
            RETURNING id
            """,
            (run_date, input_path, out_dir, Json({"url": url})),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def download_live_ocean(conn, run_date: str, url: str, input_path: str, out_dir: str) -> int:
    """Download layers.nc and move run to pending_process."""
    run_id = create_live_ocean_run(conn, run_date, url, input_path, out_dir)
    try:
        _download_layers(url, input_path)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE live_ocean_runs
                SET status='pending_process', attempts=attempts+1, last_attempt=NOW()
                WHERE id=%s
                """,
                (run_id,),
            )
        conn.commit()
        return run_id
    except Exception:
        logger.exception("Live Ocean download failed")
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE live_ocean_runs SET status='failed_download', attempts=attempts+1, last_attempt=NOW() WHERE id=%s",
                (run_id,),
            )
        conn.commit()
        raise


def process_pending_live_ocean(conn, limit: int = 1):
    """Get Live Ocean row from datasets"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id,base_url,meta FROM datasets
            WHERE title='Live Ocean'
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("No Live Ocean dataset found in datasets")
        ds_id = row[0]
        dataset_base_url = row[1]
        existing_meta = row[2]

    """Process pending Live Ocean runs into daily nc files and create nc_jobs rows."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, run_date, input_path, out_dir, meta
            FROM live_ocean_runs
            WHERE status='pending_process'
            ORDER BY run_date
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
        
    for run_id, run_date, input_path, out_dir, meta in rows:
        # with conn.cursor() as cur:
        #     cur.execute(
        #         "UPDATE live_ocean_runs SET status='processing', last_attempt=NOW() WHERE id=%s",
        #         (run_id,),
        #     )
        # conn.commit()

        try:
            depths_meta = existing_meta.get("depths")
            merged_meta = {
                **existing_meta,
                "source": "liveocean",
                "run_date": str(run_date),
                "base_url": dataset_base_url,
            }

            outputs: List[dict] = process_live_ocean(
                url=dataset_base_url,
                input_path=input_path,
                out_dir=out_dir,
                skip_download=True,
                depth_order_meta=depths_meta,
            )

            # ds_id = upsert_dataset(
            #     conn,
            #     dataset_base_url,
            #     title="Live Ocean",
            #     meta=merged_meta,
            # )

            with conn.cursor() as cur:
                for rec in outputs:
                    variable = rec["variable"]
                    start_time = rec["start_time"]
                    end_time = rec["end_time"]
                    path = rec["path"]

                    var_id = ensure_variable(conn, ds_id, variable)

                    cur.execute(
                        """
                        INSERT INTO nc_jobs (dataset_id, variable_id, start_time, end_time, status, nc_path, attempts, last_attempt)
                        VALUES (%s, %s, %s, %s, 'pending_image', %s, 0, NOW())
                        ON CONFLICT (dataset_id, variable_id, start_time, end_time)
                        DO UPDATE SET status = EXCLUDED.status, nc_path = EXCLUDED.nc_path, last_attempt = NOW()
                        """,
                        (ds_id, var_id, start_time, end_time, path),
                    )

                    cur.execute(
                        "UPDATE fields SET last_downloaded_at = GREATEST(COALESCE(last_downloaded_at, to_timestamp(0)), %s) WHERE id = %s",
                        (end_time, var_id),
                    )

                latest_end = max([rec["end_time"] for rec in outputs]) if outputs else None
                if latest_end is not None:
                    cur.execute(
                        "UPDATE datasets SET last_downloaded_at = GREATEST(COALESCE(last_downloaded_at, to_timestamp(0)), %s) WHERE id = %s",
                        (latest_end, ds_id),
                    )

                cur.execute(
                    "UPDATE live_ocean_runs SET status='success', last_attempt=NOW() WHERE id=%s",
                    (run_id,),
                )

            conn.commit()
        except Exception:
            logger.exception("Live Ocean processing failed")
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE live_ocean_runs SET status='failed_process', attempts=attempts+1, last_attempt=NOW() WHERE id=%s",
                    (run_id,),
                )
            conn.commit()
            raise


def today_utc_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()