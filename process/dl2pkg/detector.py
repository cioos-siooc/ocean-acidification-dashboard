"""Detection and scheduling helpers: chunking and creating nc_jobs rows."""
from datetime import datetime, timedelta, timezone
from typing import List, Tuple
import logging
from .db import get_db_conn
from .compute import get_compute_variables

logger = logging.getLogger("dl2.detector")


def compute_daily_chunks(start_dt: datetime, end_dt: datetime, require_full_day: bool = True) -> List[Tuple[datetime, datetime]]:
    chunks = []
    cur_day = start_dt.date()
    last_day = end_dt.date()
    day = cur_day
    while day <= last_day:
        day_start = datetime(day.year, day.month, day.day, 0, 30, tzinfo=timezone.utc)
        day_end = datetime(day.year, day.month, day.day, 23, 30, tzinfo=timezone.utc)
        cs = max(start_dt, day_start)
        ce = min(end_dt, day_end)
        if require_full_day:
            if start_dt <= day_start and end_dt >= day_end:
                chunks.append((day_start, day_end))
            else:
                logger.debug('Skipping partial day %s (available %s->%s), waiting for full day %s->%s', day, cs, ce, day_start, day_end)
        else:
            if cs <= ce:
                chunks.append((cs, ce))
        day = (datetime(day.year, day.month, day.day, tzinfo=timezone.utc) + timedelta(days=1)).date()
    return chunks


from psycopg2.extras import Json
from .db import ensure_variable


def create_nc_file_row(conn, ds_id, variable, start_time, end_time, meta=None):
    # Ensure the variable exists and obtain its id, then insert into nc_jobs.variable_id
    var_id = ensure_variable(conn, ds_id, variable)
    with conn.cursor() as cur:
        try:
            cur.execute(
                "INSERT INTO nc_jobs (dataset_id, variable_id, start_time, end_time) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING id",
                (
                    ds_id,
                    var_id,
                    start_time,
                    end_time,
                ),
            )
            res = cur.fetchone()
            if res:
                conn.commit()
                return res[0]
            else:
                conn.commit()
                return None
        except Exception:
            conn.rollback()
            raise


def ensure_pending_nc_file(conn, ds_id, variable, start_time, end_time, force=False, meta=None):
    """Ensure a nc_files row exists for the exact start/end times.
    If force=True, set status='pending_download' and reset attempts even if a success row exists.
    Returns the id of the created/updated row or None if it already existed and force=False.
    """
    with conn.cursor() as cur:
        if force:
            # ensure variable exists and get id
            var_id = ensure_variable(conn, ds_id, variable)
            cur.execute(
                """
                INSERT INTO nc_jobs (dataset_id, variable_id, start_time, end_time, status)
                VALUES (%s,%s,%s,%s,'pending_download')
                ON CONFLICT (dataset_id, variable_id, start_time, end_time)
                DO UPDATE SET status = 'pending_download', attempts = 0
                RETURNING id
                """,
                (ds_id, var_id, start_time, end_time),
            )
            res = cur.fetchone()
            if res:
                conn.commit()
                return res[0]
            conn.commit()
            return None
        else:
            var_id = ensure_variable(conn, ds_id, variable)
            cur.execute(
                "INSERT INTO nc_jobs (dataset_id, variable_id, start_time, end_time) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING id",
                (ds_id, var_id, start_time, end_time),
            )
            res = cur.fetchone()
            if res:
                conn.commit()
                return res[0]
            conn.commit()
            return None


def create_rows_for_date(conn, ds_id, variables, date_str, force=False):
    """Create pending nc_files rows for a given YYYY-MM-DD (UTC) for each variable in variables.
    start = YYYY-MM-DDT00:30Z, end = YYYY-MM-DDT23:30Z
    """
    try:
        day = __import__('datetime').datetime.strptime(date_str, '%Y-%m-%d').date()
    except Exception:
        raise ValueError('date must be YYYY-MM-DD')
    start_dt = __import__('datetime').datetime(day.year, day.month, day.day, 0, 30, tzinfo=__import__('datetime').timezone.utc)
    end_dt = __import__('datetime').datetime(day.year, day.month, day.day, 23, 30, tzinfo=__import__('datetime').timezone.utc)

    logger.info('Creating rows for date %s -> %s for variables: %s (force=%s)', start_dt, end_dt, variables, force)
    created_any = False
    for variable in variables:
        ensure_variable(conn, ds_id, variable)
        rid = ensure_pending_nc_file(conn, ds_id, variable, start_dt, end_dt, force=force, meta={'created_by': 'dl2_date'})
        if rid:
            logger.info('Inserted pending nc_jobs for %s %s->%s (id=%s)', variable, start_dt, end_dt, rid)
            created_any = True
        else:
            logger.info('pending nc_jobs already exists for %s %s->%s', variable, start_dt, end_dt)
    return created_any


def create_compute_rows_for_group(conn, date_str):
    compute_vars = get_compute_variables(conn)
    try:
        day = datetime.strptime(date_str, '%Y-%m-%d').date()
    except Exception:
        raise ValueError('date must be YYYY-MM-DD')
    start_time = datetime(day.year, day.month, day.day, 0, 30, tzinfo=timezone.utc)
    end_time = datetime(day.year, day.month, day.day, 23, 30, tzinfo=timezone.utc)
    
    # For each compute variable, ensure a pending_compute row exists for the dataset/start_time/end_time
    for icv, cv in compute_vars:
        with conn.cursor() as cur2:
            cur2.execute(
                "INSERT INTO nc_jobs (dataset_id, variable_id, start_time, end_time, status) VALUES (%s, %s , %s, %s, 'pending_compute') ON CONFLICT DO NOTHING",
                (None, icv, start_time, end_time),
            )
            conn.commit()