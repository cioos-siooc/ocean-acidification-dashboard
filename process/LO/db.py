"""ClickHouse client, schema and status bookkeeping for the LiveOcean pipeline.

LiveOcean_status holds one row per run (run_date): the run's layers.nc is the
unit of work, not a (date, variable) pair as in SalishSeaCast_status. Like
SSC it's a ReplacingMergeTree(updated_at) read with FINAL.

`steps` is how many 4-hourly steps of the run to keep: 6 (day 0) when a newer
run follows the next day — its day 0 replaces this run's forecast anyway —
and more when it doesn't (the newest run: all 19; a run followed by a gap in
the archive: enough to cover the gap). See downloader.plan_steps().
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from SSC.db import get_client  # noqa: F401  (same CH_* env, same factory)
from shared import liveocean_schema

from .config import MAX_RETRY_ATTEMPTS, STAGE_INPUTS, STATUS_TABLE

logger = logging.getLogger('LiveOcean.db')

_CREATE_STATUS = f"""
CREATE TABLE IF NOT EXISTS {STATUS_TABLE} (
    run_date      Date     CODEC(DoubleDelta, ZSTD(4)),
    status        LowCardinality(String),
    steps         UInt8    DEFAULT 6,
    nc_path       String   DEFAULT '',
    attempts      UInt8    DEFAULT 0,
    error_message String   DEFAULT '',
    updated_at    DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY run_date
"""

_COLS = ['run_date', 'status', 'steps', 'nc_path', 'attempts', 'error_message', 'updated_at']


def ensure_schema(client) -> None:
    liveocean_schema.ensure_schema(client)
    client.command(_CREATE_STATUS)
    logger.info('LiveOcean ClickHouse schema verified')


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def get_run(client, run_date: date) -> Optional[dict]:
    rows = client.query(
        f'SELECT status, steps, nc_path, attempts, error_message '
        f'FROM {STATUS_TABLE} FINAL WHERE run_date = %(d)s',
        parameters={'d': run_date},
    ).result_rows
    if not rows:
        return None
    status, steps, nc_path, attempts, err = rows[0]
    return {'run_date': run_date, 'status': status, 'steps': steps, 'nc_path': nc_path,
            'attempts': attempts, 'error_message': err}


def set_status(client, run_date: date, status: str, *, steps: Optional[int] = None,
               nc_path: Optional[str] = None, attempts: Optional[int] = None,
               error_message: str = '') -> None:
    """Write a new state for run_date, carrying over the fields not given."""
    cur = get_run(client, run_date) or {'steps': 6, 'nc_path': '', 'attempts': 0}
    client.insert(STATUS_TABLE, [[
        run_date, status,
        cur['steps'] if steps is None else steps,
        cur['nc_path'] if nc_path is None else nc_path,
        cur['attempts'] if attempts is None else attempts,
        error_message, _now(),
    ]], column_names=_COLS)
    # Same reason as SSC/db.py's upsert_status: keep one visible row per key.
    client.command(f'OPTIMIZE TABLE {STATUS_TABLE} FINAL')


def known_runs(client) -> dict[date, dict]:
    rows = client.query(
        f'SELECT run_date, status, steps FROM {STATUS_TABLE} FINAL ORDER BY run_date'
    ).result_rows
    return {r[0]: {'status': r[1], 'steps': r[2]} for r in rows}


def seed_runs(client, plan: dict[date, int]) -> None:
    """Insert pending_download rows for new runs, and widen `steps` on runs
    that haven't been downloaded yet when the plan now needs more of them
    (e.g. a run seeded as day-0 whose successor turned out to be missing)."""
    known = known_runs(client)
    now = _now()
    rows = []
    for run_date, steps in sorted(plan.items()):
        cur = known.get(run_date)
        if cur is None:
            rows.append([run_date, 'pending_download', steps, '', 0, '', now])
        elif cur['status'] == 'pending_download' and cur['steps'] != steps:
            rows.append([run_date, 'pending_download', steps, '', 0, '', now])
    if rows:
        client.insert(STATUS_TABLE, rows, column_names=_COLS)
        client.command(f'OPTIMIZE TABLE {STATUS_TABLE} FINAL')
        logger.info('Seeded/updated %d LiveOcean run(s): %s', len(rows),
                    ', '.join(f'{r[0]}({r[2]} steps)' for r in rows))


def superseded_days(client, run_date: date) -> set[date]:
    """Valid days run_date must not write: days a newer run already owns,
    plus every day from the next newer known run onwards (that run's day 0
    and forecast take over, even if it hasn't been ingested yet)."""
    owners = liveocean_schema.day_owners(client)
    skip = {d for d, owner in owners.items() if owner > run_date}
    newer = [d for d in known_runs(client) if d > run_date]
    if newer:
        cutoff = min(newer)
        skip |= {cutoff + timedelta(days=i) for i in range(4)}
    return skip


def pending_runs(client, stage: str, limit: int = 10) -> list[dict]:
    """Rows ready for `stage`: at its input status, or at its failed status
    with attempts under the retry cap. Newest first — the forecast people are
    looking at matters more than backfill."""
    ok, failed = STAGE_INPUTS[stage]
    lim = f'LIMIT {int(limit)}' if limit else ''
    rows = client.query(
        f"""
        SELECT run_date FROM {STATUS_TABLE} FINAL
        WHERE status = %(ok)s OR (status = %(failed)s AND attempts < %(cap)s)
        ORDER BY run_date DESC {lim}
        """,
        parameters={'ok': ok, 'failed': failed, 'cap': MAX_RETRY_ATTEMPTS},
    ).result_rows
    return [get_run(client, r[0]) for r in rows]
