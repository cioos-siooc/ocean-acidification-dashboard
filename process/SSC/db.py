"""ClickHouse client and schema management for the SalishSeaCast pipeline.

Tables managed here:
  SalishSeaCast_hourly   — raw hourly 4D ocean data
  SalishSeaCast_status   — one row per (date, variable); status column carries
                           the full combined pipeline state

Status table uses ReplacingMergeTree(updated_at): each state transition is a
new insert with the same (date, variable) key and a fresher updated_at.
FINAL queries always return the current state per row.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import clickhouse_connect

from .config import (
    ALL_VARIABLES, COMPUTE_VARIABLES, DOWNLOAD_VARIABLES,
    CH_HOST, CH_PASSWORD, CH_PORT, CH_REMOTE_URL, CH_USE_REMOTE, CH_USER,
    MAX_RETRY_ATTEMPTS, PAST_DOWNLOAD_STATUSES,
    STATUS_FAILED_COMPUTE, STATUS_FAILED_DOWNLOAD, STATUS_FAILED_IMAGE,
    STATUS_FAILED_INGEST, STATUS_FAILED_SYNC,
    STATUS_IMAGING, STATUS_INGESTING,
    STATUS_PENDING_COMPUTE, STATUS_PENDING_DOWNLOAD,
    STATUS_PENDING_IMAGE, STATUS_PENDING_INGEST, STATUS_PENDING_SYNC,
    STATUS_SUCCESS_COMPUTE, STATUS_SUCCESS_DOWNLOAD, STATUS_SUCCESS_IMAGE,
    STATUS_SUCCESS_INGEST,
)

logger = logging.getLogger('SalishSeaCast.db')


# ---------------------------------------------------------------------------
# Client factory
# ---------------------------------------------------------------------------

def get_client() -> clickhouse_connect.driver.Client:
    if CH_USE_REMOTE:
        if not CH_REMOTE_URL:
            raise RuntimeError('CH_USE_REMOTE is set but CH_REMOTE_URL is empty')
        url = CH_REMOTE_URL if '://' in CH_REMOTE_URL else f'https://{CH_REMOTE_URL}'
        parsed = urlparse(url)
        secure = parsed.scheme != 'http'
        port = parsed.port or (8443 if secure else 8123)
        user = parsed.username or CH_USER
        password = parsed.password or CH_PASSWORD
        return clickhouse_connect.get_client(
            host=parsed.hostname, port=port,
            username=user, password=password, secure=secure,
        )
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD,
    )


def _limit_sql(limit: int) -> str:
    """Render a LIMIT clause. A limit of 0 means unlimited (clause omitted)."""
    return f'LIMIT {int(limit)}' if limit else ''


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_VARIABLE_COLS = '\n    '.join(
    f'{var} Float32 CODEC(Gorilla, ZSTD(4)),' for var in ALL_VARIABLES
)

_CREATE_HOURLY = f"""
CREATE TABLE IF NOT EXISTS SalishSeaCast_hourly (
    time     DateTime  CODEC(DoubleDelta, ZSTD(4)),
    depth    Float32   CODEC(Gorilla, ZSTD(4)),
    gridX    UInt16    CODEC(DoubleDelta, ZSTD(4)),
    gridY    UInt16    CODEC(DoubleDelta, ZSTD(4)),
    {_VARIABLE_COLS}
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (gridX, gridY, depth, time)
"""

_CREATE_STATUS = """
CREATE TABLE IF NOT EXISTS SalishSeaCast_status (
    date          Date     CODEC(DoubleDelta, ZSTD(4)),
    variable      LowCardinality(String),
    status        LowCardinality(String),
    nc_path       String   DEFAULT '',
    attempts      UInt8    DEFAULT 0,
    error_message String   DEFAULT '',
    updated_at    DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, variable)
"""

# Daily mean/min/max per variable, derived from SalishSeaCast_hourly during
# ingest (see ingest.py) — same column layout as the historical bulk-load
# table this feeds, so both the PROCESS
# machine's local table and the API machine's table stay schema-compatible.
_DAILY_VARIABLE_COLS = '\n    '.join(
    f'{var}_{stat} Float32 CODEC(Gorilla, ZSTD(4)),'
    for var in ALL_VARIABLES for stat in ('mean', 'min', 'max')
)

_CREATE_DAILY = f"""
CREATE TABLE IF NOT EXISTS SalishSeaCast_daily (
    time     Date      CODEC(DoubleDelta, ZSTD(4)),
    depth    Float32   CODEC(Gorilla, ZSTD(4)),
    gridX    UInt16    CODEC(DoubleDelta, ZSTD(4)),
    gridY    UInt16    CODEC(DoubleDelta, ZSTD(4)),
    {_DAILY_VARIABLE_COLS}
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (gridX, gridY, time, depth)
"""


def ensure_schema(client) -> None:
    client.command(_CREATE_HOURLY)
    client.command(_CREATE_DAILY)
    client.command(_CREATE_STATUS)
    logger.info('SalishSeaCast ClickHouse schema verified')


# ---------------------------------------------------------------------------
# Low-level status helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def upsert_status(
    client,
    date_val: date,
    variable: str,
    status: str,
    nc_path: str = '',
    attempts: int = 0,
    error_message: str = '',
) -> None:
    client.insert(
        'SalishSeaCast_status',
        [[date_val, variable, status, nc_path, attempts, error_message, _now()]],
        column_names=['date', 'variable', 'status', 'nc_path', 'attempts', 'error_message', 'updated_at'],
    )
    # Force immediate deduplication so the table always shows exactly one row per
    # (date, variable). ReplacingMergeTree otherwise keeps old versions until a
    # background merge runs, which makes raw table inspection confusing.
    partition = date_val.strftime('%Y%m')
    client.command(f"OPTIMIZE TABLE SalishSeaCast_status PARTITION '{partition}' FINAL")


def get_row(client, date_val: date, variable: str) -> Optional[dict]:
    result = client.query(
        """
        SELECT status, nc_path, attempts, error_message
        FROM SalishSeaCast_status FINAL
        WHERE date = %(d)s AND variable = %(v)s
        LIMIT 1
        """,
        parameters={'d': date_val, 'v': variable},
    )
    if not result.result_rows:
        return None
    r = result.result_rows[0]
    return {'status': r[0], 'nc_path': r[1], 'attempts': r[2], 'error_message': r[3]}


# ---------------------------------------------------------------------------
# Convenience mark helpers (each step calls these)
# ---------------------------------------------------------------------------

def mark_running(client, date_val: date, variable: str, running_status: str, attempts: int) -> None:
    upsert_status(client, date_val, variable, running_status, attempts=attempts + 1)


def mark_success(client, date_val: date, variable: str, success_status: str,
                 nc_path: str = '', attempts: int = 0) -> None:
    upsert_status(client, date_val, variable, success_status, nc_path=nc_path, attempts=attempts)


def mark_failed(client, date_val: date, variable: str, failed_status: str,
                error: str, attempts: int = 0) -> None:
    upsert_status(client, date_val, variable, failed_status,
                  error_message=error, attempts=attempts)


# ---------------------------------------------------------------------------
# Gate checks
# ---------------------------------------------------------------------------

def downloads_complete(client, date_val: date) -> bool:
    """True when all 5 download variables have moved past the download stage."""
    past = "', '".join(PAST_DOWNLOAD_STATUSES)
    result = client.query(
        f"""
        SELECT COUNT() FROM SalishSeaCast_status FINAL
        WHERE date = %(d)s
          AND variable IN ('{"', '".join(DOWNLOAD_VARIABLES)}')
          AND status IN ('{past}')
        """,
        parameters={'d': date_val},
    )
    return int(result.result_rows[0][0]) == len(DOWNLOAD_VARIABLES)


def compute_complete(client, date_val: date) -> bool:
    """True when all 3 compute variables have reached success_compute or later."""
    past_compute = (
        STATUS_SUCCESS_IMAGE, STATUS_PENDING_IMAGE, STATUS_IMAGING,
        STATUS_PENDING_INGEST, STATUS_INGESTING,
        'success_compute', 'success_ingest',
    )
    statuses = "', '".join(past_compute)
    result = client.query(
        f"""
        SELECT COUNT() FROM SalishSeaCast_status FINAL
        WHERE date = %(d)s
          AND variable IN ('{"', '".join(COMPUTE_VARIABLES)}')
          AND status IN ('{statuses}')
        """,
        parameters={'d': date_val},
    )
    return int(result.result_rows[0][0]) == len(COMPUTE_VARIABLES)


def images_complete(client, date_val: date) -> bool:
    """True when all 8 variables are at success_image."""
    result = client.query(
        """
        SELECT COUNT() FROM SalishSeaCast_status FINAL
        WHERE date = %(d)s AND status = %(s)s
        """,
        parameters={'d': date_val, 's': STATUS_SUCCESS_IMAGE},
    )
    return int(result.result_rows[0][0]) == len(ALL_VARIABLES)


# ---------------------------------------------------------------------------
# Promotions (advance variables to the next pending stage)
# ---------------------------------------------------------------------------

def promote_to_pending_image(client, date_val: date, variable: str, nc_path: str = '') -> None:
    """Move a single variable from success_download / success_compute to pending_image."""
    upsert_status(client, date_val, variable, STATUS_PENDING_IMAGE, nc_path=nc_path)


def _promote_all_variables(client, date_val: date, new_status: str) -> None:
    """Move all 8 variables for date_val to new_status in one bulk insert."""
    now = _now()
    rows = [[date_val, var, new_status, '', 0, '', now] for var in ALL_VARIABLES]
    client.insert(
        'SalishSeaCast_status', rows,
        column_names=['date', 'variable', 'status', 'nc_path', 'attempts', 'error_message', 'updated_at'],
    )
    # Force immediate deduplication, same as upsert_status — otherwise ReplacingMergeTree
    # keeps both the old and new row until a background merge.
    partition = date_val.strftime('%Y%m')
    client.command(f"OPTIMIZE TABLE SalishSeaCast_status PARTITION '{partition}' FINAL")
    logger.info('Promoted all variables for %s to %s', date_val, new_status)


def check_image_ready_for_date(client, date_val: date) -> bool:
    """Single-date variant of check_image_ready: promote date_val to pending_image
    if all 8 variables have reached their stage's success state. Returns True if
    promoted, False if not yet ready (a no-op, safe to call repeatedly)."""
    result = client.query(
        f"""
        SELECT COUNT(DISTINCT variable)
        FROM SalishSeaCast_status FINAL
        WHERE date = %(d)s
          AND (
                (variable IN ('{"', '".join(DOWNLOAD_VARIABLES)}') AND status = '{STATUS_SUCCESS_DOWNLOAD}')
             OR (variable IN ('{"', '".join(COMPUTE_VARIABLES)}') AND status = '{STATUS_SUCCESS_COMPUTE}')
          )
        """,
        parameters={'d': date_val},
    )
    if int(result.result_rows[0][0]) != len(ALL_VARIABLES):
        return False
    paths_result = client.query(
        "SELECT variable, nc_path FROM SalishSeaCast_status FINAL WHERE date = %(d)s",
        parameters={'d': date_val},
    )
    paths = {r[0]: r[1] for r in paths_result.result_rows}
    for var in ALL_VARIABLES:
        promote_to_pending_image(client, date_val, var, nc_path=paths.get(var, ''))
    logger.info('check_image: %s ready — promoted all %d variables to %s',
                date_val, len(ALL_VARIABLES), STATUS_PENDING_IMAGE)
    return True


def check_image_ready(client, limit: int = 20) -> int:
    """Find dates where ALL 8 variables have reached their stage's success state
    (5 download vars at success_download, 3 compute vars at success_compute) and
    promote all 8 to pending_image together.

    This is the 'check_image' stage: imaging never starts on a partial date, so a
    date is only ever promoted once, atomically, regardless of whether downloads
    or compute finished last.

    Returns the number of status rows promoted (a multiple of 8).
    """
    result = client.query(
        f"""
        SELECT date
        FROM SalishSeaCast_status FINAL
        WHERE (variable IN ('{"', '".join(DOWNLOAD_VARIABLES)}') AND status = '{STATUS_SUCCESS_DOWNLOAD}')
           OR (variable IN ('{"', '".join(COMPUTE_VARIABLES)}') AND status = '{STATUS_SUCCESS_COMPUTE}')
        GROUP BY date
        HAVING COUNT(DISTINCT variable) = %(n)s
        ORDER BY date
        {_limit_sql(limit)}
        """,
        parameters={'n': len(ALL_VARIABLES)},
    )
    promoted = 0
    for (date_val,) in result.result_rows:
        if check_image_ready_for_date(client, date_val):
            promoted += len(ALL_VARIABLES)
    return promoted


def _promote_dates_at_status(client, from_status: str, to_status: str, limit: int) -> int:
    """Find dates where all 8 variables are at from_status and promote them to
    to_status together. Returns the number of status rows promoted."""
    result = client.query(
        f"""
        SELECT date
        FROM SalishSeaCast_status FINAL
        WHERE status = %(s)s
        GROUP BY date
        HAVING COUNT(DISTINCT variable) = %(n)s
        ORDER BY date
        {_limit_sql(limit)}
        """,
        parameters={'s': from_status, 'n': len(ALL_VARIABLES)},
    )
    promoted = 0
    for (date_val,) in result.result_rows:
        _promote_all_variables(client, date_val, to_status)
        promoted += len(ALL_VARIABLES)
    return promoted


def promote_ready_dates(client, limit: int = 20) -> int:
    """Scan for dates ready to advance and promote them:
      - fully imaged (all 8 at success_image)  -> pending_ingest
      - fully ingested (all 8 at success_ingest) -> pending_sync

    Returns the number of status rows promoted (a multiple of 8).
    """
    promoted = _promote_dates_at_status(client, STATUS_SUCCESS_IMAGE, STATUS_PENDING_INGEST, limit)
    promoted += _promote_dates_at_status(client, STATUS_SUCCESS_INGEST, STATUS_PENDING_SYNC, limit)
    return promoted


def promote_date_if_ready(client, date_val: date) -> Optional[str]:
    """Single-date variant of promote_ready_dates: promote date_val if fully
    imaged (-> pending_ingest) or fully ingested (-> pending_sync). Returns the
    new status if promoted, else None."""
    for from_status, to_status in (
        (STATUS_SUCCESS_IMAGE, STATUS_PENDING_INGEST),
        (STATUS_SUCCESS_INGEST, STATUS_PENDING_SYNC),
    ):
        result = client.query(
            "SELECT COUNT(DISTINCT variable) FROM SalishSeaCast_status FINAL WHERE date = %(d)s AND status = %(s)s",
            parameters={'d': date_val, 's': from_status},
        )
        if int(result.result_rows[0][0]) == len(ALL_VARIABLES):
            _promote_all_variables(client, date_val, to_status)
            return to_status
    return None


# ---------------------------------------------------------------------------
# Pending-work queries (used by batch processors)
# ---------------------------------------------------------------------------

def get_pending_downloads(client, limit: int = 20) -> list[tuple[date, str]]:
    """pending_download rows, plus failed_download rows under the retry cap
    (transient failures self-heal in bulk sweeps without --force)."""
    result = client.query(
        f"""
        SELECT date, variable FROM SalishSeaCast_status FINAL
        WHERE status = %(sp)s
           OR (status = %(sf)s AND attempts < %(cap)s)
        ORDER BY date, variable
        {_limit_sql(limit)}
        """,
        parameters={'sp': STATUS_PENDING_DOWNLOAD, 'sf': STATUS_FAILED_DOWNLOAD, 'cap': MAX_RETRY_ATTEMPTS},
    )
    return [(r[0], r[1]) for r in result.result_rows]


def get_dates_pending_compute(client, limit: int = 10) -> list[date]:
    """Dates where compute vars are pending_compute (or failed_compute under the
    retry cap) AND all 5 downloads are done."""
    past = "', '".join(PAST_DOWNLOAD_STATUSES)
    result = client.query(
        f"""
        SELECT DISTINCT date
        FROM SalishSeaCast_status FINAL
        WHERE variable IN ('{"', '".join(COMPUTE_VARIABLES)}')
          AND (status = %(sp)s OR (status = %(sf)s AND attempts < %(cap)s))
          AND date IN (
              SELECT date FROM SalishSeaCast_status FINAL
              WHERE variable IN ('{"', '".join(DOWNLOAD_VARIABLES)}')
                AND status IN ('{past}')
              GROUP BY date
              HAVING COUNT(DISTINCT variable) = %(ndl)s
          )
        ORDER BY date
        {_limit_sql(limit)}
        """,
        parameters={
            'sp': STATUS_PENDING_COMPUTE,
            'sf': STATUS_FAILED_COMPUTE,
            'cap': MAX_RETRY_ATTEMPTS,
            'ndl': len(DOWNLOAD_VARIABLES),
        },
    )
    return [r[0] for r in result.result_rows]


def get_pending_images(client, limit: int = 40) -> list[tuple[date, str]]:
    result = client.query(
        f"""
        SELECT date, variable FROM SalishSeaCast_status FINAL
        WHERE status = %(sp)s
           OR (status = %(sf)s AND attempts < %(cap)s)
        ORDER BY date, variable
        {_limit_sql(limit)}
        """,
        parameters={'sp': STATUS_PENDING_IMAGE, 'sf': STATUS_FAILED_IMAGE, 'cap': MAX_RETRY_ATTEMPTS},
    )
    return [(r[0], r[1]) for r in result.result_rows]


def get_dates_pending_ingest(client, limit: int = 5) -> list[date]:
    result = client.query(
        f"""
        SELECT DISTINCT date FROM SalishSeaCast_status FINAL
        WHERE status = %(sp)s
           OR (status = %(sf)s AND attempts < %(cap)s)
        ORDER BY date
        {_limit_sql(limit)}
        """,
        parameters={'sp': STATUS_PENDING_INGEST, 'sf': STATUS_FAILED_INGEST, 'cap': MAX_RETRY_ATTEMPTS},
    )
    return [r[0] for r in result.result_rows]


def get_dates_pending_sync(client, limit: int = 5) -> list[date]:
    result = client.query(
        f"""
        SELECT DISTINCT date FROM SalishSeaCast_status FINAL
        WHERE status = %(sp)s
           OR (status = %(sf)s AND attempts < %(cap)s)
        ORDER BY date
        {_limit_sql(limit)}
        """,
        parameters={'sp': STATUS_PENDING_SYNC, 'sf': STATUS_FAILED_SYNC, 'cap': MAX_RETRY_ATTEMPTS},
    )
    return [r[0] for r in result.result_rows]


# ---------------------------------------------------------------------------
# Date initialisation
# ---------------------------------------------------------------------------

def init_date(client, date_val: date, download_variables: Optional[list[str]] = None) -> None:
    """Seed status rows for a new date.

    Download variables start at pending_download. Only `download_variables`
    (default: all of them) are seeded — callers pass a subset when a date is
    only confirmed available upstream for some variables (e.g. one of the two
    ERDDAP datasets hasn't published that day yet); the others get seeded on
    a later check() once their own dataset catches up.
    Compute variables start at pending_compute.
    Idempotent: existing rows are not overwritten.
    """
    if download_variables is None:
        download_variables = DOWNLOAD_VARIABLES

    existing_result = client.query(
        "SELECT variable FROM SalishSeaCast_status FINAL WHERE date = %(d)s",
        parameters={'d': date_val},
    )
    already = {r[0] for r in existing_result.result_rows}

    now = _now()
    rows = []
    for var in download_variables:
        if var not in already:
            rows.append([date_val, var, STATUS_PENDING_DOWNLOAD, '', 0, '', now])
    for var in COMPUTE_VARIABLES:
        if var not in already:
            rows.append([date_val, var, STATUS_PENDING_COMPUTE, '', 0, '', now])

    if rows:
        client.insert(
            'SalishSeaCast_status', rows,
            column_names=['date', 'variable', 'status', 'nc_path', 'attempts', 'error_message', 'updated_at'],
        )
        logger.info('Initialised %d status rows for %s', len(rows), date_val)
