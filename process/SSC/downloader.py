"""ERDDAP download step for the SalishSeaCast pipeline.

Downloads one NetCDF file per variable per day from the SalishSeaCast ERDDAP
griddap endpoint and updates SalishSeaCast_status accordingly.

File naming: {NC_BASE_DIR}/{variable}/{variable}_{YYYYMMDD}.nc
Time range:  T00:30Z – T23:30Z (SalishSeaCast half-hour offset convention).

After a successful download, the variable's status stays at success_download.
Call db.check_image_ready() (or the `check_image` CLI command) once all 8
variables for the date have succeeded to advance them together to
pending_image.
"""
from __future__ import annotations

import logging
import os
import re
import shutil
import tempfile
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import requests

from .config import (
    DOWNLOAD_VARIABLES, NC_BASE_DIR, erddap_base_url,
    STATUS_DOWNLOADING, STATUS_FAILED_DOWNLOAD, STATUS_SUCCESS_DOWNLOAD,
)
from .db import (
    get_pending_downloads, get_row, init_date,
    mark_failed, mark_running, mark_success,
)

logger = logging.getLogger('SalishSeaCast.downloader')


# ---------------------------------------------------------------------------
# ERDDAP helpers
# ---------------------------------------------------------------------------

def _fetch_das(base_url: str, timeout: int = 30) -> str:
    url = base_url.rstrip('/')
    url = url if url.endswith('.das') else f'{url}.das'
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text


def _parse_time_coverage_end(das_text: str) -> Optional[datetime]:
    m = re.search(r'time_coverage_end\s+"([^"]+)"', das_text)
    if m:
        try:
            return datetime.fromisoformat(m.group(1).replace('Z', '+00:00')).astimezone(timezone.utc)
        except Exception:
            pass
    m2 = re.search(r'time\s*\{[^}]*actual_range\s*([0-9.eE+-]+)\s*,\s*([0-9.eE+-]+)\s*;', das_text, flags=re.S)
    if m2:
        try:
            return datetime.fromtimestamp(float(m2.group(2)), tz=timezone.utc)
        except Exception:
            pass
    return None


def _build_griddap_url(base_url: str, variable: str, start_iso: str, end_iso: str,
                       das_text: Optional[str] = None) -> str:
    base = base_url.rstrip('/')
    for suffix in ('.das', '.nc'):
        if base.endswith(suffix):
            base = base[:-len(suffix)]
    if not base.endswith('.nc?'):
        base = f'{base}.nc?'

    time_slice = f'{variable}[({start_iso}):1:({end_iso})]'
    range_suffix = ''
    if das_text:
        def _find_range(name: str):
            m = re.search(
                rf'{name}\s*\{{[^}}]*actual_range\s*([0-9.eE+-]+)\s*,\s*([0-9.eE+-]+)\s*;',
                das_text, flags=re.S,
            )
            return m.groups() if m else None

        depth_r = _find_range('depth') or _find_range('deptht')
        gy_r    = _find_range('gridY') or _find_range('gridy')
        gx_r    = _find_range('gridX') or _find_range('gridx')

        parts = []
        if depth_r:
            parts.append(f'[({depth_r[0]}):1:({depth_r[1]})]')
        if gy_r:
            parts.append(f'[({gy_r[0]}):1:({gy_r[1]})]')
        if gx_r:
            parts.append(f'[({gx_r[0]}):1:({gx_r[1]})]')
        range_suffix = ''.join(parts)

    return base + time_slice + range_suffix


# ---------------------------------------------------------------------------
# ERDDAP check — discover new dates
# ---------------------------------------------------------------------------

def check_erddap(client, init_days: int = 3, date_override: Optional[str] = None) -> list[date]:
    """Query ERDDAP for each download variable and create status rows for new dates.

    Returns the list of new dates initialised.
    """
    new_dates: set[date] = set()

    for variable in DOWNLOAD_VARIABLES:
        base_url = erddap_base_url(variable)
        try:
            das_text = _fetch_das(base_url)
            remote_max = _parse_time_coverage_end(das_text)
        except Exception:
            logger.exception('Failed to fetch DAS for %s (%s)', variable, base_url)
            continue

        if remote_max is None:
            logger.warning('Could not determine remote time coverage end for %s', variable)
            continue

        if date_override:
            dates_to_check = [datetime.strptime(date_override, '%Y-%m-%d').date()]
        else:
            result = client.query(
                """
                SELECT max(date) FROM SalishSeaCast_status FINAL
                WHERE variable = %(v)s AND status = %(s)s
                """,
                parameters={'v': variable, 's': STATUS_SUCCESS_DOWNLOAD},
            )
            last_success = result.result_rows[0][0] if result.result_rows and result.result_rows[0][0] else None

            if last_success:
                start_date = last_success + timedelta(days=1)
            else:
                start_date = (remote_max - timedelta(days=init_days)).date()

            end_date = remote_max.date()
            dates_to_check = []
            cur = start_date
            while cur <= end_date:
                dates_to_check.append(cur)
                cur += timedelta(days=1)

        for d in dates_to_check:
            if get_row(client, d, variable) is None:
                new_dates.add(d)

    for d in sorted(new_dates):
        init_date(client, d)
        logger.info('Queued new date: %s', d)

    return sorted(new_dates)


# ---------------------------------------------------------------------------
# Single-variable download
# ---------------------------------------------------------------------------

def download_variable(client, date_val: date, variable: str) -> bool:
    """Download one variable for one date from ERDDAP. Returns True on success."""
    base_url = erddap_base_url(variable)

    current  = get_row(client, date_val, variable) or {}
    attempts = current.get('attempts', 0)
    mark_running(client, date_val, variable, STATUS_DOWNLOADING, attempts)

    day_start = datetime(date_val.year, date_val.month, date_val.day, 0, 30, tzinfo=timezone.utc)
    day_end   = datetime(date_val.year, date_val.month, date_val.day, 23, 30, tzinfo=timezone.utc)
    start_iso = day_start.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_iso   = day_end.strftime('%Y-%m-%dT%H:%M:%SZ')

    das_text = None
    try:
        das_text = _fetch_das(base_url)
    except Exception:
        logger.warning('Could not fetch DAS for %s — proceeding without dimension ranges', variable)

    url = _build_griddap_url(base_url, variable, start_iso, end_iso, das_text)
    logger.info('Downloading %s %s', variable, date_val)

    out_dir = os.path.join(NC_BASE_DIR, variable)
    os.makedirs(out_dir, exist_ok=True)
    final_path = os.path.join(out_dir, f'{variable}_{date_val.strftime("%Y%m%d")}.nc')

    tmpfd, tmpfn = tempfile.mkstemp(suffix='.nc.part')
    os.close(tmpfd)
    try:
        with requests.get(url, stream=True, timeout=600) as r:
            r.raise_for_status()
            with open(tmpfn, 'wb') as fh:
                for chunk in r.iter_content(8192):
                    if chunk:
                        fh.write(chunk)
        shutil.move(tmpfn, final_path)
        mark_success(client, date_val, variable, STATUS_SUCCESS_DOWNLOAD,
                     nc_path=final_path, attempts=attempts + 1)
        logger.info('Downloaded %s %s → %s', variable, date_val, final_path)
        return True
    except Exception as exc:
        logger.exception('Download failed for %s %s', variable, date_val)
        mark_failed(client, date_val, variable, STATUS_FAILED_DOWNLOAD,
                    str(exc), attempts=attempts + 1)
        try:
            os.remove(tmpfn)
        except OSError:
            pass
        return False


# ---------------------------------------------------------------------------
# Batch processor
# ---------------------------------------------------------------------------

def process_pending_downloads(client, limit: int = 20) -> None:
    pending = get_pending_downloads(client, limit=limit)
    if not pending:
        logger.info('No pending downloads')
        return
    for date_val, variable in pending:
        download_variable(client, date_val, variable)
