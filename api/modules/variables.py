"""Database-backed helpers for the `/variables` API endpoint.

One entry per (source, variable): metadata from shared/variable_config.yml
(display metadata from `variables:`, grid/depths/time step/capabilities from
the source's `sources:` block) plus the datetimes this server can serve,
from its own ClickHouse.
"""
from __future__ import annotations

import datetime
from typing import Dict, List, Optional

from shared.variable_config import load_variable_config

from modules.clickhouse_helpers import get_ch_client
from modules.sync_hourly import ensure_schema as _ensure_sync_schema


def _ssc_dts(client) -> List[datetime.datetime]:
    """SalishSeaCast's available datetimes come from this server's own
    SalishSeaCast_sync_log, not from PROCESS's SalishSeaCast_status:
    status='success' here means this server has independently verified (by
    row count, see modules/sync_hourly.py) that the date's WebP tiles and
    ClickHouse rows have actually landed *here*.

    All 8 SalishSeaCast variables move through the pipeline as a bundle (see
    process/SSC/cli.py's check_image/promote semantics), so they share one
    datetimes list — 24 hourly outputs at :30 per synced day.
    """
    _ensure_sync_schema(client)
    result = client.query(
        "SELECT date FROM SalishSeaCast_sync_log FINAL WHERE status = 'success' ORDER BY date"
    )
    return sorted(
        datetime.datetime.combine(row[0], datetime.time(0, 30)) + datetime.timedelta(hours=h)
        for row in result.result_rows
        for h in range(24)
    )


def _lo_dts(client, step_hours: int) -> tuple[List[datetime.datetime], Optional[datetime.date]]:
    """LiveOcean's available datetimes and newest run, from LiveOcean_days
    (one row per valid day: owning run and how many 4-hourly steps it has —
    see modules/sync_liveocean.py). Read-only: on a server that has never
    received LiveOcean data the table may not exist, and this must not
    create it."""
    if not client.query("EXISTS TABLE LiveOcean_days").result_rows[0][0]:
        return [], None
    rows = client.query(
        "SELECT day, max(run_date), argMax(steps, run_date) FROM LiveOcean_days GROUP BY day ORDER BY day"
    ).result_rows
    dts = [
        datetime.datetime.combine(day, datetime.time(0)) + datetime.timedelta(hours=step_hours * k)
        for day, _, steps in rows
        for k in range(int(steps))
    ]
    latest_run = max((run for _, run, _ in rows), default=None)
    return dts, latest_run


def _entries(source: str, src: dict, variables_meta: dict, dts: list,
             run_date: Optional[datetime.date]) -> List[Dict]:
    # A run's own day is its nowcast-ish day 0; anything after is forecast.
    forecast_from = (
        datetime.datetime.combine(run_date + datetime.timedelta(days=1), datetime.time(0))
        if src.get('forecast') and run_date else None
    )
    return [
        {
            "var": var,
            "name": variables_meta[var].get('name', var),
            "dts": dts,
            "colormapMin": variables_meta[var]['colormapMin'],
            "colormapMax": variables_meta[var]['colormapMax'],
            "depths": src['depths'],
            "precision": variables_meta[var]['precision'],
            "colormap": variables_meta[var]['colormap'],
            "bounds": src['bounds'],
            "source": source,
            "sourceLabel": src.get('label', source),
            "unit": variables_meta[var]['unit'],
            "alt_units": variables_meta[var].get('alt_units', []),
            "stepHours": src['step_hours'],
            "forecast": bool(src.get('forecast')),
            "forecastFrom": forecast_from,
            "runDate": run_date,
            "capabilities": src.get('capabilities', {}),
        }
        for var in src['variables']
    ]


def get_variables() -> List[Dict]:
    config = load_variable_config()
    out: List[Dict] = []
    client = get_ch_client()
    try:
        for source, src in config['sources'].items():
            if source == 'SalishSeaCast':
                dts, run_date = _ssc_dts(client), None
            elif source == 'LiveOcean':
                dts, run_date = _lo_dts(client, src['step_hours'])
                if not dts:
                    continue  # nothing synced here yet: don't offer the source
            else:
                continue
            out.extend(_entries(source, src, config['variables'], dts, run_date))
    finally:
        client.close()
    return out
