"""Database-backed helpers for the `/variables` API endpoint.

Returns SalishSeaCast metadata from shared/variable_config.yml + this
server's own ClickHouse. No Postgres involved — LiveOcean is out of scope
entirely (not returned by this endpoint at all).
"""
from __future__ import annotations

import datetime
from typing import Dict, List

from shared.variable_config import load_variable_config

from modules.clickhouse_helpers import get_ch_client
from modules.sync_hourly import ensure_schema as _ensure_sync_schema


def _get_ssc_variables() -> List[Dict]:
    """SalishSeaCast variable metadata + available datetimes.

    Metadata (unit/colormap/bounds/depths/precision) comes from
    shared/variable_config.yml — see that file for why it's safe to hardcode.

    Available datetimes come from this server's own SalishSeaCast_sync_log,
    not from PROCESS's SalishSeaCast_status: status='success' here means
    this server has independently verified (by row count, see
    modules/sync_hourly.py) that the date's WebP tiles and ClickHouse rows
    have actually landed *here* — which is what the frontend needs, and is a
    stronger signal than PROCESS's own pipeline-progress bookkeeping on a
    different machine.

    All 8 SalishSeaCast variables move through the pipeline as a bundle (see
    process/SSC/cli.py's check_image/promote semantics), so they all share
    the exact same available-datetimes list — computed once here, not per
    variable.
    """
    config = load_variable_config()
    bounds = config['bounds']
    depths = config['depths']

    client = get_ch_client()
    try:
        _ensure_sync_schema(client)
        result = client.query(
            "SELECT date FROM SalishSeaCast_sync_log FINAL WHERE status = 'success' ORDER BY date"
        )
        success_dates = [row[0] for row in result.result_rows]
    finally:
        client.close()

    dts = sorted(
        datetime.datetime.combine(d, datetime.time(0, 30)) + datetime.timedelta(hours=h)
        for d in success_dates
        for h in range(24)
    )

    return [
        {
            "var": var,
            "name": meta.get('name', var),
            "dts": dts,
            "colormapMin": meta['colormapMin'],
            "colormapMax": meta['colormapMax'],
            "depths": depths,
            "precision": meta['precision'],
            "colormap": meta['colormap'],
            "bounds": bounds,
            "source": config['source'],
            "unit": meta['unit'],
            "alt_units": meta.get('alt_units', []),
        }
        for var, meta in config['variables'].items()
    ]


def get_variables() -> List[Dict]:
    return _get_ssc_variables()
