"""ClickHouse schema and day-replacement helpers for LiveOcean.

Shared by process/LO (the PROCESS machine's own tables) and api/modules/
sync_liveocean.py (the API machine's copy), so both sides stay
schema-compatible for the Native-format sync.

Why day partitions: LiveOcean is a forecast. Every new run re-forecasts the
next three days, and the newest run's values must replace the older ones.
With `PARTITION BY toDate(time)` a valid day is swapped atomically with
`REPLACE PARTITION` from a staging table — no mutations, no FINAL on reads.

Latest-wins: each row carries the `run_date` that produced it, and
LiveOcean_days records which run owns each valid day. A run may only replace
a day whose owner is not newer than itself (`is_stale()`), so processing old
runs (backfill, retries) after new ones can never overwrite fresher data. The
ownership table (not the data) is the record, so it still holds after the
PROCESS machine drops its local copy of a synced day.
"""
from __future__ import annotations

import math
from datetime import date
from typing import Iterable, Optional

from shared.variable_config import get_source, load_variable_config

SOURCE = 'LiveOcean'
_SRC = get_source(SOURCE)
GRID_TABLE = _SRC['grid_table']
DATA_TABLE = _SRC['data_table']
DAILY_TABLE = _SRC['daily_table']
DAYS_TABLE = 'LiveOcean_days'
VARIABLES: list[str] = list(_SRC['variables'])


def staging_table(table: str) -> str:
    return f'{table}_staging'


# Values are stored as scaled integers: `<col>_q` = round(value / precision)
# as Int32 under T64+ZSTD (~11 bytes/row for the 4-hourly table vs ~18 for
# rounded Float32), with NaN as QUANT_NULL. Each `<col>_q` has a Float32
# ALIAS under the variable's own name that scales it back on read, so every
# reader queries `temperature`, `ph_total_mean`, … exactly as for
# SalishSeaCast. A column left out of an insert (LO_VARIABLES) defaults to
# QUANT_NULL, so it reads as NaN, not 0. ALIAS columns aren't in `SELECT *`, so the Native sync
# export carries only the integers.
#
# The scale is baked into the table at creation: changing a variable's
# precision in variable_config.yml means rebuilding these tables.
QUANT_NULL = -2**31


def decimals(var: str) -> int:
    """Decimal places of a variable's configured precision (0.01 -> 2)."""
    precision = load_variable_config()['variables'][var]['precision']
    return max(0, -math.floor(math.log10(precision) + 1e-9))


def _base_var(col: str) -> str:
    """'ph_total_mean' -> 'ph_total'; plain variable names pass through."""
    for suffix in ('_mean', '_min', '_max'):
        if col.endswith(suffix) and col[:-len(suffix)] in VARIABLES:
            return col[:-len(suffix)]
    return col


def quantize(col: str, values):
    """Float values of column `col` as its stored Int32 (`<col>_q`)."""
    import numpy as np
    scaled = np.asarray(values, dtype=np.float64) * 10 ** decimals(_base_var(col))
    out = np.full(scaled.shape, QUANT_NULL, dtype=np.int32)
    finite = np.isfinite(scaled)
    out[finite] = np.rint(scaled[finite])
    return out


def _var_cols(cols: Iterable[str]) -> str:
    lines = []
    for c in cols:
        scale = 10 ** decimals(_base_var(c))
        lines.append(f'{c}_q Int32 DEFAULT {QUANT_NULL} CODEC(T64, ZSTD(6)),')
        lines.append(f'{c} Float32 ALIAS if({c}_q = {QUANT_NULL}, nan, toFloat32({c}_q / {scale})),')
    return '\n    '.join(lines)


def _data_ddl(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    time     DateTime  CODEC(DoubleDelta, ZSTD(4)),
    depth    Float32   CODEC(Gorilla, ZSTD(4)),
    gridX    UInt16    CODEC(DoubleDelta, ZSTD(4)),
    gridY    UInt16    CODEC(DoubleDelta, ZSTD(4)),
    {_var_cols(VARIABLES)}
    run_date Date      CODEC(DoubleDelta, ZSTD(4))
) ENGINE = MergeTree()
PARTITION BY toDate(time)
ORDER BY (gridX, gridY, depth, time)
"""


def _daily_ddl(table: str) -> str:
    stats = [f'{v}_{s}' for v in VARIABLES for s in ('mean', 'min', 'max')]
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    time     Date      CODEC(DoubleDelta, ZSTD(4)),
    depth    Float32   CODEC(Gorilla, ZSTD(4)),
    gridX    UInt16    CODEC(DoubleDelta, ZSTD(4)),
    gridY    UInt16    CODEC(DoubleDelta, ZSTD(4)),
    {_var_cols(stats)}
    run_date Date      CODEC(DoubleDelta, ZSTD(4))
) ENGINE = MergeTree()
PARTITION BY time
ORDER BY (gridX, gridY, time, depth)
"""


# gridX = xi (column), gridY = eta (row) of LiveOcean's rho grid; wet cells
# only. `h` is the bathymetry (m), used for the bottom layer's pressure.
_GRID_DDL = f"""
CREATE TABLE IF NOT EXISTS {GRID_TABLE} (
    gridX     UInt16  CODEC(DoubleDelta, ZSTD(1)),
    gridY     UInt16  CODEC(DoubleDelta, ZSTD(1)),
    longitude Float64 CODEC(Gorilla, ZSTD(3)),
    latitude  Float64 CODEC(Gorilla, ZSTD(3)),
    h         Float32 CODEC(Gorilla, ZSTD(3))
) ENGINE = MergeTree()
ORDER BY (longitude, latitude)
"""


# One row per valid day: the run that owns it and how many 4-hourly rows it
# has. ReplacingMergeTree(run_date) keeps the newest run's row per day.
_DAYS_DDL = f"""
CREATE TABLE IF NOT EXISTS {DAYS_TABLE} (
    day        Date,
    run_date   Date,
    rows       UInt64,
    daily_rows UInt64,
    steps      UInt8,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(run_date)
ORDER BY day
"""


def ensure_schema(client) -> None:
    client.command(_GRID_DDL)
    client.command(_DAYS_DDL)
    for table in (DATA_TABLE, staging_table(DATA_TABLE)):
        client.command(_data_ddl(table))
    for table in (DAILY_TABLE, staging_table(DAILY_TABLE)):
        client.command(_daily_ddl(table))


def _partition_expr(day: date) -> str:
    # Both tables' partition key is a Date; ClickHouse accepts the literal.
    return f"'{day.isoformat()}'"


def day_owners(client) -> dict[date, date]:
    """{valid day: owning run_date} for every recorded day."""
    rows = client.query(f'SELECT day, max(run_date) FROM {DAYS_TABLE} GROUP BY day').result_rows
    return {r[0]: r[1] for r in rows}


def day_owner(client, day: date) -> Optional[date]:
    rows = client.query(
        f'SELECT max(run_date), count() FROM {DAYS_TABLE} WHERE day = %(d)s',
        parameters={'d': day},
    ).result_rows
    return rows[0][0] if rows and rows[0][1] else None


def is_stale(client, day: date, run_date: date) -> bool:
    """True when `day` is already owned by a run newer than run_date."""
    owner = day_owner(client, day)
    return owner is not None and owner > run_date


def record_day(client, day: date, run_date: date, rows: int, daily_rows: int, steps: int) -> None:
    client.insert(DAYS_TABLE, [[day, run_date, rows, daily_rows, steps]],
                  column_names=['day', 'run_date', 'rows', 'daily_rows', 'steps'])
    client.command(f'OPTIMIZE TABLE {DAYS_TABLE} FINAL')


def clear_staging_day(client, table: str, day: date) -> None:
    client.command(
        f'ALTER TABLE {staging_table(table)} DROP PARTITION {_partition_expr(day)}'
    )


def promote_staging_day(client, table: str, day: date) -> None:
    """Atomically swap `day` in `table` for what's in its staging table, then
    empty the staging partition."""
    part = _partition_expr(day)
    client.command(f'ALTER TABLE {table} REPLACE PARTITION {part} FROM {staging_table(table)}')
    client.command(f'ALTER TABLE {staging_table(table)} DROP PARTITION {part}')


def drop_day(client, table: str, day: date) -> None:
    client.command(f'ALTER TABLE {table} DROP PARTITION {_partition_expr(day)}')
