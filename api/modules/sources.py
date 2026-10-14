"""Model sources the API serves, from shared/variable_config.yml's `sources:`.

Every extraction module resolves its ClickHouse tables through here instead
of hardcoding SalishSeaCast's, and validates `var` against the source's own
variable list (the variable is interpolated into SQL as a column name, so
this check is also what keeps that safe).
"""
from __future__ import annotations

from shared.variable_config import get_source, load_variable_config

_SOURCES = load_variable_config()['sources']

DATA_TABLE_BY_SOURCE = {name: s['data_table'] for name, s in _SOURCES.items()}
DAILY_TABLE_BY_SOURCE = {name: s['daily_table'] for name, s in _SOURCES.items()}
GRID_TABLE_BY_SOURCE = {name: s['grid_table'] for name, s in _SOURCES.items()}
DOMAIN_LABEL_BY_SOURCE = {name: s.get('domain_label', f'{name} model domain') for name, s in _SOURCES.items()}

# Every variable any source has — a column name in at least one data table.
ALLOWED_VARIABLES = frozenset(v for s in _SOURCES.values() for v in s['variables'])


class CapabilityUnavailable(ValueError):
    """A feature the source's record can't support (e.g. a climatology from
    LiveOcean's ~1-year archive). SERVER.py turns it into a 400 with
    `error.code == "capability_unavailable"`."""

    code = 'capability_unavailable'

    def __init__(self, source: str, capability: str):
        super().__init__(source, capability)
        self.source = source
        self.capability = capability

    def __str__(self) -> str:
        return f"{self.capability} is not available for {self.source}: its record is too short."


def require_source(source: str) -> dict:
    """The source's config block; ValueError for an unknown source."""
    try:
        return get_source(source)
    except KeyError as e:
        raise ValueError(str(e).strip('"')) from None


def check_variable(source: str, var: str) -> None:
    src = require_source(source)
    if var not in src['variables']:
        raise ValueError(f"Unknown variable '{var}' for {source}. Supported variables: {sorted(src['variables'])}")


def require_capability(source: str, capability: str) -> None:
    if not require_source(source).get('capabilities', {}).get(capability, False):
        raise CapabilityUnavailable(source, capability)


def has_bottom_rows(source: str) -> bool:
    """Whether the source's data table stores the bottom layer as depth -1
    rows (LiveOcean) — all-depth queries must then exclude them."""
    return -1 in require_source(source)['depths']
