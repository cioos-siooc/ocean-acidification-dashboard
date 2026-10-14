"""Loader for shared/variable_config.yml and shared/colormaps.json.

Single source of truth for model-source and variable/display metadata, read by
both the `api` and `process` containers (this file lives in shared/, mounted
into both — see docker-compose.dev.yml). See variable_config.yml for the
rationale (it replaces the old Postgres fields/datasets tables).
"""
from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any, Dict, List

import yaml

_DIR = os.path.dirname(os.path.abspath(__file__))
_VARIABLE_CONFIG_PATH = os.path.join(_DIR, 'variable_config.yml')
_COLORMAPS_PATH = os.path.join(_DIR, 'colormaps.json')


@lru_cache(maxsize=1)
def load_variable_config() -> Dict[str, Any]:
    """Return the parsed contents of variable_config.yml.

    Shape: {"sources": {source_name: {"label", "domain_label",
    "grid_table", "data_table", "daily_table", "bounds", "step_hours",
    "step_offset_minutes", "forecast", "capabilities", "depths",
    "variables": [var_name, ...]}},
    "variables": {var_name: {"name", "unit", "colormap", "colormapMin",
    "colormapMax", "precision", "alt_units"?}}}.
    """
    with open(_VARIABLE_CONFIG_PATH) as f:
        return yaml.safe_load(f)


def source_names() -> List[str]:
    return list(load_variable_config()['sources'])


def get_source(name: str) -> Dict[str, Any]:
    """Return one model source's block; KeyError (naming the known sources)
    for anything else, so a typo'd source fails loudly instead of silently
    reading SalishSeaCast's tables."""
    sources = load_variable_config()['sources']
    if name not in sources:
        raise KeyError(f"Unknown source '{name}'. Supported sources: {sorted(sources)}")
    return sources[name]


def source_for_table(table: str) -> str | None:
    """The source whose grid/data/daily table is `table`, or None."""
    for name, src in load_variable_config()['sources'].items():
        if table in (src['grid_table'], src['data_table'], src['daily_table']):
            return name
    return None


@lru_cache(maxsize=1)
def load_colormaps() -> List[Dict[str, Any]]:
    """Return the parsed contents of colormaps.json: a list of
    {"name", "description", "stops", "type", "mode", "meta"} dicts.
    """
    with open(_COLORMAPS_PATH) as f:
        return json.load(f)
