"""Loader for shared/variable_config.yml and shared/colormaps.json.

Single source of truth for SalishSeaCast variable/display metadata, read by
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

    Shape: {"source": str, "bounds": [float, float, float, float],
    "depths": [{"depth_nc": float, "depth_image": str}, ...],
    "variables": {var_name: {"unit", "colormap", "colormapMin",
    "colormapMax", "precision"}}}.
    """
    with open(_VARIABLE_CONFIG_PATH) as f:
        return yaml.safe_load(f)


@lru_cache(maxsize=1)
def load_colormaps() -> List[Dict[str, Any]]:
    """Return the parsed contents of colormaps.json: a list of
    {"name", "description", "stops", "type", "mode", "meta"} dicts.
    """
    with open(_COLORMAPS_PATH) as f:
        return json.load(f)
