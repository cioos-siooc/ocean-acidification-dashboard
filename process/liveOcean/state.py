"""Local state management for remote processing mode.

When processing on the PROCESS machine and transferring files to the API
machine, this module tracks processing state in per-date JSON files instead
of writing to the API machine's database during extract/image stages.

State file format (one JSON file per source_date):
{
    "source_date": "2026-02-04",
    "extract_status": "success" | "failed" | null,
    "image_status": "success" | "failed" | null,
    "transfer_status": "transferred" | null,
    "files": [
        {
            "variable": "temperature",
            "local_nc_path": "/opt/data/local/LO/nc/temperature/temperature_20260204.nc",
            "start_time": "2026-02-04T00:00:00",
            "end_time": "2026-02-04T20:00:00",
            "date": "20260204"
        },
        ...
    ]
}
"""

from __future__ import annotations

import json
import logging
import os
from typing import List

logger = logging.getLogger("liveocean.state")


def _state_path(state_dir: str, source_date: str) -> str:
    os.makedirs(state_dir, exist_ok=True)
    return os.path.join(state_dir, f"{source_date}.json")


def load_state(state_dir: str, source_date: str) -> dict:
    """Load processing state for a source_date. Returns a fresh state dict if not found."""
    path = _state_path(state_dir, source_date)
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {
        "source_date": source_date,
        "extract_status": None,
        "image_status": None,
        "transfer_status": None,
        "files": [],
    }


def save_state(state_dir: str, source_date: str, state: dict) -> None:
    """Persist processing state for a source_date."""
    path = _state_path(state_dir, source_date)
    with open(path, "w") as f:
        json.dump(state, f, indent=2, default=str)
    logger.debug(f"State saved: {path}")


def get_pending_transfer_dates(state_dir: str) -> List[str]:
    """Return all source_dates ready for transfer (image complete, not yet transferred)."""
    if not os.path.exists(state_dir):
        return []
    pending = []
    for fname in sorted(os.listdir(state_dir)):
        if not fname.endswith(".json"):
            continue
        source_date = fname[:-5]
        with open(os.path.join(state_dir, fname), "r") as f:
            state = json.load(f)
        if state.get("image_status") == "success" and state.get("transfer_status") != "transferred":
            pending.append(source_date)
    return pending
