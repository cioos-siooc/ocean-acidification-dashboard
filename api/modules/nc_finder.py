"""nc_finder.py — helpers for locating NC files across one or more data directories.

All public functions accept *data_dirs* as either a single directory string or a
list of directory strings.  Directories are tried in order; the first match wins
for point lookups, and all are merged for listing operations.
"""
from __future__ import annotations

import os
import re
from datetime import datetime
from glob import glob
from typing import List, Optional, Union

_DirSpec = Union[str, List[str]]
_DATE_RE = re.compile(r"(\d{8})")
_ISO_DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def find_nc_file(
    data_dirs: _DirSpec,
    variable: str,
    dt: Union[str, datetime],
    *,
    legacy: bool = False,
) -> Optional[str]:
    """Find an NC file for *variable* on the date given by *dt*.

    Tries each directory in *data_dirs* in order:
      1. Exact match  ``{var_dir}/{variable}_{YYYYMMDD}.nc``
      2. Legacy match ``{var_dir}/{variable}_{YYYYMMDD}T0030_{YYYYMMDD}T2330.nc``
         (only when *legacy=True*)
      3. Closest-date glob fallback across all dirs

    Returns the first path found, or *None* if nothing matches.
    """
    date_str = _to_date_str(dt)
    dirs = _to_list(data_dirs)

    # Phase 1: exact (and optional legacy) matches — prefer primary dir
    for d in dirs:
        var_dir = os.path.join(d, variable)
        if not os.path.isdir(var_dir):
            continue
        exact = os.path.join(var_dir, f"{variable}_{date_str}.nc")
        if os.path.exists(exact):
            return exact
        if legacy:
            leg = os.path.join(
                var_dir, f"{variable}_{date_str}T0030_{date_str}T2330.nc"
            )
            if os.path.exists(leg):
                return leg

    # Phase 2: closest-date fallback across all dirs
    for d in dirs:
        var_dir = os.path.join(d, variable)
        if not os.path.isdir(var_dir):
            continue
        files = sorted(glob(os.path.join(var_dir, "*.nc")))
        if files:
            closest = _find_closest(files, variable, date_str)
            if closest:
                return closest

    return None


def list_nc_files(data_dirs: _DirSpec, variable: str) -> List[str]:
    """Return a sorted list of all NC files for *variable* across all *data_dirs*.

    When the same filename exists in multiple directories, only the copy from
    the first directory in which it appears is included.
    """
    seen: set = set()
    result: List[str] = []
    for d in _to_list(data_dirs):
        var_dir = os.path.join(d, variable)
        if not os.path.isdir(var_dir):
            continue
        for f in sorted(glob(os.path.join(var_dir, "*.nc"))):
            bn = os.path.basename(f)
            if bn not in seen:
                seen.add(bn)
                result.append(f)
    return sorted(result)


def find_file(data_dirs: _DirSpec, relative_path: str) -> Optional[str]:
    """Find *relative_path* under any of *data_dirs*.

    Useful for fixed-path lookups such as climatology or stats files that live
    in a predictable location relative to a configurable root.

    Example::

        find_file(["/opt/data/SSC", "/opt/data/archive/SSC"],
                  "climatology/5d/temperature/temperature_10.nc")
    """
    for d in _to_list(data_dirs):
        full = os.path.join(d, relative_path)
        if os.path.exists(full):
            return full
    return None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _to_list(data_dirs: _DirSpec) -> List[str]:
    return [data_dirs] if isinstance(data_dirs, str) else list(data_dirs)


def _to_date_str(dt: Union[str, datetime]) -> str:
    """Return YYYYMMDD string for *dt*."""
    if isinstance(dt, datetime):
        return dt.strftime("%Y%m%d")
    s = str(dt)
    # Try 8 consecutive digits first (bare YYYYMMDD or YYYYMMDDTHHMMSS)
    m = _DATE_RE.search(s)
    if m:
        return m.group(1)
    # Fallback: ISO date with hyphens (YYYY-MM-DD or YYYY-MM-DDTHH...)
    m = _ISO_DATE_RE.search(s)
    if m:
        return m.group(1) + m.group(2) + m.group(3)
    raise ValueError(f"Cannot extract date from: {dt!r}")


def _find_closest(files: List[str], variable: str, date_str: str) -> Optional[str]:
    """Return the file whose embedded YYYYMMDD is closest to *date_str*."""
    try:
        target = datetime.strptime(date_str, "%Y%m%d")
    except ValueError:
        return None

    closest_file = None
    closest_diff = float("inf")
    for f in files:
        base = os.path.basename(f).replace(".nc", "")
        if base.startswith(f"{variable}_"):
            base = base[len(variable) + 1:]
        m = _DATE_RE.search(base)
        if not m:
            continue
        try:
            file_dt = datetime.strptime(m.group(1), "%Y%m%d")
            diff = abs((file_dt - target).total_seconds())
            if diff < closest_diff:
                closest_diff = diff
                closest_file = f
        except ValueError:
            continue
    return closest_file
