"""Centralised, thread-safe NetCDF / HDF5 file reader.

All API modules that need to open NetCDF files SHOULD use the helpers in this
module to benefit from LRU caching.

Strategy: Per-file RLocks keyed by canonical file path. This allows concurrent
reads of different NC files while preventing races on the same file.
Different files (model, sensor, climatology) can be opened concurrently by
different threads, unlike a global lock which serialises everything.

This module provides:

* ``open_nc(path)``        – thread-safe open with LRU caching.
* ``open_nc_uncached(path)`` – thread-safe open **without** caching.
* ``get_file_lock(path)``  – exported per-file RLock for other modules
                              that open NC files with raw netCDF4 (e.g.,
                              extractSensorTimeseries, pngGenerator).
"""

from __future__ import annotations

import logging
import os
import threading
from collections import OrderedDict
from typing import Optional

import xarray as xr

logger = logging.getLogger(__name__)

# ── Per-file lock store ───────────────────────────────────────────────────
# Maps canonical file path → RLock. Allows concurrent access to DIFFERENT
# files while serialising concurrent access to the SAME file.
_file_locks: dict[str, threading.RLock] = {}
_file_locks_meta = threading.Lock()  # protects the _file_locks dict itself


def get_file_lock(path: str) -> threading.RLock:
    """Return (or create) the per-file RLock for *path*.

    Uses the canonical (realpath) form so that symlinks to the same file share
    a single lock. Exported for use by modules that open NC files directly
    with netCDF4 (e.g. extractSensorTimeseries, pngGenerator).
    """
    canonical = os.path.realpath(path)
    with _file_locks_meta:
        if canonical not in _file_locks:
            _file_locks[canonical] = threading.RLock()
        return _file_locks[canonical]


# ── Cache lock ────────────────────────────────────────────────────────────
# Protects the LRU cache dict (add/evict operations).
_cache_lock = threading.Lock()

# ── LRU cache ──────────────────────────────────────────────────────────────
_cache: OrderedDict[str, xr.Dataset] = OrderedDict()
MAX_CACHED = int(os.getenv("NC_CACHE_SIZE", "12"))


def open_nc(path: str) -> Optional[xr.Dataset]:
    """Return a (possibly cached) ``xr.Dataset`` for *path*.

    The dataset is kept open in an LRU cache so that repeat requests for the
    same file are nearly free. When the cache exceeds ``MAX_CACHED`` entries
    the least-recently-used dataset is closed and evicted.

    All HDF5 operations are serialized using a global RLock passed to xarray,
    ensuring thread-safe concurrent access.

    Parameters
    ----------
    path : str
        Absolute path to the ``.nc`` file.

    Returns
    -------
    xr.Dataset or None
        ``None`` if the file does not exist.
    """
    # Fast path – cache hit (no lock needed for dict lookup)
    if path in _cache:
        # Move to end (most recently used)
        with _cache_lock:
            if path in _cache:
                _cache.move_to_end(path)
                logger.debug("cache hit: %s", os.path.basename(path))
                return _cache[path]

    if not os.path.exists(path):
        logger.warning("file not found: %s", path)
        return None

    with _cache_lock:
        # Double-check if cached
        if path in _cache:
            _cache.move_to_end(path)
            return _cache[path]

        # Evict oldest if full
        while len(_cache) >= MAX_CACHED:
            evict_path, evict_ds = _cache.popitem(last=False)
            try:
                evict_ds.close()
                logger.info("evicted from cache: %s", os.path.basename(evict_path))
            except Exception:
                pass

        logger.info("opening (cached): %s", os.path.basename(path))
        try:
            # Per-file lock: allows different files to open concurrently
            # while serialising concurrent opens of the SAME file.
            with get_file_lock(path):
                ds = xr.open_dataset(path)
            _cache[path] = ds
            return ds
        except Exception:
            logger.exception("failed to open %s", path)
            return None


def open_nc_uncached(path: str) -> Optional[xr.Dataset]:
    """Open a NetCDF file **without** caching. Caller must close it.

    Each call gets its own independent Dataset instance. The entire open operation
    is protected by a global RLock to ensure thread-safe concurrent access, preventing
    HDF5 attribute read races during the initial file open.

    Parameters
    ----------
    path : str
        Absolute path to the ``.nc`` file.

    Returns
    -------
    xr.Dataset or None
        ``None`` if the file does not exist or cannot be opened.
    """
    if not os.path.exists(path):
        logger.warning("file not found: %s", path)
        return None

    logger.info("opening (uncached): %s", os.path.basename(path))
    
    try:
        # Per-file lock: allows different files to open concurrently
        # while serialising concurrent opens of the SAME file.
        with get_file_lock(path):
            return xr.open_dataset(path)
    except Exception:
        logger.exception("failed to open %s", path)
        return None


def close_nc(ds: Optional[xr.Dataset]) -> None:
    """Safely close a dataset (for uncached opens).

    No lock is needed — xarray close() is safe to call from the thread
    that opened the Dataset.
    """
    if ds is not None:
        try:
            ds.close()
        except Exception:
            pass
