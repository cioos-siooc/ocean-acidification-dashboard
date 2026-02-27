"""Centralised, thread-safe NetCDF / HDF5 file reader.

All API modules that need to open NetCDF files MUST use the helpers in this
module instead of calling ``xr.open_dataset()`` directly.

Why?
----
The HDF5 C library (used by netCDF4-python and therefore xarray) is **not**
thread-safe.  When FastAPI dispatches multiple requests to a thread-pool,
concurrent ``open_dataset`` calls can segfault or raise opaque HDF errors.

This module provides:

* ``open_nc(path)``  – thread-safe open with LRU caching.  Returns a lazily-
  opened ``xr.Dataset`` that can be indexed / sliced by multiple threads
  (the underlying data access is still serialised by the lock when needed).
* ``open_nc_uncached(path)``  – thread-safe open **without** caching.  The
  caller is responsible for closing the dataset.

Both functions use a single process-wide ``threading.Lock`` so that no two
threads ever call into HDF5 at the same time, regardless of which API module
triggered the read.
"""

from __future__ import annotations

import logging
import os
import threading
from collections import OrderedDict
from typing import Optional

import xarray as xr

logger = logging.getLogger(__name__)

# ── Process-wide lock ──────────────────────────────────────────────────────
# A single lock shared by every call site in the API process.
_nc_lock = threading.Lock()

# ── LRU cache ──────────────────────────────────────────────────────────────
_cache: OrderedDict[str, xr.Dataset] = OrderedDict()
MAX_CACHED = int(os.getenv("NC_CACHE_SIZE", "12"))


def open_nc(path: str) -> Optional[xr.Dataset]:
    """Return a (possibly cached) ``xr.Dataset`` for *path*.

    The dataset is kept open in an LRU cache so that repeat requests for the
    same file are nearly free.  When the cache exceeds ``MAX_CACHED`` entries
    the least-recently-used dataset is closed and evicted.

    Thread-safe: the HDF5 open call is serialised behind ``_nc_lock``.

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
        with _nc_lock:
            if path in _cache:
                _cache.move_to_end(path)
                logger.debug("cache hit: %s", os.path.basename(path))
                return _cache[path]

    if not os.path.exists(path):
        logger.warning("file not found: %s", path)
        return None

    with _nc_lock:
        # Double-check after acquiring lock
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
            ds = xr.open_dataset(path)
            _cache[path] = ds
            return ds
        except Exception:
            logger.exception("failed to open %s", path)
            return None


def open_nc_uncached(path: str) -> Optional[xr.Dataset]:
    """Open a NetCDF file **without** caching.  Caller must close it.

    Thread-safe: the HDF5 open is serialised behind ``_nc_lock``.

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

    with _nc_lock:
        logger.info("opening (uncached): %s", os.path.basename(path))
        try:
            return xr.open_dataset(path)
        except Exception:
            logger.exception("failed to open %s", path)
            return None


def close_nc(ds: Optional[xr.Dataset]) -> None:
    """Safely close a dataset (for uncached opens)."""
    if ds is not None:
        try:
            ds.close()
        except Exception:
            pass
