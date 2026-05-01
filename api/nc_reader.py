"""Centralised, thread-safe NetCDF / HDF5 file reader.

All API modules that need to open NetCDF files SHOULD use the helpers in this
module to benefit from LRU caching.

Strategy: Use a global RLock passed to xarray's open_dataset(lock=...) parameter.
Xarray will use this lock to serialize HDF5 access. This is the recommended
approach for thread-safe concurrent access in the xarray documentation.

This module provides:

* ``open_nc(path)``  – thread-safe open with LRU caching.
* ``open_nc_uncached(path)``  – thread-safe open **without** caching.
"""

from __future__ import annotations

import logging
import os
import threading
from collections import OrderedDict
from typing import Optional

import xarray as xr

logger = logging.getLogger(__name__)

# ── Global HDF5 lock ──────────────────────────────────────────────────────
# Passed to xarray to serialize HDF5 operations across all threads and files.
# This is the recommended way in xarray documentation for thread-safe access.
_hdf5_lock = threading.RLock()

# ── Cache lock ────────────────────────────────────────────────────────────
# Simple lock to protect the LRU cache dict (add/evict operations).
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
            # Wrap entire xr.open_dataset() in _hdf5_lock to protect metadata AND data access
            # This prevents "Can't open HDF5 attribute" errors during concurrent attribute reads
            with _hdf5_lock:
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
        # Wrap entire xr.open_dataset() in lock to protect both metadata AND data access
        # This prevents "Can't open HDF5 attribute" errors during concurrent opens
        with _hdf5_lock:
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
