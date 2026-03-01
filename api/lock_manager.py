"""Backwards-compatible shim – delegates to nc_reader._nc_lock.

All new code should import from nc_reader directly.
"""
from nc_reader import _nc_lock as io_lock

__all__ = ["io_lock"]
