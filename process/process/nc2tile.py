# Proxy module that imports the actual implementation from the top-level `nc2tile` module.
from importlib import import_module

_nc2tile = import_module('nc2tile')

# Re-export public names from top-level nc2tile
for _name in dir(_nc2tile):
    if not _name.startswith('_'):
        globals()[_name] = getattr(_nc2tile, _name)

# Ensure module-level attributes exist
__all__ = [n for n in globals() if not n.startswith('_')]
