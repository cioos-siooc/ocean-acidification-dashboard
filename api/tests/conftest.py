import os
import sys

_API_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MODULES_DIR = os.path.join(_API_DIR, "modules")

for _p in (_API_DIR, _MODULES_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)
