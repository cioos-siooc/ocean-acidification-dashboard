import os
import sys

_SENSORS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if _SENSORS_DIR not in sys.path:
    sys.path.insert(0, _SENSORS_DIR)
