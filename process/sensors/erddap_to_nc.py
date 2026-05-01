"""
erddap_to_nc.py

Dispatcher: routes each active ERDDAP sensor to the appropriate downloader
based on its source link:

    /tabledap/  →  erddapTable_to_nc.fetch_and_store
    /griddap/   →  erddapGrid_to_nc.fetch_and_store

Each sub-script also guards itself, so they remain independently runnable.
"""

import argparse
import sys
import os

# Allow running from the sensors/ directory or from the process/ root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import erddapTable_to_nc
import erddapGrid_to_nc


def main():
    parser = argparse.ArgumentParser(
        description="Download all active ERDDAP sensor data (tabledap + griddap)."
    )
    parser.add_argument(
        "--sensor-id",
        type=int,
        default=None,
        help="Process only this sensor ID (default: all active ERDDAP sensors)",
    )
    args = parser.parse_args()

    # Each sub-script does its own DB query and skips sensors whose link
    # doesn't match its type, so we can call both unconditionally.
    erddapTable_to_nc.fetch_and_store(sensor_id_filter=args.sensor_id)
    erddapGrid_to_nc.fetch_and_store(sensor_id_filter=args.sensor_id)


if __name__ == "__main__":
    main()
