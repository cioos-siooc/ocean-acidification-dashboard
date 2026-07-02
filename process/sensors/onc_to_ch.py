"""
onc_to_ch.py

Fetch ONC scalar data and store in ClickHouse sensor_timeseries.

Sensor metadata is read from CH `sensors FINAL`.  The last stored timestamp
per (sensor, variable) is determined with a MAX(time) query so no NC files
are needed.  Canonical variable names are stored directly — no code-resolution
step at query time.

Usage
-----
    uv run python sensors/onc_to_ch.py              # all ONC sensors
    uv run python sensors/onc_to_ch.py --sensor-id 3
"""

import argparse
import json
import os
from datetime import datetime, timezone

import numpy as np
from onc import ONC

from ch_helpers import get_ch_client

TOKEN = os.getenv("ONC_TOKEN", "7d291a6a-b57e-49cd-acb1-83f59010d32b")
onc = ONC(TOKEN)

BATCH_SIZE = 10_000


# ── CH helpers ────────────────────────────────────────────────────────────────

def get_active_onc_sensors(ch_client, sensor_id_filter: int | None) -> list[dict]:
    where = "source LIKE '%\"type\": \"ONC\"%' OR source LIKE '%\"type\":\"ONC\"%'"
    if sensor_id_filter is not None:
        where += f" AND id = {sensor_id_filter}"
    rows = ch_client.query(
        f"SELECT id, name, variables, device_config FROM sensors FINAL "
        f"WHERE active = 1 AND ({where})"
    ).result_rows
    sensors = []
    for row in rows:
        sensors.append({
            "id": int(row[0]),
            "name": row[1],
            "variables": json.loads(row[2]) if row[2] else {},
            "device_config": json.loads(row[3]) if row[3] else {},
        })
    return sensors


def get_last_stored_time(ch_client, sensor_id: int, canonical: str) -> datetime | None:
    rows = ch_client.query(
        "SELECT max(time) FROM sensor_timeseries "
        f"WHERE sensor_id = {sensor_id} AND variable = '{canonical}'"
    ).result_rows
    if rows and rows[0][0]:
        dt = rows[0][0]
        if hasattr(dt, "tzinfo"):
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        return datetime.fromtimestamp(dt, tz=timezone.utc)
    return None


# ── Data helpers ──────────────────────────────────────────────────────────────

def dt_to_naive_utc(dt_str: str) -> datetime | None:
    dt_str = dt_str.rstrip("Z").replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    return None


def apply_conversion(value: float, canonical: str, variables: dict) -> float:
    info = variables.get(canonical)
    if isinstance(info, dict):
        return value * info.get("conversion_factor", 1.0)
    return value


# ── Main fetch loop ───────────────────────────────────────────────────────────

def fetch_and_store(sensor_id_filter: int | None = None):
    ch_client = get_ch_client()
    sensors = get_active_onc_sensors(ch_client, sensor_id_filter)

    if not sensors:
        print("No active ONC sensors found in ClickHouse.")
        return

    date_to = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    for sensor in sensors:
        sensor_id = sensor["id"]
        variables = sensor["variables"]
        device_config = sensor["device_config"]
        print(f"\nSensor: {sensor['name']} (ID: {sensor_id})")

        if not device_config:
            print("  No device_config, skipping.")
            continue

        location_code = device_config.get("locationCode")
        code_rows = device_config.get("codes", [])

        # Build reverse map: ONC code → canonical name
        code_to_canonical: dict[str, str] = {}
        for canonical, info in variables.items():
            if isinstance(info, dict) and info.get("name"):
                code_to_canonical[info["name"]] = canonical

        for code_row in code_rows:
            device_category = code_row.get("deviceCategoryCode")
            sensor_codes = code_row.get("sensorCategoryCodes", "")

            # Determine the canonical names involved so we can find the latest stored time
            involved_canonicals = [
                code_to_canonical[c] for c in sensor_codes.split(",")
                if c.strip() in code_to_canonical
            ]
            if not involved_canonicals:
                print(f"  No canonical mapping for codes '{sensor_codes}', skipping.")
                continue

            # Use the earliest last-stored time across all involved variables so we
            # don't create a gap in any of them.
            last_times = [get_last_stored_time(ch_client, sensor_id, c) for c in involved_canonicals]
            valid_times = [t for t in last_times if t is not None]
            if valid_times:
                overlap = min(valid_times)
                from_dt_obj = overlap.replace(hour=0, minute=0, second=0, microsecond=0)
                # one-day overlap
                from_dt_obj = from_dt_obj.replace(
                    day=max(1, from_dt_obj.day - 1)
                )
                date_from = from_dt_obj.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            else:
                date_from = None

            print(f"  Fetching {location_code}/{device_category}/{sensor_codes} from {date_from or 'beginning'}")

            try:
                data = onc.getScalardataByLocation({
                    "locationCode": location_code,
                    "deviceCategoryCode": device_category,
                    "getLatest": True,
                    "resamplePeriod": 3600,
                    "resampleType": "avg",
                    "qualityControl": "clean",
                    "sensorCategoryCodes": sensor_codes,
                    "dateFrom": date_from,
                    "dateTo": date_to,
                })
            except Exception as e:
                print(f"  ERROR fetching ONC data: {e}")
                continue

            if "sensorData" not in data:
                print(f"  No sensorData returned.")
                continue

            for s in data["sensorData"]:
                onc_code = s.get("sensorCategoryCode")
                canonical = code_to_canonical.get(onc_code)
                if not canonical:
                    print(f"  Skipping unmapped code '{onc_code}'")
                    continue

                vals = s.get("data", {}).get("values", [])
                raw_times = s.get("data", {}).get("sampleTimes", [])

                rows = []
                for t_str, v in zip(raw_times, vals):
                    if v is None or (isinstance(v, float) and np.isnan(v)):
                        continue
                    t_dt = dt_to_naive_utc(t_str)
                    if t_dt is None:
                        continue
                    converted = apply_conversion(float(v), canonical, variables)
                    # depth for ONC sensors comes from device position; use 0 as default
                    # and let the sensors.depth field in metadata carry the actual depth
                    rows.append([sensor_id, t_dt, 0.0, canonical, converted])

                for start in range(0, len(rows), BATCH_SIZE):
                    ch_client.insert(
                        "sensor_timeseries",
                        rows[start:start + BATCH_SIZE],
                        column_names=["sensor_id", "time", "depth", "variable", "value"],
                    )
                print(f"  {onc_code} → '{canonical}': {len(rows)} point(s) inserted")

    print("\nDone.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch ONC sensor data → ClickHouse.")
    parser.add_argument("--sensor-id", type=int, default=None)
    args = parser.parse_args()
    fetch_and_store(sensor_id_filter=args.sensor_id)
