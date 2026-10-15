"""
onc_to_ch.py

Fetch ONC scalar data and store in ClickHouse sensor_timeseries.

Sensor metadata is read from CH `sensors FINAL`.  The last stored timestamp
per (sensor, variable) is determined with a MAX(time) query so no NC files
are needed.  Canonical variable names are stored directly — no code-resolution
step at query time.

Each variable lists its ONC sources (device_config.sources, from the
catalog): a site's instruments often sit on child locations (CRIP.C1,
CRIP.C2) and come and go between deployments. Every source is stored as its
own series, tagged in the `source` column (LOCATION/DEVICE_CATEGORY); readers
merge them by the list's priority order.

Sensors with depth -1 (profilers) take store_profiler(): each value is stored
at the CTD's measured depth, binned to (hour, 1 m) — see PROFILER_* below.

Usage
-----
    uv run python sensors/onc_to_ch.py                          # all ONC sensors
    uv run python sensors/onc_to_ch.py --sensor-id <uuid>
"""

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import numpy as np
from onc import ONC

from ch_helpers import get_ch_client

TOKEN = os.getenv("ONC_TOKEN", "7d291a6a-b57e-49cd-acb1-83f59010d32b")
onc = ONC(TOKEN)

BATCH_SIZE = 10_000

# How far back to re-fetch from the last stored timestamp on each run.
# ONC data for the most recent hours may be revised after initial delivery,
# so we always overlap by this amount and let ReplacingMergeTree overwrite
# stale rows during its background merge.
OVERLAP_HOURS = 6

# ONC's error for "a device of this category was deployed here, but not during
# the requested time range".
ONC_NOT_DEPLOYED = "API Error 127"

# ── Variable-depth (profiler) sensors: sensors.depth == -1 ───────────────────
# A profiling package parks at one depth and casts through the water column
# every few hours, so the fixed-depth path's hourly average would blend each
# cast into one meaningless value. Instead: a short ONC-side average (the CTD
# samples at 4 Hz — the raw stream is too heavy to pull for a decade of
# history), paired with the CTD's own depth reading at the same timestamp,
# then averaged into (hour, 1 m) cells — the model's own time step — so a
# parked stretch collapses to one row per hour while a cast keeps its vertical
# structure. The API snaps these onto model levels per request
# (extract_depth_profile.py), as it does ERDDAP profilers' raw casts.
PROFILER_RESAMPLE_SECONDS = 10    # ONC fixed windows: same timestamps across devices
PROFILER_BIN_MINUTES = 60         # must divide an hour (floor_to_bin)
PROFILER_BIN_METRES = 1.0
PROFILER_CHUNK_DAYS = 7           # ~60k rows per sensor code per request
DEPTH_DEVICE_CATEGORY = "CTD"     # supplies the depth every variable is paired with
DEPTH_SENSOR_CODE = "depth"


# ── CH helpers ────────────────────────────────────────────────────────────────

def get_active_onc_sensors(ch_client, sensor_id_filter: str | None) -> list[dict]:
    where = "JSONExtractString(source, 'api') = 'ONC'"
    if sensor_id_filter is not None:
        where += f" AND id = '{sensor_id_filter}'"
    rows = ch_client.query(
        f"SELECT id, name, depth, variables, device_config FROM sensors FINAL "
        f"WHERE active = 1 AND {where}"
    ).result_rows
    sensors = []
    for row in rows:
        sensors.append({
            "id": str(row[0]),
            "name": row[1],
            "depth": float(row[2]),
            "variables": json.loads(row[3]) if row[3] else {},
            "device_config": json.loads(row[4]) if row[4] else {},
        })
    return sensors


def get_last_stored_time(ch_client, sensor_id: str, canonical: str,
                         source: str | None = None) -> datetime | None:
    where = f" AND source = '{source}'" if source is not None else ""
    rows = ch_client.query(
        "SELECT max(time) FROM sensor_timeseries "
        f"WHERE sensor_id = '{sensor_id}' AND variable = '{canonical}'{where} "
        "HAVING count() > 0"  # max() of no rows is the epoch, not NULL
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


def utc_now_str() -> str:
    """Now, in the timestamp format ONC's dateTo expects."""
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def apply_conversion(value: float, canonical: str, variables: dict) -> float:
    info = variables.get(canonical)
    if isinstance(info, dict):
        return value * info.get("conversion_factor", 1.0)
    return value


def onc_time_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def floor_to_bin(dt: datetime) -> datetime:
    return dt.replace(minute=dt.minute - dt.minute % PROFILER_BIN_MINUTES, second=0, microsecond=0)


def bin_profile(samples) -> list[tuple[datetime, float, float]]:
    """Average (time, depth, value) samples into PROFILER_BIN_MINUTES ×
    PROFILER_BIN_METRES cells, keyed by the cell's start time and centre depth.
    The keys are deterministic, so a cell that was only partly filled on one
    run is replaced whole (ReplacingMergeTree) when the overlap re-fetches it."""
    cells: dict[tuple[datetime, float], list[float]] = defaultdict(list)
    for t, depth, value in samples:
        d = round(depth / PROFILER_BIN_METRES) * PROFILER_BIN_METRES + 0.0  # + 0.0: no -0.0 key
        cells[(floor_to_bin(t), d)].append(value)
    return [(t, d, sum(vs) / len(vs)) for (t, d), vs in sorted(cells.items())]


def plan_windows(deployments: list[tuple[datetime, datetime | None]],
                 date_from: datetime | None, date_to: datetime) -> list[tuple[datetime, datetime]]:
    """Request windows covering the deployed periods within [date_from, date_to],
    each at most PROFILER_CHUNK_DAYS long. Skipping undeployed gaps saves
    requests that could only come back empty."""
    chunk = timedelta(days=PROFILER_CHUNK_DAYS)
    windows = []
    for begin, end in sorted(deployments):
        start = max(begin, date_from) if date_from else begin
        stop = min(end, date_to) if end else date_to
        while start < stop:
            windows.append((start, min(start + chunk, stop)))
            start += chunk
    return windows


def get_deployments(location_code: str) -> list[tuple[datetime, datetime | None]]:
    """(begin, end) of every depth-device deployment at the location; end is
    None for the one still in the water."""
    deps = onc.getDeployments({"locationCode": location_code,
                               "deviceCategoryCode": DEPTH_DEVICE_CATEGORY}) or []
    return [(dt_to_naive_utc(d["begin"]), dt_to_naive_utc(d["end"]) if d.get("end") else None)
            for d in deps if d.get("begin")]


def fetch_profiler_window(location_code: str, device_category: str, codes: list[str],
                          start: datetime, stop: datetime) -> dict[str, list[tuple[datetime, float]]]:
    """{sensor code: [(time, value)]} for one device category over [start, stop),
    NaN/missing samples dropped."""
    data = onc.getScalardataByLocation({
        "locationCode": location_code,
        "deviceCategoryCode": device_category,
        "sensorCategoryCodes": ",".join(codes),
        "resamplePeriod": PROFILER_RESAMPLE_SECONDS,
        "resampleType": "avg",
        "qualityControl": "clean",
        "dateFrom": onc_time_str(start),
        "dateTo": onc_time_str(stop),
    }, allPages=True)
    series: dict[str, list[tuple[datetime, float]]] = {}
    for s in data.get("sensorData") or []:
        pts = []
        for t_str, v in zip(s.get("data", {}).get("sampleTimes", []), s.get("data", {}).get("values", [])):
            if v is None or (isinstance(v, float) and np.isnan(v)):
                continue
            t = dt_to_naive_utc(t_str)
            if t is not None:
                pts.append((t, float(v)))
        series[s.get("sensorCategoryCode")] = pts
    return series


# ── Main fetch loop ───────────────────────────────────────────────────────────

class FetchError(RuntimeError):
    """An ONC request for the sensor failed; its rows are left as they were."""


def variable_sources(device_config: dict) -> dict[str, list[tuple[str, str, str]]]:
    """{canonical: [(locationCode, deviceCategoryCode, sensorCategoryCode)]},
    highest priority first."""
    return {
        canonical: [(s["locationCode"], s["deviceCategoryCode"], s["sensorCategoryCode"]) for s in srcs]
        for canonical, srcs in (device_config.get("sources") or {}).items()
    }


def fetch_hourly(location_code: str, device_category: str, codes: list[str],
                 date_from: str | None, date_to: str) -> dict[str, dict[datetime, float]]:
    """{sensor code: {hour: value}} — ONC's hourly means for one device
    category at one location, NaN/missing samples dropped. Empty when nothing
    was deployed there in the window. Paged (`allPages`): one response holds
    at most 100k rows, and a first backfill of a site running since 2008 is
    ~160k hours."""
    try:
        data = onc.getScalardataByLocation({
            "locationCode": location_code,
            "deviceCategoryCode": device_category,
            "resamplePeriod": 3600,
            "resampleType": "avg",
            "qualityControl": "clean",
            "sensorCategoryCodes": ",".join(codes),
            "dateFrom": date_from,
            "dateTo": date_to,
        }, allPages=True)
    except Exception as e:
        # A 400, not an empty result, for a retired or not-yet-deployed source.
        if ONC_NOT_DEPLOYED in str(e):
            return {}
        raise
    series: dict[str, dict[datetime, float]] = {}
    for s in data.get("sensorData") or []:
        pts: dict[datetime, float] = {}
        for t_str, v in zip(s.get("data", {}).get("sampleTimes", []), s.get("data", {}).get("values", [])):
            if v is None or (isinstance(v, float) and np.isnan(v)):
                continue
            t = dt_to_naive_utc(t_str)
            if t is not None:
                pts[t] = float(v)
        series[s.get("sensorCategoryCode")] = pts
    return series


def store_sensor(ch_client, sensor: dict, date_to: str):
    """Fetch new data from each of the sensor's sources up to `date_to` and
    insert it. Raises FetchError, after trying every source, if any ONC
    request failed."""
    sensor_id = sensor["id"]
    variables = sensor["variables"]
    device_config = sensor["device_config"]
    print(f"\nSensor: {sensor['name']} (ID: {sensor_id})")

    sources = variable_sources(device_config)
    if not sources:
        # Rows written before per-variable sources existed: re-import the catalog.
        raise FetchError("device_config has no sources — run `manage_sensors.py import --overwrite`")

    if sensor.get("depth") == -1.0:
        store_profiler(ch_client, sensor, device_config.get("locationCode"), sources, date_to)
        return

    # One request per (location, device category), for every variable it
    # supplies, from the earliest point any of them needs. Each (variable,
    # source) series resumes OVERLAP_HOURS before its own last stored
    # timestamp, so recently revised ONC data overwrites any stale rows.
    groups: dict[tuple[str, str], dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for canonical, srcs in sources.items():
        for loc, cat, code in srcs:
            groups[(loc, cat)][code].append(canonical)

    errors: list[str] = []
    for (loc, cat), canonicals_by_code in groups.items():
        label = f"{loc}/{cat}"
        since: dict[str, datetime | None] = {}
        for canonical in (c for cs in canonicals_by_code.values() for c in cs):
            last = get_last_stored_time(ch_client, sensor_id, canonical, label)
            since[canonical] = (last.astimezone(timezone.utc).replace(tzinfo=None)
                                - timedelta(hours=OVERLAP_HOURS)) if last else None
        start = None if None in since.values() else onc_time_str(min(since.values()))

        codes = sorted(canonicals_by_code)
        print(f"  Fetching {label}/{','.join(codes)} from {start or 'beginning'}")
        try:
            fetched = fetch_hourly(loc, cat, codes, start, date_to)
        except Exception as e:
            # Carry on with the other sources; raised below.
            print(f"  ERROR fetching ONC data: {e}")
            errors.append(f"{label}: {e}")
            continue

        for code, canonicals in canonicals_by_code.items():
            for canonical in canonicals:
                rows = [[sensor_id, t, 0.0, canonical, apply_conversion(v, canonical, variables), label]
                        # depth for ONC sensors comes from device position; use 0 as default
                        # and let the sensors.depth field in metadata carry the actual depth
                        for t, v in sorted(fetched.get(code, {}).items())
                        if since[canonical] is None or t >= since[canonical]]
                for i in range(0, len(rows), BATCH_SIZE):
                    ch_client.insert(
                        "sensor_timeseries",
                        rows[i:i + BATCH_SIZE],
                        column_names=["sensor_id", "time", "depth", "variable", "value", "source"],
                    )
                print(f"  {code} → '{canonical}': {len(rows)} point(s) inserted")

    if errors:
        raise FetchError("; ".join(errors))


def store_profiler(ch_client, sensor: dict, location_code: str,
                   sources: dict[str, list[tuple[str, str, str]]], date_to: str):
    """store_sensor for a variable-depth sensor: every variable at the CTD's
    measured depth, binned by bin_profile(). Only each variable's first source
    is used, and it must be at `location_code`, where the CTD depth comes from.
    Windows are fetched oldest first and each is inserted before the next is
    requested, so a long backfill that dies midway resumes from where it got
    to. A failed request stops the run (raising FetchError) rather than
    skipping ahead — rows past a gap would move MAX(time) beyond it and the gap
    would never be re-fetched."""
    sensor_id = sensor["id"]
    variables = sensor["variables"]

    code_to_canonical: dict[str, str] = {}
    codes_by_category: dict[str, list[str]] = defaultdict(list)
    for canonical, srcs in sources.items():
        loc, cat, code = srcs[0]
        if loc != location_code:
            print(f"  '{canonical}': source {loc} isn't the profiler's location {location_code}, skipping")
            continue
        code_to_canonical[code] = canonical
        codes_by_category[cat].append(code)
    if not codes_by_category:
        print("  No canonical mappings, skipping.")
        return

    last_times = [get_last_stored_time(ch_client, sensor_id, c) for c in code_to_canonical.values()]
    valid_times = [t.astimezone(timezone.utc).replace(tzinfo=None) for t in last_times if t is not None]
    date_from = floor_to_bin(min(valid_times) - timedelta(hours=OVERLAP_HOURS)) if valid_times else None

    try:
        deployments = get_deployments(location_code)
    except Exception as e:
        raise FetchError(f"deployments: {e}") from e
    windows = plan_windows(deployments, date_from, dt_to_naive_utc(date_to))
    print(f"  Profiler: {location_code} from {date_from or 'first deployment'}, "
          f"{len(windows)} window(s) of ≤{PROFILER_CHUNK_DAYS} d")

    for start, stop in windows:
        try:
            # Depth first: every other device category is paired against it.
            ctd = fetch_profiler_window(location_code, DEPTH_DEVICE_CATEGORY,
                                        [DEPTH_SENSOR_CODE, *codes_by_category.get(DEPTH_DEVICE_CATEGORY, [])],
                                        start, stop)
            fetched = [(DEPTH_DEVICE_CATEGORY, ctd)] + [
                (cat, fetch_profiler_window(location_code, cat, codes, start, stop))
                for cat, codes in codes_by_category.items() if cat != DEPTH_DEVICE_CATEGORY]
        except Exception as e:
            raise FetchError(f"{start:%Y-%m-%d}–{stop:%Y-%m-%d}: {e}") from e

        depth_at = dict(ctd.pop(DEPTH_SENSOR_CODE, []))
        rows = []
        for cat, series in fetched:
            for code, pts in series.items():
                canonical = code_to_canonical.get(code)
                if not canonical:
                    continue
                samples = [(t, depth_at[t], apply_conversion(v, canonical, variables))
                           for t, v in pts if t in depth_at]
                rows += [[sensor_id, t, d, canonical, v, f"{location_code}/{cat}"]
                         for t, d, v in bin_profile(samples)]

        for i in range(0, len(rows), BATCH_SIZE):
            ch_client.insert(
                "sensor_timeseries",
                rows[i:i + BATCH_SIZE],
                column_names=["sensor_id", "time", "depth", "variable", "value", "source"],
            )
        print(f"  {start:%Y-%m-%d}–{stop:%Y-%m-%d}: {len(depth_at)} depth sample(s) → {len(rows)} row(s)")


def fetch_and_store(sensor_id_filter: str | None = None):
    ch_client = get_ch_client()
    sensors = get_active_onc_sensors(ch_client, sensor_id_filter)

    if not sensors:
        print("No active ONC sensors found in ClickHouse.")
        return

    date_to = utc_now_str()

    for sensor in sensors:
        try:
            store_sensor(ch_client, sensor, date_to)
        except FetchError:
            pass  # already printed per device category

    print("\nDone.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch ONC sensor data → ClickHouse.")
    parser.add_argument("--sensor-id", type=str, default=None, metavar="UUID")
    args = parser.parse_args()
    fetch_and_store(sensor_id_filter=args.sensor_id)
