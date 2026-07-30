"""
erddap_to_ch.py

Fetch ERDDAP sensor data and store in ClickHouse sensor_timeseries.

Handles all three ERDDAP source layouts in one script:
  - tabledap, fixed depth   — hourly-binned 1D time series
  - tabledap, variable depth — raw (obs, time+depth) point cloud
  - griddap                  — 2D (time × depth) grid, flattened to rows

Sensor metadata (variable mappings, source URLs) is read from CH `sensors
FINAL`.  The last stored timestamp per (sensor, variable) is determined from
CH so no NC files are needed.

Usage
-----
    uv run python sensors/erddap_to_ch.py              # all active ERDDAP sensors
    uv run python sensors/erddap_to_ch.py --sensor-id 5
"""

import argparse
import csv
import io
import json
import math
import os
from datetime import datetime, timedelta, timezone

import gsw
import netCDF4
import numpy as np
import requests
import urllib3

from ch_helpers import get_ch_client

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ERDDAP_TIME_FMT = "%Y-%m-%dT%H:%M:%SZ"
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
BATCH_SIZE = 50_000


# ── CH helpers ────────────────────────────────────────────────────────────────

def get_active_erddap_sensors(ch_client, sensor_id_filter: str | None) -> list[dict]:
    where = "JSONExtractString(source, 'api') = 'ERDDAP'"
    if sensor_id_filter is not None:
        where += f" AND id = '{sensor_id_filter}'"
    rows = ch_client.query(
        f"SELECT id, name, latitude, longitude, depth, variables, source FROM sensors FINAL "
        f"WHERE active = 1 AND {where}"
    ).result_rows
    sensors = []
    for row in rows:
        source = json.loads(row[6]) if row[6] else {}
        sensors.append({
            "id": str(row[0]),
            "name": row[1],
            "latitude": float(row[2]),
            "longitude": float(row[3]),
            "depth": float(row[4]) if row[4] is not None else -1.0,
            "variables": json.loads(row[5]) if row[5] else {},
            "source": source,
            "link": source.get("link", ""),
        })
    return sensors


def get_last_stored_time(ch_client, sensor_id: str, canonical: str) -> datetime | None:
    rows = ch_client.query(
        "SELECT max(time) FROM sensor_timeseries "
        f"WHERE sensor_id = '{sensor_id}' AND variable = '{canonical}' "
        "HAVING count() > 0"
    ).result_rows
    if rows and rows[0][0]:
        dt = rows[0][0]
        if hasattr(dt, "tzinfo"):
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        return datetime.fromtimestamp(dt, tz=timezone.utc)
    return None


def insert_rows(ch_client, rows: list):
    for start in range(0, len(rows), BATCH_SIZE):
        ch_client.insert(
            "sensor_timeseries",
            rows[start:start + BATCH_SIZE],
            column_names=["sensor_id", "time", "depth", "variable", "value"],
        )


# ── Conversion ────────────────────────────────────────────────────────────────

def apply_conversion(value: float, canonical: str, variables: dict) -> float:
    info = variables.get(canonical)
    if isinstance(info, dict):
        return value * info.get("conversion_factor", 1.0)
    return value


# Depth-axis units that are actually pressure and need a TEOS-10 conversion,
# not the linear `conversion_factor` used for ordinary data variables.
PRESSURE_UNITS = {"dbar", "decibar"}


def depth_axis_unit(variables: dict) -> str:
    """The declared unit of the `depth` canonical entry, defaulting to metres."""
    return (variables.get("depth") or {}).get("unit", "m")


def resolve_depth_value(raw_value: float, axis_unit: str, latitude: float) -> float:
    """Convert a raw depth-axis reading to depth in metres, positive down.

    Some profilers (e.g. wirewalkers) have no "depth" variable on the wire at
    all — they report pressure in dbar instead. `sensor_timeseries.depth` is
    always metres, so this conversion happens once, here at ingestion, and
    the database never sees a raw pressure value. dbar-to-metres isn't linear
    (it depends on latitude and the local density profile via the seawater
    equation of state), so this uses gsw's TEOS-10 implementation rather than
    a fixed `conversion_factor`. gsw.z_from_p returns height (negative below
    the sea surface); depth is its negation.
    """
    if axis_unit.lower() not in PRESSURE_UNITS:
        return raw_value
    return float(-gsw.z_from_p(raw_value, latitude))


# ── ERDDAP tabledap fetch ─────────────────────────────────────────────────────

def fetch_tabledap_csv(
    base_url: str,
    erddap_cols: list[str],
    date_from: datetime | None,
    include_depth: bool,
    time_col: str,
    depth_col: str,
) -> list[dict]:
    extra = [depth_col] if include_depth else []
    fields = [time_col] + extra + erddap_cols
    query = ",".join(fields)
    constraints = f"&{time_col}>={date_from.strftime(ERDDAP_TIME_FMT)}" if date_from else ""
    url = f"{base_url}.csv?{query}{constraints}"
    print(f"  GET {url}")

    resp = requests.get(url, timeout=120)
    if resp.status_code == 404:
        raise RuntimeError(f"Dataset not found (404): {base_url}")
    if resp.status_code != 200:
        raise RuntimeError(f"ERDDAP HTTP {resp.status_code}: {resp.text[:200]}")

    reader = csv.reader(io.StringIO(resp.text))
    rows_raw = list(reader)
    if len(rows_raw) < 2:
        return []
    headers = rows_raw[0]
    records = []
    for row in rows_raw[2:]:
        if len(row) != len(headers):
            continue
        rec: dict = {}
        valid = True
        for header, value in zip(headers, row):
            if value in ("", "NaN"):
                if header == time_col:
                    valid = False
                continue
            if header == time_col:
                try:
                    rec["time"] = datetime.strptime(value, ERDDAP_TIME_FMT).replace(tzinfo=timezone.utc)
                except ValueError:
                    valid = False
            elif header == depth_col and include_depth:
                try:
                    v = float(value)
                    if not math.isnan(v):
                        rec["depth"] = v
                except ValueError:
                    pass
            else:
                try:
                    v = float(value)
                    if not math.isnan(v):
                        rec[header] = v
                except ValueError:
                    pass
        if valid and "time" in rec:
            records.append(rec)
    return records


def bin_to_hourly(records: list[dict]) -> list[dict]:
    """Average records into hourly slots centred at HH:30:00 UTC."""
    bins: dict[tuple, list[dict]] = {}
    for rec in records:
        t = rec["time"]
        bins.setdefault((t.year, t.month, t.day, t.hour), []).append(rec)
    result = []
    for (y, mo, d, h), group in sorted(bins.items()):
        bin_time = datetime(y, mo, d, h, 30, 0, tzinfo=timezone.utc)
        var_vals: dict[str, list[float]] = {}
        for rec in group:
            for k, v in rec.items():
                if k in ("time", "depth"):
                    continue
                var_vals.setdefault(k, []).append(v)
        avg = {"time": bin_time}
        for var, vals in var_vals.items():
            avg[var] = sum(vals) / len(vals)
        result.append(avg)
    return result


# ── ERDDAP griddap fetch ──────────────────────────────────────────────────────

def fetch_griddap_nc(
    base_url: str,
    erddap_cols: list[str],
    date_from: datetime | None,
    time_col: str,
    depth_col: str,
) -> tuple[np.ndarray, np.ndarray, dict[str, np.ndarray]]:
    """Return (times_epoch, depth_levels, {var: 2D array (n_time, n_depth)})."""
    t_sub = f"({date_from.strftime(ERDDAP_TIME_FMT)}):1:(last)" if date_from else "0:1:last"
    queries = [f"{var}[{t_sub}][0:1:last]" for var in erddap_cols]
    url = f"{base_url}.nc?{','.join(queries)}"
    print(f"  GET {url}")

    resp = requests.get(url, timeout=300, verify=False)
    if resp.status_code == 404:
        raise RuntimeError(f"Dataset not found (404): {base_url}")
    if resp.status_code != 200:
        raise RuntimeError(f"ERDDAP HTTP {resp.status_code}: {resp.text[:200]}")

    ds = netCDF4.Dataset("in_memory.nc", mode="r", memory=resp.content)
    try:
        t_var = ds.variables[time_col]
        units = getattr(t_var, "units", "")
        if "since" in units:
            times_dt = netCDF4.num2date(
                t_var[:], units=units,
                calendar=getattr(t_var, "calendar", "standard"),
            )
            times_epoch = np.array([
                (datetime(d.year, d.month, d.day, d.hour, d.minute, d.second,
                          tzinfo=timezone.utc) - EPOCH).total_seconds()
                for d in times_dt
            ], dtype=np.float64)
        else:
            times_epoch = np.array(t_var[:], dtype=np.float64)

        depth_levels = np.array(ds.variables[depth_col][:], dtype=np.float32)

        grids: dict[str, np.ndarray] = {}
        for var in erddap_cols:
            if var not in ds.variables:
                print(f"  Warning: '{var}' not in griddap response, skipping.")
                continue
            arr = np.array(ds.variables[var][:], dtype=np.float32)
            for fill_attr in ("_FillValue", "missing_value", "fill_value"):
                if hasattr(ds.variables[var], fill_attr):
                    fill = float(getattr(ds.variables[var], fill_attr))
                    arr[arr == fill] = np.nan
            if arr.ndim == 1:
                arr = arr.reshape(len(times_epoch), len(depth_levels))
            grids[var] = arr

        print(f"  {len(times_epoch)} time step(s) × {len(depth_levels)} depth level(s)")
        return times_epoch, depth_levels, grids
    finally:
        ds.close()


# ── Main fetch loop ───────────────────────────────────────────────────────────

def fetch_and_store(sensor_id_filter: str | None = None):
    ch_client = get_ch_client()
    sensors = get_active_erddap_sensors(ch_client, sensor_id_filter)

    if not sensors:
        print("No active ERDDAP sensors found in ClickHouse.")
        return

    for sensor in sensors:
        sensor_id = sensor["id"]
        variables = sensor["variables"]
        link = sensor["link"]
        variable_depth = sensor["depth"] == -1.0
        print(f"\nSensor: {sensor['name']} (ID: {sensor_id})  "
              f"{'variable' if variable_depth else 'fixed'} depth")
        print(f"  Dataset: {link}")

        if not link:
            print("  No source link, skipping.")
            continue

        # Resolve axis column names (allow override via variables mapping)
        time_col  = (variables.get("time")  or {}).get("name", "time")
        depth_col = (variables.get("depth") or {}).get("name", "depth")

        # Build {erddap_col: canonical} for data variables (exclude axis entries)
        erddap_to_canonical = {
            info["name"]: canonical
            for canonical, info in variables.items()
            if canonical not in ("time", "depth")
            and isinstance(info, dict) and info.get("name")
        }
        if not erddap_to_canonical:
            print("  No variable mappings, skipping.")
            continue

        erddap_cols = list(erddap_to_canonical.keys())

        # --- griddap path ------------------------------------------------
        if "/griddap/" in link:
            last_times = [
                get_last_stored_time(ch_client, sensor_id, c)
                for c in erddap_to_canonical.values()
            ]
            valid = [t for t in last_times if t is not None]
            date_from = (min(valid) - timedelta(days=1)) if valid else None

            try:
                times_epoch, depth_levels, grids = fetch_griddap_nc(
                    link, erddap_cols, date_from, time_col, depth_col
                )
            except Exception as e:
                print(f"  ERROR: {e}")
                continue

            # Convert once per level, not per (time, level) cell — the axis
            # unit doesn't vary across the grid.
            axis_unit = depth_axis_unit(variables)
            depth_levels_m = [
                resolve_depth_value(float(d), axis_unit, sensor["latitude"])
                for d in depth_levels
            ]
            if axis_unit.lower() in PRESSURE_UNITS:
                print(f"  Depth axis '{depth_col}' is pressure ({axis_unit}) — converted via gsw.z_from_p")

            for erddap_col, canonical in erddap_to_canonical.items():
                if erddap_col not in grids:
                    continue
                arr = grids[erddap_col]
                rows = []
                for ti, t_epoch in enumerate(times_epoch):
                    t_dt = (EPOCH + timedelta(seconds=float(t_epoch))).replace(tzinfo=None)
                    for di, d in enumerate(depth_levels_m):
                        v = float(arr[ti, di])
                        if math.isnan(v):
                            continue
                        converted = apply_conversion(v, canonical, variables)
                        rows.append([sensor_id, t_dt, float(d), canonical, converted])
                insert_rows(ch_client, rows)
                print(f"  {erddap_col} → '{canonical}': {len(rows)} row(s) inserted")
            continue

        # --- tabledap path -----------------------------------------------
        if "/tabledap/" not in link:
            print(f"  SKIP: link contains neither /tabledap/ nor /griddap/")
            continue

        last_times = [
            get_last_stored_time(ch_client, sensor_id, c)
            for c in erddap_to_canonical.values()
        ]
        valid = [t for t in last_times if t is not None]
        date_from = (min(valid) - timedelta(days=1)) if valid else None
        print(f"  Fetching from: {date_from.strftime(ERDDAP_TIME_FMT) if date_from else 'beginning'}")

        try:
            records = fetch_tabledap_csv(
                link, erddap_cols, date_from,
                include_depth=variable_depth,
                time_col=time_col, depth_col=depth_col,
            )
        except Exception as e:
            print(f"  ERROR: {e}")
            continue

        if not records:
            print("  No data returned.")
            continue
        print(f"  Fetched {len(records)} raw record(s).")

        if not variable_depth:
            records = bin_to_hourly(records)
            fixed_depth = sensor["depth"] if sensor["depth"] >= 0 else 0.0
            print(f"  Binned into {len(records)} hourly slot(s).")
        else:
            # Per-cast depth came straight off the wire in fetch_tabledap_csv —
            # convert once here if that axis was actually pressure.
            axis_unit = depth_axis_unit(variables)
            if axis_unit.lower() in PRESSURE_UNITS:
                print(f"  Depth axis '{depth_col}' is pressure ({axis_unit}) — converted via gsw.z_from_p")
                for rec in records:
                    if "depth" in rec:
                        rec["depth"] = resolve_depth_value(rec["depth"], axis_unit, sensor["latitude"])

        for erddap_col, canonical in erddap_to_canonical.items():
            rows = []
            for rec in records:
                t = rec.get("time")
                v = rec.get(erddap_col)
                if t is None or v is None:
                    continue
                converted = apply_conversion(v, canonical, variables)
                depth_val = rec.get("depth", fixed_depth if not variable_depth else 0.0)
                rows.append([sensor_id, t.replace(tzinfo=None), float(depth_val), canonical, converted])
            insert_rows(ch_client, rows)
            print(f"  {erddap_col} → '{canonical}': {len(rows)} row(s) inserted")

    print("\nDone.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch ERDDAP sensor data → ClickHouse.")
    parser.add_argument("--sensor-id", type=str, default=None, metavar="UUID")
    args = parser.parse_args()
    fetch_and_store(sensor_id_filter=args.sensor_id)
