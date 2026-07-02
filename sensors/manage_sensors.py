"""
manage_sensors.py

CLI for managing the CH `sensors` table.  Sensor IDs are auto-generated UUIDs.

Commands
--------
list
    Print all sensors with their UUIDs.

add
    Insert a new sensor.  Required: --name, --lat, --lon, --depth,
    --source-type, and at least one --variable.

    For ONC sensors also provide --location-code.
    For ERDDAP sensors also provide --source-link.

    Variable format
    ---------------
    ERDDAP (4 fields):
        canonical:source_name:unit:factor
        e.g.  temperature:temperature:C:1.0

    ONC (5 fields — the 5th ties the variable to its ONC device category):
        canonical:source_name:unit:factor:device_category
        e.g.  dissolved_oxygen:oxygen_corrected:ml/L:44.66:OXYSENSOR
              temperature:temperature:C:1.0:CTD
              salinity:salinity:PSU:1.0:CTD

    The device_category field explicitly pairs each variable with the ONC
    deviceCategoryCode used to fetch it, so there is no ambiguity.

update
    Update one or more fields on an existing sensor (identified by --id UUID).
    Unspecified fields are left unchanged.

deactivate / activate
    Toggle active flag by --id UUID.

Examples
--------
    # Add a new ONC sensor (UUID auto-generated)
    uv run python sensors/manage_sensors.py add \\
      --name "Central Strait of Georgia VENUS platform" \\
      --lat 49.0405 --lon -123.4247 --depth 298.93 \\
      --source-type ONC --location-code SCVIP \\
      --variable "dissolved_oxygen:oxygen_corrected:ml/L:44.66:OXYSENSOR" \\
      --variable "temperature:temperature:C:1.0:CTD" \\
      --variable "salinity:salinity:PSU:1.0:CTD"

    # Add an ERDDAP tabledap sensor (fixed depth)
    uv run python sensors/manage_sensors.py add \\
      --name "Hakai Wirewalker" \\
      --lat 50.112 --lon -125.093 --depth 20.0 \\
      --source-type ERDDAP \\
      --source-link "https://catalogue.hakai.org/erddap/tabledap/HakaiWirewalker" \\
      --variable "temperature:temperature:C:1.0" \\
      --variable "salinity:salinity:PSU:1.0"

    # Add an ERDDAP griddap sensor (variable depth → depth=-1)
    uv run python sensors/manage_sensors.py add \\
      --name "ORCA Buoy" --lat 47.35 --lon -122.65 --depth -1 \\
      --source-type ERDDAP \\
      --source-link "https://nwem.apl.uw.edu/erddap/griddap/orca1_L3" \\
      --variable "temperature:temperature:C:1.0" \\
      --variable "salinity:salinity:PSU:1.0"

    uv run python sensors/manage_sensors.py list
    uv run python sensors/manage_sensors.py deactivate --id <uuid>
    uv run python sensors/manage_sensors.py activate   --id <uuid>
"""

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml

from ch_helpers import get_ch_client


# ── Variable parsing ──────────────────────────────────────────────────────────

def _parse_variable(s: str) -> tuple[str, dict, str | None]:
    """
    Parse a --variable string into (canonical, info_dict, device_category|None).

    ERDDAP format (4 fields):  canonical:source_name:unit:factor
    ONC format   (5 fields):   canonical:source_name:unit:factor:device_category
    """
    parts = s.split(":")
    if len(parts) not in (4, 5):
        raise argparse.ArgumentTypeError(
            f"--variable must have 4 or 5 colon-separated fields "
            f"(canonical:source_name:unit:factor[:device_category]).  Got: {s!r}"
        )
    canonical, source_name, unit, factor_str = parts[:4]
    device_category = parts[4] if len(parts) == 5 else None
    try:
        factor = float(factor_str)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"conversion_factor must be a number (got {factor_str!r})"
        )
    info = {"name": source_name, "unit": unit, "conversion_factor": factor}
    return canonical, info, device_category


def _build_variables_and_device_config(
    var_strings: list[str],
    location_code: str | None,
) -> tuple[dict, dict]:
    """
    Build the `variables` JSON dict and `device_config` JSON dict from the
    parsed --variable flags.

    ONC variables (5 fields) populate device_config.codes.  The device_category
    is the explicit link between a variable and the ONC deviceCategoryCode used
    to request it — no separate --device flag needed.
    """
    variables: dict = {}
    codes: list[dict] = []

    for s in var_strings:
        canonical, info, device_cat = _parse_variable(s)
        variables[canonical] = info
        if device_cat:
            codes.append({
                "deviceCategoryCode": device_cat,
                "sensorCategoryCodes": info["name"],
            })

    device_config: dict = {}
    if location_code:
        device_config = {"locationCode": location_code, "codes": codes}

    return variables, device_config


# ── CH helpers ────────────────────────────────────────────────────────────────

def _upsert(ch_client, row: dict):
    ch_client.insert(
        "sensors",
        [[
            row["id"],               # UUID object or string — CH auto-converts
            row["name"],
            float(row["latitude"]),
            float(row["longitude"]),
            float(row["depth"]),
            json.dumps(row["variables"]),
            json.dumps(row.get("device_config", {})),
            json.dumps(row["source"]),
            int(row["active"]),
            datetime.now(tz=timezone.utc).replace(tzinfo=None),
        ]],
        column_names=["id", "name", "latitude", "longitude", "depth",
                      "variables", "device_config", "source", "active", "updated_at"],
    )


def _get_sensor(ch_client, sensor_id: str) -> dict | None:
    rows = ch_client.query(
        f"SELECT id, name, latitude, longitude, depth, variables, "
        f"device_config, source, active "
        f"FROM sensors FINAL WHERE id = '{sensor_id}'"
    ).result_rows
    if not rows:
        return None
    r = rows[0]
    return {
        "id": str(r[0]),
        "name": r[1],
        "latitude": float(r[2]),
        "longitude": float(r[3]),
        "depth": float(r[4]),
        "variables": json.loads(r[5]) if r[5] else {},
        "device_config": json.loads(r[6]) if r[6] else {},
        "source": json.loads(r[7]) if r[7] else {},
        "active": int(r[8]),
    }


# ── Commands ──────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS default.sensors
(
    id            UUID DEFAULT generateUUIDv4(),
    name          String,
    latitude      Float64,
    longitude     Float64,
    depth         Float32,
    variables     String,
    device_config String,
    source        String,
    active        UInt8,
    updated_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;

CREATE TABLE IF NOT EXISTS default.sensor_timeseries
(
    sensor_id  UUID                      CODEC(ZSTD(4)),
    time       DateTime                  CODEC(DoubleDelta, ZSTD(4)),
    depth      Float32                   CODEC(Gorilla(4),  ZSTD(4)),
    variable   LowCardinality(String),
    value      Float32                   CODEC(Gorilla(4),  ZSTD(4))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(time)
ORDER BY (sensor_id, variable, depth, time);
"""


def cmd_setup(ch_client, _args):
    for statement in SCHEMA_SQL.strip().split(";"):
        statement = statement.strip()
        if statement:
            ch_client.command(statement)
    print("Tables created (or already exist):")
    print("  default.sensors")
    print("  default.sensor_timeseries")


def cmd_list(ch_client, _args):
    rows = ch_client.query(
        "SELECT id, name, depth, source, active FROM sensors FINAL ORDER BY name"
    ).result_rows
    if not rows:
        print("No sensors in ClickHouse.")
        return
    print(f"{'Active':>6}  {'Depth':>7}  {'Type':>5}  {'ID':<36}  Name")
    print("─" * 80)
    for r in rows:
        src = json.loads(r[3]) if r[3] else {}
        flag = "✓" if r[4] else "✗"
        print(f"{flag:>6}  {r[2]:>7.1f}  {src.get('type','?'):>5}  {str(r[0]):<36}  {r[1]}")


def cmd_add(ch_client, args):
    if args.source_type == "ONC" and not args.location_code:
        print("ERROR: --location-code is required for ONC sensors.", file=sys.stderr)
        sys.exit(1)
    if args.source_type == "ERDDAP" and not args.source_link:
        print("ERROR: --source-link is required for ERDDAP sensors.", file=sys.stderr)
        sys.exit(1)
    if not args.variable:
        print("ERROR: at least one --variable is required.", file=sys.stderr)
        sys.exit(1)

    variables, device_config = _build_variables_and_device_config(
        args.variable, args.location_code
    )
    source: dict = {"type": args.source_type}
    if args.source_link:
        source["link"] = args.source_link

    sensor_id = str(uuid.uuid4())
    row = {
        "id": sensor_id,
        "name": args.name,
        "latitude": args.lat,
        "longitude": args.lon,
        "depth": args.depth,
        "variables": variables,
        "device_config": device_config,
        "source": source,
        "active": 0 if args.inactive else 1,
    }
    _upsert(ch_client, row)
    print(f"Added sensor  {sensor_id}  ({args.name})")
    print(f"Variables:    {', '.join(variables)}")
    if device_config.get("codes"):
        print(f"ONC devices:  " +
              ", ".join(f"{c['deviceCategoryCode']}→{c['sensorCategoryCodes']}"
                        for c in device_config["codes"]))
    print(f"\nTo backfill:  uv run python sensors/onc_to_ch.py --sensor-id {sensor_id}")


def cmd_update(ch_client, args):
    sensor = _get_sensor(ch_client, args.id)
    if not sensor:
        print(f"ERROR: sensor {args.id} not found.", file=sys.stderr)
        sys.exit(1)

    if args.name:
        sensor["name"] = args.name
    if args.lat is not None:
        sensor["latitude"] = args.lat
    if args.lon is not None:
        sensor["longitude"] = args.lon
    if args.depth is not None:
        sensor["depth"] = args.depth
    if args.variable:
        variables, device_config = _build_variables_and_device_config(
            args.variable, args.location_code or sensor["device_config"].get("locationCode")
        )
        sensor["variables"] = variables
        sensor["device_config"] = device_config
    if args.source_link:
        sensor["source"]["link"] = args.source_link

    _upsert(ch_client, sensor)
    print(f"Updated sensor {args.id} ({sensor['name']}).")


def cmd_set_active(ch_client, args, active: int):
    sensor = _get_sensor(ch_client, args.id)
    if not sensor:
        print(f"ERROR: sensor {args.id} not found.", file=sys.stderr)
        sys.exit(1)
    sensor["active"] = active
    _upsert(ch_client, sensor)
    verb = "activated" if active else "deactivated"
    print(f"Sensor {args.id} ({sensor['name']}) {verb}.")


# ── CLI wiring ────────────────────────────────────────────────────────────────

def cmd_import(ch_client, args):
    """Batch-insert sensors from a YAML catalog file."""
    path = Path(args.file)
    if not path.exists():
        print(f"ERROR: file not found: {path}", file=sys.stderr)
        sys.exit(1)

    with open(path) as f:
        catalog = yaml.safe_load(f)

    entries = catalog.get("sensors", [])
    if not entries:
        print("No sensors found in catalog.")
        return

    # Build a name→uuid index of what's already in CH
    existing = {
        row[1]: str(row[0])
        for row in ch_client.query(
            "SELECT id, name FROM sensors FINAL"
        ).result_rows
    }

    added = skipped = 0
    for entry in entries:
        name = entry["name"]
        if name in existing and not args.overwrite:
            print(f"  SKIP  {name}  (already exists, use --overwrite to update)")
            skipped += 1
            continue

        var_strings = entry.get("variables", [])
        variables, device_config = _build_variables_and_device_config(
            var_strings, entry.get("location_code")
        )

        source: dict = {"type": entry["source_type"]}
        if entry.get("source_link"):
            source["link"] = entry["source_link"]

        sensor_id = existing[name] if (name in existing and args.overwrite) else str(uuid.uuid4())
        row = {
            "id": sensor_id,
            "name": name,
            "latitude": float(entry["lat"]),
            "longitude": float(entry["lon"]),
            "depth": float(entry["depth"]),
            "variables": variables,
            "device_config": device_config,
            "source": source,
            "active": 0 if entry.get("inactive") else 1,
        }
        _upsert(ch_client, row)
        verb = "UPDATE" if (name in existing and args.overwrite) else "  ADD "
        print(f"  {verb}  {name}  ({sensor_id})")
        added += 1

    print(f"\n{added} sensor(s) added/updated, {skipped} skipped.")
    if added:
        print("Run onc_to_ch.py or erddap_to_ch.py (no --sensor-id) to backfill all new sensors.")


def _add_variable_args(p: argparse.ArgumentParser):
    p.add_argument("--variable", action="append",
                   metavar="CANONICAL:SOURCE:UNIT:FACTOR[:DEVICE_CAT]",
                   help="Variable mapping. Repeat for each variable. "
                        "ONC sensors: add :deviceCategoryCode as 5th field.")
    p.add_argument("--location-code", metavar="CODE",
                   help="ONC location code (required for ONC sensors).")
    p.add_argument("--source-link", metavar="URL",
                   help="ERDDAP dataset URL (required for ERDDAP sensors).")


def main():
    parser = argparse.ArgumentParser(
        prog="manage_sensors.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Manage sensors in ClickHouse.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("setup", help="Create CH tables (safe to re-run, uses IF NOT EXISTS).")
    sub.add_parser("list", help="List all sensors.")

    imp_p = sub.add_parser("import", help="Batch-add sensors from a YAML catalog file.")
    imp_p.add_argument("--file", default="catalog.yaml", metavar="PATH",
                       help="Path to the catalog YAML file (default: catalog.yaml).")
    imp_p.add_argument("--overwrite", action="store_true",
                       help="Update sensors that already exist by name.")

    add_p = sub.add_parser("add", help="Add a new sensor (UUID auto-generated).")
    add_p.add_argument("--name", required=True)
    add_p.add_argument("--lat", type=float, required=True)
    add_p.add_argument("--lon", type=float, required=True)
    add_p.add_argument("--depth", type=float, required=True,
                       help="Metres; use -1 for variable-depth sensors.")
    add_p.add_argument("--source-type", required=True, choices=["ONC", "ERDDAP"])
    add_p.add_argument("--inactive", action="store_true")
    _add_variable_args(add_p)

    upd_p = sub.add_parser("update", help="Update fields on an existing sensor.")
    upd_p.add_argument("--id", required=True, metavar="UUID")
    upd_p.add_argument("--name")
    upd_p.add_argument("--lat", type=float)
    upd_p.add_argument("--lon", type=float)
    upd_p.add_argument("--depth", type=float)
    upd_p.add_argument("--source-type", choices=["ONC", "ERDDAP"])
    _add_variable_args(upd_p)

    for cmd, help_str in [("deactivate", "Set active=0."), ("activate", "Set active=1.")]:
        p = sub.add_parser(cmd, help=help_str)
        p.add_argument("--id", required=True, metavar="UUID")

    args = parser.parse_args()
    ch_client = get_ch_client()

    if args.command == "import":
        cmd_import(ch_client, args)
    elif args.command == "setup":
        cmd_setup(ch_client, args)
    elif args.command == "list":
        cmd_list(ch_client, args)
    elif args.command == "add":
        cmd_add(ch_client, args)
    elif args.command == "update":
        cmd_update(ch_client, args)
    elif args.command == "deactivate":
        cmd_set_active(ch_client, args, active=0)
    elif args.command == "activate":
        cmd_set_active(ch_client, args, active=1)


if __name__ == "__main__":
    main()
