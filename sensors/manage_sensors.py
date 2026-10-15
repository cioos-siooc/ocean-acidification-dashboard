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

    ONC (5 fields — the 5th is the ONC device category at --location-code):
        canonical:source_name:unit:factor:device_category
        e.g.  dissolved_oxygen:oxygen_corrected:ml/L:44.66:OXYSENSOR
              temperature:temperature:C:1.0:CTD

    A CLI-added ONC variable has one source. For several (child locations,
    fallback instruments), describe the sensor in a catalog file instead.

import
    Upsert sensors from the catalog_*.yml files next to this script (or only
    the --file ones), matched by name. See README "Catalog format".

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
      --api ONC --organization ONC --location-code SCVIP \\
      --variable "dissolved_oxygen:oxygen_corrected:ml/L:44.66:OXYSENSOR" \\
      --variable "temperature:temperature:C:1.0:CTD" \\
      --variable "salinity:salinity:PSU:1.0:CTD"

    # Add an ERDDAP tabledap sensor (fixed depth)
    uv run python sensors/manage_sensors.py add \\
      --name "Hakai Wirewalker" \\
      --lat 50.112 --lon -125.093 --depth 20.0 \\
      --api ERDDAP --organization Hakai \\
      --source-link "https://catalogue.hakai.org/erddap/tabledap/HakaiWirewalker" \\
      --variable "temperature:temperature:C:1.0" \\
      --variable "salinity:salinity:PSU:1.0"

    # Add an ERDDAP griddap sensor (variable depth → depth=-1)
    uv run python sensors/manage_sensors.py add \\
      --name "ORCA Buoy" --lat 47.35 --lon -122.65 --depth -1 \\
      --api ERDDAP --organization UW \\
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
#
# Both the CLI and the catalogs end up as the same two JSON columns:
#   variables     {canonical: {name, unit, conversion_factor}}; `name` is the
#                 ERDDAP column, or the sensor code of the first ONC source.
#   device_config ONC only: {locationCode, sources: {canonical: [
#                 {locationCode, deviceCategoryCode, sensorCategoryCode}, …]}},
#                 sources in priority order (onc_to_ch.py fills each hour from
#                 the first one that has it).

CATALOG_GLOB = "catalog_*.yml"


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
    Build the `variables` and `device_config` JSON dicts from --variable flags.
    An ONC variable's 5th field is its device category at `location_code`.
    """
    variables: dict = {}
    sources: dict[str, list[dict]] = {}

    for s in var_strings:
        canonical, info, device_cat = _parse_variable(s)
        variables[canonical] = info
        if device_cat and location_code:
            sources[canonical] = [{
                "locationCode": location_code,
                "deviceCategoryCode": device_cat,
                "sensorCategoryCode": info["name"],
            }]

    device_config: dict = {}
    if location_code:
        device_config = {"locationCode": location_code, "sources": sources}

    return variables, device_config


def _parse_onc_source(s: str) -> dict:
    """`LOCATION/DEVICE_CATEGORY/sensor_code`, e.g. `CRIP.C1/CTD/temperature`."""
    parts = str(s).split("/")
    if len(parts) != 3 or not all(parts):
        raise ValueError(f"ONC source must be LOCATION/DEVICE_CATEGORY/sensor_code, got {s!r}")
    return {"locationCode": parts[0], "deviceCategoryCode": parts[1], "sensorCategoryCode": parts[2]}


def catalog_variables(entry: dict) -> tuple[dict, dict]:
    """Build the `variables` and `device_config` JSON dicts from a catalog
    entry's `variables:` mapping. ERDDAP variables name their column with
    `code`; ONC ones list `sources` instead."""
    mapping = entry.get("variables") or {}
    if not isinstance(mapping, dict):
        raise ValueError("`variables` must be a mapping of canonical name → {unit, factor, code|sources} "
                         "(the old one-line string list is no longer read)")
    is_onc = entry.get("api") == "ONC"

    variables: dict = {}
    sources: dict[str, list[dict]] = {}
    for canonical, spec in mapping.items():
        spec = spec or {}
        if is_onc:
            if not spec.get("sources"):
                raise ValueError(f"ONC variable {canonical!r} has no `sources`")
            sources[canonical] = [_parse_onc_source(s) for s in spec["sources"]]
            name = sources[canonical][0]["sensorCategoryCode"]
        else:
            if not spec.get("code"):
                raise ValueError(f"ERDDAP variable {canonical!r} has no `code`")
            name = spec["code"]
        variables[canonical] = {
            "name": name,
            "unit": "" if spec.get("unit") is None else str(spec["unit"]),
            "conversion_factor": float(spec.get("factor", 1.0)),
        }

    device_config: dict = {}
    if is_onc:
        if not entry.get("location_code"):
            raise ValueError("ONC sensors need a `location_code`")
        device_config = {"locationCode": entry["location_code"], "sources": sources}
    return variables, device_config


def load_catalogs(paths: list[Path]) -> list[dict]:
    """Every sensor entry across `paths`, each tagged with its file. A name
    may appear only once in all of them, since import matches sensors by name."""
    entries: list[dict] = []
    seen: dict[str, Path] = {}
    for path in paths:
        with open(path) as f:
            catalog = yaml.safe_load(f) or {}
        for entry in catalog.get("sensors") or []:
            name = entry["name"]
            if name in seen:
                raise ValueError(f"sensor {name!r} is in both {seen[name].name} and {path.name}")
            seen[name] = path
            entries.append({**entry, "_file": path.name})
    return entries


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
            row.get("organization", ""),
        ]],
        column_names=["id", "name", "latitude", "longitude", "depth",
                      "variables", "device_config", "source", "active", "updated_at",
                      "organization"],
    )


def _get_sensor(ch_client, sensor_id: str) -> dict | None:
    rows = ch_client.query(
        f"SELECT id, name, latitude, longitude, depth, variables, "
        f"device_config, source, active, organization "
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
        "organization": r[9] or "",
    }


# ── Commands ──────────────────────────────────────────────────────────────────

# Placeholder that marks pre-`source` rows of a multi-source variable for dropping.
DROP_UNATTRIBUTED = "__unattributed__"

# `source` is the instrument a value came from (ONC: LOCATION/DEVICE_CATEGORY;
# '' for ERDDAP). It is part of the key so every instrument at a site keeps its
# own series; readers merge them by the catalog's priority order.
TIMESERIES_DDL = """
CREATE TABLE IF NOT EXISTS {name}
(
    sensor_id  UUID                      CODEC(ZSTD(4)),
    time       DateTime                  CODEC(DoubleDelta, ZSTD(4)),
    depth      Float32                   CODEC(Gorilla(4),  ZSTD(4)),
    variable   LowCardinality(String),
    value      Float32                   CODEC(Gorilla(4),  ZSTD(4)),
    source     LowCardinality(String)    DEFAULT ''
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(time)
ORDER BY (sensor_id, variable, source, depth, time)
"""


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
    updated_at    DateTime DEFAULT now(),
    organization  String DEFAULT ''
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;

""" + TIMESERIES_DDL.format(name="default.sensor_timeseries") + ";"


def cmd_setup(ch_client, _args):
    for statement in SCHEMA_SQL.strip().split(";"):
        statement = statement.strip()
        if statement:
            ch_client.command(statement)
    print("Tables created (or already exist):")
    print("  default.sensors")
    print("  default.sensor_timeseries")
    key = ch_client.query(
        "SELECT sorting_key FROM system.tables WHERE database = 'default' AND name = 'sensor_timeseries'"
    ).result_rows[0][0]
    if "source" not in key:
        print("sensor_timeseries predates the per-instrument key — run `migrate-source-key`.")


def cmd_migrate_source_key(ch_client, _args):
    """Rebuild sensor_timeseries with `source` in its sorting key (ClickHouse
    can't add an existing column to one), copying every row. Rows stored before
    the column existed get their variable's source when it has only one; for a
    variable with several, nobody recorded which instrument a row came from, so
    those rows are dropped and the next harvest re-fetches each source. Run it
    after `import --overwrite`, with the sensors scheduler stopped."""
    key = ch_client.query(
        "SELECT sorting_key FROM system.tables WHERE database = 'default' AND name = 'sensor_timeseries'"
    ).result_rows
    if not key:
        print("No sensor_timeseries table — run `setup`.")
        return
    if "source" in key[0][0]:
        print("sensor_timeseries is already keyed by source.")
        return

    ch_client.command("ALTER TABLE default.sensor_timeseries "
                      "ADD COLUMN IF NOT EXISTS source LowCardinality(String) DEFAULT ''")

    mapping: list[list] = []
    for sensor_id, depth, device_config in ch_client.query(
        "SELECT id, depth, device_config FROM sensors FINAL"
    ).result_rows:
        sources = (json.loads(device_config) if device_config else {}).get("sources") or {}
        for variable, srcs in sources.items():
            # A profiler only ever harvested its first source.
            if len(srcs) == 1 or float(depth) == -1.0:
                mapping.append([sensor_id, variable, f"{srcs[0]['locationCode']}/{srcs[0]['deviceCategoryCode']}"])
            else:
                mapping.append([sensor_id, variable, DROP_UNATTRIBUTED])

    ch_client.command("DROP TABLE IF EXISTS default.sensor_source_map")
    ch_client.command("CREATE TABLE default.sensor_source_map "
                      "(sensor_id UUID, variable String, source String) ENGINE = Memory")
    if mapping:
        ch_client.insert("default.sensor_source_map", mapping, column_names=["sensor_id", "variable", "source"])

    ch_client.command("DROP TABLE IF EXISTS default.sensor_timeseries_new")
    ch_client.command(TIMESERIES_DDL.format(name="default.sensor_timeseries_new"))
    ch_client.command(f"""
        INSERT INTO default.sensor_timeseries_new
        SELECT t.sensor_id, t.time, t.depth, t.variable, t.value,
               if(t.source != '', t.source, m.source)
        FROM default.sensor_timeseries AS t
        LEFT JOIN default.sensor_source_map AS m
               ON t.sensor_id = m.sensor_id AND t.variable = m.variable
        WHERE NOT (t.source = '' AND m.source = '{DROP_UNATTRIBUTED}')
    """)
    count = lambda t: ch_client.query(f"SELECT count() FROM default.{t}").result_rows[0][0]
    before, after = count("sensor_timeseries"), count("sensor_timeseries_new")

    ch_client.command("EXCHANGE TABLES default.sensor_timeseries AND default.sensor_timeseries_new")
    ch_client.command("DROP TABLE IF EXISTS default.sensor_timeseries_old")
    ch_client.command("RENAME TABLE default.sensor_timeseries_new TO default.sensor_timeseries_old")
    ch_client.command("DROP TABLE default.sensor_source_map")
    print(f"Rebuilt sensor_timeseries: {before} row(s) copied as {after} "
          f"({before - after} unattributed multi-source row(s) dropped for re-fetch).")
    print("The previous table is kept as sensor_timeseries_old; drop it once the app looks right:")
    print("  DROP TABLE default.sensor_timeseries_old")


def cmd_list(ch_client, _args):
    rows = ch_client.query(
        "SELECT id, name, depth, source, active, organization FROM sensors FINAL ORDER BY name"
    ).result_rows
    if not rows:
        print("No sensors in ClickHouse.")
        return
    print(f"{'Active':>6}  {'Depth':>7}  {'API':>6}  {'Org':<10}  {'ID':<36}  Name")
    print("─" * 90)
    for r in rows:
        src = json.loads(r[3]) if r[3] else {}
        flag = "✓" if r[4] else "✗"
        org = r[5] or ""
        print(f"{flag:>6}  {r[2]:>7.1f}  {src.get('api','?'):>6}  {org:<10}  {str(r[0]):<36}  {r[1]}")


def cmd_add(ch_client, args):
    if args.api == "ONC" and not args.location_code:
        print("ERROR: --location-code is required for ONC sensors.", file=sys.stderr)
        sys.exit(1)
    if args.api == "ERDDAP" and not args.source_link:
        print("ERROR: --source-link is required for ERDDAP sensors.", file=sys.stderr)
        sys.exit(1)
    if not args.variable:
        print("ERROR: at least one --variable is required.", file=sys.stderr)
        sys.exit(1)

    # Check for an existing sensor with the same name
    existing = ch_client.query(
        f"SELECT id FROM sensors FINAL WHERE name = '{args.name.replace(chr(39), chr(39)*2)}'"
    ).result_rows
    if existing:
        existing_id = str(existing[0][0])
        print(f"ERROR: a sensor named '{args.name}' already exists ({existing_id}).", file=sys.stderr)
        print(f"  To update it:  uv run python manage_sensors.py update --id {existing_id} ...", file=sys.stderr)
        sys.exit(1)

    variables, device_config = _build_variables_and_device_config(
        args.variable, args.location_code
    )
    source: dict = {"api": args.api}
    if args.source_link:
        source["link"] = args.source_link
    if args.description:
        source["description"] = args.description
    if args.constraints:
        source["constraints"] = args.constraints

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
        "organization": args.organization or "",
        "active": 0 if args.inactive else 1,
    }
    _upsert(ch_client, row)
    print(f"Added sensor  {sensor_id}  ({args.name})")
    print(f"Variables:    {', '.join(variables)}")
    if device_config.get("sources"):
        print(f"ONC sources:  " +
              ", ".join(f"{c['locationCode']}/{c['deviceCategoryCode']}/{c['sensorCategoryCode']}"
                        for srcs in device_config["sources"].values() for c in srcs))
    backfill_script = "onc_to_ch.py" if args.api == "ONC" else "erddap_to_ch.py"
    print(f"\nTo backfill:  uv run python sensors/{backfill_script} --sensor-id {sensor_id}")


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
    if args.description:
        sensor["source"]["description"] = args.description
    if args.constraints:
        sensor["source"]["constraints"] = args.constraints
    if args.api:
        sensor["source"]["api"] = args.api
    if args.organization is not None:
        sensor["organization"] = args.organization

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
    """Batch-upsert sensors from the catalog files."""
    paths = [Path(f) for f in args.file] if args.file else \
        sorted(Path(__file__).resolve().parent.glob(CATALOG_GLOB))
    missing = [p for p in paths if not p.exists()]
    if missing or not paths:
        print(f"ERROR: no catalog file at {', '.join(map(str, missing)) or CATALOG_GLOB}", file=sys.stderr)
        sys.exit(1)

    try:
        entries = load_catalogs(paths)
    except (ValueError, KeyError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    if not entries:
        print("No sensors found in catalog.")
        return
    print(f"Reading {', '.join(p.name for p in paths)}")

    # Build a name→uuid index of what's already in CH
    existing = {
        row[1]: str(row[0])
        for row in ch_client.query(
            "SELECT id, name FROM sensors FINAL"
        ).result_rows
    }

    added = skipped = failed = 0
    for entry in entries:
        name = entry["name"]
        if name in existing and not args.overwrite:
            print(f"  SKIP  {name}  (already exists, use --overwrite to update)")
            skipped += 1
            continue

        try:
            variables, device_config = catalog_variables(entry)
        except ValueError as e:
            print(f"  ERROR {name}  ({entry['_file']}): {e}", file=sys.stderr)
            failed += 1
            continue

        source: dict = {"api": entry["api"]}
        if entry.get("source_link"):
            source["link"] = entry["source_link"]
        if entry.get("description"):
            source["description"] = entry["description"]
        if entry.get("constraints"):
            source["constraints"] = entry["constraints"]

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
            "organization": entry.get("organization", ""),
            "active": 0 if entry.get("inactive") else 1,
        }
        _upsert(ch_client, row)
        verb = "UPDATE" if (name in existing and args.overwrite) else "  ADD "
        print(f"  {verb}  {name}  ({sensor_id})")
        added += 1

    print(f"\n{added} sensor(s) added/updated, {skipped} skipped, {failed} failed.")
    if added:
        print("Run onc_to_ch.py or erddap_to_ch.py (no --sensor-id) to backfill all new sensors.")
    if failed:
        sys.exit(1)


def _add_variable_args(p: argparse.ArgumentParser):
    p.add_argument("--variable", action="append",
                   metavar="CANONICAL:SOURCE:UNIT:FACTOR[:DEVICE_CAT]",
                   help="Variable mapping. Repeat for each variable. "
                        "ONC sensors: add :deviceCategoryCode as 5th field.")
    p.add_argument("--location-code", metavar="CODE",
                   help="ONC location code (required for ONC sensors).")
    p.add_argument("--source-link", metavar="URL",
                   help="Reference URL for the sensor (ERDDAP dataset URL, or ONC "
                        "dataSearchURL — required for ERDDAP sensors).")
    p.add_argument("--description", metavar="TEXT",
                   help="Human-readable description shown in the app.")
    p.add_argument("--constraints", metavar="EXPR",
                   help="Extra ERDDAP tabledap constraints appended to every fetch, "
                        "e.g. 'STN_ID=\"C46146\"&SSTP_UQL<=2'. Use for datasets that "
                        "hold many stations in one tabledap dataset.")


def main():
    parser = argparse.ArgumentParser(
        prog="manage_sensors.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Manage sensors in ClickHouse.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("setup", help="Create CH tables (safe to re-run, uses IF NOT EXISTS).")
    sub.add_parser("list", help="List all sensors.")
    sub.add_parser("migrate-source-key",
                   help="Rebuild sensor_timeseries keyed by source (one-off, after import --overwrite).")

    imp_p = sub.add_parser("import", help="Batch-add sensors from the catalog_*.yml files.")
    imp_p.add_argument("--file", action="append", metavar="PATH",
                       help="Import only this catalog file (repeatable). "
                            "Default: every catalog_*.yml next to this script.")
    imp_p.add_argument("--overwrite", action="store_true",
                       help="Update sensors that already exist by name.")

    add_p = sub.add_parser("add", help="Add a new sensor (UUID auto-generated).")
    add_p.add_argument("--name", required=True)
    add_p.add_argument("--lat", type=float, required=True)
    add_p.add_argument("--lon", type=float, required=True)
    add_p.add_argument("--depth", type=float, required=True,
                       help="Metres; use -1 for variable-depth sensors.")
    add_p.add_argument("--api", required=True, choices=["ONC", "ERDDAP"],
                       help="Data retrieval method.")
    add_p.add_argument("--organization", required=True,
                       help="Owning organization (e.g. ONC, Hakai, UW).")
    add_p.add_argument("--inactive", action="store_true")
    _add_variable_args(add_p)

    upd_p = sub.add_parser("update", help="Update fields on an existing sensor.")
    upd_p.add_argument("--id", required=True, metavar="UUID")
    upd_p.add_argument("--name")
    upd_p.add_argument("--lat", type=float)
    upd_p.add_argument("--lon", type=float)
    upd_p.add_argument("--depth", type=float)
    upd_p.add_argument("--api", choices=["ONC", "ERDDAP"])
    upd_p.add_argument("--organization")
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
    elif args.command == "migrate-source-key":
        cmd_migrate_source_key(ch_client, args)
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
