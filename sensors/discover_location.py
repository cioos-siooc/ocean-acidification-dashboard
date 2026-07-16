"""
discover_location.py

Query the ONC Oceans 3.0 API to find what sensors are available at a given
location, then print a ready-to-paste catalog.yaml entry.

Usage
-----
    uv run python discover_location.py SCVIP
    uv run python discover_location.py SCVIP SEVIP FGPD   # multiple at once

The script prints a YAML block for each location.  Review the output, remove
any variables you don't want, then paste into sensors/catalog.yaml.

Canonical variable mapping (extend as needed):
    ONC sensor code       →  canonical name in this system
    temperature           →  temperature
    salinity              →  salinity
    oxygen_corrected      →  dissolved_oxygen
    pressure              →  pressure
    conductivity          →  conductivity
    chlorophyll           →  chlorophyll
    turbidity             →  turbidity
    co2_partial_pressure  →  co2_partial_pressure
"""

import argparse
import json
import os
import sys

from onc import ONC

TOKEN = os.getenv("ONC_TOKEN", "7d291a6a-b57e-49cd-acb1-83f59010d32b")
onc = ONC(TOKEN)

# Map ONC sensor category codes → canonical names used in this system.
# Add entries here as new sensor types are encountered.
CANONICAL = {
    "temperature":           ("temperature",           "C",      1.0),
    "salinity":              ("salinity",              "PSU",    1.0),
    "oxygen_corrected":      ("dissolved_oxygen",      "ml/L",  44.66),
    "oxygen":                ("dissolved_oxygen",      "ml/L",  44.66),
    # "pressure":              ("pressure",              "dbar",   1.0),
    # "conductivity":          ("conductivity",          "S/m",    1.0),
    "chlorophyll":           ("chlorophyll",           "µg/L",   1.0),
    # "turbidity":             ("turbidity",             "NTU",    1.0),
    "co2_partial_pressure":  ("co2_partial_pressure",  "µatm",   1.0),
    "ph":                    ("ph_total",              "",       1.0),
}


def discover(location_code: str, show_unmapped: bool = False) -> str:
    # ── Location metadata ─────────────────────────────────────────────────────
    loc_result = onc.getLocations({"locationCode": location_code})
    if not loc_result:
        return f"# ERROR: location '{location_code}' not found in ONC API\n"

    loc = loc_result[0]
    name        = loc.get("locationName", location_code)
    lat         = loc.get("lat") or loc.get("latitude", 0.0)
    lon         = loc.get("lon") or loc.get("longitude", 0.0)
    depth       = loc.get("depth", 0.0) or 0.0
    # ONC's "description" is frequently blank (populated mainly for named
    # sites, not generic platforms/buoys) — dataSearchURL is always present
    # so it's kept separately as the reference link regardless.
    description = (loc.get("description") or "").strip()
    source_link = loc.get("dataSearchURL", "")

    # ── Device categories at this location ────────────────────────────────────
    dev_cats = onc.getDeviceCategories({"locationCode": location_code})
    if not dev_cats:
        return (
            f"# WARNING: no device categories found for '{location_code}'\n"
            f"# - name: \"{name}\"\n"
            f"#   lat: {lat}\n#   lon: {lon}\n#   depth: {depth}\n"
        )

    # ── Sensor codes per device category ─────────────────────────────────────
    # Collect all (canonical_name, onc_code, unit, factor, dev_code) tuples,
    # then deduplicate by canonical name keeping the highest-priority device.
    # Priority: specialist devices (CTD, OXYSENSOR) beat general ones (ADCP, etc.)
    DEVICE_PRIORITY = {"CTD": 0, "OXYSENSOR": 1, "FLUOROMETER": 2, "PHTPH": 3}

    best: dict[str, tuple] = {}   # canonical → (priority, onc_code, unit, factor, dev_code)
    unmapped: list[str] = []

    for dev in dev_cats:
        dev_code = dev.get("deviceCategoryCode", "")
        priority = DEVICE_PRIORITY.get(dev_code, 99)
        sensor_cats = onc.getSensorCategoryCodes({
            "locationCode": location_code,
            "deviceCategoryCode": dev_code,
        })
        for sc in (sensor_cats or []):
            code = sc.get("sensorCategoryCode", "")
            if code not in CANONICAL:
                unit_raw = sc.get("unitOfMeasure", "?")
                entry = f'    # - "UNMAPPED:{code}:{unit_raw}:1.0:{dev_code}"'
                if entry not in unmapped:
                    unmapped.append(entry)
                continue
            canonical, unit, factor = CANONICAL[code]
            existing = best.get(canonical)
            if existing is None or priority < existing[0]:
                best[canonical] = (priority, code, unit, factor, dev_code)

    variable_lines = [
        f'    - "{canonical}:{v[1]}:{v[2]}:{v[3]}:{v[4]}"'
        for canonical, v in sorted(best.items())
    ]

    if not variable_lines:
        variable_lines = ["    # No mappable variables found"]
    if unmapped and show_unmapped:
        variable_lines += ["    # ── unmapped (not in CANONICAL dict) ──"] + unmapped

    vars_block = "\n".join(variable_lines)

    desc_line = f"    description: {json.dumps(description)}\n" if description else ""

    return f"""\
  - name: "{name}"
    lat: {lat}
    lon: {lon}
    depth: {depth}
    api: ONC
    organization: ONC
    location_code: {location_code}
    source_link: {json.dumps(source_link)}
{desc_line}    variables:
{vars_block}
"""


def main():
    parser = argparse.ArgumentParser(
        description="Discover ONC sensor variables for one or more location codes."
    )
    parser.add_argument("location_codes", nargs="+", metavar="LOCATION_CODE")
    parser.add_argument("--all", dest="show_unmapped", action="store_true",
                        help="Also show unmapped sensor codes as commented-out lines.")
    args = parser.parse_args()

    print("# Generated by discover_location.py — review before pasting into catalog.yaml")
    if args.show_unmapped:
        print("# Unmapped variables shown as comments — add to CANONICAL dict to enable.\n")
    print("sensors:")
    failed = []
    for code in args.location_codes:
        try:
            print(discover(code.upper(), show_unmapped=args.show_unmapped))
        except Exception as e:
            failed.append((code.upper(), str(e)))

    if failed:
        print("\n# Failed location codes:", file=sys.stderr)
        for code, err in failed:
            print(f"#   {code}: {err}", file=sys.stderr)


if __name__ == "__main__":
    main()
