"""
discover_location.py

Query the ONC Oceans 3.0 API to find what sensors are available at a given
location and its child locations (e.g. CRIP → CRIP.C1, CRIP.C2), then print a
catalog entry to review and paste into catalog_ONC.yml.

Usage
-----
    uv run python discover_location.py SCVIP
    uv run python discover_location.py SCVIP SEVIP FGPD   # multiple at once

The script prints a YAML block for each location. Each variable lists every
instrument that reports it as `sources`, most recently deployed first (ties:
specialist devices before general ones) — the order onc_to_ch.py fills hours
in. Review the order, drop sources you don't trust, then paste into
sensors/catalog_ONC.yml.

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
import math
import os
import sys
from datetime import datetime

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


def child_locations(location_code: str) -> list[str]:
    """Location codes under `location_code` in ONC's hierarchy, depth first."""
    def walk(nodes):
        for n in nodes or []:
            yield n.get("locationCode")
            yield from walk(n.get("children"))
    tree = onc.getLocationHierarchy({"locationCode": location_code}) or []
    return [c for n in tree for c in walk(n.get("children"))]


def last_deployed(location_code: str, device_category: str) -> float:
    """Epoch seconds the category's latest deployment there ended (inf if one
    is still in the water, 0 if none)."""
    deps = onc.getDeployments({"locationCode": location_code, "deviceCategoryCode": device_category}) or []
    if any(not d.get("end") for d in deps):
        return math.inf
    ends = [datetime.fromisoformat(d["end"].replace("Z", "+00:00")).timestamp() for d in deps]
    return max(ends, default=0.0)


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

    # ── Sensor codes per device category, here and at child locations ────────
    # Every (location, device category, code) that maps to a canonical name is
    # a source. They're ordered most recently deployed first, so a retired
    # instrument only fills gaps; ties go to specialist devices (CTD,
    # OXYSENSOR) over general ones (ADCP, etc.).
    DEVICE_PRIORITY = {"CTD": 0, "OXYSENSOR": 1, "FLUOROMETER": 2, "PHTPH": 3}

    found: dict[str, list[tuple]] = {}   # canonical → [(sort key, "LOC/CAT/code")]
    units: dict[str, tuple[str, float]] = {}
    unmapped: list[str] = []

    for loc_code in [location_code, *child_locations(location_code)]:
        for dev in onc.getDeviceCategories({"locationCode": loc_code}) or []:
            dev_code = dev.get("deviceCategoryCode", "")
            sensor_cats = onc.getSensorCategoryCodes({
                "locationCode": loc_code,
                "deviceCategoryCode": dev_code,
            })
            mapped = [sc for sc in (sensor_cats or []) if sc.get("sensorCategoryCode", "") in CANONICAL]
            for sc in sensor_cats or []:
                code = sc.get("sensorCategoryCode", "")
                if code not in CANONICAL:
                    entry = f"      # UNMAPPED {loc_code}/{dev_code}/{code} ({sc.get('unitOfMeasure', '?')})"
                    if entry not in unmapped:
                        unmapped.append(entry)
            if not mapped:
                continue
            key = (-last_deployed(loc_code, dev_code), DEVICE_PRIORITY.get(dev_code, 99))
            for sc in mapped:
                code = sc["sensorCategoryCode"]
                canonical, unit, factor = CANONICAL[code]
                units.setdefault(canonical, (unit, factor))
                found.setdefault(canonical, []).append((key, f"{loc_code}/{dev_code}/{code}"))

    variable_lines = []
    for canonical in sorted(found):
        unit, factor = units[canonical]
        sources = [src for _, src in sorted(found[canonical])]
        variable_lines += [
            f"      {canonical}:",
            f"        unit: {json.dumps(unit) if not unit else unit}",
            f"        factor: {factor}",
            "        sources:",
            *[f"        - {src}" for src in sources],
        ]

    if not variable_lines:
        variable_lines = ["      # No mappable variables found"]
    if unmapped and show_unmapped:
        variable_lines += ["      # ── unmapped (not in CANONICAL dict) ──"] + unmapped

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

    print("# Generated by discover_location.py — review before pasting into catalog_ONC.yml")
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
