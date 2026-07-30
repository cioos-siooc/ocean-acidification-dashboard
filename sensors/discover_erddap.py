"""
discover_erddap.py

Query an ERDDAP server to find tabledap datasets and generate ready-to-paste
catalog.yaml entries.

Usage
-----
    # List all tabledap datasets on a server
    uv run python discover_erddap.py https://catalogue.hakai.org/erddap

    # One specific dataset
    uv run python discover_erddap.py https://catalogue.hakai.org/erddap --dataset HakaiBIOOSBuoy1hour

    # Filter by keyword in title or dataset ID
    uv run python discover_erddap.py https://catalogue.hakai.org/erddap --filter buoy

    # Also show unmapped variables as commented-out lines (useful for extending CANONICAL)
    uv run python discover_erddap.py https://catalogue.hakai.org/erddap --filter buoy --all

The script writes catalog YAML to stdout and progress messages to stderr, so
you can redirect stdout to a file cleanly:
    uv run python discover_erddap.py ... > candidates.yaml

Canonical variable mapping (extend CANONICAL below as needed):
    ERDDAP variable name / CF standard_name  →  canonical name in this system
    temperature / sea_water_temperature      →  temperature
    salinity / sea_water_practical_salinity  →  salinity
    oxygen / DOXY                            →  dissolved_oxygen  (check units!)
    chlorophyll / chlorophyll_a              →  chlorophyll
    ph / pH_Total                            →  ph_total
    pco2 / co2_partial_pressure              →  co2_partial_pressure

NOTE on dissolved oxygen units
-------------------------------
ERDDAP sources vary: some report in µmol/L, others in ml/L, others in mg/L.
This script defaults to factor=1.0 and notes the source unit in a comment.
Adjust the factor to match what erddap_to_ch.py expects:
    µmol/L → ml/L : factor = 0.02239   (÷ 44.66)
    ml/L   → ml/L : factor = 1.0
    mg/L   → ml/L : factor = 0.6997

NOTE on profilers that report pressure instead of depth
---------------------------------------------------------
Some datasets (e.g. wirewalkers) have no "depth" variable at all — only
"pressure" (dbar). This script detects a "pressure" axis variable the same
way it detects "depth" (sets depth: -1, variable-depth sensor) and emits a
`"depth:<pressure_col>:dbar:1.0"` variable line instead of a "depth:...:m:..."
one. erddap_to_ch.py checks that unit at ingestion — "dbar" triggers a
TEOS-10 pressure→depth conversion (gsw.z_from_p) instead of treating the raw
number as already being depth in metres. Nothing downstream of ingestion
(sensor_timeseries, the API, the frontend) ever sees a pressure value.
"""

from __future__ import annotations

import argparse
import csv
import io
import sys

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Canonical variable mapping ────────────────────────────────────────────────
# Key: lowercase ERDDAP variable name OR lowercase CF standard_name.
# Value: (canonical_name, unit_stored, default_factor)
# Add entries here as new variable types are encountered.

CANONICAL: dict[str, tuple[str, str, float]] = {
    # ── temperature ───────────────────────────────────────────────────────────
    "temperature":                      ("temperature",           "C",    1.0),
    "sea_water_temperature":            ("temperature",           "C",    1.0),
    "sea_surface_temperature":          ("temperature",           "C",    1.0),
    "watertemp":                        ("temperature",           "C",    1.0),
    "temp":                             ("temperature",           "C",    1.0),
    "wtemp":                            ("temperature",           "C",    1.0),
    # ── salinity ──────────────────────────────────────────────────────────────
    "salinity":                         ("salinity",              "PSU",  1.0),
    "sea_water_practical_salinity":     ("salinity",              "PSU",  1.0),
    "sea_water_salinity":               ("salinity",              "PSU",  1.0),
    "practical_salinity":               ("salinity",              "PSU",  1.0),
    "watersalinity":                    ("salinity",              "PSU",  1.0),
    "psal":                             ("salinity",              "PSU",  1.0),
    # ── dissolved oxygen ──────────────────────────────────────────────────────
    # Factor 1.0 is a safe default; annotate the source unit so the user
    # knows whether conversion is needed (see module docstring).
    "dissolved_oxygen":                 ("dissolved_oxygen",      "ml/L", 1.0),
    "oxygen":                           ("dissolved_oxygen",      "ml/L", 1.0),
    "doxy":                             ("dissolved_oxygen",      "ml/L", 1.0),
    "do":                               ("dissolved_oxygen",      "ml/L", 1.0),
    "mole_concentration_of_dissolved_molecular_oxygen_in_sea_water":
                                        ("dissolved_oxygen",      "ml/L", 1.0),
    "volume_fraction_of_oxygen_in_sea_water":
                                        ("dissolved_oxygen",      "ml/L", 1.0),
    # ── chlorophyll ───────────────────────────────────────────────────────────
    "chlorophyll":                      ("chlorophyll",           "µg/L", 1.0),
    "chlorophyll_a":                    ("chlorophyll",           "µg/L", 1.0),
    "chl":                              ("chlorophyll",           "µg/L", 1.0),
    "chla":                             ("chlorophyll",           "µg/L", 1.0),
    "chl_a":                            ("chlorophyll",           "µg/L", 1.0),
    "mass_concentration_of_chlorophyll_a_in_sea_water":
                                        ("chlorophyll",           "µg/L", 1.0),
    # ── pH ────────────────────────────────────────────────────────────────────
    "ph":                               ("ph_total",              "",     1.0),
    "ph_total":                         ("ph_total",              "",     1.0),
    "sea_water_ph_reported_on_total_scale":
                                        ("ph_total",              "",     1.0),
    # ── pCO₂ ─────────────────────────────────────────────────────────────────
    "co2_partial_pressure":             ("co2_partial_pressure",  "µatm", 1.0),
    "pco2":                             ("co2_partial_pressure",  "µatm", 1.0),
    "xco2_wet":                         ("co2_partial_pressure",  "µatm", 1.0),
    "surface_partial_pressure_of_carbon_dioxide_in_sea_water":
                                        ("co2_partial_pressure",  "µatm", 1.0),
    # ── omega_arag ───────────────────────────────────────────────────────────
    "omega_arag":                       ("omega_arag",            "",     1.0),
    "saturation_state_of_aragonite_in_sea_water":                ("omega_arag",            "",     1.0),
    
    # ── omega_cal ───────────────────────────────────────────────────────────
    "omega_cal":                        ("omega_cal",             "",     1.0),
    "omega_calc":                        ("omega_cal",             "",     1.0),
    "saturation_state_of_calcite_in_sea_water":                 ("omega_cal",             "",     1.0),
}

# Variables that are coordinate axes — not data variables to map.
# "pressure" is included here because some profilers (e.g. wirewalkers) report
# pressure (dbar) instead of depth — it's a vertical axis, not a data variable,
# even though the value needs a TEOS-10 conversion before it means "depth".
AXIS_VARS = {"time", "latitude", "longitude", "depth", "altitude", "z",
             "station", "profile", "trajectory", "obs", "row", "pressure"}


# ── ERDDAP helpers ────────────────────────────────────────────────────────────

def normalize_base(url: str) -> str:
    """Strip trailing slash and any /tabledap or /griddap suffix."""
    url = url.rstrip("/")
    for suffix in ("/tabledap", "/griddap", "/info"):
        if url.endswith(suffix):
            url = url[: -len(suffix)]
    return url


def _get_csv(url: str) -> list[dict]:
    resp = requests.get(url, timeout=60, verify=False)
    resp.raise_for_status()
    reader = csv.DictReader(io.StringIO(resp.text))
    return list(reader)


def list_tabledap_datasets(base: str) -> list[dict]:
    """Return [{"Dataset ID": ..., "Title": ..., ...}] for all accessible tabledap datasets."""
    rows = _get_csv(f"{base}/tabledap/index.csv?page=1&itemsPerPage=10000")
    return [r for r in rows if r.get("Dataset ID") and r["Dataset ID"] != "allDatasets"]


def fetch_info(base: str, dataset_id: str) -> list[dict]:
    return _get_csv(f"{base}/info/{dataset_id}/index.csv")


# ── Info parsing ──────────────────────────────────────────────────────────────

def _parse_info(rows: list[dict]) -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    """Split info rows into global attributes and per-variable attribute dicts."""
    g_attrs: dict[str, str] = {}
    v_attrs: dict[str, dict[str, str]] = {}
    for row in rows:
        row_type = row.get("Row Type", "")
        var = row.get("Variable Name", "")
        attr = row.get("Attribute Name", "")
        val = row.get("Value", "")
        if row_type == "variable":
            v_attrs.setdefault(var, {})
        elif row_type == "attribute":
            if not var or var == "NC_GLOBAL":
                g_attrs[attr] = val
            else:
                v_attrs.setdefault(var, {})[attr] = val
    return g_attrs, v_attrs


def _float(s: str | None, default: float = 0.0) -> float:
    try:
        return float(s)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _infer_location(g: dict[str, str]) -> tuple[float, float, float]:
    """
    Extract lat, lon, depth (metres, positive down) from global attributes.
    Returns depth=0.0 when vertical info is absent.
    """
    lat = (_float(g.get("geospatial_lat_min")) + _float(g.get("geospatial_lat_max"))) / 2
    lon = (_float(g.get("geospatial_lon_min")) + _float(g.get("geospatial_lon_max"))) / 2

    v_min_str = g.get("geospatial_vertical_min")
    if v_min_str is None:
        return lat, lon, 0.0

    v_min = _float(v_min_str)
    v_max = _float(g.get("geospatial_vertical_max"), v_min)
    depth_mid = (v_min + v_max) / 2

    # ERDDAP vertical_positive="up" means values are altitude (negative depth)
    if g.get("geospatial_vertical_positive", "down").lower() == "up":
        depth_mid = -depth_mid

    return lat, lon, round(depth_mid, 3)


# ── Variable mapping ──────────────────────────────────────────────────────────

def _resolve(var_name: str, attrs: dict[str, str]) -> tuple[str, str, float] | None:
    """Try to map one ERDDAP variable to (canonical, unit, factor). Returns None if unmapped."""
    # 1. Variable name (lowercase)
    hit = CANONICAL.get(var_name.lower())
    if hit:
        return hit

    # 2. CF standard_name
    std = attrs.get("standard_name", "").lower()
    if std:
        hit = CANONICAL.get(std)
        if hit:
            return hit

    # 3. long_name normalised (spaces → underscores, lowercase)
    long = attrs.get("long_name", "").lower().replace(" ", "_")
    if long:
        hit = CANONICAL.get(long)
        if hit:
            return hit

    return None


def _map_variables(
    v_attrs: dict[str, dict[str, str]],
    show_unmapped: bool,
) -> tuple[list[str], list[str], bool, str | None]:
    """
    Returns (variable_lines, unmapped_lines, has_depth_variable, pressure_col).
    variable_lines are ready for catalog.yaml. pressure_col is the ERDDAP
    variable name of the pressure axis when the dataset has one and no literal
    "depth" variable — erddap_to_ch.py converts pressure → depth at ingestion
    (see resolve_depth_value), never storing raw pressure.
    """
    mapped: dict[str, tuple[str, str, float, str]] = {}  # canonical → (erddap_col, unit, factor, src_unit)
    unmapped: list[tuple[str, str]] = []
    has_depth_var = False
    pressure_col: str | None = None

    for var_name, attrs in v_attrs.items():
        if var_name.lower() in AXIS_VARS:
            if var_name.lower() == "depth":
                has_depth_var = True
            elif var_name.lower() == "pressure":
                pressure_col = var_name
            continue

        result = _resolve(var_name, attrs)
        if result:
            canonical, unit, factor = result
            if canonical not in mapped:
                src_unit = attrs.get("units", unit)
                mapped[canonical] = (var_name, unit, factor, src_unit)
        else:
            src_unit = attrs.get("units", "?")
            unmapped.append((var_name, src_unit))

    variable_lines: list[str] = []
    # A pressure-only dataset's vertical axis isn't a "depth" ERDDAP variable,
    # but it maps to the same "depth" canonical key erddap_to_ch.py looks for —
    # unit "dbar" is what tells it to run the pressure→depth conversion.
    if not has_depth_var and pressure_col:
        variable_lines.append(f'    - "depth:{pressure_col}:dbar:1.0"  # pressure axis — converted to depth at ingestion')

    for canonical, (erddap_col, unit, factor, src_unit) in sorted(mapped.items()):
        line = f'    - "{canonical}:{erddap_col}:{unit}:{factor}"'
        # Annotate dissolved oxygen with source unit so the user can verify factor
        if canonical == "dissolved_oxygen" and src_unit != unit:
            line += f"  # source unit: {src_unit} — check factor!"
        variable_lines.append(line)

    if not variable_lines:
        variable_lines = ["    # No mappable variables found"]

    unmapped_lines: list[str] = []
    if show_unmapped and unmapped:
        unmapped_lines = ["    # ── unmapped (add to CANONICAL to enable) ──"] + [
            f'    # - "UNMAPPED:{v}:{u}:1.0"' for v, u in unmapped
        ]

    return variable_lines, unmapped_lines, has_depth_var, pressure_col


# ── Per-dataset entry ─────────────────────────────────────────────────────────

def discover_one(base: str, dataset_id: str, show_unmapped: bool) -> str:
    try:
        info_rows = fetch_info(base, dataset_id)
    except requests.HTTPError as e:
        return f"  # ERROR {dataset_id}: {e}\n"

    g_attrs, v_attrs = _parse_info(info_rows)
    title = g_attrs.get("title", dataset_id)
    lat, lon, depth = _infer_location(g_attrs)
    var_lines, unmapped_lines, has_depth_var, pressure_col = _map_variables(v_attrs, show_unmapped)

    if has_depth_var or pressure_col:
        depth = -1  # variable-depth sensor (depth axis, or pressure axis converted to depth)

    link = f"{base}/tabledap/{dataset_id}"
    vars_block = "\n".join(var_lines + unmapped_lines)

    return (
        f'  - name: "{title}"\n'
        f"    lat: {lat}\n"
        f"    lon: {lon}\n"
        f"    depth: {depth}\n"
        f"    api: ERDDAP\n"
        f"    organization: \n"
        f'    source_link: "{link}"\n'
        f"    variables:\n"
        f"{vars_block}\n"
    )


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Discover ERDDAP tabledap datasets and generate catalog.yaml entries."
    )
    parser.add_argument(
        "erddap_url",
        metavar="ERDDAP_URL",
        help="ERDDAP base URL, e.g. https://catalogue.hakai.org/erddap",
    )
    parser.add_argument(
        "--dataset",
        metavar="DATASET_ID",
        help="Only process this one dataset ID.",
    )
    parser.add_argument(
        "--filter",
        metavar="KEYWORD",
        help="Only include datasets whose title or ID contains KEYWORD (case-insensitive).",
    )
    parser.add_argument(
        "--all",
        dest="show_unmapped",
        action="store_true",
        help="Show unmapped variables as commented-out lines.",
    )
    args = parser.parse_args()

    base = normalize_base(args.erddap_url)

    if args.dataset:
        dataset_ids = [args.dataset]
    else:
        print(f"Fetching dataset list from {base} …", file=sys.stderr)
        try:
            datasets = list_tabledap_datasets(base)
        except Exception as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(1)

        if args.filter:
            kw = args.filter.lower()
            datasets = [
                d for d in datasets
                if kw in d.get("Title", "").lower() or kw in d.get("Dataset ID", "").lower()
            ]
            print(f"Filtered to {len(datasets)} dataset(s) matching '{args.filter}'", file=sys.stderr)
        else:
            print(f"Found {len(datasets)} tabledap dataset(s)", file=sys.stderr)

        dataset_ids = [d["Dataset ID"] for d in datasets]

    if not dataset_ids:
        print("No datasets to process.", file=sys.stderr)
        sys.exit(0)

    print("# Generated by discover_erddap.py — review before pasting into catalog.yaml")
    print("sensors:")
    for i, ds_id in enumerate(dataset_ids, 1):
        print(f"  [{i}/{len(dataset_ids)}] {ds_id}", file=sys.stderr, end="\r", flush=True)
        print(discover_one(base, ds_id, args.show_unmapped))

    print(f"\nDone — {len(dataset_ids)} dataset(s) processed.         ", file=sys.stderr)


if __name__ == "__main__":
    main()
