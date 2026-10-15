from pathlib import Path

import pytest

from manage_sensors import CATALOG_GLOB, catalog_variables, load_catalogs

SENSORS_DIR = Path(__file__).resolve().parent.parent


def test_onc_variable_sources_in_priority_order():
    variables, device_config = catalog_variables({
        "api": "ONC",
        "location_code": "CRIP",
        "variables": {"dissolved_oxygen": {"unit": "ml/L", "factor": 44.66, "sources": [
            "CRIP.O1/OXYSENSOR/oxygen_corrected",
            "CRIP/WETLABS_WQM/oxygen",
        ]}},
    })
    # `name` is the first source's code — what the frontend shows as the source name
    assert variables == {"dissolved_oxygen": {"name": "oxygen_corrected", "unit": "ml/L", "conversion_factor": 44.66}}
    assert device_config == {"locationCode": "CRIP", "sources": {"dissolved_oxygen": [
        {"locationCode": "CRIP.O1", "deviceCategoryCode": "OXYSENSOR", "sensorCategoryCode": "oxygen_corrected"},
        {"locationCode": "CRIP", "deviceCategoryCode": "WETLABS_WQM", "sensorCategoryCode": "oxygen"},
    ]}}


def test_erddap_variable_uses_code_and_has_no_device_config():
    variables, device_config = catalog_variables({
        "api": "ERDDAP",
        "variables": {"ph_total": {"code": "pH_total", "unit": "", "factor": 1}},
    })
    assert variables == {"ph_total": {"name": "pH_total", "unit": "", "conversion_factor": 1.0}}
    assert device_config == {}


@pytest.mark.parametrize("entry, message", [
    ({"api": "ONC", "location_code": "X", "variables": ["temperature:temperature:C:1.0:CTD"]}, "mapping"),
    ({"api": "ONC", "location_code": "X", "variables": {"temperature": {"unit": "C"}}}, "no `sources`"),
    ({"api": "ONC", "location_code": "X", "variables": {"temperature": {"sources": ["X/CTD"]}}}, "LOCATION/"),
    ({"api": "ONC", "variables": {"temperature": {"sources": ["X/CTD/temperature"]}}}, "location_code"),
    ({"api": "ERDDAP", "variables": {"temperature": {"unit": "C"}}}, "no `code`"),
])
def test_invalid_entries_are_rejected(entry, message):
    with pytest.raises(ValueError, match=message):
        catalog_variables(entry)


def test_duplicate_names_across_catalogs_are_rejected(tmp_path):
    for f in ("catalog_a.yml", "catalog_b.yml"):
        (tmp_path / f).write_text("sensors:\n  - name: Same\n")
    with pytest.raises(ValueError, match="both"):
        load_catalogs(sorted(tmp_path.glob("*.yml")))


def test_repo_catalogs_all_load():
    entries = load_catalogs(sorted(SENSORS_DIR.glob(CATALOG_GLOB)))
    assert entries
    for entry in entries:
        catalog_variables(entry)
