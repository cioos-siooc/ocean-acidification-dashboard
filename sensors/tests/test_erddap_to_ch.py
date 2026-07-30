import gsw
import pytest

from erddap_to_ch import PRESSURE_UNITS, depth_axis_unit, resolve_depth_value


def test_resolve_depth_value_passes_through_metres():
    assert resolve_depth_value(41.4, "m", latitude=49.0) == 41.4


def test_resolve_depth_value_passes_through_when_unit_missing():
    # default depth_axis_unit() result when no "depth" entry is configured
    assert resolve_depth_value(20.0, "m", latitude=49.0) == 20.0


@pytest.mark.parametrize("unit", ["dbar", "decibar", "DBAR"])
def test_resolve_depth_value_converts_pressure(unit):
    lat = 50.5744  # Bute Inlet Wirewalker latitude
    depth = resolve_depth_value(100.0, unit, latitude=lat)
    # Matches the TEOS-10 reference conversion directly (not a re-derivation
    # of gsw's own formula — this pins the sign convention: gsw.z_from_p
    # returns height, negative below the surface, so depth is its negation).
    assert depth == pytest.approx(-gsw.z_from_p(100.0, lat))
    assert depth > 0  # depth is positive-down in this codebase
    # 100 dbar is close to but not exactly 100m — a real conversion happened,
    # not a pass-through or a naive 1:1 approximation.
    assert depth != 100.0
    assert depth == pytest.approx(99.12, abs=0.05)


def test_resolve_depth_value_pressure_increases_monotonically_with_depth():
    lat = 50.0
    shallow = resolve_depth_value(10.0, "dbar", latitude=lat)
    deep = resolve_depth_value(500.0, "dbar", latitude=lat)
    assert deep > shallow


def test_depth_axis_unit_defaults_to_metres():
    assert depth_axis_unit({}) == "m"
    assert depth_axis_unit({"temperature": {"name": "temp", "unit": "C"}}) == "m"


def test_depth_axis_unit_reads_declared_unit():
    variables = {"depth": {"name": "pressure", "unit": "dbar", "conversion_factor": 1.0}}
    assert depth_axis_unit(variables) == "dbar"


def test_pressure_units_set_is_case_handled_by_callers():
    # resolve_depth_value lowercases before checking; the set itself stays lowercase
    assert PRESSURE_UNITS == {"dbar", "decibar"}
