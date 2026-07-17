from discover_erddap import AXIS_VARS, _map_variables


def test_pressure_recognized_as_axis_not_unmapped_data_variable():
    v_attrs = {
        "pressure": {"units": "dbar"},
        "temperature": {"units": "degree_C"},
    }
    variable_lines, unmapped_lines, has_depth_var, pressure_col = _map_variables(
        v_attrs, show_unmapped=True
    )

    assert has_depth_var is False
    assert pressure_col == "pressure"
    # pressure never shows up as an unmapped data variable
    assert not any("pressure" in line.lower() for line in unmapped_lines)


def test_pressure_only_dataset_emits_depth_line_with_dbar_unit():
    v_attrs = {
        "pressure": {"units": "dbar"},
        "temperature": {"units": "degree_C"},
    }
    variable_lines, _, _, pressure_col = _map_variables(v_attrs, show_unmapped=False)

    depth_lines = [l for l in variable_lines if l.strip().startswith('- "depth:')]
    assert len(depth_lines) == 1
    assert f'"depth:{pressure_col}:dbar:1.0"' in depth_lines[0]


def test_literal_depth_variable_still_detected_and_no_pressure_line_emitted():
    v_attrs = {
        "depth": {"units": "m"},
        "temperature": {"units": "degree_C"},
    }
    variable_lines, _, has_depth_var, pressure_col = _map_variables(v_attrs, show_unmapped=False)

    assert has_depth_var is True
    assert pressure_col is None
    assert not any(l.strip().startswith('- "depth:') for l in variable_lines)


def test_fixed_depth_dataset_has_neither_axis():
    v_attrs = {"temperature": {"units": "degree_C"}}
    _, _, has_depth_var, pressure_col = _map_variables(v_attrs, show_unmapped=False)

    assert has_depth_var is False
    assert pressure_col is None


def test_pressure_in_axis_vars():
    assert "pressure" in AXIS_VARS
