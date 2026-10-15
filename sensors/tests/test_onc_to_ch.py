from datetime import datetime, timedelta

import pytest

from onc_to_ch import PROFILER_CHUNK_DAYS, bin_profile, floor_to_bin, plan_windows, variable_sources


def test_floor_to_bin_drops_to_the_hour():
    assert floor_to_bin(datetime(2026, 9, 1, 7, 38, 12, 500)) == datetime(2026, 9, 1, 7)


def test_bin_profile_averages_within_a_cell_and_splits_by_depth():
    t = datetime(2026, 9, 1, 7, 31)
    rows = bin_profile([
        (t, 122.6, 9.0),
        (t + timedelta(minutes=25), 122.7, 10.0),  # same hour, rounds to 123 m
        (t + timedelta(minutes=1), 84.9, 10.6),    # same hour, different depth
        (t + timedelta(minutes=29), 122.6, 11.0),  # 08:00 — next cell
    ])
    assert rows == [
        (datetime(2026, 9, 1, 7), 85.0, 10.6),
        (datetime(2026, 9, 1, 7), 123.0, 9.5),
        (datetime(2026, 9, 1, 8), 123.0, 11.0),
    ]


def test_bin_profile_has_no_negative_zero_depth():
    # A sample just above the surface must share the 0 m cell's key,
    # or ReplacingMergeTree would keep two rows for one cell.
    rows = bin_profile([(datetime(2026, 1, 1), -0.2, 1.0)])
    assert str(rows[0][1]) == "0.0"


def test_plan_windows_clips_to_range_and_chunks():
    dep = [(datetime(2026, 1, 1), None)]
    windows = plan_windows(dep, datetime(2026, 1, 10), datetime(2026, 1, 20))
    assert windows[0] == (datetime(2026, 1, 10), datetime(2026, 1, 10) + timedelta(days=PROFILER_CHUNK_DAYS))
    assert windows[-1][1] == datetime(2026, 1, 20)
    assert all(b - a <= timedelta(days=PROFILER_CHUNK_DAYS) for a, b in windows)


def test_plan_windows_skips_undeployed_gaps_and_ended_deployments():
    deps = [
        (datetime(2025, 1, 1), datetime(2025, 1, 3)),   # recovered before date_from
        (datetime(2025, 6, 1), datetime(2025, 6, 2)),
        (datetime(2026, 1, 1), None),
    ]
    windows = plan_windows(deps, datetime(2025, 5, 1), datetime(2026, 1, 2))
    assert windows == [
        (datetime(2025, 6, 1), datetime(2025, 6, 2)),
        (datetime(2026, 1, 1), datetime(2026, 1, 2)),
    ]


@pytest.mark.parametrize("date_from", [None, datetime(2024, 1, 1)])
def test_plan_windows_starts_at_first_deployment_when_nothing_stored(date_from):
    deps = [(datetime(2025, 6, 1), datetime(2025, 6, 3))]
    assert plan_windows(deps, date_from, datetime(2026, 1, 1)) == [(datetime(2025, 6, 1), datetime(2025, 6, 3))]


def test_variable_sources_keeps_priority_order():
    config = {"locationCode": "CRIP", "sources": {"dissolved_oxygen": [
        {"locationCode": "CRIP.O1", "deviceCategoryCode": "OXYSENSOR", "sensorCategoryCode": "oxygen_corrected"},
        {"locationCode": "CRIP", "deviceCategoryCode": "WETLABS_WQM", "sensorCategoryCode": "oxygen"},
    ]}}
    assert variable_sources(config) == {"dissolved_oxygen": [
        ("CRIP.O1", "OXYSENSOR", "oxygen_corrected"),
        ("CRIP", "WETLABS_WQM", "oxygen"),
    ]}


def test_variable_sources_empty_for_pre_sources_device_config():
    assert variable_sources({"locationCode": "SCVIP", "codes": [{"deviceCategoryCode": "CTD"}]}) == {}
