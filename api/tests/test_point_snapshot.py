from datetime import datetime

import pytest

import point_snapshot
from point_snapshot import SNAPSHOT_VARIABLES, extract_point_snapshot


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClient:
    """Answers the handful of query shapes point_snapshot issues."""

    def __init__(self, grid_row, model_rows=None, sensor_vars_json=None, last_row=None):
        self.grid_row = grid_row
        self.model_rows = model_rows or []
        self.sensor_vars_json = sensor_vars_json
        self.last_row = last_row
        self.closed = False

    def query(self, query, parameters=None):
        q = " ".join(query.lower().split())
        if "from grid_ssc" in q:
            return FakeResult([self.grid_row])
        if "select variables from sensors" in q:
            return FakeResult([(self.sensor_vars_json,)] if self.sensor_vars_json else [])
        if "argmax(value, time)" in q:
            return FakeResult([self.last_row] if self.last_row else [])
        return FakeResult(self.model_rows)

    def close(self):
        self.closed = True


@pytest.fixture
def no_depth_lookup(monkeypatch):
    # Depth levels are cached per table in extractTimeseries; pin the resolver.
    monkeypatch.setattr(point_snapshot, "_resolve_depth", lambda client, table, gx, gy, depth: 0.5)


def _row(t, base):
    return (t, *[base + i for i in range(len(SNAPSHOT_VARIABLES))])


def test_model_values_come_back_per_variable(monkeypatch, no_depth_lookup):
    fake = FakeClient(
        grid_row=(10, 20, 49.2, -123.5, 300.0),
        model_rows=[_row(datetime(2026, 10, 21, 15), 1.0), _row(datetime(2026, 10, 21, 16), 2.0)],
    )
    monkeypatch.setattr(point_snapshot, "get_ch_client", lambda: fake)

    out = extract_point_snapshot(source="SalishSeaCast", lat=49.2, lon=-123.5, depth=0.5,
                                 to="2026-10-21T16:00:00", hours=72)

    model = out["model"]
    assert model["depth"] == 0.5
    assert model["time"] == ["2026-10-21T15:00:00", "2026-10-21T16:00:00"]
    assert model["values"]["temperature"] == [1.0, 2.0]
    assert model["values"]["total_alkalinity"] == [8.0, 9.0]
    assert out["sensor"] is None
    assert fake.closed


def test_outside_domain_keeps_sensor_half(monkeypatch, no_depth_lookup):
    fake = FakeClient(
        grid_row=(10, 20, 60.0, -130.0, 80_000.0),
        sensor_vars_json='{"temperature": {}, "chlorophyll": {}}',
    )
    monkeypatch.setattr(point_snapshot, "get_ch_client", lambda: fake)
    monkeypatch.setattr(point_snapshot, "extract_sensor_timeseries",
                        lambda *a, **k: {"time": ["2026-10-21T10:00:00"], "value": [9.6], "depth": 0.0})

    out = extract_point_snapshot(source="SalishSeaCast", lat=0.0, lon=0.0, depth=0.5,
                                 to="2026-10-21T16:00:00", sensor_id="abc", sensor_depth=41.0)

    assert out["model"] is None
    assert out["modelError"]["code"] == "outside_model_domain"
    # Only variables the app models are reported, in snapshot order.
    assert list(out["sensor"]["variables"]) == ["temperature"]
    assert out["sensor"]["variables"]["temperature"]["last"] == {"time": "2026-10-21T10:00:00", "value": 9.6}
    # The catalog depth, not whatever depth the rows were stored at.
    assert out["sensor"]["depth"] == 41.0


def test_stale_sensor_falls_back_to_last_reading(monkeypatch, no_depth_lookup):
    fake = FakeClient(
        grid_row=(10, 20, 49.2, -123.5, 300.0),
        sensor_vars_json='{"salinity": {}}',
        last_row=(datetime(2025, 12, 14, 3), 30.1),
    )
    monkeypatch.setattr(point_snapshot, "get_ch_client", lambda: fake)
    monkeypatch.setattr(point_snapshot, "extract_sensor_timeseries",
                        lambda *a, **k: {"time": [], "value": [], "depth": 41.0})

    out = extract_point_snapshot(source="SalishSeaCast", lat=49.2, lon=-123.5, depth=0.5,
                                 to="2026-10-21T16:00:00", sensor_id="abc", sensor_depth=41.0)

    sal = out["sensor"]["variables"]["salinity"]
    assert sal["time"] == []
    assert sal["last"] == {"time": "2025-12-14T03:00:00", "value": 30.1}


def test_unknown_source_is_rejected():
    with pytest.raises(ValueError):
        extract_point_snapshot(source="LiveOcean", lat=49.2, lon=-123.5, depth=0.5)
