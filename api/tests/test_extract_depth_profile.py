from datetime import datetime

import pytest

from extract_depth_profile import extract_depth_profile


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClient:
    def __init__(self, grid_row=None, depths=None, model_rows=None, sensor_rows=None):
        self.grid_row = grid_row
        self.depths = depths or []
        self.model_rows = model_rows or []
        self.sensor_rows = sensor_rows or []
        self.closed = False

    def query(self, query, parameters=None):
        q = " ".join(query.lower().split())
        if "from grid_ssc" in q:
            return FakeResult([self.grid_row])
        if "select distinct depth" in q:
            return FakeResult([(d,) for d in self.depths])
        if "from sensor_timeseries" in q:
            return FakeResult(self.sensor_rows)
        return FakeResult(self.model_rows)  # SalishSeaCast_hourly model query

    def close(self):
        self.closed = True


GRID_ROW = (10, 20, 49.0, -123.0, 500.0)
DEPTHS = [0.0, 5.0, 10.0]

MODEL_ROWS = [
    (datetime(2026, 1, 1, 0), 0.0, 9.0),
    (datetime(2026, 1, 1, 0), 5.0, 8.0),
    (datetime(2026, 1, 1, 0), 10.0, 7.0),
    (datetime(2026, 1, 1, 1), 0.0, 9.1),
    (datetime(2026, 1, 1, 1), 5.0, 8.1),
    (datetime(2026, 1, 1, 1), 10.0, 7.1),
]


def _patch(monkeypatch, fake):
    monkeypatch.setattr('extract_depth_profile.get_ch_client', lambda: fake)


def test_happy_path_grids_shape_and_values(monkeypatch):
    sensor_rows = [
        (datetime(2026, 1, 1, 0, 5), 0.3, 9.4),    # hour0, nearest level 0.0
        (datetime(2026, 1, 1, 0, 10), 4.8, 8.0),   # hour0, nearest level 5.0 (median group)
        (datetime(2026, 1, 1, 0, 15), 5.1, 8.4),   # hour0, nearest level 5.0 (median group)
        (datetime(2026, 1, 1, 0, 20), 4.9, 8.9),   # hour0, nearest level 5.0 (median group)
        (datetime(2026, 1, 1, 1, 5), 9.8, 7.2),    # hour1, nearest level 10.0
    ]
    fake = FakeClient(grid_row=GRID_ROW, depths=DEPTHS, model_rows=MODEL_ROWS, sensor_rows=sensor_rows)
    _patch(monkeypatch, fake)

    result = extract_depth_profile(
        source='SalishSeaCast', var='temperature', sensor_id='abc',
        lat=49.0, lon=-123.0, from_date='2026-01-01T00:00:00', to_date='2026-01-01T01:59:59',
    )

    assert result['time'] == ['2026-01-01T00:00:00', '2026-01-01T01:00:00']
    assert result['depths'] == DEPTHS
    assert result['model'] == [[9.0, 9.1], [8.0, 8.1], [7.0, 7.1]]

    # depth index 0 (0.0m): one cast at hour0, none at hour1
    assert result['sensor'][0] == [9.4, None]
    # depth index 1 (5.0m): three casts at hour0 -> median, none at hour1
    assert result['sensor'][1] == [8.4, None]
    # depth index 2 (10.0m): none at hour0, one cast at hour1
    assert result['sensor'][2] == [None, 7.2]

    assert fake.closed


def test_no_casts_in_window_returns_all_null_sensor_grid(monkeypatch):
    fake = FakeClient(grid_row=GRID_ROW, depths=DEPTHS, model_rows=MODEL_ROWS, sensor_rows=[])
    _patch(monkeypatch, fake)

    result = extract_depth_profile(
        source='SalishSeaCast', var='temperature', sensor_id='abc',
        lat=49.0, lon=-123.0, from_date='2026-01-01T00:00:00', to_date='2026-01-01T01:59:59',
    )

    assert result['sensor'] == [[None, None], [None, None], [None, None]]
    # model side is unaffected by an empty sensor window
    assert result['model'] == [[9.0, 9.1], [8.0, 8.1], [7.0, 7.1]]


def test_cast_outside_model_hour_range_is_dropped(monkeypatch):
    sensor_rows = [(datetime(2026, 1, 1, 5, 0), 0.0, 99.0)]  # no matching model hour
    fake = FakeClient(grid_row=GRID_ROW, depths=DEPTHS, model_rows=MODEL_ROWS, sensor_rows=sensor_rows)
    _patch(monkeypatch, fake)

    result = extract_depth_profile(
        source='SalishSeaCast', var='temperature', sensor_id='abc',
        lat=49.0, lon=-123.0, from_date='2026-01-01T00:00:00', to_date='2026-01-01T01:59:59',
    )

    assert result['sensor'] == [[None, None], [None, None], [None, None]]


def test_nan_sensor_value_is_dropped(monkeypatch):
    sensor_rows = [(datetime(2026, 1, 1, 0, 5), 0.0, float('nan'))]
    fake = FakeClient(grid_row=GRID_ROW, depths=DEPTHS, model_rows=MODEL_ROWS, sensor_rows=sensor_rows)
    _patch(monkeypatch, fake)

    result = extract_depth_profile(
        source='SalishSeaCast', var='temperature', sensor_id='abc',
        lat=49.0, lon=-123.0, from_date='2026-01-01T00:00:00', to_date='2026-01-01T01:59:59',
    )

    assert result['sensor'][0] == [None, None]


def test_no_model_data_raises(monkeypatch):
    fake = FakeClient(grid_row=GRID_ROW, depths=DEPTHS, model_rows=[], sensor_rows=[])
    _patch(monkeypatch, fake)

    with pytest.raises(RuntimeError, match="No model data found"):
        extract_depth_profile(
            source='SalishSeaCast', var='temperature', sensor_id='abc',
            lat=49.0, lon=-123.0, from_date='2026-01-01T00:00:00', to_date='2026-01-01T01:59:59',
        )


def test_out_of_domain_raises(monkeypatch):
    fake = FakeClient(grid_row=(10, 20, 60.0, -130.0, 50_000.0), depths=DEPTHS)
    _patch(monkeypatch, fake)

    with pytest.raises(RuntimeError, match="km from the nearest grid point"):
        extract_depth_profile(
            source='SalishSeaCast', var='temperature', sensor_id='abc',
            lat=0.0, lon=0.0, from_date='2026-01-01T00:00:00', to_date='2026-01-01T01:59:59',
        )


def test_unsupported_source_raises(monkeypatch):
    _patch(monkeypatch, FakeClient(grid_row=GRID_ROW))

    with pytest.raises(ValueError, match="not yet available"):
        extract_depth_profile(
            source='LiveOcean', var='temperature', sensor_id='abc',
            lat=49.0, lon=-123.0, from_date='2026-01-01T00:00:00', to_date='2026-01-01T01:59:59',
        )


def test_unknown_variable_raises(monkeypatch):
    _patch(monkeypatch, FakeClient(grid_row=GRID_ROW))

    with pytest.raises(ValueError, match="Unknown variable"):
        extract_depth_profile(
            source='SalishSeaCast', var='not_a_variable', sensor_id='abc',
            lat=49.0, lon=-123.0, from_date='2026-01-01T00:00:00', to_date='2026-01-01T01:59:59',
        )
