from datetime import date, datetime

import pytest

from extract_depth_profile import extract_depth_profile


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


COVERAGE = (datetime(2007, 1, 1), datetime(2026, 8, 1))


class FakeClient:
    def __init__(self, grid_row=None, depths=None, model_rows=None, sensor_rows=None, daily_rows=None,
                 coverage=COVERAGE):
        self.grid_row = grid_row
        self.depths = depths or []
        self.model_rows = model_rows or []
        self.sensor_rows = sensor_rows or []
        self.daily_rows = daily_rows or []
        self.coverage = coverage
        self.closed = False

    def query(self, query, parameters=None):
        q = " ".join(query.lower().split())
        if "from grid_ssc" in q:
            return FakeResult([self.grid_row])
        if "select distinct depth" in q:
            return FakeResult([(d,) for d in self.depths])
        # Must precede the table-name branches below: the coverage aggregate
        # runs against whichever table the requested mode reads.
        if "min(time)" in q:
            return FakeResult([self.coverage] if self.coverage else [])
        if "from sensor_timeseries" in q:
            return FakeResult(self.sensor_rows)
        if "from salishseacast_daily" in q:
            return FakeResult(self.daily_rows)
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


@pytest.mark.parametrize('bin_hours', [3, 6])
def test_unsupported_bin_hours_raises(monkeypatch, bin_hours):
    _patch(monkeypatch, FakeClient(grid_row=GRID_ROW))

    with pytest.raises(ValueError, match="Unsupported bin_hours"):
        extract_depth_profile(
            source='SalishSeaCast', var='temperature', sensor_id='abc',
            lat=49.0, lon=-123.0, from_date='2026-01-01T00:00:00', to_date='2026-01-01T01:59:59',
            bin_hours=bin_hours,
        )


def test_bin_hours_24_reads_daily_table_directly(monkeypatch):
    # NOTE: extractTimeseries._get_depth_levels caches by table name at module scope,
    # so once test_happy_path_grids_shape_and_values (above) has run in this process,
    # every later test sharing source='SalishSeaCast' gets that cached DEPTHS list
    # regardless of its own `depths=` fixture — hence reusing DEPTHS here too.
    #
    # daily_rows simulate SalishSeaCast_daily's real shape: one pre-aggregated row per
    # (day, depth, grid cell), `time` as a plain `date` (not `datetime`) — the actual
    # ClickHouse column type — to exercise the date->datetime conversion for real.
    daily_model_rows = [
        (date(2026, 1, 1), 0.0, 9.0),
        (date(2026, 1, 1), 5.0, 8.0),
        (date(2026, 1, 1), 10.0, 7.0),
        (date(2026, 1, 2), 0.0, 9.2),
        (date(2026, 1, 2), 5.0, 8.2),
        (date(2026, 1, 2), 10.0, 7.2),
    ]
    sensor_rows = [
        (datetime(2026, 1, 1, 14, 0), 0.0, 9.05),  # falls within day 1
        (datetime(2026, 1, 2, 3, 0), 0.0, 9.25),   # falls within day 2
    ]
    fake = FakeClient(grid_row=GRID_ROW, depths=DEPTHS, daily_rows=daily_model_rows, sensor_rows=sensor_rows)
    _patch(monkeypatch, fake)

    result = extract_depth_profile(
        source='SalishSeaCast', var='temperature', sensor_id='abc',
        lat=49.0, lon=-123.0, from_date='2026-01-01T00:00:00', to_date='2026-01-02T23:59:59',
        bin_hours=24,
    )

    assert result['time'] == ['2026-01-01T00:00:00', '2026-01-02T00:00:00']
    assert result['model'] == [[9.0, 9.2], [8.0, 8.2], [7.0, 7.2]]
    assert result['sensor'][0] == [9.05, 9.25]
    assert result['sensor'][1] == [None, None]
    assert result['sensor'][2] == [None, None]


def test_model_only_omits_sensor_grid(monkeypatch):
    # The Depth tab calls without a sensor_id. The sensor query must not run at
    # all (a sensor_rows fixture that would otherwise populate the grid proves
    # it), and "sensor" comes back as None rather than an all-null grid — the
    # frontend distinguishes "no sensor requested" from "sensor had no casts".
    sensor_rows_should_be_ignored = [(datetime(2026, 1, 1, 0, 5), 0.0, 9.4)]
    fake = FakeClient(
        grid_row=GRID_ROW, depths=DEPTHS, model_rows=MODEL_ROWS,
        sensor_rows=sensor_rows_should_be_ignored,
    )
    _patch(monkeypatch, fake)

    result = extract_depth_profile(
        source='SalishSeaCast', var='temperature',
        lat=49.0, lon=-123.0, from_date='2026-01-01T00:00:00', to_date='2026-01-01T01:59:59',
    )

    assert result['sensor'] is None
    assert result['model'] == [[9.0, 9.1], [8.0, 8.1], [7.0, 7.1]]
    assert result['time'] == ['2026-01-01T00:00:00', '2026-01-01T01:00:00']
    # Coverage is what the frontend clamps its paging to, so it must come back
    # on the model-only path too, not just alongside a sensor grid.
    assert result['coverage'] == {'from': '2007-01-01T00:00:00', 'to': '2026-08-01T00:00:00'}
    assert fake.closed


def test_coverage_is_null_when_cell_has_no_rows(monkeypatch):
    fake = FakeClient(grid_row=GRID_ROW, depths=DEPTHS, model_rows=MODEL_ROWS, coverage=None)
    _patch(monkeypatch, fake)

    result = extract_depth_profile(
        source='SalishSeaCast', var='temperature',
        lat=49.0, lon=-123.0, from_date='2026-01-01T00:00:00', to_date='2026-01-01T01:59:59',
    )

    assert result['coverage'] == {'from': None, 'to': None}


def test_bin_mode_monthly_rolls_up_daily_table(monkeypatch):
    # toStartOfMonth returns a `date` on the 1st, same as the daily branch's
    # column type — two months here, so a bin boundary is actually crossed.
    monthly_model_rows = [
        (date(2026, 1, 1), 0.0, 9.0),
        (date(2026, 1, 1), 5.0, 8.0),
        (date(2026, 1, 1), 10.0, 7.0),
        (date(2026, 2, 1), 0.0, 9.5),
        (date(2026, 2, 1), 5.0, 8.5),
        (date(2026, 2, 1), 10.0, 7.5),
    ]
    # Casts must floor to the 1st of their own month — the calendar branch in
    # _floor_bin, which epoch-modulo cannot express.
    sensor_rows = [
        (datetime(2026, 1, 17, 14, 0), 0.0, 9.1),
        (datetime(2026, 2, 28, 3, 0), 0.0, 9.6),
    ]
    fake = FakeClient(grid_row=GRID_ROW, depths=DEPTHS, daily_rows=monthly_model_rows, sensor_rows=sensor_rows)
    _patch(monkeypatch, fake)

    result = extract_depth_profile(
        source='SalishSeaCast', var='temperature', sensor_id='abc',
        lat=49.0, lon=-123.0, from_date='2026-01-01T00:00:00', to_date='2026-02-28T23:59:59',
        bin_mode='monthly',
    )

    assert result['time'] == ['2026-01-01T00:00:00', '2026-02-01T00:00:00']
    assert result['model'] == [[9.0, 9.5], [8.0, 8.5], [7.0, 7.5]]
    assert result['sensor'][0] == [9.1, 9.6]


def test_bin_mode_overrides_bin_hours(monkeypatch):
    # An explicit bin_mode wins over the legacy int, so a caller sending both
    # (bin_hours defaults to 1) still gets the mode it asked for.
    daily_model_rows = [(date(2026, 1, 1), 0.0, 9.0), (date(2026, 1, 2), 0.0, 9.2)]
    fake = FakeClient(grid_row=GRID_ROW, depths=DEPTHS, model_rows=MODEL_ROWS, daily_rows=daily_model_rows)
    _patch(monkeypatch, fake)

    result = extract_depth_profile(
        source='SalishSeaCast', var='temperature',
        lat=49.0, lon=-123.0, from_date='2026-01-01T00:00:00', to_date='2026-01-02T23:59:59',
        bin_hours=1, bin_mode='daily',
    )

    assert result['time'] == ['2026-01-01T00:00:00', '2026-01-02T00:00:00']
    assert result['model'][0] == [9.0, 9.2]


def test_unsupported_bin_mode_raises(monkeypatch):
    _patch(monkeypatch, FakeClient(grid_row=GRID_ROW))

    with pytest.raises(ValueError, match="Unsupported bin_mode"):
        extract_depth_profile(
            source='SalishSeaCast', var='temperature',
            lat=49.0, lon=-123.0, from_date='2026-01-01T00:00:00', to_date='2026-01-01T01:59:59',
            bin_mode='weekly',
        )


def test_bin_hours_1_ignores_daily_table(monkeypatch):
    # If bin_hours=1 accidentally read from daily_rows instead of model_rows, this
    # would return the (wrong) daily fixture values instead of the hourly ones.
    daily_rows_should_be_ignored = [(date(2026, 1, 1), 0.0, 999.0)]
    fake = FakeClient(
        grid_row=GRID_ROW, depths=DEPTHS, model_rows=MODEL_ROWS,
        sensor_rows=[], daily_rows=daily_rows_should_be_ignored,
    )
    _patch(monkeypatch, fake)

    result = extract_depth_profile(
        source='SalishSeaCast', var='temperature', sensor_id='abc',
        lat=49.0, lon=-123.0, from_date='2026-01-01T00:00:00', to_date='2026-01-01T01:59:59',
        bin_hours=1,
    )

    assert result['model'] == [[9.0, 9.1], [8.0, 8.1], [7.0, 7.1]]
