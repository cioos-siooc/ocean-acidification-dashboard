from datetime import datetime

import pandas as pd
import pytest

from extractTimeseries import extract_timeseries


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClient:
    def __init__(self, grid_row, depths=None, point_rows=None, all_depth_rows=None):
        self.grid_row = grid_row
        self.depths = depths or []
        self.point_rows = point_rows or []
        self.all_depth_rows = all_depth_rows or []
        self.closed = False

    def query(self, query, parameters=None):
        q = " ".join(query.lower().split())
        if "from grid_ssc" in q:
            return FakeResult([self.grid_row])
        if "select distinct depth" in q:
            return FakeResult([(d,) for d in self.depths])
        if "select time, depth," in q:
            return FakeResult(self.all_depth_rows)
        return FakeResult(self.point_rows)

    def close(self):
        self.closed = True


def test_extract_timeseries_single_depth(monkeypatch):
    fake = FakeClient(
        grid_row=(10, 20, 49.0, -123.0, 500.0),
        depths=[0.0, 5.0, 10.0],
        point_rows=[(datetime(2026, 1, 1), 7.5), (datetime(2026, 1, 2), 8.0)],
    )
    monkeypatch.setattr('extractTimeseries.get_ch_client', lambda: fake)

    times, values = extract_timeseries(source='SalishSeaCast', var='temperature', lat=49.0, lon=-123.0, depth=5.0)

    assert isinstance(times, pd.Series)
    assert isinstance(values, pd.Series)
    assert list(values) == [7.5, 8.0]
    assert fake.closed


def test_extract_timeseries_bottom_depth_selects_max(monkeypatch):
    fake = FakeClient(
        grid_row=(10, 20, 49.0, -123.0, 500.0),
        depths=[0.0, 5.0, 10.0],
        point_rows=[(datetime(2026, 1, 1), 1.0)],
    )
    monkeypatch.setattr('extractTimeseries.get_ch_client', lambda: fake)

    from extractTimeseries import _resolve_depth
    assert _resolve_depth(fake, 'SalishSeaCast_hourly', 10, 20, -1) == 10.0


def test_extract_timeseries_all_depths(monkeypatch):
    fake = FakeClient(
        grid_row=(10, 20, 49.0, -123.0, 500.0),
        all_depth_rows=[
            (datetime(2026, 1, 1), 0.0, 1.0),
            (datetime(2026, 1, 1), 5.0, 2.0),
            (datetime(2026, 1, 2), 0.0, 1.5),
        ],
    )
    monkeypatch.setattr('extractTimeseries.get_ch_client', lambda: fake)

    df = extract_timeseries(source='SalishSeaCast', var='temperature', lat=49.0, lon=-123.0, depth=None)

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ['time', 'depth', 'value']
    assert len(df) == 3


def test_extract_timeseries_out_of_domain_raises(monkeypatch):
    fake = FakeClient(grid_row=(10, 20, 60.0, -130.0, 50_000.0))
    monkeypatch.setattr('extractTimeseries.get_ch_client', lambda: fake)

    with pytest.raises(RuntimeError, match="km from the nearest grid point"):
        extract_timeseries(source='SalishSeaCast', var='temperature', lat=0.0, lon=0.0, depth=5.0)


def test_extract_timeseries_unsupported_source(monkeypatch):
    monkeypatch.setattr('extractTimeseries.get_ch_client', lambda: FakeClient(grid_row=(0, 0, 0.0, 0.0, 0.0)))

    with pytest.raises(RuntimeError, match="not yet available via ClickHouse"):
        extract_timeseries(source='Live Ocean', var='temperature', lat=49.0, lon=-123.0, depth=5.0)


def test_extract_timeseries_unknown_variable(monkeypatch):
    monkeypatch.setattr('extractTimeseries.get_ch_client', lambda: FakeClient(grid_row=(0, 0, 0.0, 0.0, 0.0)))

    with pytest.raises(ValueError, match="Unknown variable"):
        extract_timeseries(source='SalishSeaCast', var='not_a_variable', lat=49.0, lon=-123.0, depth=5.0)
