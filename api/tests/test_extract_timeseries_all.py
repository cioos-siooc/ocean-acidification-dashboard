from datetime import datetime

import pandas as pd
import pytest

from extractTimeseries import extract_timeseries, OutsideDomainError


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClient:
    def __init__(self, grid_row=None, depths=None, point_rows=None, all_depth_rows=None, polygon_rows=None):
        self.grid_row = grid_row
        self.depths = depths or []
        self.point_rows = point_rows or []
        self.all_depth_rows = all_depth_rows or []
        self.polygon_rows = polygon_rows or []
        self.closed = False

    def query(self, query, parameters=None):
        q = " ".join(query.lower().split())
        if "pointinpolygon" in q:
            return FakeResult(self.polygon_rows)
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


def test_extract_timeseries_depth_out_of_tolerance_raises(monkeypatch):
    # Requested depth (400m) is far beyond this cell's deepest level (10m) —
    # should report "no data", not silently return the 10m series.
    fake = FakeClient(
        grid_row=(10, 20, 49.0, -123.0, 500.0),
        depths=[0.0, 5.0, 10.0],
        point_rows=[(datetime(2026, 1, 1), 1.0)],
    )
    monkeypatch.setattr('extractTimeseries.get_ch_client', lambda: fake)

    with pytest.raises(RuntimeError, match="No data available at depth"):
        extract_timeseries(source='SalishSeaCast', var='temperature', lat=49.0, lon=-123.0, depth=400.0)


def test_extract_timeseries_depth_within_tolerance_matches(monkeypatch):
    fake = FakeClient(
        grid_row=(10, 20, 49.0, -123.0, 500.0),
        depths=[0.0, 5.0, 10.0],
        point_rows=[(datetime(2026, 1, 1), 7.5)],
    )
    monkeypatch.setattr('extractTimeseries.get_ch_client', lambda: fake)

    times, values = extract_timeseries(source='SalishSeaCast', var='temperature', lat=49.0, lon=-123.0, depth=5.05)

    assert list(values) == [7.5]


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

    with pytest.raises(OutsideDomainError) as excinfo:
        extract_timeseries(source='SalishSeaCast', var='temperature', lat=0.0, lon=0.0, depth=5.0)

    # The numbers, not just the sentence: SERVER.py forwards them to the
    # frontend, which renders the out-of-domain state from `code`/`distanceKm`
    # rather than by matching on the message text.
    exc = excinfo.value
    assert exc.code == 'outside_model_domain'
    assert exc.distance_km == pytest.approx(50.0)
    assert exc.max_distance_km == 25.0
    assert exc.to_payload()['nearest'] == {'lat': -130.0, 'lon': 60.0}


class BoundingBoxMissClient(FakeClient):
    """A grid table whose nearest cell lies outside the 0.5-degree prefilter box.

    The bounded lookup finds nothing; only the unfiltered fallback does. Before
    that fallback existed, this case surfaced as "Grid table is empty or not
    found" — blaming the database for a point the user deliberately clicked far
    offshore, and giving the UI no distance to report.
    """

    def query(self, query, parameters=None):
        q = " ".join(query.lower().split())
        if "from grid_ssc" in q:
            bounded = "latitude between" in q
            return FakeResult([] if bounded else [self.grid_row])
        return super().query(query, parameters)


def test_extract_timeseries_beyond_bounding_box_reports_real_distance(monkeypatch):
    fake = BoundingBoxMissClient(grid_row=(10, 20, -125.4, 49.2, 122_900.0))
    monkeypatch.setattr('extractTimeseries.get_ch_client', lambda: fake)

    with pytest.raises(OutsideDomainError) as excinfo:
        extract_timeseries(source='SalishSeaCast', var='temperature', lat=48.67, lon=-126.85, depth=441.5)

    exc = excinfo.value
    assert exc.code == 'outside_model_domain'
    assert exc.distance_km == pytest.approx(122.9)
    assert 'outside the SalishSeaCast model domain' in str(exc)


def test_extract_timeseries_empty_grid_table_still_raises_plain_error(monkeypatch):
    """An actually-empty grid table must stay distinguishable from a far point."""

    class EmptyGridClient(FakeClient):
        def query(self, query, parameters=None):
            if "from grid_ssc" in " ".join(query.lower().split()):
                return FakeResult([])
            return super().query(query, parameters)

    monkeypatch.setattr('extractTimeseries.get_ch_client', lambda: EmptyGridClient())

    with pytest.raises(RuntimeError, match="is empty or not found") as excinfo:
        extract_timeseries(source='SalishSeaCast', var='temperature', lat=49.0, lon=-123.0, depth=5.0)
    assert not isinstance(excinfo.value, OutsideDomainError)


def test_extract_timeseries_unsupported_source(monkeypatch):
    monkeypatch.setattr('extractTimeseries.get_ch_client', lambda: FakeClient(grid_row=(0, 0, 0.0, 0.0, 0.0)))

    with pytest.raises(RuntimeError, match="not yet available via ClickHouse"):
        extract_timeseries(source='Live Ocean', var='temperature', lat=49.0, lon=-123.0, depth=5.0)


def test_extract_timeseries_unknown_variable(monkeypatch):
    monkeypatch.setattr('extractTimeseries.get_ch_client', lambda: FakeClient(grid_row=(0, 0, 0.0, 0.0, 0.0)))

    with pytest.raises(ValueError, match="Unknown variable"):
        extract_timeseries(source='SalishSeaCast', var='not_a_variable', lat=49.0, lon=-123.0, depth=5.0)


def test_extract_timeseries_polygon_single_depth(monkeypatch):
    fake = FakeClient(
        depths=[0.0, 5.0, 10.0],
        polygon_rows=[(10, 20), (11, 20)],
        point_rows=[(datetime(2026, 1, 1), 7.5), (datetime(2026, 1, 2), 8.0)],
    )
    monkeypatch.setattr('extractTimeseries.get_ch_client', lambda: fake)

    polygon = [(-123.1, 48.9), (-122.9, 48.9), (-122.9, 49.1), (-123.1, 49.1), (-123.1, 48.9)]
    times, values = extract_timeseries(source='SalishSeaCast', var='temperature', polygon=polygon, depth=5.0)

    assert isinstance(times, pd.Series)
    assert list(values) == [7.5, 8.0]
    assert fake.closed


def test_extract_timeseries_polygon_all_depths(monkeypatch):
    fake = FakeClient(
        polygon_rows=[(10, 20), (11, 20)],
        all_depth_rows=[
            (datetime(2026, 1, 1), 0.0, 1.0),
            (datetime(2026, 1, 1), 5.0, 2.0),
        ],
    )
    monkeypatch.setattr('extractTimeseries.get_ch_client', lambda: fake)

    polygon = [(-123.1, 48.9), (-122.9, 48.9), (-122.9, 49.1), (-123.1, 49.1), (-123.1, 48.9)]
    df = extract_timeseries(source='SalishSeaCast', var='temperature', polygon=polygon, depth=None)

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ['time', 'depth', 'value']
    assert len(df) == 2


def test_extract_timeseries_polygon_no_grid_cells_raises(monkeypatch):
    fake = FakeClient(polygon_rows=[])
    monkeypatch.setattr('extractTimeseries.get_ch_client', lambda: fake)

    polygon = [(-123.1, 48.9), (-122.9, 48.9), (-122.9, 49.1), (-123.1, 49.1), (-123.1, 48.9)]
    with pytest.raises(RuntimeError, match="does not cover any active marine grid cells"):
        extract_timeseries(source='SalishSeaCast', var='temperature', polygon=polygon, depth=5.0)


def test_extract_timeseries_requires_point_or_polygon(monkeypatch):
    with pytest.raises(ValueError, match="Provide either a polygon"):
        extract_timeseries(source='SalishSeaCast', var='temperature', depth=5.0)
