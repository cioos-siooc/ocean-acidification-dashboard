from datetime import datetime

import pytest

from extract_profile import extract_profile
from modules.extractTimeseries import OutsideDomainError  # same module object the code under test raises from


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClient:
    def __init__(self, grid_row=None, time_row=None, profile_rows=None):
        self.grid_row = grid_row
        self.time_row = time_row
        self.profile_rows = profile_rows or []
        self.closed = False

    def query(self, query, parameters=None):
        q = " ".join(query.lower().split())
        if "from grid_ssc" in q:
            return FakeResult([self.grid_row])
        if "datediff" in q:
            return FakeResult([self.time_row] if self.time_row is not None else [])
        return FakeResult(self.profile_rows)

    def close(self):
        self.closed = True


def test_extract_profile_happy_path(monkeypatch):
    fake = FakeClient(
        grid_row=(10, 20, 49.0, -123.0, 500.0),
        time_row=(datetime(2026, 1, 1, 12), 0),
        profile_rows=[(0.0, 8.0), (5.0, 7.5), (10.0, 7.0)],
    )
    monkeypatch.setattr('extract_profile.get_ch_client', lambda: fake)

    profile = extract_profile(source='SalishSeaCast', var='temperature', lat=49.0, lon=-123.0, dt='2026-01-01T12:00:00')

    assert profile == [
        {"depth": 0.0, "value": 8.0},
        {"depth": 5.0, "value": 7.5},
        {"depth": 10.0, "value": 7.0},
    ]
    assert fake.closed


def test_extract_profile_out_of_domain_raises(monkeypatch):
    fake = FakeClient(grid_row=(10, 20, 60.0, -130.0, 50_000.0))
    monkeypatch.setattr('extract_profile.get_ch_client', lambda: fake)

    with pytest.raises(OutsideDomainError):
        extract_profile(source='SalishSeaCast', var='temperature', lat=0.0, lon=0.0, dt='2026-01-01T12:00:00')


def test_extract_profile_time_out_of_tolerance_raises(monkeypatch):
    fake = FakeClient(
        grid_row=(10, 20, 49.0, -123.0, 500.0),
        time_row=(datetime(2026, 1, 1, 0), 7200),
    )
    monkeypatch.setattr('extract_profile.get_ch_client', lambda: fake)

    with pytest.raises(RuntimeError, match="no matching data within"):
        extract_profile(source='SalishSeaCast', var='temperature', lat=49.0, lon=-123.0, dt='2026-01-01T12:00:00')


def test_extract_profile_no_rows_for_grid_cell_raises(monkeypatch):
    fake = FakeClient(grid_row=(10, 20, 49.0, -123.0, 500.0), time_row=None)
    monkeypatch.setattr('extract_profile.get_ch_client', lambda: fake)

    with pytest.raises(RuntimeError, match="No data available for grid point"):
        extract_profile(source='SalishSeaCast', var='temperature', lat=49.0, lon=-123.0, dt='2026-01-01T12:00:00')


def test_extract_profile_unknown_variable_raises(monkeypatch):
    monkeypatch.setattr('extract_profile.get_ch_client', lambda: FakeClient(grid_row=(0, 0, 0.0, 0.0, 0.0)))

    with pytest.raises(ValueError, match="Unknown variable"):
        extract_profile(source='SalishSeaCast', var='not_a_variable', lat=49.0, lon=-123.0, dt='2026-01-01T12:00:00')


def test_extract_profile_unsupported_source_raises(monkeypatch):
    monkeypatch.setattr('extract_profile.get_ch_client', lambda: FakeClient(grid_row=(0, 0, 0.0, 0.0, 0.0)))

    with pytest.raises(RuntimeError, match="not yet available via ClickHouse"):
        extract_profile(source='Live Ocean', var='temperature', lat=49.0, lon=-123.0, dt='2026-01-01T12:00:00')


def test_extract_profile_unsupported_bin_mode_raises(monkeypatch):
    monkeypatch.setattr('extract_profile.get_ch_client', lambda: FakeClient(grid_row=(0, 0, 0.0, 0.0, 0.0)))

    with pytest.raises(ValueError, match="Unsupported bin_mode"):
        extract_profile(source='SalishSeaCast', var='temperature', lat=49.0, lon=-123.0, dt='2026-01-01T12:00:00', bin_mode='weekly')


def test_extract_profile_daily_reads_daily_table(monkeypatch):
    # Daily/monthly never hit the `datediff` nearest-time lookup at all — the
    # target day/month is derived straight from `dt`, so a fake with no
    # `time_row` still succeeds (unlike the hourly-mode tests above).
    fake = FakeClient(
        grid_row=(10, 20, 49.0, -123.0, 500.0),
        profile_rows=[(0.0, 8.2), (5.0, 7.6)],
    )
    monkeypatch.setattr('extract_profile.get_ch_client', lambda: fake)

    profile = extract_profile(
        source='SalishSeaCast', var='temperature', lat=49.0, lon=-123.0,
        dt='2026-01-15T12:00:00', bin_mode='daily',
    )

    assert profile == [{"depth": 0.0, "value": 8.2}, {"depth": 5.0, "value": 7.6}]
    assert fake.closed


def test_extract_profile_monthly_reads_daily_table_grouped(monkeypatch):
    fake = FakeClient(
        grid_row=(10, 20, 49.0, -123.0, 500.0),
        profile_rows=[(0.0, 8.0), (5.0, 7.5)],
    )
    monkeypatch.setattr('extract_profile.get_ch_client', lambda: fake)

    profile = extract_profile(
        source='SalishSeaCast', var='temperature', lat=49.0, lon=-123.0,
        dt='2026-01-15T12:00:00', bin_mode='monthly',
    )

    assert profile == [{"depth": 0.0, "value": 8.0}, {"depth": 5.0, "value": 7.5}]


def test_extract_profile_daily_filters_null_values(monkeypatch):
    fake = FakeClient(
        grid_row=(10, 20, 49.0, -123.0, 500.0),
        profile_rows=[(0.0, 8.2), (5.0, None)],
    )
    monkeypatch.setattr('extract_profile.get_ch_client', lambda: fake)

    profile = extract_profile(
        source='SalishSeaCast', var='temperature', lat=49.0, lon=-123.0,
        dt='2026-01-15T12:00:00', bin_mode='daily',
    )

    assert profile == [{"depth": 0.0, "value": 8.2}]
