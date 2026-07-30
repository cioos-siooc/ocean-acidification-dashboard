from datetime import datetime

import pytest

from extractMinMax import extract_minmax


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClient:
    def __init__(self, minmax_row=(1.0, 9.0, 1)):
        self.minmax_row = minmax_row
        self.queries = []
        self.closed = False

    def query(self, query, parameters=None):
        self.queries.append(query)
        return FakeResult([self.minmax_row])

    def close(self):
        self.closed = True


def test_extract_minmax_explicit_depth_with_bounds(monkeypatch):
    fake = FakeClient(minmax_row=(2.0, 8.0, 2))
    monkeypatch.setattr('extractMinMax.get_ch_client', lambda: fake)

    min_val, max_val = extract_minmax(
        source='SalishSeaCast', var='temperature', dt=datetime(2026, 1, 1, 0, 30),
        depth=5.0, north=50, south=48, east=-122, west=-124,
    )

    assert (min_val, max_val) == (2.0, 8.0)
    assert fake.closed
    # The bbox filter must be a subquery against grid_SSC, not a Python-side
    # IN-list — a wide visible-area selection can have tens of thousands of
    # grid points, which blows past ClickHouse's max_query_size once inlined
    # as literal SQL text (the bug this test guards against).
    assert len(fake.queries) == 1
    main_query = " ".join(fake.queries[0].lower().split())
    assert "from grid_ssc" in main_query
    assert "in %(grid_points)s" not in main_query
    assert "argmin" not in main_query and "argmax" not in main_query
    # Tolerant match, not strict equality — native depths are stored as
    # Float32, so a nominal "14.6" round-trips as 14.600000381469727, which
    # would never satisfy `depth = %(depth)s` against the Float64 request
    # value (the bug this test guards against).
    assert "abs(depth - %(depth)s) <= %(depth_tol)s" in main_query


def test_extract_minmax_bottom_uses_argmax(monkeypatch):
    fake = FakeClient(minmax_row=(2.0, 8.0, 2))
    monkeypatch.setattr('extractMinMax.get_ch_client', lambda: fake)

    extract_minmax(source='SalishSeaCast', var='temperature', dt=datetime(2026, 1, 1, 0, 30), depth=-1)

    main_query = " ".join(fake.queries[-1].lower().split())
    assert "argmax(temperature, depth)" in main_query


def test_extract_minmax_no_depth_uses_argmin(monkeypatch):
    fake = FakeClient(minmax_row=(2.0, 8.0, 2))
    monkeypatch.setattr('extractMinMax.get_ch_client', lambda: fake)

    extract_minmax(source='SalishSeaCast', var='temperature', dt=datetime(2026, 1, 1, 0, 30), depth=None)

    main_query = " ".join(fake.queries[-1].lower().split())
    assert "argmin(temperature, depth)" in main_query


def test_extract_minmax_without_bounds_omits_bbox_filter(monkeypatch):
    fake = FakeClient(minmax_row=(2.0, 8.0, 2))
    monkeypatch.setattr('extractMinMax.get_ch_client', lambda: fake)

    extract_minmax(source='SalishSeaCast', var='temperature', dt=datetime(2026, 1, 1, 0, 30), depth=5.0)

    assert len(fake.queries) == 1
    assert "grid_ssc" not in " ".join(fake.queries[0].lower().split())


def test_extract_minmax_no_data_raises(monkeypatch):
    # ClickHouse's min()/max() return the column type's default (0 for
    # Float32), not NULL, when zero rows match — cnt=0 is what actually
    # signals "no data" here.
    fake = FakeClient(minmax_row=(0.0, 0.0, 0))
    monkeypatch.setattr('extractMinMax.get_ch_client', lambda: fake)

    with pytest.raises(RuntimeError, match="No data available for"):
        extract_minmax(source='SalishSeaCast', var='temperature', dt=datetime(2026, 1, 1, 0, 30), depth=400.0)


def test_extract_minmax_genuine_zero_value_not_treated_as_no_data(monkeypatch):
    # A real value of exactly 0 (e.g. temperature at freezing) must not be
    # mistaken for "no data" just because it matches ClickHouse's empty-set
    # default — only cnt distinguishes the two.
    fake = FakeClient(minmax_row=(0.0, 0.0, 3))
    monkeypatch.setattr('extractMinMax.get_ch_client', lambda: fake)

    min_val, max_val = extract_minmax(source='SalishSeaCast', var='temperature', dt=datetime(2026, 1, 1, 0, 30), depth=0.5)

    assert (min_val, max_val) == (0.0, 0.0)


def test_extract_minmax_unsupported_source(monkeypatch):
    monkeypatch.setattr('extractMinMax.get_ch_client', lambda: FakeClient())

    with pytest.raises(RuntimeError, match="not yet available via ClickHouse"):
        extract_minmax(source='Live Ocean', var='temperature', dt=datetime(2026, 1, 1, 0, 30))


def test_extract_minmax_unknown_variable(monkeypatch):
    monkeypatch.setattr('extractMinMax.get_ch_client', lambda: FakeClient())

    with pytest.raises(ValueError, match="Unknown variable"):
        extract_minmax(source='SalishSeaCast', var='not_a_variable', dt=datetime(2026, 1, 1, 0, 30))
