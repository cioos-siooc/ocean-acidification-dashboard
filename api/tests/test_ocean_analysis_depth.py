import pytest

import ocean_analysis
from ocean_analysis import _resolve_nearest_depth


@pytest.fixture(autouse=True)
def _reset_depth_cache():
    """_get_depth_levels() memoizes into a module-global, so without a reset the
    first test's fake depths leak into later tests (e.g. the empty-depths case
    would otherwise see a stale non-empty cache and return a match)."""
    ocean_analysis._CACHED_DEPTH_LEVELS = None
    yield
    ocean_analysis._CACHED_DEPTH_LEVELS = None


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClient:
    def __init__(self, depths):
        self.depths = depths

    def query(self, query, parameters=None):
        return FakeResult([(d,) for d in self.depths])


def test_resolve_nearest_depth_bottom_selects_max():
    fake = FakeClient(depths=[0.0, 5.0, 10.0])
    assert _resolve_nearest_depth(fake, 10, 20, -1) == 10.0


def test_resolve_nearest_depth_within_tolerance():
    fake = FakeClient(depths=[0.0, 5.0, 10.0])
    assert _resolve_nearest_depth(fake, 10, 20, 5.05) == 5.0


def test_resolve_nearest_depth_out_of_tolerance_returns_none():
    fake = FakeClient(depths=[0.0, 5.0, 10.0])
    assert _resolve_nearest_depth(fake, 10, 20, 400.0) is None


def test_resolve_nearest_depth_no_data_returns_none():
    fake = FakeClient(depths=[])
    assert _resolve_nearest_depth(fake, 10, 20, 5.0) is None
