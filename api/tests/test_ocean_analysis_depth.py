from ocean_analysis import _resolve_nearest_depth


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
