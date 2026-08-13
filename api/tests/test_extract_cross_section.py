import pytest

from extract_cross_section import _haversine_km, extract_cross_section, resample_polyline


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClient:
    def __init__(self, hourly_rows=None, daily_rows=None):
        self.hourly_rows = hourly_rows or []
        self.daily_rows = daily_rows or []
        self.queries = []
        self.closed = False

    def query(self, query, parameters=None):
        self.queries.append((query, parameters))
        q = " ".join(query.lower().split())
        if "from salishseacast_daily" in q:
            return FakeResult(self.daily_rows)
        return FakeResult(self.hourly_rows)  # SalishSeaCast_hourly

    def close(self):
        self.closed = True


def _patch_client(monkeypatch, fake):
    monkeypatch.setattr('extract_cross_section.get_ch_client', lambda: fake)


def _patch_grid(monkeypatch, fn):
    """fn(lons, lats) -> (gridX_list, gridY_list); replaces the real KD-tree lookup."""
    monkeypatch.setattr('extract_cross_section.grid_lookup.nearest_grid_cells', fn)


def _single_cell(gx, gy):
    return lambda lons, lats: ([gx] * len(lons), [gy] * len(lons))


# ── resample_polyline ────────────────────────────────────────────────────

def test_resample_polyline_two_vertices_spans_full_length():
    vertices = [(49.0, -123.0), (49.1, -123.0)]
    expected_total = _haversine_km(*vertices[0], *vertices[1])

    points, distances_km, vertex_distances_km = resample_polyline(
        vertices, target_spacing_km=2.0, max_samples=50,
    )

    assert distances_km[0] == 0.0
    assert distances_km[-1] == pytest.approx(expected_total)
    assert vertex_distances_km == [0.0, pytest.approx(expected_total)]
    assert points[0] == vertices[0]
    assert points[-1] == pytest.approx(vertices[1])


def test_resample_polyline_respects_max_samples():
    vertices = [(49.0, -123.0), (50.0, -123.0)]  # ~111 km
    points, distances_km, _ = resample_polyline(vertices, target_spacing_km=0.1, max_samples=20)

    assert len(points) == 20
    assert len(distances_km) == 20


def test_resample_polyline_vertex_distances_at_bends():
    vertices = [(0.0, 0.0), (0.1, 0.0), (0.1, 0.1)]
    seg1 = _haversine_km(0.0, 0.0, 0.1, 0.0)
    seg2 = _haversine_km(0.1, 0.0, 0.1, 0.1)

    _, _, vertex_distances_km = resample_polyline(vertices, target_spacing_km=1.0, max_samples=100)

    assert vertex_distances_km == pytest.approx([0.0, seg1, seg1 + seg2])


def test_resample_polyline_needs_two_vertices():
    with pytest.raises(ValueError, match="at least 2 vertices"):
        resample_polyline([(49.0, -123.0)])


def test_resample_polyline_rejects_zero_length():
    with pytest.raises(ValueError, match="zero length"):
        resample_polyline([(49.0, -123.0), (49.0, -123.0)])


# ── extract_cross_section ────────────────────────────────────────────────

def test_happy_path_single_cell_broadcasts_to_every_sample(monkeypatch):
    fake = FakeClient(hourly_rows=[(10, 20, 0.0, 9.0), (10, 20, 5.0, 8.0)])
    _patch_client(monkeypatch, fake)
    _patch_grid(monkeypatch, _single_cell(10, 20))

    result = extract_cross_section(
        source='SalishSeaCast', var='temperature',
        vertices=[(49.0, -123.0), (49.05, -123.0)],
        dt='2026-01-01T00:00:00', bin_mode='hourly',
    )

    n = len(result['distances_km'])
    assert result['depths'] == [0.0, 5.0]
    assert result['model'][0] == [9.0] * n
    assert result['model'][1] == [8.0] * n
    assert result['vertex_distances_km'][0] == 0.0
    assert fake.closed


def test_multi_cell_values_broadcast_per_sample(monkeypatch):
    fake = FakeClient(hourly_rows=[(10, 20, 0.0, 9.0), (11, 20, 0.0, 9.5)])
    _patch_client(monkeypatch, fake)

    def fake_nearest(lons, lats):
        n = len(lons)
        half = n // 2
        return [10] * half + [11] * (n - half), [20] * n

    _patch_grid(monkeypatch, fake_nearest)

    result = extract_cross_section(
        source='SalishSeaCast', var='temperature',
        vertices=[(49.0, -123.0), (49.1, -123.0)],
        dt='2026-01-01T00:00:00', bin_mode='hourly',
    )

    n = len(result['distances_km'])
    half = n // 2
    assert result['model'][0][:half] == [9.0] * half
    assert result['model'][0][half:] == [9.5] * (n - half)


def test_distinct_cells_are_deduped_in_query(monkeypatch):
    fake = FakeClient(hourly_rows=[(10, 20, 0.0, 9.0)])
    _patch_client(monkeypatch, fake)
    _patch_grid(monkeypatch, _single_cell(10, 20))  # every sample snaps to the same cell

    extract_cross_section(
        source='SalishSeaCast', var='temperature',
        vertices=[(49.0, -123.0), (49.2, -123.0)],
        dt='2026-01-01T00:00:00', bin_mode='hourly',
    )

    _, params = fake.queries[0]
    assert params['cells'] == [(10, 20)]


def test_daily_mode_reads_daily_table(monkeypatch):
    fake = FakeClient(daily_rows=[(10, 20, 0.0, 9.5)])
    _patch_client(monkeypatch, fake)
    _patch_grid(monkeypatch, _single_cell(10, 20))

    result = extract_cross_section(
        source='SalishSeaCast', var='temperature',
        vertices=[(49.0, -123.0), (49.05, -123.0)],
        dt='2026-01-15', bin_mode='daily',
    )

    assert result['model'][0] == [9.5] * len(result['distances_km'])
    _, params = fake.queries[0]
    assert params['day'] == '2026-01-15'


def test_monthly_mode_reads_daily_table_with_month_param(monkeypatch):
    fake = FakeClient(daily_rows=[(10, 20, 0.0, 9.5)])
    _patch_client(monkeypatch, fake)
    _patch_grid(monkeypatch, _single_cell(10, 20))

    extract_cross_section(
        source='SalishSeaCast', var='temperature',
        vertices=[(49.0, -123.0), (49.05, -123.0)],
        dt='2026-01', bin_mode='monthly',
    )

    _, params = fake.queries[0]
    assert params['month'] == '2026-01-01'


def test_no_data_raises(monkeypatch):
    fake = FakeClient(hourly_rows=[])
    _patch_client(monkeypatch, fake)
    _patch_grid(monkeypatch, _single_cell(10, 20))

    with pytest.raises(RuntimeError, match="No model data found"):
        extract_cross_section(
            source='SalishSeaCast', var='temperature',
            vertices=[(49.0, -123.0), (49.05, -123.0)],
            dt='2026-01-01T00:00:00', bin_mode='hourly',
        )


def test_unsupported_source_raises():
    with pytest.raises(ValueError, match="not yet available"):
        extract_cross_section(
            source='LiveOcean', var='temperature',
            vertices=[(49.0, -123.0), (49.05, -123.0)],
            dt='2026-01-01T00:00:00', bin_mode='hourly',
        )


def test_unknown_variable_raises():
    with pytest.raises(ValueError, match="Unknown variable"):
        extract_cross_section(
            source='SalishSeaCast', var='not_a_variable',
            vertices=[(49.0, -123.0), (49.05, -123.0)],
            dt='2026-01-01T00:00:00', bin_mode='hourly',
        )


def test_unsupported_bin_mode_raises():
    with pytest.raises(ValueError, match="Unsupported bin_mode"):
        extract_cross_section(
            source='SalishSeaCast', var='temperature',
            vertices=[(49.0, -123.0), (49.05, -123.0)],
            dt='2026-01-01T00:00:00', bin_mode='weekly',
        )
