from datetime import date, datetime

from variables import _get_ssc_variables

_SSC_VARS = {
    'temperature', 'salinity', 'total_alkalinity', 'dissolved_oxygen',
    'dissolved_inorganic_carbon', 'ph_total', 'omega_arag', 'omega_cal',
}


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeChClient:
    def __init__(self, sync_dates):
        self.sync_dates = sync_dates
        self.closed = False

    def command(self, query, **kwargs):
        pass

    def query(self, query, parameters=None):
        q = " ".join(query.lower().split())
        if "from salishseacast_sync_log" in q:
            return FakeResult([(d,) for d in self.sync_dates])
        raise AssertionError(f"unexpected query: {query}")

    def close(self):
        self.closed = True


def test_get_ssc_variables_expands_24_hourly_dts(monkeypatch):
    fake = FakeChClient(sync_dates=[date(2026, 1, 1), date(2026, 1, 2)])
    monkeypatch.setattr('variables.get_ch_client', lambda: fake)

    variables = _get_ssc_variables()

    assert {v['var'] for v in variables} == _SSC_VARS
    assert fake.closed

    temp = next(v for v in variables if v['var'] == 'temperature')
    assert len(temp['dts']) == 48  # 2 days x 24 hours
    assert temp['dts'][0] == datetime(2026, 1, 1, 0, 30)
    assert temp['dts'][23] == datetime(2026, 1, 1, 23, 30)
    assert temp['dts'][24] == datetime(2026, 1, 2, 0, 30)
    assert temp['precision'] == 0.01
    assert temp['unit'] == '°C'
    assert temp['colormap'] == 'thermal'
    assert temp['source'] == 'SalishSeaCast'
    assert len(temp['depths']) == 41  # 40 native sigma levels + bottom
    assert temp['depths'][0] == 0.5
    assert temp['depths'][-1] == -1  # bottom sentinel
    assert temp['bounds'][0] < temp['bounds'][2]  # min_lon < max_lon

    # All 8 variables share the exact same dts/depths/bounds, since they move
    # through the pipeline as a bundle (see _get_ssc_variables docstring).
    for v in variables:
        assert v['dts'] == temp['dts']
        assert v['depths'] == temp['depths']
        assert v['bounds'] == temp['bounds']


def test_get_ssc_variables_no_synced_dates(monkeypatch):
    fake = FakeChClient(sync_dates=[])
    monkeypatch.setattr('variables.get_ch_client', lambda: fake)

    variables = _get_ssc_variables()
    assert len(variables) == 8
    assert all(v['dts'] == [] for v in variables)
