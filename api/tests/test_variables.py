from datetime import date, datetime

from variables import get_variables

_SSC_VARS = {
    'temperature', 'salinity', 'total_alkalinity', 'dissolved_oxygen',
    'dissolved_inorganic_carbon', 'ph_total', 'omega_arag', 'omega_cal',
}
_LO_VARS = _SSC_VARS | {'nitrate', 'phytoplankton'}


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeChClient:
    def __init__(self, sync_dates, lo_days=None):
        self.sync_dates = sync_dates
        # None = LiveOcean_days doesn't exist on this server
        self.lo_days = lo_days
        self.closed = False
        self.commands = []

    def command(self, query, **kwargs):
        self.commands.append(query)

    def query(self, query, parameters=None):
        q = " ".join(query.lower().split())
        if "from salishseacast_sync_log" in q:
            return FakeResult([(d,) for d in self.sync_dates])
        if q == "exists table liveocean_days":
            return FakeResult([(int(self.lo_days is not None),)])
        if "from liveocean_days" in q:
            return FakeResult(self.lo_days)
        raise AssertionError(f"unexpected query: {query}")

    def close(self):
        self.closed = True


def _by_source(variables, source):
    return [v for v in variables if v['source'] == source]


def test_ssc_expands_24_hourly_dts(monkeypatch):
    fake = FakeChClient(sync_dates=[date(2026, 1, 1), date(2026, 1, 2)])
    monkeypatch.setattr('variables.get_ch_client', lambda: fake)

    variables = _by_source(get_variables(), 'SalishSeaCast')

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
    assert len(temp['depths']) == 40  # native sigma levels (bottom layer dropped for now)
    assert temp['depths'][0] == 0.5
    assert temp['depths'][-1] == 441.5  # deepest native sigma level
    assert temp['bounds'][0] < temp['bounds'][2]  # min_lon < max_lon
    assert temp['forecast'] is False and temp['forecastFrom'] is None
    assert temp['capabilities'] == {'climatology': True, 'trend': True}

    # All 8 variables share the exact same dts/depths/bounds, since they move
    # through the pipeline as a bundle.
    for v in variables:
        assert v['dts'] == temp['dts']
        assert v['depths'] == temp['depths']
        assert v['bounds'] == temp['bounds']


def test_ssc_no_synced_dates(monkeypatch):
    fake = FakeChClient(sync_dates=[])
    monkeypatch.setattr('variables.get_ch_client', lambda: fake)

    variables = get_variables()
    assert len(variables) == 8
    assert all(v['dts'] == [] for v in variables)


def test_liveocean_absent_without_table_and_never_created(monkeypatch):
    fake = FakeChClient(sync_dates=[date(2026, 1, 1)], lo_days=None)
    monkeypatch.setattr('variables.get_ch_client', lambda: fake)

    variables = get_variables()
    assert _by_source(variables, 'LiveOcean') == []
    assert not any('liveocean' in c.lower() for c in fake.commands)


def test_liveocean_dts_follow_steps_and_forecast_from_latest_run(monkeypatch):
    # Day 0 of run 10-13, then run 10-14's full 3-day forecast (+ trailing 00Z).
    lo_days = [
        (date(2026, 10, 13), date(2026, 10, 13), 6),
        (date(2026, 10, 14), date(2026, 10, 14), 6),
        (date(2026, 10, 15), date(2026, 10, 14), 6),
        (date(2026, 10, 16), date(2026, 10, 14), 6),
        (date(2026, 10, 17), date(2026, 10, 14), 1),
    ]
    fake = FakeChClient(sync_dates=[], lo_days=lo_days)
    monkeypatch.setattr('variables.get_ch_client', lambda: fake)

    lo = _by_source(get_variables(), 'LiveOcean')
    assert {v['var'] for v in lo} == _LO_VARS
    temp = next(v for v in lo if v['var'] == 'temperature')
    assert len(temp['dts']) == 25
    assert temp['dts'][0] == datetime(2026, 10, 13, 0, 0)
    assert temp['dts'][1] == datetime(2026, 10, 13, 4, 0)
    assert temp['dts'][-1] == datetime(2026, 10, 17, 0, 0)
    assert temp['runDate'] == date(2026, 10, 14)
    assert temp['forecastFrom'] == datetime(2026, 10, 15, 0, 0)
    assert temp['stepHours'] == 4
    assert temp['depths'][-1] == -1  # bottom sentinel
    assert temp['capabilities'] == {'climatology': False, 'trend': False}
