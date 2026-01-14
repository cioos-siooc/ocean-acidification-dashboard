import numpy as np
import xarray as xr
from extractTimeseries import extract_timeseries
from unittest.mock import patch, MagicMock
import pandas as pd


def test_extract_timeseries_collects_all_files(tmp_path, monkeypatch):
    # create two files covering two days
    times1 = np.array(['2026-01-01T00:30:00','2026-01-01T01:30:00'], dtype='datetime64[s]')
    times2 = np.array(['2026-01-02T00:30:00','2026-01-02T01:30:00'], dtype='datetime64[s]')
    data1 = np.zeros((2,1,1), dtype=float)
    data2 = np.ones((2,1,1), dtype=float)
    ds1 = xr.Dataset({'temp': (('time','y','x'), data1)}, coords={'time':times1, 'y':[0], 'x':[0]})
    ds2 = xr.Dataset({'temp': (('time','y','x'), data2)}, coords={'time':times2, 'y':[0], 'x':[0]})
    f1 = tmp_path / 'temp_2026-01-01T00:30:00_to_2026-01-01T23:30:00.nc'
    f2 = tmp_path / 'temp_2026-01-02T00:30:00_to_2026-01-02T23:30:00.nc'
    ds1.to_netcdf(f1)
    ds2.to_netcdf(f2)

    # monkeypatch DB helpers to avoid DB access
    monkeypatch.setattr('extractTimeseries.connect_db', lambda *a, **k: MagicMock())
    monkeypatch.setattr('extractTimeseries.query_nearest_rowcol', lambda conn, table, lat, lon: (0,0,0.0,0.0))
    monkeypatch.setattr('extractTimeseries.get_grid_shape_from_db', lambda conn, table: (1,1))

    times, values = extract_timeseries(var='temp', lat=0.0, lon=0.0, data_dir=str(tmp_path), db_dsn=None, db_host='db')
    assert isinstance(times, pd.Series)
    assert isinstance(values, pd.Series)
    # expect 4 time entries and values [0,0,1,1]
    assert len(times) == 4
    assert list(values) == [0.0, 0.0, 1.0, 1.0]
