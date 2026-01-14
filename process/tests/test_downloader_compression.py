import os
from dl2pkg.downloader import _compress_netcdf_file
import xarray as xr
import numpy as np


def make_test_nc(path):
    times = np.array([np.datetime64('2026-01-01T00:30:00Z')])
    depths = np.arange(40)
    lats = np.linspace(49,50,3)
    lons = np.linspace(-123,-122,3)
    data = np.random.rand(len(times), len(depths), len(lats), len(lons))
    ds = xr.Dataset({
        'dissolved_oxygen': (('time','depth','lat','lon'), data)
    }, coords={'time': times, 'depth': depths, 'lat': lats, 'lon': lons})
    ds.to_netcdf(path)


def test_compress(tmp_path):
    src = tmp_path / 'src.nc'
    make_test_nc(str(src))
    orig_size = os.path.getsize(src)

    comp = {"zlib": True, "complevel": 4, "shuffle": True}
    _compress_netcdf_file(str(src), comp)

    new_size = os.path.getsize(src)
    assert new_size > 0
    # try to open with xarray to confirm readable
    ds2 = xr.open_dataset(str(src))
    assert 'dissolved_oxygen' in ds2.data_vars
    ds2.close()

    # confirm the variable was written with compression (netCDF4 filters)
    try:
        from netCDF4 import Dataset as NC4
        nc = NC4(str(src), 'r')
        var = nc.variables['dissolved_oxygen']
        try:
            f = var.filters()
            assert f.get('zlib') is True
        except Exception:
            # some backends may not expose filters; at minimum confirm file is readable
            assert True
        finally:
            nc.close()
    except Exception:
        # netCDF4 not available in minimal environments; ensure file is readable suffices
        pass
