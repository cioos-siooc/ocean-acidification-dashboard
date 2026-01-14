import os
from datetime import datetime, timezone
import xarray as xr
import numpy as np
from dl2pkg.sublevel import load_depth_indices, load_configs, process_sublevel
from unittest.mock import MagicMock


def make_test_nc(path):
    # create small dataset with dims time x depth x lat x lon
    import numpy as _np
    times = _np.array([_np.datetime64('2026-01-01T00:30:00Z')])
    depths = np.arange(40)
    lats = np.linspace(49,50,3)
    lons = np.linspace(-123,-122,3)
    data = np.random.rand(len(times), len(depths), len(lats), len(lons))
    ds = xr.Dataset({
        'dissolved_oxygen': (('time','depth','lat','lon'), data)
    }, coords={'time': times, 'depth': depths, 'lat': lats, 'lon': lons})
    ds.to_netcdf(path)


def test_load_config():
    cfg = load_configs()
    assert isinstance(cfg, dict)
    assert "depth_indices" in cfg
    assert "compression" in cfg
    assert isinstance(cfg["compression"], dict)


def test_process_sublevel(tmp_path):
    # create source nc
    src = tmp_path / "src.nc"
    make_test_nc(str(src))

    conn = MagicMock()
    cur = MagicMock()

    def cursor_cm():
        cm = MagicMock()
        cm.__enter__.return_value = cur
        cm.__exit__.return_value = False
        return cm

    conn.cursor.side_effect = cursor_cm

    row = {'id': 1, 'file_path': str(src), 'variable': 'dissolved_oxygen', 'start_time': datetime(2026,1,1,0,30,tzinfo=timezone.utc), 'end_time': datetime(2026,1,1,23,30,tzinfo=timezone.utc)}

    ok = process_sublevel(conn, row, dry_run=True)
    assert ok is True
