import os
from datetime import datetime, timezone
import xarray as xr
import numpy as np
from dl2pkg.sublevel import process_sublevel
from dl2pkg.db import get_db_conn
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


def test_process_sublevel_compressed(tmp_path):
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
    # advisory lock returns True
    cur.fetchone.return_value = (True,)

    row = {'id': 999, 'file_path': str(src), 'variable': 'dissolved_oxygen', 'start_time': datetime(2026,1,1,0,30,tzinfo=timezone.utc), 'end_time': datetime(2026,1,1,23,30,tzinfo=timezone.utc)}

    ok = process_sublevel(conn, row, dry_run=False)
    assert ok is True
    # ensure output exists and is a netcdf; check the file has size
    out_dir = os.path.join(os.getenv('NC_ROOT', '/opt/data/nc'), 'sublevels', 'dissolved_oxygen')
    # find the file written
    files = [f for f in sorted(os.listdir(out_dir)) if f.endswith('.sub.nc')]
    assert len(files) > 0
    p = os.path.join(out_dir, files[-1])
    assert os.path.getsize(p) > 0
    # try opening with xarray to confirm readable
    ds2 = xr.open_dataset(p)
    assert 'dissolved_oxygen' in ds2.data_vars
    ds2.close()
