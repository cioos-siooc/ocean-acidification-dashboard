from unittest.mock import MagicMock, patch
from dl2pkg.png_worker import process_png
from datetime import datetime


@patch('nc2tile.main')
def test_process_png_updates_available_datetimes(mock_nc2, tmp_path):
    # nc2tile will report multiple processed times
    mock_nc2.return_value = ['2026-01-05T00:30:00', '2026-01-05T01:30:00']

    # create dummy sublevel file
    p = tmp_path / 's.nc'
    p.write_text('dummy')
    conn = MagicMock()
    cur = MagicMock()

    def cursor_cm():
        cm = MagicMock()
        cm.__enter__.return_value = cur
        cm.__exit__.return_value = False
        return cm

    conn.cursor.side_effect = cursor_cm

    # cur.fetchone will be called first for pg_try_advisory_lock (return True),
    # then for SELECT available_datetimes (return None)
    cur.fetchone.side_effect = [(True,), (None,)]

    row = {'id': 2, 'file_path_sublevel': str(p), 'variable': 'dissolved_oxygen', 'start_time': datetime(2026,1,5,0,30), 'dataset_id': 7}

    ok = process_png(conn, row, dry_run=False)
    assert ok is True

    # look for an UPDATE erddap_variables call that contains both datetimes as Python datetimes
    found = False
    for call in cur.execute.call_args_list:
        sql = call[0][0] if call and call[0] else ''
        if isinstance(sql, str) and sql.lower().startswith('update erddap_variables'):
            # check params contain dataset_id and variable
            params = call[0][1]
            assert params[1] == 7
            assert params[2] == 'dissolved_oxygen'
            # params[0] should be a list of datetimes
            arr = params[0]
            assert isinstance(arr, list)
            assert any(getattr(x, 'isoformat', None) and x.isoformat().startswith('2026-01-05T00:30:00') for x in arr)
            assert any(getattr(x, 'isoformat', None) and x.isoformat().startswith('2026-01-05T01:30:00') for x in arr)
            # ensure sorted order
            assert arr[0] <= arr[1]
            found = True
    assert found is True
