from unittest.mock import MagicMock, patch
from datetime import datetime
from dl2pkg.png_worker import process_png


@patch('nc2tile.main')
def test_process_png_handles_mixed_naive_and_aware(mock_nc2, tmp_path):
    # nc2tile returns aware string and naive string
    mock_nc2.return_value = ['2026-01-05T00:30:00', '2026-01-05T01:30:00Z']

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

    # Simulate an existing naive datetime in the DB
    existing_naive = datetime(2026, 1, 4, 0, 30)
    # first cursor calls: pg_try_advisory_lock -> (True,), then SELECT available_datetimes -> ( [existing_naive], )
    cur.fetchone.side_effect = [(True,), ([existing_naive],)]

    row = {'id': 2, 'file_path_sublevel': str(p), 'variable': 'dissolved_oxygen', 'start_time': None, 'dataset_id': 7}

    ok = process_png(conn, row, dry_run=False, simulate=True)
    assert ok is True

    # find UPDATE call and ensure it contains a list of datetimes and is sorted
    found = False
    for call in cur.execute.call_args_list:
        sql = call[0][0] if call and call[0] else ''
        if isinstance(sql, str) and sql.lower().startswith('update erddap_variables'):
            params = call[0][1]
            arr = params[0]
            assert isinstance(arr, list)
            # should include three distinct datetimes (existing + two returned)
            iso_list = [x.isoformat() for x in arr]
            assert any(s.startswith('2026-01-04T00:30:00') for s in iso_list)
            assert any(s.startswith('2026-01-05T00:30:00') for s in iso_list)
            assert any(s.startswith('2026-01-05T01:30:00') for s in iso_list)
            # ensure sorted ascending
            assert arr[0] <= arr[1] <= arr[2]
            found = True
    assert found is True
