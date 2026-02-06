from unittest.mock import MagicMock, patch
from dl2pkg.png_worker import process_image


def _make_conn_with_lock():
    conn = MagicMock()
    cur = MagicMock()

    def cursor_cm():
        cm = MagicMock()
        cm.__enter__.return_value = cur
        cm.__exit__.return_value = False
        return cm

    conn.cursor.side_effect = cursor_cm
    cur.fetchone.return_value = (True,)
    return conn, cur


def test_process_image_missing(tmp_path):
    conn, cur = _make_conn_with_lock()

    row = {'row_id': 1, 'variable_id': 10, 'nc_path': str(tmp_path / 'nope.nc'), 'start_time': None, 'end_time': None}

    ok = process_image(conn, row)
    assert ok is False


@patch('dl2pkg.png_worker.get_variable_precision', return_value=0.1)
@patch('dl2pkg.png_worker.get_variable_from_id', return_value='dissolved_oxygen')
@patch('nc2tile.main')
def test_process_image_calls_nc2tile(mock_nc2, _mock_var, _mock_prec, tmp_path):
    p = tmp_path / 's.nc'
    p.write_text('dummy')
    conn, cur = _make_conn_with_lock()

    row = {'row_id': 2, 'variable_id': 10, 'nc_path': str(p), 'start_time': None, 'end_time': None}

    ok = process_image(conn, row)
    assert ok is True
    mock_nc2.assert_called()
