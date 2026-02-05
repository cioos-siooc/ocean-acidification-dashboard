from unittest.mock import MagicMock, patch
from dl2pkg.png_worker import process_image


@patch('dl2pkg.png_worker.get_variable_precision', return_value=0.1)
@patch('dl2pkg.png_worker.get_variable_from_id', return_value='dissolved_oxygen')
@patch('nc2tile.main')
def test_process_image_passes_workers(mock_nc2, _mock_var, _mock_prec, tmp_path):
    conn = MagicMock()
    cur = MagicMock()

    def cursor_cm():
        cm = MagicMock()
        cm.__enter__.return_value = cur
        cm.__exit__.return_value = False
        return cm

    conn.cursor.side_effect = cursor_cm
    cur.fetchone.return_value = (True,)
    # create dummy file
    p = tmp_path / 's.nc'
    p.write_text('dummy')

    row = {'row_id': 2, 'variable_id': 10, 'nc_path': str(p), 'start_time': None, 'end_time': None}

    ok = process_image(conn, row, workers=7)
    assert ok is True
    # ensure nc2tile was called with --workers 7
    called_args = mock_nc2.call_args[0][0]
    assert '--workers' in called_args and '7' in called_args
