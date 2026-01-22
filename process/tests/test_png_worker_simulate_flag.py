from unittest.mock import MagicMock, patch
from dl2pkg.png_worker import process_png


@patch('nc2tile.main')
def test_process_png_called_with_simulate(mock_nc2, tmp_path):
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

    # ensure lock check returns True
    cur.fetchone.side_effect = [(True,)]

    row = {'id': 2, 'file_path': str(p), 'variable': 'dissolved_oxygen', 'start_time': None, 'dataset_id': 7}

    ok = process_png(conn, row, dry_run=False, simulate=True)
    assert ok is True

    mock_nc2.assert_called_once()
    args = mock_nc2.call_args[0][0]
    assert '--simulate' in args
