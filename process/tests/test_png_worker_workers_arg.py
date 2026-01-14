from unittest.mock import MagicMock, patch
from dl2pkg.png_worker import process_png

@patch('nc2tile.main')
def test_process_png_passes_workers(mock_nc2, tmp_path):
    conn = MagicMock()
    cur = MagicMock()

    def cursor_cm():
        cm = MagicMock()
        cm.__enter__.return_value = cur
        cm.__exit__.return_value = False
        return cm

    conn.cursor.side_effect = cursor_cm
    # create dummy file
    p = tmp_path / 's.nc'
    p.write_text('dummy')

    row = {'id': 2, 'file_path_sublevel': str(p), 'variable': 'dissolved_oxygen'}

    ok = process_png(conn, row, dry_run=False, workers=7)
    assert ok is True
    # ensure nc2tile was called with --workers 7
    called_args = mock_nc2.call_args[0][0]
    assert '--workers' in called_args and '7' in called_args
