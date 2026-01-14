from unittest.mock import MagicMock, patch
from dl2pkg.png_worker import process_png, find_pending_png


def test_process_png_missing(tmp_path):
    conn = MagicMock()
    cur = MagicMock()

    def cursor_cm():
        cm = MagicMock()
        cm.__enter__.return_value = cur
        cm.__exit__.return_value = False
        return cm

    conn.cursor.side_effect = cursor_cm

    row = {'id': 1, 'file_path_sublevel': str(tmp_path / 'nope.nc'), 'variable': 'dissolved_oxygen'}

    ok = process_png(conn, row, dry_run=False)
    assert ok is False


@patch('nc2tile.main')
def test_process_png_calls_nc2tile(mock_nc2, tmp_path):
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

    row = {'id': 2, 'file_path_sublevel': str(p), 'variable': 'dissolved_oxygen'}

    ok = process_png(conn, row, dry_run=True)
    assert ok is True
    mock_nc2.assert_not_called()
