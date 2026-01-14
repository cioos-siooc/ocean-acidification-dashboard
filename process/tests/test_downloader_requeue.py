from dl2pkg.downloader import requeue_failed
from unittest.mock import MagicMock


def test_requeue_dry_run(monkeypatch):
    conn = MagicMock()
    cur = MagicMock()
    def cursor_cm():
        cm = MagicMock()
        cm.__enter__.return_value = cur
        cm.__exit__.return_value = False
        return cm
    conn.cursor.side_effect = cursor_cm

    # mock count
    cur.fetchone.return_value = (5,)
    cnt = requeue_failed(conn, dataset='foo', date='2026-01-05', variable='bar', dry_run=True)
    assert cnt == 5


def test_requeue_exec(monkeypatch):
    conn = MagicMock()
    cur = MagicMock()
    def cursor_cm():
        cm = MagicMock()
        cm.__enter__.return_value = cur
        cm.__exit__.return_value = False
        return cm
    conn.cursor.side_effect = cursor_cm

    # mock count
    cur.fetchone.return_value = (3,)
    cnt = requeue_failed(conn, dataset=None, date=None, variable=None, dry_run=False)
    assert cnt == 3
    # ensure the update was called
    assert any('UPDATE nc_files SET status_dl=' in str(call) for call in cur.execute.call_args_list)
