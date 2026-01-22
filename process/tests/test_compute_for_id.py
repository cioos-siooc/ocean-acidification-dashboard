from unittest.mock import MagicMock, patch
from dl2pkg.compute import compute_for_id


def test_compute_for_id_calls_compute(monkeypatch):
    conn = MagicMock()
    cur = MagicMock()

    # Cursor context manager
    def cursor_cm():
        cm = MagicMock()
        cm.__enter__.return_value = cur
        cm.__exit__.return_value = False
        return cm
    conn.cursor.side_effect = cursor_cm

    # Simulate SELECT returning the row with status pending_compute
    cur.fetchone.return_value = (42, 2, '2026-01-16T00:30:00+00', '2026-01-16T23:30:00+00', 'pending_compute')

    # Patch compute_for_row to observe being called
    called = {'ok': False}
    def fake_compute_for_row(conn_arg, row, workers=2, base_dir=None):
        called['ok'] = True
        return True
    monkeypatch.setattr('dl2pkg.compute.compute_for_row', fake_compute_for_row)

    res = compute_for_id(conn, 42, workers=1, base_dir='/tmp', dry_run=False)
    assert res is True
    assert called['ok'] is True
