from dl2pkg.downloader import maybe_create_compute_rows
from unittest.mock import MagicMock
from datetime import datetime


def test_no_create_when_missing_deps(monkeypatch):
    conn = MagicMock()
    cur = MagicMock()

    # cursor context manager
    def cursor_cm():
        cm = MagicMock()
        cm.__enter__.return_value = cur
        cm.__exit__.return_value = False
        return cm
    conn.cursor.side_effect = cursor_cm

    # Simulate counts: DIC exists, TA missing
    def fetchone_side():
        # First check DIC -> return (1,)
        # Second check TA -> return (0,)
        # etc.
        seq = [(1,), (0,), (0,), (0,)]
        for v in seq:
            yield v
    gen = fetchone_side()
    cur.fetchone.side_effect = lambda: next(gen)

    res = maybe_create_compute_rows(conn, 1, datetime(2026,1,5,0,30), datetime(2026,1,5,23,30))
    assert res is False


def test_create_when_all_deps_present(monkeypatch):
    conn = MagicMock()
    cur = MagicMock()

    # cursor context manager
    def cursor_cm():
        cm = MagicMock()
        cm.__enter__.return_value = cur
        cm.__exit__.return_value = False
        return cm
    conn.cursor.side_effect = cursor_cm

    # Simulate counts: DIC,TA,TEMP,SAL each exist -> four (1,1,1,1) then INSERT returns (42,)
    def fetchone_side():
        seq = [(1,), (1,), (1,), (1,), (42,)]
        for v in seq:
            yield v
    gen = fetchone_side()
    cur.fetchone.side_effect = lambda: next(gen)

    res = maybe_create_compute_rows(conn, 1, datetime(2026,1,5,0,30), datetime(2026,1,5,23,30))
    assert res is True
    # Ensure we performed an INSERT with status='pending_compute'
    assert any("status='pending_compute'" in str(call) for call in cur.execute.call_args_list)
