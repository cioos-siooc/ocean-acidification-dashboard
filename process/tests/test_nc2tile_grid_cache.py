from unittest.mock import MagicMock
import nc2tile


def test_get_grid_from_db_caches(monkeypatch):
    # prepare fake rows as returned by the DB
    rows = [
        {'row_idx': 0, 'col_idx': 0, 'lon': -123.0, 'lat': 49.0},
        {'row_idx': 0, 'col_idx': 1, 'lon': -122.9, 'lat': 49.0},
        {'row_idx': 1, 'col_idx': 0, 'lon': -123.0, 'lat': 49.1},
        {'row_idx': 1, 'col_idx': 1, 'lon': -122.9, 'lat': 49.1},
    ]

    cur = MagicMock()
    cur.fetchall.return_value = rows
    cm = MagicMock()
    cm.__enter__.return_value = cur
    cm.__exit__.return_value = False

    conn = MagicMock()
    conn.cursor.return_value = cm

    call_count = {'n': 0}

    def fake_get_db_conn():
        call_count['n'] += 1
        return conn

    monkeypatch.setattr(nc2tile, 'get_db_conn', fake_get_db_conn)

    # clear any existing cache
    nc2tile.GRID_CACHE = None

    g1 = nc2tile.get_grid_from_db('grid')
    g2 = nc2tile.get_grid_from_db('grid')

    # same object returned from cache and DB called only once
    assert g1 is g2
    assert call_count['n'] == 1
