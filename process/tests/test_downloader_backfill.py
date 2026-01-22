from unittest.mock import MagicMock
from dl2pkg.downloader import do_download


def test_do_download_backfill_calls_maybe_create(monkeypatch):
    conn = MagicMock()
    # configure find_pending_rows to return one pending row
    monkeypatch.setattr('dl2pkg.downloader.find_pending_rows', lambda conn, limit=5: [{'id': 1, 'dataset_id': 2, 'variable': 'dissolved_inorganic_carbon', 'start_time': '2026-01-16T00:30:00+00', 'end_time': '2026-01-16T23:30:00+00'}])

    # make download_nc succeed and update DB
    monkeypatch.setattr('dl2pkg.downloader.download_nc', lambda conn, row, erddap_base, dry_run=False: True)

    called = {'count': 0}
    def fake_maybe(conn, ds, st, en):
        called['count'] += 1
        return True
    monkeypatch.setattr('dl2pkg.downloader.maybe_create_compute_rows', fake_maybe)

    do_download(conn, 'https://example', dry_run=False, limit=5)
    assert called['count'] == 1
