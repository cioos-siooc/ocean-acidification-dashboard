from dl2pkg import cli
from unittest.mock import MagicMock


def test_run_forwards_requeue_with_date(monkeypatch):
    conn = MagicMock()
    # stub DB connection and helpers
    monkeypatch.setattr('dl2pkg.cli.get_db_conn', lambda: conn)
    monkeypatch.setattr('dl2pkg.cli.ensure_schema', lambda c: None)
    monkeypatch.setattr('dl2pkg.cli.upsert_dataset', lambda c, ds: 1)
    monkeypatch.setattr('dl2pkg.cli.create_rows_for_date', lambda *a, **k: None)
    monkeypatch.setattr('dl2pkg.cli.do_check', lambda *a, **k: None)

    calls = {}

    def fake_requeue_failed(conn_arg, dataset=None, date=None, variable=None, dry_run=None):
        calls['requeue'] = (conn_arg is conn, dataset, date, variable, dry_run)
        return 2

    monkeypatch.setattr('dl2pkg.downloader.requeue_failed', fake_requeue_failed)
    monkeypatch.setattr('dl2pkg.cli.do_download', lambda *a, **k: calls.setdefault('download', True))

    cli.main(['run', '--date', '2026-01-05', '--requeue-failed'])

    assert 'requeue' in calls
    assert calls['requeue'] == (True, None, '2026-01-05', None, False)
    assert calls.get('download') is True


def test_run_forwards_requeue_without_date(monkeypatch):
    conn = MagicMock()
    monkeypatch.setattr('dl2pkg.cli.get_db_conn', lambda: conn)
    monkeypatch.setattr('dl2pkg.cli.ensure_schema', lambda c: None)
    monkeypatch.setattr('dl2pkg.cli.upsert_dataset', lambda c, ds: 1)
    monkeypatch.setattr('dl2pkg.cli.do_check', lambda *a, **k: None)

    calls = {}

    def fake_requeue_failed(conn_arg, dataset=None, date=None, variable=None, dry_run=None):
        calls['requeue'] = (conn_arg is conn, dataset, date, variable, dry_run)
        return 5

    monkeypatch.setattr('dl2pkg.downloader.requeue_failed', fake_requeue_failed)
    monkeypatch.setattr('dl2pkg.cli.do_download', lambda *a, **k: calls.setdefault('download', True))

    cli.main(['run', '--requeue-failed'])

    assert 'requeue' in calls
    assert calls['requeue'] == (True, None, None, None, False)
    assert calls.get('download') is True
