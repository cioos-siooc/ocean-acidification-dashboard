import os
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
import tempfile

from dl2pkg.downloader import download_nc


class DummyResponse:
    def __init__(self, chunks):
        self._chunks = chunks

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size=8192):
        for c in self._chunks:
            yield c

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def make_fake_conn():
    # simple MagicMock-style conn that records execute calls and returns dataset_id when asked
    conn = MagicMock()
    cur = MagicMock()

    def cursor_cm():
        cm = MagicMock()
        cm.__enter__.return_value = cur
        cm.__exit__.return_value = False
        return cm

    conn.cursor.side_effect = cursor_cm

    # fetchone behavior: when first SELECT is for dataset_id, return ('TEST_DS',)
    def fake_fetchone():
        return ("TEST_DS",)

    cur.fetchone.side_effect = fake_fetchone
    return conn, cur


@patch("dl2pkg.downloader.requests.get")
def test_download_nc_success(mock_get, tmp_path):
    mock_get.return_value = DummyResponse([b"abc", b"def"])
    conn, cur = make_fake_conn()

    # set NC_ROOT to writable temp dir
    os.environ["NC_ROOT"] = str(tmp_path)

    row = {"id": 1, "dataset_id": 1, "variable": "dissolved_oxygen", "start_time": datetime(2026,1,1,0,30,tzinfo=timezone.utc), "end_time": datetime(2026,1,1,23,30,tzinfo=timezone.utc)}

    ok = download_nc(conn, row, "https://example.erddap.org", dry_run=False)
    assert ok is True

    # ensure that we updated nc_jobs status to success_download
    assert any("UPDATE nc_jobs SET status='success_download'" in str(c) for c in cur.execute.call_args_list)


@patch("dl2pkg.downloader.requests.get")
def test_download_nc_failure(mock_get, tmp_path):
    # simulate HTTP error
    def raise_error(*args, **kwargs):
        raise Exception("network error")

    mock_get.side_effect = raise_error
    conn, cur = make_fake_conn()

    os.environ["NC_ROOT"] = str(tmp_path)

    row = {"id": 2, "dataset_id": 1, "variable": "dissolved_oxygen", "start_time": datetime(2026,1,2,0,30,tzinfo=timezone.utc), "end_time": datetime(2026,1,2,23,30,tzinfo=timezone.utc)}

    ok = download_nc(conn, row, "https://example.erddap.org", dry_run=False)
    assert ok is False

    # ensure that we updated nc_jobs status to failed_download
    assert any("UPDATE nc_jobs SET status='failed_download'" in str(c) for c in cur.execute.call_args_list)
