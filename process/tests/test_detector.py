from datetime import datetime, timezone
from dl2pkg.detector import compute_daily_chunks


def test_compute_daily_chunks_full_days():
    start = datetime(2026, 1, 1, 0, 30, tzinfo=timezone.utc)
    end = datetime(2026, 1, 3, 23, 30, tzinfo=timezone.utc)
    chunks = compute_daily_chunks(start, end, require_full_day=True)
    assert len(chunks) == 3
    assert chunks[0][0] == datetime(2026, 1, 1, 0, 30, tzinfo=timezone.utc)
    assert chunks[-1][1] == datetime(2026, 1, 3, 23, 30, tzinfo=timezone.utc)


def test_compute_daily_chunks_partial_skipped():
    # partial day (starts at 01:30) should be skipped when require_full_day=True
    start = datetime(2026, 1, 1, 1, 30, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, 23, 30, tzinfo=timezone.utc)
    chunks = compute_daily_chunks(start, end, require_full_day=True)
    assert len(chunks) == 0

    # but returned when require_full_day=False
    chunks2 = compute_daily_chunks(start, end, require_full_day=False)
    assert len(chunks2) == 1
    assert chunks2[0][0] == start
    assert chunks2[0][1] == end


def test_compute_daily_chunks_single_day():
    start = datetime(2026, 1, 5, 0, 30, tzinfo=timezone.utc)
    end = datetime(2026, 1, 5, 23, 30, tzinfo=timezone.utc)
    chunks = compute_daily_chunks(start, end, require_full_day=True)
    assert len(chunks) == 1
    cs, ce = chunks[0]
    assert cs.hour == 0 and cs.minute == 30
    assert ce.hour == 23 and ce.minute == 30
