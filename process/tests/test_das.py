from dl2pkg.das import parse_das_for_times
from datetime import timezone

def test_parse_das_sample():
    sample = '''Attributes {
  time {
    Float64 actual_range 1.1676114e+9, 1.7678286e+9;
    String time_coverage_end "2026-01-07T23:30:00Z";
  }
}'''
    time_cov, actual_max = parse_das_for_times(sample)
    assert time_cov is not None
    assert time_cov.tzinfo is not None and time_cov.tzinfo == timezone.utc
    assert time_cov.isoformat().startswith('2026-01-07T23:30')
    assert actual_max is not None
    assert actual_max.tzinfo is not None and actual_max.tzinfo == timezone.utc
