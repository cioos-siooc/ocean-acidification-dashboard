"""DAS fetching and parsing utilities."""
import re
from datetime import datetime, timezone
import logging

logger = logging.getLogger("dl2.das")


def parse_das_for_times(das_text):
    """Return (time_coverage_end_datetime_or_None, actual_range_max_or_None)"""
    time_cov = None
    m = re.search(r'time_coverage_end\s+"([^"]+)"', das_text)
    if m:
        try:
            time_cov = datetime.fromisoformat(m.group(1).replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            try:
                time_cov = datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            except Exception:
                logger.debug("Failed parse time_coverage_end: %s", m.group(1))
                time_cov = None

    m2 = re.search(r'time\s*\{[^}]*actual_range\s*([0-9.eE+-]+)\s*,\s*([0-9.eE+-]+)\s*;', das_text, flags=re.S)
    actual_max = None
    if m2:
        try:
            actual_max = float(m2.group(2))
            # Use timezone-aware conversion (avoid deprecated utcfromtimestamp)
            actual_max = datetime.fromtimestamp(actual_max, tz=timezone.utc)
        except Exception:
            logger.debug("Failed parse actual_range: %s", m2.groups())
            actual_max = None

    return time_cov, actual_max


def fetch_das(base_url, timeout=30):
    import requests
    base = base_url.rstrip('/')
    if base.endswith('.das'):
        url = base
    else:
        url = f"{base}.das"
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text
