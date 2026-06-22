"""ClickHouse connection helper shared by API modules.

Provides `get_ch_client()`, which returns a `clickhouse_connect` client for
either the local docker-compose ClickHouse instance (`db-ch`) or a remote
ClickHouse instance, selected via environment variables:

  CH_HOST / CH_PORT / CH_USER / CH_PASSWORD
      Connection settings for the local instance (used when CH_USE_REMOTE
      is not enabled). CH_PASSWORD falls back to CLICKHOUSE_PASSWORD.

  CH_USE_REMOTE
      Set to "true"/"1"/"yes" to query a remote ClickHouse instance instead.

  CH_REMOTE_URL
      Required when CH_USE_REMOTE is enabled. A host, optionally with a
      scheme, port and credentials, e.g. "https://user:pass@host:8443" or
      just "host:8123". Defaults to https (port 8443) when no scheme/port
      is given.

  CH_REMOTE_USER / CH_REMOTE_PASSWORD
      Credentials for the remote instance, used when not embedded in
      CH_REMOTE_URL. Fall back to CH_USER / CH_PASSWORD.
"""
from __future__ import annotations

import os
from typing import Optional, Tuple
from urllib.parse import urlparse

import clickhouse_connect
from clickhouse_connect import common as ch_common
from clickhouse_connect.driver.httputil import get_pool_manager

# clickhouse_connect's default (300s) can be too short for the multi-GB
# Native-format inserts the SSC sync stage does (see api/modules/sync_hourly.py)
# — a slow insert that exceeds it still lands the data server-side, but the
# client times out waiting for the response and reports a false failure.
_SEND_RECEIVE_TIMEOUT = 1800

# clickhouse_connect keeps one shared, process-wide connection pool for every
# client and periodically force-closes it (default: every 600s) to guard
# against infrastructure (load balancers, NAT) silently dropping long-lived
# connections without telling the client. db-ch is a direct same-host Docker
# bridge connection with nothing in between that would ever do that — and the
# periodic clear can instead race with an in-flight request (hands it a
# connection the pool just tore down), surfacing as a spurious
# "Connection aborted" / ProtocolError on an otherwise-successful request.
# Disabling it removes that failure mode entirely for this deployment.
ch_common.set_setting('max_connection_age', 0)

def _fresh_pool_mgr():
    # get_client() defaults to one SHARED, process-wide connection pool
    # (clickhouse_connect.driver.httputil.default_pool_manager()) unless given
    # its own. get_ch_client() is called fresh per request throughout the API,
    # but they'd all draw from that same shared pool — so a connection idle
    # since some unrelated earlier request can get handed to a brand-new one.
    # The SSC sync stage's own timing (minutes between consecutive
    # /admin/syncHourly calls, while PROCESS exports+rsyncs) is longer than
    # ClickHouse's server-side HTTP keep-alive idle timeout, so a pooled
    # connection reused after that gap is often already closed server-side —
    # the client doesn't find out until it tries to use it, surfacing as
    # "Connection aborted" / ProtocolError. A dedicated, non-shared pool per
    # client means it never inherits a connection from an unrelated call.
    return get_pool_manager()


def _truthy(value: Optional[str]) -> bool:
    return (value or "").strip().lower() in ("1", "true", "yes")


def _parse_remote_url(url: str) -> Tuple[str, int, bool, Optional[str], Optional[str]]:
    if "://" not in url:
        url = f"https://{url}"
    parsed = urlparse(url)
    if not parsed.hostname:
        raise RuntimeError(f"CH_REMOTE_URL is not a valid host[:port] or URL: {url!r}")
    secure = parsed.scheme != "http"
    port = parsed.port or (8443 if secure else 8123)
    return parsed.hostname, port, secure, parsed.username, parsed.password


def get_ch_client():
    """Return a connected clickhouse_connect client (local or remote)."""
    if _truthy(os.getenv("CH_USE_REMOTE")):
        remote_url = os.getenv("CH_REMOTE_URL")
        if not remote_url:
            raise RuntimeError("CH_USE_REMOTE is enabled but CH_REMOTE_URL is not set")
        host, port, secure, url_user, url_password = _parse_remote_url(remote_url)
        user = url_user or os.getenv("CH_REMOTE_USER", os.getenv("CH_USER", "default"))
        password = url_password or os.getenv("CH_REMOTE_PASSWORD", os.getenv("CH_PASSWORD", os.getenv("CLICKHOUSE_PASSWORD", "")))
        return clickhouse_connect.get_client(
            host=host, port=port, username=user, password=password, secure=secure,
            autogenerate_session_id=False, send_receive_timeout=_SEND_RECEIVE_TIMEOUT,
            pool_mgr=_fresh_pool_mgr(),
        )

    host = os.getenv("CH_HOST", "localhost")
    port = int(os.getenv("CH_PORT", "8123"))
    user = os.getenv("CH_USER", "default")
    password = os.getenv("CH_PASSWORD", os.getenv("CLICKHOUSE_PASSWORD", ""))
    # Each call gets a fresh, short-lived client for one-shot queries — no temp
    # tables or session-scoped SET statements rely on ClickHouse sessions
    # anywhere in this codebase. Without this, clickhouse_connect's default
    # auto-generated session id can get rejected with SESSION_IS_LOCKED if
    # anything overlaps on it (a slow OPTIMIZE ... FINAL, a large insert still
    # draining, a retried request, etc.) — sessionless queries can't hit that.
    return clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password,
        autogenerate_session_id=False, send_receive_timeout=_SEND_RECEIVE_TIMEOUT,
        pool_mgr=_fresh_pool_mgr(),
    )
