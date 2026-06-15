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
        return clickhouse_connect.get_client(host=host, port=port, username=user, password=password, secure=secure)

    host = os.getenv("CH_HOST", "localhost")
    port = int(os.getenv("CH_PORT", "8123"))
    user = os.getenv("CH_USER", "default")
    password = os.getenv("CH_PASSWORD", os.getenv("CLICKHOUSE_PASSWORD", ""))
    return clickhouse_connect.get_client(host=host, port=port, username=user, password=password)
