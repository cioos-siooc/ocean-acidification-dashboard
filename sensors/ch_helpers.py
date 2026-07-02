"""ClickHouse connection helper for the process container.

Reads the same CH_* env vars as the API container so both use identical
connection config.  No pool management needed here — process scripts are
batch jobs that open one client, do their work, and exit.
"""
from __future__ import annotations

import os
from urllib.parse import urlparse

import clickhouse_connect


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in ("1", "true", "yes")


def _parse_remote_url(url: str):
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
            autogenerate_session_id=False,
        )

    host = os.getenv("CH_HOST", "db-ch")
    port = int(os.getenv("CH_PORT", "8123"))
    user = os.getenv("CH_USER", "default")
    password = os.getenv("CH_PASSWORD", os.getenv("CLICKHOUSE_PASSWORD", ""))
    return clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password,
        autogenerate_session_id=False,
    )
