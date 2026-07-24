"""PostHog Cloud client helper shared by API modules.

Tracks API usage (which variables/sources/regions are queried) for product
analytics. Configured via environment variables:

  POSTHOG_API_KEY
      PostHog Cloud project API key. Capture is a no-op when unset, so
      analytics never breaks or slows down a request in environments
      that don't set it (e.g. local dev by default).

  POSTHOG_HOST
      Region-specific ingestion host, e.g. https://us.i.posthog.com or
      https://eu.i.posthog.com. Defaults to the US host.
"""
from __future__ import annotations

import ipaddress
import logging
import os
import time
from typing import Optional

from posthog import Posthog

logger = logging.getLogger(__name__)

_api_key = os.getenv("POSTHOG_API_KEY", "")
_host = os.getenv("POSTHOG_HOST", "https://us.i.posthog.com")

# One long-lived client for the process: capture() enqueues onto an internal
# queue and a background thread batches/flushes over HTTP, so a single
# shared instance (not one per request) is the correct usage here.
_client: Optional[Posthog] = Posthog(_api_key, host=_host) if _api_key else None


def _is_routable_public_ip(ip: str) -> bool:
    """False for private/loopback/link-local addresses and anything unparsable.

    The API sits behind nginx on the same host (see
    nginx/maintenance.nginx.conf), which forwards X-Forwarded-For/X-Real-IP.
    Without those headers, http_request.client.host is the Docker bridge
    gateway (e.g. 172.18.0.1) for every request — a private address PostHog
    can't geolocate anyway, so geoip stays off for those.
    """
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return not (addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved)


def client_ip(http_request) -> str:
    """Best-effort real client IP, preferring the reverse proxy's forwarded
    header over the raw TCP peer.

    This app has no user accounts, so client IP stands in for both the
    PostHog distinct_id (fine for aggregate "which variable/region is
    queried most" analysis, though it under/overcounts unique users behind
    shared NAT or rotating IPs) and the geoip lookup source.
    """
    forwarded = http_request.headers.get("x-forwarded-for")
    if forwarded:
        # First entry is the original client; proxies may append their own.
        candidate = forwarded.split(",")[0].strip()
        if candidate:
            return candidate
    real_ip = http_request.headers.get("x-real-ip")
    if real_ip and real_ip.strip():
        return real_ip.strip()
    return http_request.client.host if http_request.client else "unknown"


def capture_event(http_request, event: str, properties: Optional[dict] = None) -> None:
    """Best-effort usage event capture. Never raises.

    Derives distinct_id/geoip from the request's real client IP (see
    client_ip) and, when the request-timing middleware in SERVER.py has
    stamped a start time on http_request.state, attaches duration_ms.
    """
    if _client is None:
        return
    try:
        ip = client_ip(http_request)
        props = dict(properties or {})
        start_time = getattr(http_request.state, "start_time", None)
        if start_time is not None:
            props["duration_ms"] = round((time.perf_counter() - start_time) * 1000, 1)
        _client.capture(
            distinct_id=ip,
            event=event,
            properties=props,
            disable_geoip=not _is_routable_public_ip(ip),
        )
    except Exception:
        logger.warning("PostHog capture failed for event %s", event, exc_info=True)
