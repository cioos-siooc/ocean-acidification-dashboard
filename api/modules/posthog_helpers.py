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

import logging
import os
from typing import Optional

from posthog import Posthog

logger = logging.getLogger(__name__)

_api_key = os.getenv("POSTHOG_API_KEY", "")
_host = os.getenv("POSTHOG_HOST", "https://us.i.posthog.com")

# One long-lived client for the process: capture() enqueues onto an internal
# queue and a background thread batches/flushes over HTTP, so a single
# shared instance (not one per request) is the correct usage here.
_client: Optional[Posthog] = Posthog(_api_key, host=_host) if _api_key else None


def capture_event(distinct_id: str, event: str, properties: Optional[dict] = None) -> None:
    """Best-effort usage event capture. Never raises."""
    if _client is None:
        return
    try:
        _client.capture(distinct_id=distinct_id, event=event, properties=properties)
    except Exception:
        logger.warning("PostHog capture failed for event %s", event, exc_info=True)


def client_distinct_id(http_request) -> str:
    """Best-effort anonymous distinct_id for usage analytics.

    This app has no user accounts, so client IP stands in for distinct_id —
    fine for aggregate "which variable/region is queried most" analysis,
    though it under/overcounts unique users behind shared NAT or rotating IPs.
    """
    return http_request.client.host if http_request.client else "unknown"
