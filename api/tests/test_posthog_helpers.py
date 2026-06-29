import posthog_helpers
from posthog_helpers import capture_event, client_distinct_id


class FakeClient:
    def __init__(self, raise_on_capture=False):
        self.calls = []
        self.raise_on_capture = raise_on_capture

    def capture(self, distinct_id, event, properties=None):
        if self.raise_on_capture:
            raise RuntimeError("boom")
        self.calls.append((distinct_id, event, properties))


class FakeClientAddr:
    def __init__(self, host):
        self.host = host


class FakeHttpRequest:
    def __init__(self, host):
        self.client = FakeClientAddr(host) if host is not None else None


def test_capture_event_noop_without_client(monkeypatch):
    monkeypatch.setattr(posthog_helpers, "_client", None)
    # Should not raise even though there's nothing to capture to.
    capture_event("1.2.3.4", "extract_timeseries", {"var": "temperature"})


def test_capture_event_forwards_to_client(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(posthog_helpers, "_client", fake)

    capture_event("1.2.3.4", "extract_timeseries", {"var": "temperature"})

    assert fake.calls == [("1.2.3.4", "extract_timeseries", {"var": "temperature"})]


def test_capture_event_swallows_client_errors(monkeypatch):
    fake = FakeClient(raise_on_capture=True)
    monkeypatch.setattr(posthog_helpers, "_client", fake)

    # Must not propagate — analytics failures can't be allowed to break a request.
    capture_event("1.2.3.4", "extract_timeseries", {"var": "temperature"})


def test_client_distinct_id_uses_client_host():
    assert client_distinct_id(FakeHttpRequest("203.0.113.5")) == "203.0.113.5"


def test_client_distinct_id_falls_back_when_no_client():
    assert client_distinct_id(FakeHttpRequest(None)) == "unknown"
