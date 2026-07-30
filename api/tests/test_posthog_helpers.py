import posthog_helpers
from posthog_helpers import capture_event, client_ip


class FakeClient:
    def __init__(self, raise_on_capture=False):
        self.calls = []
        self.raise_on_capture = raise_on_capture

    def capture(self, distinct_id, event, properties=None, disable_geoip=None):
        if self.raise_on_capture:
            raise RuntimeError("boom")
        self.calls.append((distinct_id, event, properties, disable_geoip))


class FakeClientAddr:
    def __init__(self, host):
        self.host = host


class FakeState:
    def __init__(self, start_time=None):
        if start_time is not None:
            self.start_time = start_time


class FakeHttpRequest:
    def __init__(self, host, headers=None, start_time=None):
        self.client = FakeClientAddr(host) if host is not None else None
        self.headers = headers or {}
        self.state = FakeState(start_time)


def test_capture_event_noop_without_client(monkeypatch):
    monkeypatch.setattr(posthog_helpers, "_client", None)
    # Should not raise even though there's nothing to capture to.
    capture_event(FakeHttpRequest("1.2.3.4"), "extract_timeseries", {"var": "temperature"})


def test_capture_event_forwards_to_client_with_public_ip(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(posthog_helpers, "_client", fake)

    capture_event(FakeHttpRequest("8.8.8.8"), "extract_timeseries", {"var": "temperature"})

    assert len(fake.calls) == 1
    distinct_id, event, properties, disable_geoip = fake.calls[0]
    assert distinct_id == "8.8.8.8"
    assert event == "extract_timeseries"
    assert properties["var"] == "temperature"
    assert disable_geoip is False


def test_capture_event_disables_geoip_for_private_ip(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(posthog_helpers, "_client", fake)

    # 172.18.0.1 is the classic Docker bridge gateway address seen when a
    # reverse proxy on the host connects into a container's published port.
    capture_event(FakeHttpRequest("172.18.0.1"), "extract_timeseries", {})

    distinct_id, _event, _properties, disable_geoip = fake.calls[0]
    assert distinct_id == "172.18.0.1"
    assert disable_geoip is True


def test_capture_event_adds_duration_from_request_state(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(posthog_helpers, "_client", fake)
    monkeypatch.setattr(posthog_helpers.time, "perf_counter", lambda: 10.5)

    capture_event(FakeHttpRequest("8.8.8.8", start_time=10.0), "extract_timeseries", {})

    _distinct_id, _event, properties, _disable_geoip = fake.calls[0]
    assert properties["duration_ms"] == 500.0


def test_capture_event_swallows_client_errors(monkeypatch):
    fake = FakeClient(raise_on_capture=True)
    monkeypatch.setattr(posthog_helpers, "_client", fake)

    # Must not propagate — analytics failures can't be allowed to break a request.
    capture_event(FakeHttpRequest("1.2.3.4"), "extract_timeseries", {"var": "temperature"})


def test_client_ip_prefers_x_forwarded_for():
    request = FakeHttpRequest("172.18.0.1", headers={"x-forwarded-for": "8.8.8.8, 172.18.0.1"})
    assert client_ip(request) == "8.8.8.8"


def test_client_ip_falls_back_to_x_real_ip():
    request = FakeHttpRequest("172.18.0.1", headers={"x-real-ip": "8.8.4.4"})
    assert client_ip(request) == "8.8.4.4"


def test_client_ip_falls_back_to_client_host_without_proxy_headers():
    assert client_ip(FakeHttpRequest("8.8.8.8")) == "8.8.8.8"


def test_client_ip_falls_back_when_no_client():
    assert client_ip(FakeHttpRequest(None)) == "unknown"
