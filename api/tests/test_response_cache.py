from typing import Optional

import pytest
from cachetools import TTLCache
from pydantic import BaseModel

import response_cache


class DummyRequest(BaseModel):
    var: str
    depth: Optional[float] = None


@pytest.fixture(autouse=True)
def clear_caches():
    response_cache.invalidate_all()
    yield
    response_cache.invalidate_all()


def test_make_key_stable_for_identical_requests():
    a = DummyRequest(var="temperature", depth=5.0)
    b = DummyRequest(var="temperature", depth=5.0)
    assert response_cache.make_key("ep", a) == response_cache.make_key("ep", b)


def test_make_key_differs_by_field():
    a = DummyRequest(var="temperature", depth=5.0)
    b = DummyRequest(var="temperature", depth=6.0)
    assert response_cache.make_key("ep", a) != response_cache.make_key("ep", b)


def test_make_key_differs_by_endpoint():
    a = DummyRequest(var="temperature")
    assert response_cache.make_key("ep1", a) != response_cache.make_key("ep2", a)


def test_model_cache_hit_and_miss():
    key = "some-key"
    assert response_cache.get_model(key) is None
    response_cache.set_model(key, {"min": 1, "max": 2})
    assert response_cache.get_model(key) == {"min": 1, "max": 2}


def test_sensor_cache_hit_and_miss():
    key = "some-sensor-key"
    assert response_cache.get_sensor(key) is None
    response_cache.set_sensor(key, {"time": [], "value": []})
    assert response_cache.get_sensor(key) == {"time": [], "value": []}


def test_model_and_sensor_caches_are_independent():
    key = "shared-key"
    response_cache.set_model(key, "model-value")
    assert response_cache.get_sensor(key) is None


def test_invalidate_all_clears_both_caches():
    response_cache.set_model("k1", "v1")
    response_cache.set_sensor("k2", "v2")
    response_cache.invalidate_all()
    assert response_cache.get_model("k1") is None
    assert response_cache.get_sensor("k2") is None


def test_ttl_expiry():
    # Exercises cachetools' TTLCache eviction directly with a fake timer,
    # rather than real time.sleep, to keep the test fast and deterministic.
    fake_time = [0.0]
    cache = TTLCache(maxsize=10, ttl=5, timer=lambda: fake_time[0])
    cache["k"] = "v"
    assert cache.get("k") == "v"
    fake_time[0] = 10.0
    assert cache.get("k") is None
