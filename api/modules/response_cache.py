"""In-process response cache for ClickHouse-backed extract endpoints.

Two TTLCache instances, split by staleness profile:
- `_model_cache` — SalishSeaCast model-only results. Backed by
  `SalishSeaCast_hourly`/`SalishSeaCast_daily`, effectively append-only for
  past dates; the only mutation path is `/admin/syncHourly`/`syncDaily`,
  which call `invalidate_all()` explicitly. The TTL here is just a safety
  net in case an invalidation call is ever missed.
- `_sensor_cache` — anything touching `sensor_timeseries`, a
  ReplacingMergeTree read via FINAL that can be revised by the separate
  `sensors/` ingestion service with no API hook to invalidate against, so a
  short TTL bounds staleness instead of explicit invalidation.
"""
import logging

from cachetools import TTLCache
from pydantic import BaseModel

logger = logging.getLogger(__name__)

_model_cache: TTLCache = TTLCache(maxsize=1000, ttl=1200)
_sensor_cache: TTLCache = TTLCache(maxsize=500, ttl=60)


def make_key(endpoint: str, request: BaseModel) -> str:
    return f"{endpoint}:{request.model_dump_json(exclude_none=True)}"


def get_model(key: str):
    value = _model_cache.get(key)
    logger.debug("model cache %s: %s", "hit" if value is not None else "miss", key)
    return value


def set_model(key: str, value) -> None:
    _model_cache[key] = value


def get_sensor(key: str):
    value = _sensor_cache.get(key)
    logger.debug("sensor cache %s: %s", "hit" if value is not None else "miss", key)
    return value


def set_sensor(key: str, value) -> None:
    _sensor_cache[key] = value


def invalidate_all() -> None:
    _model_cache.clear()
    _sensor_cache.clear()
    logger.info("response_cache invalidated (model + sensor caches cleared)")
