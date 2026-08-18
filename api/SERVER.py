import uvicorn
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Literal, Optional, Tuple
from functools import partial
from contextlib import asynccontextmanager
import os
import logging
import asyncio
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import json
from starlette.concurrency import run_in_threadpool
import numpy as np

from modules.extractTimeseries import extract_timeseries
from modules.extract_profile import extract_profile
from modules.extract_climate_timeseries import extract_climate_timeseries
from modules.extractMinMax import extract_minmax
from modules.extractSensorTimeseries import extract_sensor_timeseries
from modules.extract_depth_profile import extract_depth_profile
from modules.extract_cross_section import extract_cross_section
from modules.extract_image import generate_image
from modules.ocean_analysis import lookup_grid_cells_for_polygon, lookup_nearest_grid_cell, query_region_timeseries
from modules.sync_hourly import import_native_file, import_daily_native_file, SyncConflict, SyncError, SYNC_API_TOKEN
from modules.posthog_helpers import capture_event
from modules import response_cache

async def run_in_process(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_extract_executor, partial(func, *args, **kwargs))


@asynccontextmanager
async def extract_slot(label: str):
    """Acquire one concurrency slot for a blocking extract, releasing it on exit.

    Raises 429 if a slot isn't free within 10s; the slot is only released when
    it was actually acquired, so a timeout never over-releases the semaphore.
    """
    try:
        await asyncio.wait_for(_extract_semaphore.acquire(), timeout=10.0)
    except (asyncio.TimeoutError, Exception):
        logger.warning("Semaphore timeout in %s", label)
        raise HTTPException(status_code=429, detail="Too many concurrent extract requests, try again later")
    try:
        yield
    finally:
        _extract_semaphore.release()


def _require_ssc(source: str) -> None:
    """SalishSeaCast is the only supported source; reject anything else as 400."""
    if source != "SalishSeaCast":
        logger.error(f"Unsupported source: {source}")
        raise HTTPException(status_code=400, detail=f"Unsupported source: {source}")





# Limit concurrent extract requests to avoid resource exhaustion (files + DB)
MAX_CONCURRENT_EXTRACTS = int(os.getenv("MAX_CONCURRENT_EXTRACTS", "4"))
_extract_semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTS)
_extract_executor = ProcessPoolExecutor(max_workers=MAX_CONCURRENT_EXTRACTS)

# Hard cap (seconds) on how long a single blocking threadpool task may run.
# If a filesystem stall or bad file causes a thread to hang, this ensures the
# semaphore slot and the anyio threadpool slot are eventually released.
THREADPOOL_TIMEOUT = int(os.getenv("THREADPOOL_TIMEOUT", "120"))

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Silence posthog's feature-flag warning noise (we don't use feature flags,
# only event capture, so its local-evaluation poller has nothing to do).
logging.getLogger("posthog").setLevel(logging.ERROR)


app = FastAPI()

# Add CORS middleware
# Use permissive origins for dev; in production restrict to known frontends.
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=".*",  # Matches any origin
    allow_credentials=True,   # Allow credentials (cookies, headers)
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def _stamp_request_start_time(request: Request, call_next):
    """Stamps a start time so posthog_helpers.capture_event can report
    duration_ms without every handler having to time itself. Also stashes the
    frontend-supplied PostHog distinct_id (if any), letting posthog_helpers
    correlate frontend/backend events without every capture_event call site
    needing to know about the header."""
    request.state.start_time = time.perf_counter()
    request.state.distinct_id = request.headers.get("x-posthog-distinct-id", "").strip() or None
    return await call_next(request)

# Explicit PNG route that sets cache-control for compatibility with Mapbox and browsers
SSC_IMAGE_DIR = os.environ.get("SSC_IMAGE_DIR", "/opt/data/SalishSeaCast/images")


def _get_image_roots(source: str) -> list:
    """Return list of image root directories to search when serving tiles.

    Searches SSC_IMAGE_DIR and its optional archive counterpart. All
    directories are searched in order when looking up a tile file.
    SalishSeaCast is the only supported source.
    """
    roots = []
    if(source == "SalishSeaCast"):
        roots = [SSC_IMAGE_DIR]
        archive_ssc = os.getenv("SSC_IMAGE_DIR_ARCHIVE", "")
        if archive_ssc:
            roots.append(archive_ssc)

    return [r for r in roots if r]




def _format_timeseries_result(result):
    import pandas as pd
    if isinstance(result, pd.DataFrame):
        def _clean(v):
            return None if (isinstance(v, float) and np.isnan(v)) else v
        return {
            "time": [t.isoformat() if hasattr(t, "isoformat") else t for t in result["time"].tolist()],
            "depth": result["depth"].tolist(),
            "value": [_clean(v) for v in result["value"].tolist()],
        }
    time, value = result
    time_list = [None if (isinstance(t, float) and np.isnan(t)) else t for t in time.tolist()]
    value_list = [None if (isinstance(v, float) and np.isnan(v)) else v for v in value.tolist()]
    return {"time": time_list, "value": value_list}



#######################################

@app.get("/")
async def read_root():
    print("DEBUG: Root endpoint hit (async)")
    return {"message": "Hello from OAH API!"}

#######################################

@app.get("/variables")
async def get_variables():
    """
    Return a list of variables with their min/max datetimes.
    """
    try:
        from modules.variables import get_variables as fetch_variables
        variables = await run_in_threadpool(fetch_variables)
        return variables
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("get_variables failed")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/sensors")
async def get_sensors():
    """Return a list of sensors with their metadata from ClickHouse."""
    def _fetch():
        from modules.clickhouse_helpers import get_ch_client
        client = get_ch_client()
        result = client.query("""
            SELECT
                s.id, s.name, s.latitude, s.longitude, s.depth,
                s.device_config, s.variables, s.active,
                ts.first_data_at, ts.latest_data_at, ts.depth_min, ts.depth_max,
                s.source, s.organization
            FROM sensors AS s FINAL
            LEFT JOIN (
                SELECT sensor_id,
                       MIN(time) AS first_data_at,
                       MAX(time) AS latest_data_at,
                       MIN(depth) AS depth_min,
                       MAX(depth) AS depth_max
                FROM sensor_timeseries
                GROUP BY sensor_id
            ) ts ON ts.sensor_id = s.id
            WHERE s.active = 1
        """)
        sensors = []
        for row in result.result_rows:
            sensors.append({
                "id": str(row[0]),
                "name": row[1],
                "latitude": float(row[2]),
                "longitude": float(row[3]),
                "depth": float(row[4]),
                "device_config": json.loads(row[5]) if row[5] else {},
                "variables": json.loads(row[6]) if row[6] else {},
                "active": bool(row[7]),
                "first_data_at": row[8].isoformat() if row[8] else None,
                "latest_data_at": row[9].isoformat() if row[9] else None,
                "depth_min": float(row[10]) if row[10] is not None else None,
                "depth_max": float(row[11]) if row[11] is not None else None,
                "source": json.loads(row[12]) if row[12] else {},
                "organization": row[13] if row[13] else "",
            })
        return sensors

    try:
        return await run_in_threadpool(_fetch)
    except Exception as exc:
        logger.exception("get_sensors failed")
        raise HTTPException(status_code=500, detail=str(exc))

#######################################

@app.get('/colormaps')
async def get_colormaps():
    """Return all colormaps from shared/colormaps.json."""
    try:
        from shared.variable_config import load_colormaps
        return sorted(load_colormaps(), key=lambda c: c['name'])
    except Exception as exc:
        logger.exception('get_colormaps failed')
        raise HTTPException(status_code=500, detail=str(exc))

#######################################

class sensorTimeseriesRequest(BaseModel):
    sensorId: str  # UUID
    modelVariable: str  # model/canonical name, e.g. "dissolved_oxygen"
    fromDate: str
    toDate: str
    depth: Optional[float] = None
    source: Optional[str] = None  # model source, only needed to resolve depth for variable-depth sensors

@app.post("/sensorTimeseries")
async def get_sensor_timeseries(request: sensorTimeseriesRequest, http_request: Request):
    """Return sensor telemetry from ClickHouse sensor_timeseries.

    Accepts a canonical variable name and queries CH directly — no
    sensor-specific code resolution or NC file I/O needed.

    Response: { time: [iso...], value: [float|null,...] }
              or with depth axis: { time: [...], depth: [...], value: [...] }
    """
    key = response_cache.make_key("sensorTimeseries", request)
    cached = response_cache.get_sensor(key)
    if cached is not None:
        capture_event(http_request, "sensor_timeseries", {
            "sensorId": request.sensorId, "modelVariable": request.modelVariable,
            "fromDate": request.fromDate, "toDate": request.toDate, "depth": request.depth,
        })
        return cached
    async with extract_slot("sensorTimeseries"):
        try:
            result = await asyncio.wait_for(
                run_in_threadpool(
                    extract_sensor_timeseries,
                    request.sensorId,
                    request.modelVariable,
                    request.fromDate,
                    request.toDate,
                    request.depth,
                    request.source,
                ),
                timeout=THREADPOOL_TIMEOUT,
            )
            response_cache.set_sensor(key, result)
            capture_event(http_request, "sensor_timeseries", {
                "sensorId": request.sensorId, "modelVariable": request.modelVariable,
                "fromDate": request.fromDate, "toDate": request.toDate, "depth": request.depth,
            })
            return result
        except HTTPException:
            raise
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except Exception as exc:
            logger.exception("get_sensor_timeseries failed")
            raise HTTPException(status_code=500, detail=str(exc))

#######################################

class depthProfileRequest(BaseModel):
    source: str
    var: str
    lat: float
    lon: float
    fromDate: str
    toDate: str
    # Omit for a model-only grid (Depth tab); pass a sensor UUID to also get
    # that profiler's casts binned onto the same grid (Comparison tab).
    sensorId: Optional[str] = None
    # binHours is the legacy form, still accepted so an already-deployed
    # frontend keeps working against a newer API — the two deploy separately.
    # binMode supersedes it and is the only way to ask for monthly.
    binHours: Literal[1, 24] = 1
    binMode: Optional[Literal["hourly", "daily", "monthly"]] = None

@app.post("/depthProfile")
async def get_depth_profile(request: depthProfileRequest, http_request: Request):
    """Build the model's time-depth (Hovmöller) grid at the cell nearest
    (lat, lon), optionally alongside a variable-depth sensor's casts binned
    onto that same grid — the Depth tab's model view and the Comparison tab's
    Depth Profile view respectively.

    Response: { time: [iso...], depths: [float...], model: [[float,...],...],
                sensor: [[float|null,...],...] | null }  — grids are depths x time;
                `sensor` is null when no sensorId was requested.
    """
    resolution = request.binMode or f"{request.binHours}h"
    logger.info(f"START depthProfile: {request.source}, {request.var}, sensor={request.sensorId}, lat={request.lat}, lon={request.lon}, from={request.fromDate}, to={request.toDate}, resolution={resolution}")
    is_sensor = request.sensorId is not None
    key = response_cache.make_key("depthProfile", request)
    cached = response_cache.get_sensor(key) if is_sensor else response_cache.get_model(key)
    if cached is not None:
        capture_event(http_request, "depth_profile", {
            "mode": "sensor" if request.sensorId else "model",
            "sensorId": request.sensorId, "source": request.source, "var": request.var,
            "fromDate": request.fromDate, "toDate": request.toDate, "resolution": resolution,
        })
        return cached
    async with extract_slot("depthProfile"):
        try:
            result = await asyncio.wait_for(
                run_in_threadpool(
                    extract_depth_profile,
                    source=request.source,
                    var=request.var,
                    sensor_id=request.sensorId,
                    lat=request.lat,
                    lon=request.lon,
                    from_date=request.fromDate,
                    to_date=request.toDate,
                    bin_hours=request.binHours,
                    bin_mode=request.binMode,
                ),
                timeout=THREADPOOL_TIMEOUT,
            )
            if is_sensor:
                response_cache.set_sensor(key, result)
            else:
                response_cache.set_model(key, result)
            logger.info(f"FINISH depthProfile: {request.var}, sensor={request.sensorId} - returned {len(result.get('depths', []))} depths x {len(result.get('time', []))} bins")
            capture_event(http_request, "depth_profile", {
                "mode": "sensor" if request.sensorId else "model",
                "sensorId": request.sensorId, "source": request.source, "var": request.var,
                "fromDate": request.fromDate, "toDate": request.toDate, "resolution": resolution,
            })
            return result
        except HTTPException:
            raise
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except RuntimeError as exc:
            # Out-of-domain coordinates, grid issues, or an empty window are client errors.
            if ("km from the nearest grid point" in str(exc) or "Grid table is empty" in str(exc)
                    or "No depth levels found" in str(exc) or "No model data found" in str(exc)):
                logger.warning(f"Depth profile request error: {exc}")
                raise HTTPException(status_code=400, detail=str(exc))
            logger.exception("extract_depth_profile failed with RuntimeError")
            raise HTTPException(status_code=500, detail=str(exc))
        except Exception as exc:
            logger.exception("get_depth_profile failed")
            raise HTTPException(status_code=500, detail=str(exc))

#######################################

class crossSectionRequest(BaseModel):
    source: str
    var: str
    # Polyline vertices as [(lat, lon), ...], >= 2 points.
    vertices: List[Tuple[float, float]]
    # A full timestamp for "hourly", "YYYY-MM-DD" for "daily", or "YYYY-MM"
    # for "monthly" — matching binMode.
    dt: str
    binMode: Literal["hourly", "daily", "monthly"]

@app.post("/crossSection")
async def get_cross_section(request: crossSectionRequest, http_request: Request):
    """Build the model's depth-vs-distance grid along an arbitrary drawn
    polyline, at one time snapshot — the Cross-Section tab's spatial
    counterpart to the Depth tab's time-depth view.

    Response: { distances_km: [float...], depths: [float...],
                model: [[float,...],...], vertex_distances_km: [float...] }
                — model is depths x distance samples.
    """
    logger.info(
        f"START crossSection: {request.source}, {request.var}, vertices={len(request.vertices)}, "
        f"dt={request.dt}, binMode={request.binMode}"
    )
    key = response_cache.make_key("crossSection", request)
    cached = response_cache.get_model(key)
    if cached is not None:
        capture_event(http_request, "cross_section_queried", {
            "source": request.source, "var": request.var, "dt": request.dt,
            "binMode": request.binMode, "vertex_count": len(request.vertices),
        })
        return cached
    async with extract_slot("crossSection"):
        try:
            result = await asyncio.wait_for(
                run_in_threadpool(
                    extract_cross_section,
                    source=request.source,
                    var=request.var,
                    vertices=request.vertices,
                    dt=request.dt,
                    bin_mode=request.binMode,
                ),
                timeout=THREADPOOL_TIMEOUT,
            )
            response_cache.set_model(key, result)
            logger.info(
                f"FINISH crossSection: {request.var} - returned {len(result.get('depths', []))} depths "
                f"x {len(result.get('distances_km', []))} samples"
            )
            capture_event(http_request, "cross_section_queried", {
                "source": request.source, "var": request.var, "dt": request.dt,
                "binMode": request.binMode, "vertex_count": len(request.vertices),
                "distance_km": result["distances_km"][-1] if result.get("distances_km") else None,
            })
            return result
        except HTTPException:
            raise
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except RuntimeError as exc:
            # Out-of-domain/empty line-time combinations are client errors.
            if "No model data found" in str(exc):
                logger.warning(f"Cross section request error: {exc}")
                raise HTTPException(status_code=400, detail=str(exc))
            logger.exception("extract_cross_section failed with RuntimeError")
            raise HTTPException(status_code=500, detail=str(exc))
        except Exception as exc:
            logger.exception("get_cross_section failed")
            raise HTTPException(status_code=500, detail=str(exc))

#######################################

def _png_file_response(full_path: str) -> FileResponse:
    ext = os.path.splitext(full_path)[1]
    headers = {
        "Cache-Control": "public, max-age=31536000, immutable",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "*",
        "Vary": "Origin",
        "ETag": f'"{full_path}-v1"',
    }
    media_type = "image/webp" if ext == '.webp' else "image/png"
    return FileResponse(full_path, media_type=media_type, headers=headers)


@app.get("/png/{source}/{var}/{dt}/{depth}")
async def get_png(source: str, var: str, dt: str, depth: str):
    """Serve a WebP/PNG tile for variable/datetime/depth.

    Most tiles are preprocessed ahead of time (see process/SSC's `image`
    step) and served directly below. For anything that step hasn't covered —
    historical dates it never ran for, and daily/monthly bin modes, which it
    never pre-renders at all (see CLAUDE.md) — this route falls back to
    rendering the tile on demand straight from ClickHouse (modules/
    extract_image.py) and caches it to disk at the same path, so only the
    first request for a given source/var/dt/depth pays the generation cost.
    `dt`'s shape alone decides hourly vs. daily vs. monthly (see
    extract_image.resolve_bin_mode) — no separate bin_mode param."""
    # Serve the PNG file for a specific variable, datetime, and depth
    safe_source = os.path.basename(source)
    safe_var = os.path.basename(var)
    safe_dt = os.path.basename(dt)
    safe_depth = depth # .replace('.', 'p')

    if safe_source != "SalishSeaCast":
        raise HTTPException(status_code=400, detail=f"Unsupported source: {safe_source}")

    image_roots = _get_image_roots(safe_source)
    for image_root in image_roots:
        path = os.path.join(image_root, safe_var, safe_dt)
        for ext in ['.webp', '.png']:
            filename = f"{safe_depth}{ext}"
            full_path = os.path.join(path, filename)
            logger.debug(f"Checking for PNG/WebP file: {full_path}")

            exists = await run_in_threadpool(os.path.isfile, full_path)
            if exists:
                return _png_file_response(full_path)

    # Not pre-generated — try rendering it on demand from ClickHouse. Writes
    # into the first (primary, non-archive) image root so it's found by the
    # fast path above on every later request.
    generated_path = None
    async with extract_slot("get_png"):
        try:
            generated_path = await asyncio.wait_for(
                run_in_process(
                    generate_image, safe_source, safe_var, safe_dt, safe_depth,
                    image_roots[0] if image_roots else None,
                ),
                timeout=THREADPOOL_TIMEOUT,
            )
        except Exception:
            logger.exception(
                "On-demand image generation failed for %s/%s/%s/%s",
                safe_source, safe_var, safe_dt, safe_depth,
            )

    if generated_path:
        return _png_file_response(generated_path)

    raise HTTPException(status_code=404, detail=f"PNG not found for {var}/{dt}/{depth}")

@app.get("/vector/{z}/{x}/{y}.pbf")
async def get_vector(z: int, x: int, y: int):
    VECTOR_ROOT = os.environ.get("VECTOR_ROOT", "/opt/data/bathymetry/NONNA/tiles")
    # Serve the vector tile file for a specific variable, datetime, and depth with appropriate headers for caching
    safe_z = os.path.basename(str(z))
    safe_x = os.path.basename(str(x))
    safe_y = os.path.basename(str(y))
    path = os.path.join(VECTOR_ROOT, safe_z, safe_x)
    filename = f"{safe_y}.pbf"
    full_path = os.path.join(path, filename)
    
    # os.path.isfile is fast but still better in a thread if the FS is slow
    exists = await run_in_threadpool(os.path.isfile, full_path)
    if not exists:
        raise HTTPException(status_code=404, detail="Vector tile not found")
    
    headers = {
        "Cache-Control": "public, max-age=31536000, immutable",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "*",
        "Vary": "Origin",
        "ETag": f'"{full_path}-v1"',
    }
    return FileResponse(full_path, media_type="application/octet-stream", headers=headers)

@app.get("/sea_names/{z}/{x}/{y}.pbf")
async def get_sea_names_vector(z: int, x: int, y: int):
    SEA_NAMES_ROOT = os.environ.get("SEA_NAMES_ROOT", "/opt/data/sea_names")
    # Serve the sea/region-name label vector tile with appropriate headers for caching
    safe_z = os.path.basename(str(z))
    safe_x = os.path.basename(str(x))
    safe_y = os.path.basename(str(y))
    path = os.path.join(SEA_NAMES_ROOT, safe_z, safe_x)
    filename = f"{safe_y}.pbf"
    full_path = os.path.join(path, filename)

    # os.path.isfile is fast but still better in a thread if the FS is slow
    exists = await run_in_threadpool(os.path.isfile, full_path)
    if not exists:
        raise HTTPException(status_code=404, detail="Sea name tile not found")

    headers = {
        "Cache-Control": "public, max-age=31536000, immutable",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "*",
        "Vary": "Origin",
        "ETag": f'"{full_path}-v1"',
    }
    return FileResponse(full_path, media_type="application/octet-stream", headers=headers)

@app.get("/raster_tiles/{z}/{x}/{y}.webp")
async def get_raster_tiles(z: int, x: int, y: int):
    RASTER_ROOT = os.environ.get("RASTER_TILES_ROOT", "/opt/data/bathymetry/NONNA/raster_tiles")
    # Serve the raster tile file for bathymetry with appropriate headers for caching
    safe_z = os.path.basename(str(z))
    safe_x = os.path.basename(str(x))
    safe_y = os.path.basename(str(y))
    path = os.path.join(RASTER_ROOT, safe_z, safe_x)
    filename = f"{safe_y}.webp"
    full_path = os.path.join(path, filename)
    
    # os.path.isfile is fast but still better in a thread if the FS is slow
    exists = await run_in_threadpool(os.path.isfile, full_path)
    if not exists:
        raise HTTPException(status_code=404, detail="Raster tile not found")
    
    headers = {
        "Cache-Control": "public, max-age=31536000, immutable",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "*",
        "Vary": "Origin",
        "ETag": f'"{full_path}-v1"',
    }
    return FileResponse(full_path, media_type="image/webp", headers=headers)


#######################################

class timeseriesRequest(BaseModel):
    source: str
    var: str
    # Either lat + lon (point mode) or polygon (area mode); at least one must be provided.
    lat: Optional[float] = None
    lon: Optional[float] = None
    polygon: Optional[List[Tuple[float, float]]] = None
    depth: Optional[float] = None
    fromDate: str
    toDate: str

@app.post("/extractTimeseries")
async def fn_extract_timeseries(request: timeseriesRequest, http_request: Request):
    has_polygon = bool(request.polygon) and len(request.polygon) >= 3
    has_point = request.lat is not None and request.lon is not None
    if not has_polygon and not has_point:
        raise HTTPException(
            status_code=400,
            detail="Provide either a polygon (area mode) or lat + lon (point mode)."
        )

    location_desc = f"polygon({len(request.polygon or [])} pts)" if has_polygon else f"{request.lat}, {request.lon}"

    # Reject requests if we are already at concurrency limit
    logger.info(f"START extractTimeseries: {request.source}, {request.var}, {location_desc}, depth={request.depth}, from={request.fromDate}, to={request.toDate}")
    key = response_cache.make_key("extractTimeseries", request)
    cached = response_cache.get_model(key)
    if cached is not None:
        capture_event(http_request, "extract_timeseries", {
            "source": request.source, "var": request.var,
            "lat": request.lat, "lon": request.lon,
            "polygon": has_polygon, "depth": request.depth,
            "fromDate": request.fromDate, "toDate": request.toDate,
        })
        return cached
    async with extract_slot("extractTimeseries"):
        try:
            # use provided depth exactly (float value passed from frontend); None means all depths
            depth = float(request.depth) if request.depth is not None else None

            result = await asyncio.wait_for(
                run_in_process(
                    extract_timeseries,
                    source=request.source,
                    var=request.var,
                    lat=request.lat,
                    lon=request.lon,
                    polygon=request.polygon,
                    depth=depth,
                    from_date=request.fromDate,
                    to_date=request.toDate,
                ),
                timeout=THREADPOOL_TIMEOUT,
            )
            payload = _format_timeseries_result(result)
            response_cache.set_model(key, payload)
            logger.info(
                "FINISH extractTimeseries: %s, %s, depth=%s, from=%s, to=%s - returned %s points",
                request.var,
                location_desc,
                request.depth if request.depth is not None else "all",
                request.fromDate,
                request.toDate,
                len(payload.get("time", [])),
            )
            capture_event(http_request, "extract_timeseries", {
                "source": request.source, "var": request.var,
                "lat": request.lat, "lon": request.lon,
                "polygon": has_polygon, "depth": request.depth,
                "fromDate": request.fromDate, "toDate": request.toDate,
            })
            return payload
        except RuntimeError as exc:
            # Out-of-domain coordinates or grid issues are client errors (400), not server errors (500)
            if ("km from the nearest grid point" in str(exc) or "Grid table is empty" in str(exc)
                    or "does not cover any active marine grid cells" in str(exc)
                    or "No data available at depth" in str(exc)):
                logger.warning(f"Out-of-domain or invalid coordinates: {exc}")
                raise HTTPException(status_code=400, detail=str(exc))
            # Other RuntimeErrors are unexpected, treat as 500
            logger.exception("extract_timeseries failed with RuntimeError")
            raise HTTPException(status_code=500, detail=str(exc))
        except Exception as exc:
            logger.exception("extract_timeseries failed")
            raise HTTPException(status_code=500, detail=str(exc))

#######################################

class climate_timeseriesRequest(BaseModel):
    var: str
    lat: float
    lon: float
    depth: float
    fromDate: str
    toDate: str

@app.post("/extract_climateTimeseries")
async def fn_extract_ClimateTimeseries(request: climate_timeseriesRequest, http_request: Request):
    logger.info(f"START extract_climateTimeseries: {request.var} lat={request.lat}, lon={request.lon}, depth={request.depth}, fromDate={request.fromDate}, toDate={request.toDate}")
    key = response_cache.make_key("extract_climateTimeseries", request)
    cached = response_cache.get_model(key)
    if cached is not None:
        capture_event(http_request, "extract_climate_timeseries", {
            "var": request.var, "lat": request.lat, "lon": request.lon,
            "depth": request.depth, "fromDate": request.fromDate, "toDate": request.toDate,
        })
        return cached
    try:
        result = await run_in_threadpool(
            extract_climate_timeseries,
            lat=request.lat, lon=request.lon, variable=request.var,
            depth=request.depth, from_date=request.fromDate, to_date=request.toDate,
        )
        if result is None:
            logger.error("extract_climate_timeseries returned None")
            raise HTTPException(status_code=500, detail="Climatology extraction failed")

        response_cache.set_model(key, result)
        logger.info(f"FINISH extract_climateTimeseries: {request.var} lat={request.lat}, lon={request.lon}, depth={request.depth}, fromDate={request.fromDate}, toDate={request.toDate}")
        capture_event(http_request, "extract_climate_timeseries", {
            "var": request.var, "lat": request.lat, "lon": request.lon,
            "depth": request.depth, "fromDate": request.fromDate, "toDate": request.toDate,
        })
        return result
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("extract_climate_timeseries failed")
        raise HTTPException(status_code=500, detail=str(exc))

#######################################

class minmaxRequest(BaseModel):
    source: str
    var: str
    dt: str
    depth: Optional[float] = None
    north: Optional[float] = None
    south: Optional[float] = None
    east: Optional[float] = None
    west: Optional[float] = None

@app.post("/getMinMax")
async def fn_get_minmax(request: minmaxRequest, http_request: Request):
    """Extract min and max values for a variable at a specific datetime and depth."""
    logger.info(f"START getMinMax: source={request.source}, var={request.var}, dt={request.dt}, depth={request.depth}")
    key = response_cache.make_key("getMinMax", request)
    cached = response_cache.get_model(key)
    if cached is not None:
        capture_event(http_request, "get_minmax", {
            "source": request.source, "var": request.var,
            "dt": request.dt, "depth": request.depth,
        })
        return cached
    async with extract_slot("getMinMax"):
        try:
            source = request.source
            var = request.var

            _require_ssc(source)

            # Parse datetime string (ISO format: YYYY-MM-DDTHH:mm:ss)
            dt = datetime.fromisoformat(request.dt.replace('Z', '+00:00'))

            min_val, max_val = await asyncio.wait_for(
                run_in_process(extract_minmax,
                    source=source,
                    var=var,
                    dt=dt,
                    depth=request.depth,
                    north=request.north,
                    south=request.south,
                    east=request.east,
                    west=request.west),
                timeout=THREADPOOL_TIMEOUT,
            )

            result = {"min": min_val, "max": max_val}
            response_cache.set_model(key, result)
            logger.info(f"FINISH getMinMax: source={request.source}, var={request.var}, range=[{min_val}, {max_val}]")
            capture_event(http_request, "get_minmax", {
                "source": request.source, "var": request.var,
                "dt": request.dt, "depth": request.depth,
            })
            return result
        except HTTPException:
            raise
        except RuntimeError as exc:
            if "No data available for" in str(exc):
                logger.warning(f"No data for getMinMax: {exc}")
                raise HTTPException(status_code=400, detail=str(exc))
            logger.exception("getMinMax failed with RuntimeError")
            raise HTTPException(status_code=500, detail=str(exc))
        except Exception as exc:
            logger.exception("getMinMax failed")
            raise HTTPException(status_code=500, detail=str(exc))

#######################################

class profileRequest(BaseModel):
    source: str
    var: Optional[str] = None
    dt: str
    lat: float
    lng: float
    bin_mode: str = "hourly"

@app.post("/getProfile")
async def fn_get_profile(request: profileRequest, http_request: Request):
    logger.info(f"START getProfile: source={request.source}, var={request.var}, lat={request.lat}, lng={request.lng}, dt={request.dt}, bin_mode={request.bin_mode}")
    key = response_cache.make_key("getProfile", request)
    cached = response_cache.get_model(key)
    if cached is not None:
        capture_event(http_request, "get_profile", {
            "source": request.source, "var": request.var or "temperature", "lat": request.lat,
            "lng": request.lng, "dt": request.dt, "bin_mode": request.bin_mode,
        })
        return cached
    async with extract_slot("getProfile"):
        try:
            source = request.source
            var = request.var or "temperature"  # Default to temperature if not specified
            lat = request.lat
            lng = request.lng
            dt = request.dt
            bin_mode = request.bin_mode

            _require_ssc(source)

            profile = await asyncio.wait_for(
                run_in_process(
                    extract_profile,
                    source=source,
                    var=var,
                    lat=lat,
                    lon=lng,
                    dt=dt,
                    bin_mode=bin_mode,
                ),
                timeout=THREADPOOL_TIMEOUT,
            )
            response_cache.set_model(key, profile)
            logger.info(f"FINISH getProfile: source={source}, var={var}, lat={lat}, lng={lng}, dt={dt}, bin_mode={bin_mode} - returned {len(profile)} points")
            capture_event(http_request, "get_profile", {
                "source": source, "var": var, "lat": lat, "lng": lng, "dt": dt, "bin_mode": bin_mode,
            })
            return profile
        except HTTPException:
            raise
        except RuntimeError as exc:
            # Out-of-domain coordinates, empty grid, or unmatched time are client errors (400), not server errors (500)
            if "km from the nearest grid point" in str(exc) or "Grid table is empty" in str(exc) or "No data available" in str(exc):
                logger.warning(f"Out-of-domain or invalid coordinates/time: {exc}")
                raise HTTPException(status_code=400, detail=str(exc))
            # Other RuntimeErrors are unexpected, treat as 500
            logger.exception("extract_profile failed with RuntimeError")
            raise HTTPException(status_code=500, detail=str(exc))
        except Exception as exc:
            logger.exception("extract_profile failed")
            raise HTTPException(status_code=500, detail=str(exc))

#######################################


# ---------------------------------------------------------------------------
# Analysis Builder endpoints
# ---------------------------------------------------------------------------

class MetricSpec(BaseModel):
    variable: str
    stat: str

class AnalysisRequest(BaseModel):
    # Either a GeoJSON-style polygon [[lon, lat], ...] for area mode,
    # or lat + lon for point mode. At least one must be provided.
    polygon: Optional[List[Tuple[float, float]]] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    depth: float
    primaryMetric: MetricSpec
    temporal: dict

# ==============================================================================
# STREAMLINED ENDPOINT
# ==============================================================================
@app.post("/analysis/timeseries")
async def analysis_timeseries(request: AnalysisRequest, http_request: Request):
    """Daily Timeseries for Analysis Builder.

    Accepts either:
      - polygon (area mode): aggregates all grid cells inside the polygon
      - lat + lon (point mode): uses the single nearest grid cell

    Returns a flat daily {time, value} series.
    """
    key = response_cache.make_key("analysis_timeseries", request)
    cached = response_cache.get_model(key)
    if cached is not None:
        capture_event(http_request, "analysis_timeseries", {
            "variable": request.primaryMetric.variable, "stat": request.primaryMetric.stat,
            "depth": request.depth, "mode": "polygon" if (request.polygon and len(request.polygon) >= 3) else "point",
            "yearRange": request.temporal.get("yearRange"),
        })
        return cached
    try:
        has_polygon = request.polygon and len(request.polygon) >= 3
        has_point = request.lat is not None and request.lon is not None

        if not has_polygon and not has_point:
            raise HTTPException(
                status_code=400,
                detail="Provide either a polygon (area mode) or lat + lon (point mode)."
            )

        if has_polygon:
            grid_points = await run_in_threadpool(
                lookup_grid_cells_for_polygon,
                polygon_coords=request.polygon
            )
            if not grid_points:
                raise HTTPException(
                    status_code=400,
                    detail="The selected polygon area does not cover any active marine grid cells."
                )
        else:
            grid_points = await run_in_threadpool(
                lookup_nearest_grid_cell,
                lat=request.lat,
                lon=request.lon
            )
            if not grid_points:
                raise HTTPException(
                    status_code=400,
                    detail="No marine grid cell found near the selected point."
                )

        result = await run_in_threadpool(
            query_region_timeseries,
            grid_points=grid_points,
            depth=request.depth,
            variable=request.primaryMetric.variable,
            stat=request.primaryMetric.stat,
            year_range=request.temporal["yearRange"],
        )
        response_cache.set_model(key, result)
        capture_event(http_request, "analysis_timeseries", {
            "variable": request.primaryMetric.variable, "stat": request.primaryMetric.stat,
            "depth": request.depth, "mode": "polygon" if has_polygon else "point",
            "yearRange": request.temporal.get("yearRange"),
        })
        return result

    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("analysis_timeseries failed")
        raise HTTPException(status_code=500, detail=str(exc))


#######################################


class SyncHourlyRequest(BaseModel):
    date: str  # YYYY-MM-DD
    expected_rows: int


@app.post("/admin/syncHourly")
async def sync_hourly(request: SyncHourlyRequest, authorization: Optional[str] = Header(None)):
    """Import a remote pipeline server's exported SalishSeaCast_hourly rows.

    Only accessible with a bearer token matching SYNC_API_TOKEN. The caller
    (process/SSC/sync.py) sends just the date — the expected Native-format
    file path is derived server-side from SSC_SYNC_STAGING_DIR, never taken
    from the request, to avoid trusting a caller-supplied path.
    """
    if not SYNC_API_TOKEN or authorization != f"Bearer {SYNC_API_TOKEN}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        result = await run_in_threadpool(import_native_file, request.date, request.expected_rows)
        response_cache.invalidate_all()
        return result
    except SyncConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except SyncError as exc:
        logger.warning("sync_hourly rejected for date %s: %s", request.date, exc)
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("sync_hourly failed for date %s", request.date)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/admin/syncDaily")
async def sync_daily(request: SyncHourlyRequest, authorization: Optional[str] = Header(None)):
    """Import a remote pipeline server's exported SalishSeaCast_daily rows.

    Counterpart to /admin/syncHourly — same bearer-token auth, same derived
    (never caller-supplied) staging path, but against the daily mean/min/max
    table and its own sync log.
    """
    if not SYNC_API_TOKEN or authorization != f"Bearer {SYNC_API_TOKEN}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        result = await run_in_threadpool(import_daily_native_file, request.date, request.expected_rows)
        response_cache.invalidate_all()
        return result
    except SyncConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except SyncError as exc:
        logger.warning("sync_daily rejected for date %s: %s", request.date, exc)
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("sync_daily failed for date %s", request.date)
        raise HTTPException(status_code=500, detail=str(exc))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=4000)