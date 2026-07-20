import uvicorn
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional, Tuple
from functools import partial
import contextvars
import os
import logging
import asyncio
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from urllib import request as urllib_request
from urllib import error as urllib_error
import json
from functools import partial
import contextvars
from starlette.concurrency import run_in_threadpool
import numpy as np

from modules.extractTimeseries import extract_timeseries
from modules.extract_profile import extract_profile
from modules.eval_extractor import extract_eval_data
from modules.extract_climate_timeseries import extract_climate_timeseries
from modules.extractMinMax import extract_minmax
from modules.pngGenerator import generate_png_for_variable
from modules.extractSensorTimeseries import extract_sensor_timeseries
from modules.extract_depth_profile import extract_depth_profile
from modules.ocean_analysis import lookup_grid_cells_for_polygon, lookup_nearest_grid_cell, query_region_timeseries
from modules.sync_hourly import import_native_file, import_daily_native_file, SyncConflict, SyncError, SYNC_API_TOKEN
from modules.posthog_helpers import capture_event, client_distinct_id

async def run_in_process(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_extract_executor, partial(func, *args, **kwargs))





# Limit concurrent extract requests to avoid resource exhaustion (files + DB)
MAX_CONCURRENT_EXTRACTS = int(os.getenv("MAX_CONCURRENT_EXTRACTS", "4"))
_extract_semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTS)
_extract_executor = ProcessPoolExecutor(max_workers=MAX_CONCURRENT_EXTRACTS)

# Hard cap (seconds) on how long a single blocking threadpool task may run.
# If a filesystem stall or bad file causes a thread to hang, this ensures the
# semaphore slot and the anyio threadpool slot are eventually released.
THREADPOOL_TIMEOUT = int(os.getenv("THREADPOOL_TIMEOUT", "120"))

# PNG generation runs in a dedicated single-process executor so CPU-heavy
# interpolation work never touches the shared anyio threadpool that serves
# all other API endpoints.  One worker = one PNG at a time, no starvation.
_png_executor = ProcessPoolExecutor(max_workers=1)
_png_gen_semaphore = asyncio.Semaphore(1)

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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

# Mount static files directory for convenience (still add explicit endpoint below to control headers)
# app.mount("/png", StaticFiles(directory="/opt/data/png"), name="png")

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


def _get_nc_data_dirs(source: str) -> str | None:
    """Return the SSC NC data directory spec (str or list) from environment.

    Set SSC_NC_DIR for the primary directory (default /opt/data/SSC/nc).
    Optionally set SSC_NC_DIR_ARCHIVE to a second directory that is searched
    when a file is not found in the primary (e.g. an external disk mount).
    """
    return f"/opt/data/{source.replace(' ', '')}/nc"


# Read DB config from environment at import time so route handlers can access it
db_host = os.getenv("DB_HOST", "db")
db_port = int(os.getenv("DB_PORT", 5432))
db_name = os.getenv("DB_NAME", "oa")
db_user = os.getenv("DB_USER", "postgres")
db_password = os.getenv("DB_PASSWORD", "postgres")

# Federation config (phase-1: SSC only)
FEDERATION_ENABLED = os.getenv("FEDERATION_ENABLED", "true").lower() in {"true", "1", "yes"}
REMOTE_B_API_BASE = os.getenv("REMOTE_B_API_BASE", "").rstrip("/") if FEDERATION_ENABLED else ""
REMOTE_B_TIMEOUT = int(os.getenv("REMOTE_B_TIMEOUT", "25"))
FED_SSC_OWNER_MISC_KEYS = ("location", "storage_location", "server", "node")


def _parse_iso_utc(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))


def _resolve_nc_job_owner(nc_path: str | None, misc) -> str:
    """Resolve owner node for an nc_jobs record.

    Resolution order:
    1) misc JSON keys location/storage_location/server/node (A/B values)
    2) local file existence check on nc_path
    3) default A
    """
    if isinstance(misc, str):
        try:
            misc = json.loads(misc)
        except Exception:
            misc = None
    if isinstance(misc, dict):
        for key in FED_SSC_OWNER_MISC_KEYS:
            value = str(misc.get(key, "")).strip().upper()
            if value in {"A", "B"}:
                return value
    if nc_path and os.path.isfile(nc_path):
        return "A"
    if REMOTE_B_API_BASE:
        return "B"
    return "A"


def _fetch_ssc_nc_jobs(var: str, from_date: str, to_date: str) -> list[dict]:
    """Fetch SSC nc_jobs rows overlapping requested range for routing decisions."""
    import psycopg2
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password,
            connect_timeout=5,
        )
        cur = conn.cursor()
        cur.execute(
            """
            SELECT nj.start_time, nj.end_time, nj.nc_path, nj.misc
            FROM nc_jobs nj
            JOIN fields f ON nj.variable_id = f.id
            JOIN datasets d ON f.dataset_id = d.id
            WHERE d.source = 'SalishSeaCast'
              AND f.variable = %s
              AND nj.status = 'success_image'
              AND nj.start_time <= %s::timestamp
              AND nj.end_time >= %s::timestamp
            ORDER BY nj.start_time
            """,
            (var, to_date, from_date),
        )
        rows = cur.fetchall()
        return [
            {
                "start_time": r[0],
                "end_time": r[1],
                "nc_path": r[2],
                "misc": r[3],
                "owner": _resolve_nc_job_owner(r[2], r[3]),
            }
            for r in rows
        ]
    finally:
        if conn:
            conn.close()


def _route_ssc_point(var: str, dt_str: str) -> str:
    """Route a point request to A/B based on nc_jobs overlap at dt."""
    rows = _fetch_ssc_nc_jobs(var, dt_str, dt_str)
    if not rows:
        return "A"
    owners = {r["owner"] for r in rows}
    if owners == {"B"}:
        return "B"
    return "A"


def _route_ssc_range(var: str, from_date: str, to_date: str) -> dict:
    """Route a range request; returns whether A and/or B should be queried."""
    rows = _fetch_ssc_nc_jobs(var, from_date, to_date)
    if not rows:
        return {"has_a": False, "has_b": False}
    owners = {r["owner"] for r in rows}
    return {"has_a": "A" in owners, "has_b": "B" in owners}


def _call_remote_b_extract_timeseries(payload: dict) -> dict:
    if not REMOTE_B_API_BASE:
        raise RuntimeError("REMOTE_B_API_BASE is not configured")
    url = f"{REMOTE_B_API_BASE}/extractTimeseries"
    req = urllib_request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib_request.urlopen(req, timeout=REMOTE_B_TIMEOUT) as resp:
        body = resp.read().decode("utf-8")
    return json.loads(body)


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


def _merge_timeseries_payloads(parts: list[dict]) -> dict:
    if not parts:
        return {"time": [], "value": []}

    # all-depth response shape
    if any("depth" in p for p in parts):
        rows = []
        for p in parts:
            for t, d, v in zip(p.get("time", []), p.get("depth", []), p.get("value", [])):
                rows.append((str(t), float(d), v))
        # de-duplicate by (time, depth), keep first non-null encountered
        best = {}
        for t, d, v in rows:
            key = (t, d)
            if key not in best or (best[key] is None and v is not None):
                best[key] = v
        merged = sorted([(t, d, v) for (t, d), v in best.items()], key=lambda x: (x[0], x[1]))
        return {
            "time": [r[0] for r in merged],
            "depth": [r[1] for r in merged],
            "value": [r[2] for r in merged],
        }

    # single-depth response shape
    rows = []
    for p in parts:
        for t, v in zip(p.get("time", []), p.get("value", [])):
            rows.append((str(t), v))
    best = {}
    for t, v in rows:
        if t not in best or (best[t] is None and v is not None):
            best[t] = v
    merged = sorted(best.items(), key=lambda x: x[0])
    return {
        "time": [r[0] for r in merged],
        "value": [r[1] for r in merged],
    }


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
                ts.first_data_at, ts.latest_data_at, s.source, s.organization
            FROM sensors AS s FINAL
            LEFT JOIN (
                SELECT sensor_id,
                       MIN(time) AS first_data_at,
                       MAX(time) AS latest_data_at
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
                "source": json.loads(row[10]) if row[10] else {},
                "organization": row[11] if row[11] else "",
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
    try:
        await asyncio.wait_for(_extract_semaphore.acquire(), timeout=10.0)
    except (asyncio.TimeoutError, Exception):
        logger.warning("Semaphore timeout in sensorTimeseries")
        raise HTTPException(status_code=429, detail="Too many concurrent extract requests, try again later")

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
        capture_event(client_distinct_id(http_request), "sensor_timeseries", {
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
    finally:
        _extract_semaphore.release()

#######################################

class depthProfileRequest(BaseModel):
    source: str
    var: str
    sensorId: str  # UUID
    lat: float
    lon: float
    fromDate: str
    toDate: str

@app.post("/depthProfile")
async def get_depth_profile(request: depthProfileRequest, http_request: Request):
    """Bin a variable-depth sensor's raw casts onto the model's depth levels
    and hourly time buckets, alongside the model's own values at that grid —
    the Comparison tab's Depth Profile (Hovmöller) view.

    Response: { time: [iso...], depths: [float...], model: [[float,...],...],
                sensor: [[float|null,...],...] }  — both grids depths x time.
    """
    logger.info(f"START depthProfile: {request.source}, {request.var}, sensor={request.sensorId}, from={request.fromDate}, to={request.toDate}")
    try:
        await asyncio.wait_for(_extract_semaphore.acquire(), timeout=10.0)
    except (asyncio.TimeoutError, Exception):
        logger.warning("Semaphore timeout in depthProfile")
        raise HTTPException(status_code=429, detail="Too many concurrent extract requests, try again later")

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
            ),
            timeout=THREADPOOL_TIMEOUT,
        )
        logger.info(f"FINISH depthProfile: {request.var}, sensor={request.sensorId} - returned {len(result.get('depths', []))} depths x {len(result.get('time', []))} hours")
        capture_event(client_distinct_id(http_request), "depth_profile", {
            "sensorId": request.sensorId, "source": request.source, "var": request.var,
            "fromDate": request.fromDate, "toDate": request.toDate,
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
    finally:
        _extract_semaphore.release()

#######################################

# @app.get("/metadata/{var}")
# async def get_metadata(var: str):
#     safe_var = os.path.basename(var)
#     path = os.path.join(IMAGE_ROOT, safe_var, "meta.json")
#     if not os.path.isfile(path):
#         raise HTTPException(status_code=404, detail="Metadata not found")
    
#     def _read():
#         with open(path) as f:
#             return f.read()
            
#     content = await run_in_threadpool(_read)
#     return JSONResponse(content=content)

@app.get("/png/{source}/{var}/{dt}/{depth}")
async def get_png(source: str, var: str, dt: str, depth: str):
    """Serve PNG for variable/datetime/depth, generating on-demand if needed."""
    # Serve the PNG file for a specific variable, datetime, and depth
    safe_source = os.path.basename(source)
    safe_var = os.path.basename(var)
    safe_dt = os.path.basename(dt)
    safe_depth = depth # .replace('.', 'p')

    if safe_source != "SalishSeaCast":
        raise HTTPException(status_code=400, detail=f"Unsupported source: {safe_source}")

    # Federation routing (phase-1: SSC only): redirect to B when ownership is remote
    if FEDERATION_ENABLED and safe_source == "SalishSeaCast" and REMOTE_B_API_BASE:
        try:
            owner = await run_in_threadpool(_route_ssc_point, safe_var, safe_dt)
            if owner == "B":
                target = f"{REMOTE_B_API_BASE}/png/{safe_source}/{safe_var}/{safe_dt}/{safe_depth}"
                return RedirectResponse(url=target, status_code=307)
        except Exception as exc:
            logger.warning("SSC png routing fallback to local due to resolver error: %s", exc)

    # Try both .webp (from on-demand generation) and .png (legacy), across all image roots
    for image_root in _get_image_roots(safe_source):
        path = os.path.join(image_root, safe_var, safe_dt)
        print("##" , path, flush=True)  # Debug log to verify path construction
        for ext in ['.webp', '.png']:
            filename = f"{safe_depth}{ext}"
            full_path = os.path.join(path, filename)

            exists = await run_in_threadpool(os.path.isfile, full_path)
            if exists:
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
    
    # File doesn't exist; try to generate it
    try:
        depth_value = float(depth)
        data_dir = _get_nc_data_dirs(safe_source)
        full_path = await generate_png_for_variable(
            source, var, dt, depth_value, data_dir, _get_image_roots(source), _png_gen_semaphore, _png_executor
        )
        
        headers = {
            "Cache-Control": "public, max-age=31536000, immutable",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "*",
            "Vary": "Origin",
            "ETag": f'"{full_path}-v1"',
        }
        return FileResponse(full_path, media_type="image/webp", headers=headers)
    except Exception as e:
        logger.error(f"Failed to generate or retrieve PNG for {var}/{dt}/{depth}: {e}")
        raise HTTPException(status_code=404, detail=f"PNG not found and generation failed: {str(e)}")

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
    try:
        await asyncio.wait_for(_extract_semaphore.acquire(), timeout=10.0)
    except (asyncio.TimeoutError, Exception):
        logger.warning("Semaphore timeout in extractTimeseries")
        raise HTTPException(status_code=429, detail="Too many concurrent extract requests, try again later")

    try:
        # use provided depth exactly (float value passed from frontend); None means all depths
        depth = float(request.depth) if request.depth is not None else None

        # Federation routing (phase-1: SSC only)
        if FEDERATION_ENABLED and request.source == "SalishSeaCast" and REMOTE_B_API_BASE:
            route = await run_in_threadpool(_route_ssc_range, request.var, request.fromDate, request.toDate)
            if not route["has_a"] and not route["has_b"]:
                raise HTTPException(status_code=422, detail="No processed data available for the requested date range")

            parts = []
            warnings = []

            if route["has_a"]:
                local_result = await asyncio.wait_for(
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
                parts.append(_format_timeseries_result(local_result))

            if route["has_b"]:
                try:
                    remote_payload = {
                        "source": request.source,
                        "var": request.var,
                        "lat": request.lat,
                        "lon": request.lon,
                        "polygon": request.polygon,
                        "depth": request.depth,
                        "fromDate": request.fromDate,
                        "toDate": request.toDate,
                    }
                    remote_result = await run_in_threadpool(_call_remote_b_extract_timeseries, remote_payload)
                    parts.append(remote_result)
                except Exception as exc:
                    if route["has_a"]:
                        warnings.append(f"Remote archive server unavailable: {exc}")
                        logger.warning("B unavailable; returning A partial data for extractTimeseries: %s", exc)
                    else:
                        raise HTTPException(status_code=503, detail=f"Remote archive server unavailable: {exc}")

            merged = _merge_timeseries_payloads(parts)
            if warnings:
                merged["warnings"] = warnings
            logger.info(
                "FINISH extractTimeseries (federated): %s, %s, from=%s, to=%s - returned %s points",
                request.var,
                location_desc,
                request.fromDate,
                request.toDate,
                len(merged.get("time", [])),
            )
            capture_event(client_distinct_id(http_request), "extract_timeseries", {
                "source": request.source, "var": request.var,
                "lat": request.lat, "lon": request.lon,
                "polygon": has_polygon, "depth": request.depth,
                "fromDate": request.fromDate, "toDate": request.toDate,
            })
            return merged

        # non-federated flow
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
        logger.info(
            "FINISH extractTimeseries: %s, %s, depth=%s, from=%s, to=%s - returned %s points",
            request.var,
            location_desc,
            request.depth if request.depth is not None else "all",
            request.fromDate,
            request.toDate,
            len(payload.get("time", [])),
        )
        capture_event(client_distinct_id(http_request), "extract_timeseries", {
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
    finally:
        _extract_semaphore.release()

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
    try:
        result = await run_in_threadpool(
            extract_climate_timeseries,
            lat=request.lat, lon=request.lon, variable=request.var,
            depth=request.depth, from_date=request.fromDate, to_date=request.toDate,
        )
        if result is None:
            logger.error("extract_climate_timeseries returned None")
            raise HTTPException(status_code=500, detail="Climatology extraction failed")

        logger.info(f"FINISH extract_climateTimeseries: {request.var} lat={request.lat}, lon={request.lon}, depth={request.depth}, fromDate={request.fromDate}, toDate={request.toDate}")
        capture_event(client_distinct_id(http_request), "extract_climate_timeseries", {
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
    try:
        await asyncio.wait_for(_extract_semaphore.acquire(), timeout=10.0)
    except (asyncio.TimeoutError, Exception):
        logger.warning("Semaphore timeout in getMinMax")
        raise HTTPException(status_code=429, detail="Too many concurrent extract requests, try again later")

    try:
        from datetime import datetime

        source = request.source
        var = request.var

        if source != "SalishSeaCast":
            logger.error(f"Unsupported source: {source}")
            raise HTTPException(status_code=400, detail=f"Unsupported source: {source}")

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

        logger.info(f"FINISH getMinMax: source={request.source}, var={request.var}, range=[{min_val}, {max_val}]")
        capture_event(client_distinct_id(http_request), "get_minmax", {
            "source": request.source, "var": request.var,
            "dt": request.dt, "depth": request.depth,
        })
        return {"min": min_val, "max": max_val}
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
    finally:
        _extract_semaphore.release()

#######################################

class monthlyClimRequest(BaseModel):
    variable: str
    lat: float
    lon: float
    depth: float

@app.post("/getMonthlyClimatologyAtCoord")
async def fn_get_monthly_climatology(request: monthlyClimRequest, http_request: Request):
    logger.info(f"START getMonthlyClimatologyAtCoord: {request.variable}, {request.lat}, {request.lon}, depth={request.depth}")
    try:
        await asyncio.wait_for(_extract_semaphore.acquire(), timeout=10.0)
    except (asyncio.TimeoutError, Exception):
        logger.warning("Semaphore timeout in getMonthlyClimatologyAtCoord")
        raise HTTPException(status_code=429, detail="Too many concurrent extract requests, try again later")

    try:
        from modules.monthly_climatology import get_monthly_climatology_at_coord
        ssc_root = os.getenv("SSC_MAIN_DIR", "/opt/data/SalishSeaCast")
        result = await run_in_threadpool(
            get_monthly_climatology_at_coord,
            lat=request.lat,
            lon=request.lon,
            depth=request.depth,
            variable=request.variable,
            data_root=ssc_root,
            # Let module pick DB environment vars
        )
        logger.info(f"FINISH getMonthlyClimatologyAtCoord: {request.variable}, {request.lat}, {request.lon}, depth={request.depth}")
        capture_event(client_distinct_id(http_request), "get_monthly_climatology", {
            "variable": request.variable, "lat": request.lat,
            "lon": request.lon, "depth": request.depth,
        })
        return result
    except FileNotFoundError as fnf:
        logger.exception("monthly climatology file not found")
        raise HTTPException(status_code=404, detail=str(fnf))
    except Exception as exc:
        logger.exception("get_monthly_climatology_at_coord failed")
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        _extract_semaphore.release()

#######################################

class profileRequest(BaseModel):
    source: str
    var: Optional[str] = None
    dt: str
    lat: float
    lng: float

@app.post("/getProfile")
async def fn_get_profile(request: profileRequest, http_request: Request):
    logger.info(f"START getProfile: source={request.source}, var={request.var}, lat={request.lat}, lng={request.lng}, dt={request.dt}")
    try:
        await asyncio.wait_for(_extract_semaphore.acquire(), timeout=10.0)
    except (asyncio.TimeoutError, Exception):
        logger.warning("Semaphore timeout in getProfile")
        raise HTTPException(status_code=429, detail="Too many concurrent extract requests, try again later")

    try:
        source = request.source
        var = request.var or "temperature"  # Default to temperature if not specified
        lat = request.lat
        lng = request.lng
        dt = request.dt

        if source != "SalishSeaCast":
            logger.error(f"Unsupported source: {source}")
            raise HTTPException(status_code=400, detail=f"Unsupported source: {source}")

        profile = await asyncio.wait_for(
            run_in_process(
                extract_profile,
                source=source,
                var=var,
                lat=lat,
                lon=lng,
                dt=dt,
            ),
            timeout=THREADPOOL_TIMEOUT,
        )
        logger.info(f"FINISH getProfile: source={source}, var={var}, lat={lat}, lng={lng}, dt={dt} - returned {len(profile)} points")
        capture_event(client_distinct_id(http_request), "get_profile", {
            "source": source, "var": var, "lat": lat, "lng": lng, "dt": dt,
        })
        return profile
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
    finally:
        _extract_semaphore.release()

#######################################

class evalRequest(BaseModel):
    sensor: str
    variable: str
    model: str

@app.post("/getEval")
async def fn_get_eval(request: evalRequest, http_request: Request):
    logger.info(f"START getEval: sensor={request.sensor}, variable={request.variable}, model={request.model}")
    
    eval_data_dir = os.getenv("EVAL_DATA_DIR", "/opt/data/eval")
    eval_nc_path = os.path.join(eval_data_dir, f"{request.sensor}.nc")
    
    # Validate model parameter
    valid_models = ["SSC"]
    model = request.model.strip()  # Remove leading/trailing whitespace
    if model not in valid_models:
        raise HTTPException(status_code=400, detail=f"Invalid model: {model}. Must be one of {valid_models}")
    
    try:
        result = await run_in_threadpool(
            extract_eval_data,
            nc_path=eval_nc_path,
            variable=request.variable,
            model=model
        )
        
        logger.info(f"FINISH getEval: {request.variable} - returned {len(result['time'])} timesteps for model={model}")
        capture_event(client_distinct_id(http_request), "get_eval", {
            "sensor": request.sensor, "variable": request.variable, "model": model,
        })
        return result
    except FileNotFoundError as e:
        logger.error(f"Evaluation file not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except (KeyError, ValueError) as e:
        logger.error(f"Invalid request for getEval: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as exc:
        logger.exception("extract_eval_data failed")
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
        capture_event(client_distinct_id(http_request), "analysis_timeseries", {
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
        return await run_in_threadpool(import_native_file, request.date, request.expected_rows)
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
        return await run_in_threadpool(import_daily_native_file, request.date, request.expected_rows)
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