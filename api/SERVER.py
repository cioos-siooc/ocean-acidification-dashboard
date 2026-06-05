import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, Tuple
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
from typing import Optional
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
from modules.ocean_analysis import query_overlay_timeseries, query_climatology, query_threshold_count, query_trend, query_correlation

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
LO_IMAGE_DIR = os.environ.get("LO_IMAGE_DIR", "/opt/data/LiveOcean/images")


def _get_image_roots(source: str) -> list:
    """Return list of image root directories to search when serving tiles.

    Searches SSC_IMAGE_DIR, LO_IMAGE_DIR, and optional archive counterparts.
    All directories are searched in order when looking up a tile file.
    """
    roots = []
    if(source == "SalishSeaCast"):
        roots = [SSC_IMAGE_DIR]
        archive_ssc = os.getenv("SSC_IMAGE_DIR_ARCHIVE", "")
        if archive_ssc:
            roots.append(archive_ssc)
    elif(source == "LiveOcean"):
        roots = [LO_IMAGE_DIR]
        archive_lo = os.getenv("LO_IMAGE_DIR_ARCHIVE", "")
        if archive_lo:
            roots.append(archive_lo)

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
        variables = await run_in_threadpool(fetch_variables, db_host, db_port, db_name, db_user, db_password)
        return variables
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("get_variables failed")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/sensors")
async def get_sensors():
    """
    Return a list of sensors with their metadata.
    """
    def _fetch():
        import psycopg2
        import psycopg2.extras
        query = "SELECT id, name, latitude, longitude, depth, device_config, variables, active FROM sensors;"
        conn = None
        try:
            conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password, connect_timeout=5)
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(query)
            rows = cur.fetchall()
            cur.close()
            
            sensors = []
            for row in rows:
                sensors.append({
                    "id": row.get("id"),
                    "name": row.get("name"),
                    "latitude": row.get("latitude"),
                    "longitude": row.get("longitude"),
                    "depth": row.get("depth"),
                    "device_config": row.get("device_config"),
                    "variables": row.get("variables"),
                    "active": row.get("active"),
                })
            return sensors
        finally:
            if conn:
                conn.close()

    try:
        return await run_in_threadpool(_fetch)
    except Exception as exc:
        logger.exception("get_sensors failed")
        raise HTTPException(status_code=500, detail=str(exc))

#######################################

@app.get('/colormaps')
async def get_colormaps():
    """Return all colormaps from the database."""
    def _fetch():
        import psycopg2
        import psycopg2.extras
        conn = None
        try:
            conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password, connect_timeout=5)
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("SELECT name, description, stops, type, mode, meta FROM colormaps ORDER BY name;")
            rows = cur.fetchall()
            cur.close()

            out = []
            for r in rows:
                out.append({
                    'name': r.get('name'),
                    'description': r.get('description'),
                    'stops': r.get('stops'),
                    'type': r.get('type'),
                    'mode': r.get('mode'),
                    'meta': r.get('meta') or {}
                })
            return out
        finally:
            if conn:
                conn.close()

    try:
        return await run_in_threadpool(_fetch)
    except Exception as exc:
        logger.exception('get_colormaps failed')
        raise HTTPException(status_code=500, detail=str(exc))

#######################################

class sensorTimeseriesRequest(BaseModel):
    sensorId: int
    modelVariable: str  # model/canonical name, e.g. "dissolved_oxygen"
    fromDate: str
    toDate: str
    depth: Optional[float] = None

@app.post("/sensorTimeseries")
async def get_sensor_timeseries(request: sensorTimeseriesRequest):
    """Return sensor telemetry read from a compressed NC file.

    Accepts a canonical variable name (model name) and resolves it to the
    sensor-specific sensorCategoryCode via the sensors.variables DB mapping
    before reading {SENSORS_ROOT}/{sensorId}/{sensorCategoryCode}.nc.

    NC files may be 1-D (time,) or 2-D (time, depth).  When depth is omitted
    and a depth dimension is present, all depths are returned.

    Response: { time: [iso...], value: [float|null,...] }
              or with depth axis: { time: [...], depth: [...], value: [...] }
    """
    import psycopg2
    import psycopg2.extras
    import json

    try:
        await asyncio.wait_for(_extract_semaphore.acquire(), timeout=10.0)
    except (asyncio.TimeoutError, Exception):
        logger.warning("Semaphore timeout in sensorTimeseries")
        raise HTTPException(status_code=429, detail="Too many concurrent extract requests, try again later")

    def _resolve_sensor_code() -> str:
        conn = None
        try:
            conn = psycopg2.connect(
                host=db_host, port=db_port, dbname=db_name,
                user=db_user, password=db_password, connect_timeout=5,
            )
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("SELECT variables FROM sensors WHERE id=%s", (request.sensorId,))
            row = cur.fetchone()
            if not row or not row.get("variables"):
                raise HTTPException(
                    status_code=400,
                    detail=f"Sensor {request.sensorId} has no variable mapping defined",
                )
            mapping = row["variables"]
            if isinstance(mapping, str):
                mapping = json.loads(mapping)
            var_info = mapping.get(request.modelVariable)
            if not var_info or not isinstance(var_info, dict):
                raise HTTPException(
                    status_code=400,
                    detail=f"Sensor {request.sensorId} has no mapping for '{request.modelVariable}'",
                )
            code = var_info.get("name")
            if not code:
                raise HTTPException(
                    status_code=400,
                    detail=f"Sensor {request.sensorId} mapping for '{request.modelVariable}' is missing 'name'",
                )
            return code
        finally:
            if conn:
                conn.close()

    try:
        sensor_code = await asyncio.wait_for(run_in_threadpool(_resolve_sensor_code), timeout=10.0)
        result = await asyncio.wait_for(
            run_in_process(
                extract_sensor_timeseries,
                request.sensorId,
                sensor_code,
                request.fromDate,
                request.toDate,
                request.depth,
            ),
            timeout=THREADPOOL_TIMEOUT,
        )
        return result
    except HTTPException:
        raise
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("get_sensor_timeseries failed")
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
    lat: float
    lon: float
    depth: Optional[float] = None
    fromDate: str
    toDate: str

@app.post("/extractTimeseries")
async def fn_extract_timeseries(request: timeseriesRequest):
    # Reject requests if we are already at concurrency limit
    logger.info(f"START extractTimeseries: {request.source}, {request.var}, {request.lat}, {request.lon}, depth={request.depth}, from={request.fromDate}, to={request.toDate}")
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
                "FINISH extractTimeseries (federated): %s, %s, %s, from=%s, to=%s - returned %s points",
                request.var,
                request.lat,
                request.lon,
                request.fromDate,
                request.toDate,
                len(merged.get("time", [])),
            )
            return merged

        # non-federated flow
        result = await asyncio.wait_for(
            run_in_process(extract_timeseries, source=request.source, var=request.var, lat=request.lat, lon=request.lon, depth=depth, from_date=request.fromDate, to_date=request.toDate),
            timeout=THREADPOOL_TIMEOUT,
        )
        payload = _format_timeseries_result(result)
        logger.info(
            "FINISH extractTimeseries: %s, %s, %s, depth=%s, from=%s, to=%s - returned %s points",
            request.var,
            request.lat,
            request.lon,
            request.depth if request.depth is not None else "all",
            request.fromDate,
            request.toDate,
            len(payload.get("time", [])),
        )
        return payload
    except RuntimeError as exc:
        # Out-of-domain coordinates or grid issues are client errors (400), not server errors (500)
        if "km from the nearest grid point" in str(exc) or "Grid table is empty" in str(exc):
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
    depth: str
    fromDate: str
    toDate: str

@app.post("/extract_climateTimeseries")
async def fn_extract_ClimateTimeseries(request: climate_timeseriesRequest):
    # Reject requests if we are already at concurrency limit
    logger.info(f"START extract_climateTimeseries: {request.var} lat={request.lat}, lon={request.lon}, depth={request.depth}, fromDate={request.fromDate}, toDate={request.toDate}")
    try:
        # Wait up to 10 seconds to acquire the semaphore
        await asyncio.wait_for(_extract_semaphore.acquire(), timeout=10.0)
    except (asyncio.TimeoutError, Exception):
        logger.warning("Semaphore timeout in extract_climateTimeseries")
        raise HTTPException(status_code=429, detail="Too many concurrent extract requests, try again later")

    try:
        lat = request.lat
        lon = request.lon
        variable = request.var
        depth = request.depth  # depth_image is already the correct string for file naming
        from_date = request.fromDate  # Pass fromDate string (ISO format) to the extraction function
        to_date = request.toDate  # Pass toDate string (ISO format) to the extraction function
        
        # Run the synchronous extraction in a threadpool to keep the event loop free
        result = await asyncio.wait_for(
            run_in_process(extract_climate_timeseries, lat=lat, lon=lon, variable=variable, depth=depth, from_date=from_date, to_date=to_date),
            timeout=THREADPOOL_TIMEOUT,
        )
        if result is None:
            logger.error("Extraction returned None")
            raise HTTPException(status_code=500, detail="Extraction failed")
        
        # Clean NaN values from all numeric columns before JSON serialization
        if isinstance(result, dict) and 'data' in result:
            data = result['data']
            if isinstance(data, list):
                for row in data:
                    for key in row:
                        if isinstance(row[key], float) and np.isnan(row[key]):
                            row[key] = None
            
        logger.info(f"FINISH extract_climateTimeseries: {request.var} lat={request.lat}, lon={request.lon}, depth={request.depth}, fromDate={request.fromDate}, toDate={request.toDate}")
        return result
    except RuntimeError as exc:
        # Out-of-domain coordinates or grid issues are client errors (400), not server errors (500)
        if "km from the nearest grid point" in str(exc) or "Grid table is empty" in str(exc):
            logger.warning(f"Out-of-domain or invalid coordinates: {exc}")
            raise HTTPException(status_code=400, detail=str(exc))
        # Other RuntimeErrors are unexpected, treat as 500
        logger.exception("extract_climate_timeseries failed with RuntimeError")
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        logger.exception("extract_climate_timeseries failed")
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        _extract_semaphore.release()

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
async def fn_get_minmax(request: minmaxRequest):
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
        depth = request.depth
        dt_str = request.dt
        
        # Parse datetime string (ISO format: YYYY-MM-DDTHH:mm:ss)
        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        
        # Get data directory from environment
        data_dir = _get_nc_data_dirs(source)
        if data_dir is None:
            logger.error(f"Unsupported source: {source}")
            raise HTTPException(status_code=400, detail=f"Unsupported source: {source}")
        
        if source == "SalishSeaCast":
            db_gridTable = "grid"
        elif source == "Live Ocean":
            db_gridTable = "lo_grid"
        else:
            logger.error(f"Unsupported source: {source}")
            raise HTTPException(status_code=400, detail=f"Unsupported source: {source}")
        
        # Extract bounds if provided
        north = request.north
        south = request.south
        east = request.east
        west = request.west
        
        # Extract min/max with database connection for bounds mapping
        min_val, max_val = await asyncio.wait_for(
            run_in_process(extract_minmax,
                data_dir=data_dir,
                variable=var,
                dt=dt,
                depth=depth,
                north=north,
                south=south,
                east=east,
                west=west,
                db_host=db_host,
                db_port=db_port,
                db_user=db_user,
                db_password=db_password,
                db_name=db_name,
                db_table=db_gridTable),
            timeout=THREADPOOL_TIMEOUT,
        )
        
        logger.info(f"FINISH getMinMax: source={request.source}, var={request.var}, range=[{min_val}, {max_val}]")
        return {"min": min_val, "max": max_val}
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
async def fn_get_monthly_climatology(request: monthlyClimRequest):
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
async def fn_get_profile(request: profileRequest):
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
        
        if source == "SalishSeaCast":
            db_gridTable = "grid"
        elif source == "Live Ocean":
            db_gridTable = "lo_grid"
        else:
            logger.error(f"Unsupported source: {source}")
            raise HTTPException(status_code=400, detail=f"Unsupported source: {source}")

        profile = await run_in_threadpool(
            extract_profile,
            var=var,
            lat=lat,
            lng=lng,
            dt=dt,
            data_dir=_get_nc_data_dirs(source),
            db_host=db_host,
            db_port=db_port,
            db_name=db_name,
            db_user=db_user,
            db_password=db_password,
            db_table=db_gridTable
        )
        logger.info(f"FINISH getProfile: source={source}, var={var}, lat={lat}, lng={lng}, dt={dt} - returned {len(profile)} points")
        return profile
    except RuntimeError as exc:
        # Out-of-domain coordinates or grid issues are client errors (400), not server errors (500)
        if "km from the nearest grid point" in str(exc) or "Grid table is empty" in str(exc):
            logger.warning(f"Out-of-domain or invalid coordinates: {exc}")
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
async def fn_get_eval(request: evalRequest):
    logger.info(f"START getEval: sensor={request.sensor}, variable={request.variable}, model={request.model}")
    
    eval_data_dir = os.getenv("EVAL_DATA_DIR", "/opt/data/eval")
    eval_nc_path = os.path.join(eval_data_dir, f"{request.sensor}.nc")
    
    # Validate model parameter
    valid_models = ["SSC", "LiveOcean"]
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

class ThresholdSpec(BaseModel):
    value: float
    direction: str  # ">" or "<"

class GridRange(BaseModel):
    min: int
    max: int

class DepthRange(BaseModel):
    min: float
    max: float

class AnalysisRequest(BaseModel):
    gridX: GridRange
    gridY: GridRange
    depth: DepthRange
    primaryMetric: MetricSpec
    temporal: dict
    threshold: Optional[ThresholdSpec] = None
    secondMetric: Optional[MetricSpec] = None


@app.post("/analysis/overlay")
async def analysis_overlay(request: AnalysisRequest):
    """Overlay Yearly Timeseries — per-year timeseries for overlay plotting."""
    try:
        result = await run_in_threadpool(
            query_overlay_timeseries,
            grid_x_range=(request.gridX.min, request.gridX.max),
            grid_y_range=(request.gridY.min, request.gridY.max),
            depth_range=(request.depth.min, request.depth.max),
            variable=request.primaryMetric.variable,
            stat=request.primaryMetric.stat,
            year_range=request.temporal["yearRange"],
            season=request.temporal.get("season", "full_year"),
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("analysis_overlay failed")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/analysis/climatology")
async def analysis_climatology(request: AnalysisRequest):
    """Aggregated Climatology Cycle — averaged monthly climatology."""
    try:
        result = await run_in_threadpool(
            query_climatology,
            grid_x_range=(request.gridX.min, request.gridX.max),
            grid_y_range=(request.gridY.min, request.gridY.max),
            depth_range=(request.depth.min, request.depth.max),
            variable=request.primaryMetric.variable,
            stat=request.primaryMetric.stat,
            year_range=request.temporal["yearRange"],
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("analysis_climatology failed")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/analysis/threshold")
async def analysis_threshold(request: AnalysisRequest):
    """Extreme Event (Threshold) Count — days per year matching threshold."""
    if not request.threshold:
        raise HTTPException(status_code=400, detail="threshold is required for threshold mode")
    try:
        result = await run_in_threadpool(
            query_threshold_count,
            grid_x_range=(request.gridX.min, request.gridX.max),
            grid_y_range=(request.gridY.min, request.gridY.max),
            depth_range=(request.depth.min, request.depth.max),
            variable=request.primaryMetric.variable,
            stat=request.primaryMetric.stat,
            year_range=request.temporal["yearRange"],
            season=request.temporal.get("season", "full_year"),
            threshold_value=request.threshold.value,
            threshold_direction=request.threshold.direction,
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("analysis_threshold failed")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/analysis/trend")
async def analysis_trend(request: AnalysisRequest):
    """Inter-Annual Trend Analysis — per-year annual means."""
    try:
        result = await run_in_threadpool(
            query_trend,
            grid_x_range=(request.gridX.min, request.gridX.max),
            grid_y_range=(request.gridY.min, request.gridY.max),
            depth_range=(request.depth.min, request.depth.max),
            variable=request.primaryMetric.variable,
            stat=request.primaryMetric.stat,
            year_range=request.temporal["yearRange"],
            season=request.temporal.get("season", "full_year"),
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("analysis_trend failed")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/analysis/correlation")
async def analysis_correlation(request: AnalysisRequest):
    """Multi-Variable Correlation — paired per-timestep values for two variables."""
    if not request.secondMetric:
        raise HTTPException(status_code=400, detail="secondMetric is required for correlation mode")
    try:
        result = await run_in_threadpool(
            query_correlation,
            grid_x_range=(request.gridX.min, request.gridX.max),
            grid_y_range=(request.gridY.min, request.gridY.max),
            depth_range=(request.depth.min, request.depth.max),
            primary_variable=request.primaryMetric.variable,
            primary_stat=request.primaryMetric.stat,
            second_variable=request.secondMetric.variable,
            second_stat=request.secondMetric.stat,
            year_range=request.temporal["yearRange"],
            season=request.temporal.get("season", "full_year"),
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("analysis_correlation failed")
        raise HTTPException(status_code=500, detail=str(exc))


#######################################


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=4000)