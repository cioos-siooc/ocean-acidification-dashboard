import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional
import os
import logging
import asyncio
from concurrent.futures import ProcessPoolExecutor
from typing import Optional
from starlette.concurrency import run_in_threadpool
import numpy as np

from modules.extractTimeseries import extract_timeseries
from modules.extract_profile import extract_profile
from modules.eval_extractor import extract_eval_data
from modules.extract_climate_timeseries import extract_climate_timeseries
from modules.extractMinMax import extract_minmax
from modules.pngGenerator import generate_png_for_variable
from modules.extractSensorTimeseries import extract_sensor_timeseries

# Limit concurrent extract requests to avoid resource exhaustion (files + DB)
MAX_CONCURRENT_EXTRACTS = int(os.getenv("MAX_CONCURRENT_EXTRACTS", "4"))
_extract_semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTS)

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
IMAGE_ROOT = os.environ.get("IMAGE_ROOT", "/opt/data/image")


def _get_image_roots() -> list:
    """Return list of image root directories to search when serving tiles.

    IMAGE_ROOT (default /opt/data/image) is the primary directory and the
    only one used for on-demand generation writes.
    IMAGE_ROOT_ARCHIVE (optional) is a second read-only directory, e.g. an
    external disk where older tiles have been moved.
    """
    archive = os.getenv("IMAGE_ROOT_ARCHIVE", "")
    return [IMAGE_ROOT, archive] if archive else [IMAGE_ROOT]


def _get_nc_data_dirs():
    """Return the NC data directory spec (str or list) from environment.

    Set NC_DATA_DIR for the primary directory (default /opt/data/nc).
    Optionally set NC_DATA_DIR_ARCHIVE to a second directory that is searched
    when a file is not found in the primary (e.g. an external disk mount).
    """
    primary = os.getenv("NC_DATA_DIR", "/opt/data/nc")
    archive = os.getenv("NC_DATA_DIR_ARCHIVE", "")
    return [primary, archive] if archive else primary

# Read DB config from environment at import time so route handlers can access it
db_host = os.getenv("DB_HOST", "db")
db_port = int(os.getenv("DB_PORT", 5432))
db_name = os.getenv("DB_NAME", "oa")
db_user = os.getenv("DB_USER", "postgres")
db_password = os.getenv("DB_PASSWORD", "postgres")


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
        query = "SELECT id, name, latitude, longitude, depth, device_config, active FROM sensors;"
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
            run_in_threadpool(
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

@app.get("/png/{var}/{dt}/{depth}")
async def get_png(var: str, dt: str, depth: str):
    """Serve PNG for variable/datetime/depth, generating on-demand if needed."""
    # Serve the PNG file for a specific variable, datetime, and depth
    safe_var = os.path.basename(var)
    safe_dt = os.path.basename(dt)
    safe_depth = depth.replace('.', 'p')

    # Try both .webp (from on-demand generation) and .png (legacy), across all image roots
    for image_root in _get_image_roots():
        path = os.path.join(image_root, safe_var, safe_dt)
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
        data_dir = _get_nc_data_dirs()
        full_path = await generate_png_for_variable(
            var, dt, depth_value, data_dir, _get_image_roots(), _png_gen_semaphore, _png_executor
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
    var: str
    lat: float
    lon: float
    depth: Optional[float] = None
    fromDate: str
    toDate: str

@app.post("/extractTimeseries")
async def fn_extract_timeseries(request: timeseriesRequest):
    # Reject requests if we are already at concurrency limit
    logger.info(f"START extractTimeseries: {request.var}, {request.lat}, {request.lon}, depth={request.depth}, from={request.fromDate}, to={request.toDate}")
    try:
        await asyncio.wait_for(_extract_semaphore.acquire(), timeout=10.0)
    except (asyncio.TimeoutError, Exception):
        logger.warning("Semaphore timeout in extractTimeseries")
        raise HTTPException(status_code=429, detail="Too many concurrent extract requests, try again later")

    try:
        # use provided depth exactly (float value passed from frontend); None means all depths
        depth = float(request.depth) if request.depth is not None else None

        # Fetch the success_image dates for this variable in the requested range.
        # Only NC files whose date is in this set will be read.
        def _fetch_allowed_dates():
            import psycopg2
            conn = None
            try:
                conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password, connect_timeout=5)
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT DISTINCT nj.start_time FROM nc_jobs nj
                    JOIN fields f ON nj.variable_id = f.id
                    WHERE f.variable = %s
                      AND DATE(nj.start_time) >= DATE(%s::timestamp)
                      AND DATE(nj.start_time) <= DATE(%s::timestamp)
                      AND nj.status = 'success_image'
                    """,
                    (request.var, request.fromDate, request.toDate),
                )
                return [row[0] for row in cur.fetchall()]
            finally:
                if conn:
                    conn.close()

        allowed_dates = await asyncio.wait_for(run_in_threadpool(_fetch_allowed_dates), timeout=15.0)
        if not allowed_dates:
            raise HTTPException(status_code=422, detail="No processed data available for the requested date range")

        result = await asyncio.wait_for(
            run_in_threadpool(extract_timeseries, var=request.var, lat=request.lat, lon=request.lon, depth=depth, from_date=request.fromDate, to_date=request.toDate, data_dir=_get_nc_data_dirs(), allowed_dates=allowed_dates),
            timeout=THREADPOOL_TIMEOUT,
        )
        import pandas as pd
        if isinstance(result, pd.DataFrame):
            # All-depths response: return time, depth, value arrays
            logger.info(f"FINISH extractTimeseries: {request.var}, {request.lat}, {request.lon}, depth=all, from={request.fromDate}, to={request.toDate} - returned {len(result)} points")
            def _clean(v): return None if (isinstance(v, float) and np.isnan(v)) else v
            return {
                "time":  [t.isoformat() if hasattr(t, "isoformat") else t for t in result["time"].tolist()],
                "depth": result["depth"].tolist(),
                "value": [_clean(v) for v in result["value"].tolist()],
            }
        else:
            time, value = result
            logger.info(f"FINISH extractTimeseries: {request.var}, {request.lat}, {request.lon}, depth={request.depth}, from={request.fromDate}, to={request.toDate} - returned {len(time)} points")
            # Replace NaN values with None (serializes to null in JSON)
            time_list = [None if (isinstance(t, float) and np.isnan(t)) else t for t in time.tolist()]
            value_list = [None if (isinstance(v, float) and np.isnan(v)) else v for v in value.tolist()]
            return {"time": time_list, "value": value_list}
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
        depth = request.depth.replace('.', 'p')  # Pass depth as string (e.g., "0p5") since that's what the module expects for file naming
        from_date = request.fromDate  # Pass fromDate string (ISO format) to the extraction function
        to_date = request.toDate  # Pass toDate string (ISO format) to the extraction function
        
        # Run the synchronous extraction in a threadpool to keep the event loop free
        result = await asyncio.wait_for(
            run_in_threadpool(extract_climate_timeseries, lat=lat, lon=lon, variable=variable, depth=depth, from_date=from_date, to_date=to_date),
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
    logger.info(f"START getMinMax: {request.var}, dt={request.dt}, depth={request.depth}")
    try:
        await asyncio.wait_for(_extract_semaphore.acquire(), timeout=10.0)
    except (asyncio.TimeoutError, Exception):
        logger.warning("Semaphore timeout in getMinMax")
        raise HTTPException(status_code=429, detail="Too many concurrent extract requests, try again later")

    try:
        from datetime import datetime
        
        var = request.var
        depth = request.depth
        dt_str = request.dt
        
        # Parse datetime string (ISO format: YYYY-MM-DDTHH:mm:ss)
        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        
        # Get data directory from environment
        data_dir = _get_nc_data_dirs()
        
        # Extract bounds if provided
        north = request.north
        south = request.south
        east = request.east
        west = request.west
        
        # Extract min/max with database connection for bounds mapping
        min_val, max_val = await asyncio.wait_for(
            run_in_threadpool(extract_minmax,
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
                db_name=db_name),
            timeout=THREADPOOL_TIMEOUT,
        )
        
        logger.info(f"FINISH getMinMax: {request.var}, range=[{min_val}, {max_val}]")
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
        ssc_root = os.getenv("SSC_DATA_DIR", "/opt/data/SalishSeaCast")
        ssc_archive = os.getenv("SSC_DATA_DIR_ARCHIVE", "")
        data_root = [ssc_root, ssc_archive] if ssc_archive else ssc_root
        result = await run_in_threadpool(
            get_monthly_climatology_at_coord,
            lat=request.lat,
            lon=request.lon,
            depth=request.depth,
            variable=request.variable,
            data_root=data_root,
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
    lat: float
    lng: float
    dt: str
    var: Optional[str] = None

@app.post("/getProfile")
async def fn_get_profile(request: profileRequest):
    logger.info(f"START getProfile: {request.var}, {request.lat}, {request.lng}, {request.dt}")
    try:
        await asyncio.wait_for(_extract_semaphore.acquire(), timeout=10.0)
    except (asyncio.TimeoutError, Exception):
        logger.warning("Semaphore timeout in getProfile")
        raise HTTPException(status_code=429, detail="Too many concurrent extract requests, try again later")

    try:
        var = request.var or "temperature"  # Default to temperature if not specified
        lat = request.lat
        lng = request.lng
        dt = request.dt

        profile = await run_in_threadpool(
            extract_profile,
            var=var,
            lat=lat,
            lng=lng,
            dt=dt,
            data_dir=_get_nc_data_dirs(),
            db_host=db_host,
            db_port=db_port,
            db_name=db_name,
            db_user=db_user,
            db_password=db_password,
        )
        logger.info(f"FINISH getProfile: {var}, {lat}, {lng}, {dt} - returned {len(profile)} points")
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
    
    eval_nc_path = f"/opt/data/eval/{request.sensor}.nc"
    
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

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=4000)
