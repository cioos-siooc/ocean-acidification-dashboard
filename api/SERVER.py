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

from extractTimeseries import extract_timeseries
from modules.extract_profile import extract_profile
from modules.eval_extractor import extract_eval_data
from extract_climate_timeseries import extract_climate_timeseries
from extractMinMax import extract_minmax
from pngGenerator import generate_png_for_variable

# Limit concurrent extract requests to avoid resource exhaustion (files + DB)
MAX_CONCURRENT_EXTRACTS = int(os.getenv("MAX_CONCURRENT_EXTRACTS", "4"))
_extract_semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTS)

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
        query = "SELECT id, name, latitude, longitude, depth, variables, device_config, active FROM sensors;"
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
                    "variables": row.get("variables"),
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
    variable: str
    sensorId: int
    fromDate: str
    toDate: str
    
@app.post("/sensorTimeseries")
async def get_sensor_timeseries(request: sensorTimeseriesRequest):
    """Return sensor telemetry for a given sensor id and variable and datetime.
    Response: { time: [iso...], value: [float,...] }
    """
    from datetime import datetime as dt, timedelta
    
    var = request.variable
    sensor_id = request.sensorId
    from_date_str = request.fromDate
    to_date_str = request.toDate
    
    # Parse the incoming ISO datetime and calculate ±5 day window
    try:
        # Handle both ISO format with Z and with +00:00
        from_date = dt.fromisoformat(from_date_str.replace('Z', '+00:00'))
        to_date = dt.fromisoformat(to_date_str.replace('Z', '+00:00'))
    except Exception as exc:
        logger.exception(f"Failed to parse datetime '{from_date_str}' or '{to_date_str}'")
        raise HTTPException(status_code=400, detail=f"Invalid datetime format: {exc}")
    
    def _fetch():
        import psycopg2
        import psycopg2.extras
        conn = None
        try:
            conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password, connect_timeout=5)
            cur = conn.cursor()
            sql = "SELECT time, (measurements->>%s)::float AS value FROM sensors_data WHERE sensor_id=%s"
            params = [var, sensor_id]
            
            # ±5 days around requested datetime to give some context, limit to 1000 points to avoid huge responses
            sql += " AND time >= %s AND time <= %s"
            params.extend([from_date.isoformat(), to_date.isoformat()])

            sql += " ORDER BY time ASC"
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
            times = [r[0].isoformat() for r in rows]
            vals = [None if r[1] is None else float(r[1]) for r in rows]
            return {"time": times, "value": vals}
        finally:
            if conn:
                conn.close()

    try:
        return await run_in_threadpool(_fetch)
    except Exception as exc:
        logger.exception("get_sensor_timeseries failed")
        raise HTTPException(status_code=500, detail=str(exc))

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
    path = os.path.join(IMAGE_ROOT, safe_var, safe_dt)
    
    # Try both .webp (from on-demand generation) and .png (legacy)
    for ext in ['.webp', '.png']:
        filename = f"{safe_depth}{ext}"
        full_path = os.path.join(path, filename)
        
        # os.path.isfile is fast but still better in a thread if the FS is slow
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
            var, dt, depth_value, data_dir, IMAGE_ROOT, _png_gen_semaphore, _png_executor
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
    depth: float
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
        # use provided depth exactly (float value passed from frontend)
        depth = float(request.depth)
        time, value = await run_in_threadpool(extract_timeseries, var=request.var, lat=request.lat, lon=request.lon, depth=depth, from_date=request.fromDate, to_date=request.toDate, data_dir=_get_nc_data_dirs())
        logger.info(f"FINISH extractTimeseries: {request.var}, {request.lat}, {request.lon}, depth={request.depth}, from={request.fromDate}, to={request.toDate} - returned {len(time)} points")
        return {"time": time.tolist(), "value": value.tolist()}
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
        result = await run_in_threadpool(extract_climate_timeseries, lat=lat, lon=lon, variable=variable, depth=depth, from_date=from_date, to_date=to_date)
        if result is None:
            logger.error("Extraction returned None")
            raise HTTPException(status_code=500, detail="Extraction failed")
            
        logger.info(f"FINISH extract_climateTimeseries: {request.var} lat={request.lat}, lon={request.lon}, depth={request.depth}, fromDate={request.fromDate}, toDate={request.toDate}")
        return result
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
        min_val, max_val = await run_in_threadpool(extract_minmax, 
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
            db_name=db_name)
        
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
