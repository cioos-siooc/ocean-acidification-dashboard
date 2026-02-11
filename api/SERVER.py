import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional
import os
import logging
import threading
import asyncio
from typing import Optional
from starlette.concurrency import run_in_threadpool

from extractTimeseries import extract_timeseries
from modules.extract_profile import extract_profile
from modules.eval_extractor import extract_eval_data
from extract_climate_timeseries import extract_climate_timeseries

# Limit concurrent extract requests to avoid resource exhaustion (files + DB)
MAX_CONCURRENT_EXTRACTS = int(os.getenv("MAX_CONCURRENT_EXTRACTS", "4"))
_extract_semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTS)
_io_lock = threading.Lock()
# from calc import bin
# from lib.bathymetry import BathymetryProcessor
# from lib.database import TrajectoryDatabase
# from lib.trajectory import TrajectoryExtractor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
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
PNG_ROOT = os.environ.get("PNG_ROOT", "/opt/data/png")

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
        query = "SELECT id, name, latitude, longitude, depths, variables, device_config, active FROM sensors;"
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
                    "depths": row.get("depths"),
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

@app.get("/sensors/{sensor_id}/timeseries")
async def get_sensor_timeseries(sensor_id: int, var: str, start: Optional[str] = None, end: Optional[str] = None, limit: Optional[int] = 10000):
    """Return sensor telemetry for a given sensor id and variable.
    Response: { time: [iso...], value: [float,...] }
    """
    def _fetch():
        import psycopg2
        import psycopg2.extras
        conn = None
        try:
            conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password, connect_timeout=5)
            cur = conn.cursor()
            sql = "SELECT time, (measurements->>%s)::float AS value FROM sensors_data WHERE sensor_id=%s"
            params = [var, sensor_id]
            if start:
                sql += " AND time >= %s"
                params.append(start)
            if end:
                sql += " AND time <= %s"
                params.append(end)
            sql += " ORDER BY time ASC LIMIT %s"
            params.append(limit)
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

@app.get("/metadata/{var}")
async def get_metadata(var: str):
    safe_var = os.path.basename(var)
    path = os.path.join(PNG_ROOT, safe_var, "meta.json")
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="Metadata not found")
    
    def _read():
        with open(path) as f:
            return f.read()
            
    content = await run_in_threadpool(_read)
    return JSONResponse(content=content)

@app.get("/png/{var}/{dt}/{depth}")
async def get_png(var: str, dt: str, depth: str):
    # Serve the PNG file for a specific variable, datetime, and depth with appropriate headers for caching
    safe_var = os.path.basename(var)
    safe_dt = os.path.basename(dt)
    safe_depth = depth.replace('.', 'p')
    path = os.path.join(PNG_ROOT, safe_var, safe_dt)
    filename = f"{safe_depth}.png"
    full_path = os.path.join(path, filename)
    
    # os.path.isfile is fast but still better in a thread if the FS is slow
    exists = await run_in_threadpool(os.path.isfile, full_path)
    if not exists:
        raise HTTPException(status_code=404, detail="PNG not found")
    
    headers = {
        "Cache-Control": "public, max-age=31536000, immutable",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "*",
        "Vary": "Origin",
    }
    return FileResponse(full_path, media_type="image/png", headers=headers)

#######################################

class timeseriesRequest(BaseModel):
    var: str
    lat: float
    lon: float
    depth: float

@app.post("/extractTimeseries")
async def fn_extract_timeseries(request: timeseriesRequest):
    # Reject requests if we are already at concurrency limit
    logger.info(f"START extractTimeseries: {request.var}, {request.lat}, {request.lon}")
    try:
        await asyncio.wait_for(_extract_semaphore.acquire(), timeout=10.0)
    except (asyncio.TimeoutError, Exception):
        logger.warning("Semaphore timeout in extractTimeseries")
        raise HTTPException(status_code=429, detail="Too many concurrent extract requests, try again later")

    try:
        var = request.var
        lat = request.lat
        lon = request.lon
        # use provided depth exactly (float value passed from frontend)
        depth = float(request.depth)

        time, value = await run_in_threadpool(extract_timeseries, var=var, lat=lat, lon=lon, depth=depth)
        logger.info(f"FINISH extractTimeseries: {request.var}")
        return {"time": time.tolist(), "value": value.tolist()}
    except Exception as exc:
        logger.exception("extract_timeseries failed")
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        _extract_semaphore.release()

#######################################

class climate_timeseriesRequest(BaseModel):
    # var: str
    lat: float
    lon: float
    # depth: float

@app.post("/extract_climateTimeseries")
async def fn_extract_ClimateTimeseries(request: climate_timeseriesRequest):
    # Reject requests if we are already at concurrency limit
    logger.info(f"START extract_climateTimeseries: {request.lat}, {request.lon}")
    try:
        # Wait up to 10 seconds to acquire the semaphore
        await asyncio.wait_for(_extract_semaphore.acquire(), timeout=10.0)
    except (asyncio.TimeoutError, Exception):
        logger.warning("Semaphore timeout in extract_climateTimeseries")
        raise HTTPException(status_code=429, detail="Too many concurrent extract requests, try again later")

    try:
        lat = request.lat
        lon = request.lon
        
        # Run the synchronous extraction in a threadpool to keep the event loop free
        result = await run_in_threadpool(extract_climate_timeseries, lat=lat, lon=lon)
        if result is None:
            logger.error("Extraction returned None")
            raise HTTPException(status_code=500, detail="Extraction failed")
            
        logger.info(f"FINISH extract_climateTimeseries: {request.lat}, {request.lon}")
        return result
    except Exception as exc:
        logger.exception("extract_climate_timeseries failed")
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
        result = await run_in_threadpool(
            get_monthly_climatology_at_coord,
            lat=request.lat,
            lon=request.lon,
            depth=request.depth,
            variable=request.variable,
            # Let module pick data root default and DB environment vars
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
    lon: float
    dt: str
    var: Optional[str] = None

@app.post("/getProfile")
async def fn_get_profile(request: profileRequest):
    logger.info(f"START getProfile: {request.var}, {request.lat}, {request.lon}, {request.dt}")
    try:
        await asyncio.wait_for(_extract_semaphore.acquire(), timeout=10.0)
    except (asyncio.TimeoutError, Exception):
        logger.warning("Semaphore timeout in getProfile")
        raise HTTPException(status_code=429, detail="Too many concurrent extract requests, try again later")

    try:
        var = request.var or "temperature"  # Default to temperature if not specified
        lat = request.lat
        lon = request.lon
        dt = request.dt

        profile = await run_in_threadpool(
            extract_profile,
            var=var,
            lat=lat,
            lon=lon,
            dt=dt,
            db_host=db_host,
            db_port=db_port,
            db_name=db_name,
            db_user=db_user,
            db_password=db_password,
        )
        logger.info(f"FINISH getProfile: {var}, {lat}, {lon}, {dt} - returned {len(profile)} points")
        return profile
    except Exception as exc:
        logger.exception("extract_profile failed")
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        _extract_semaphore.release()

#######################################

class evalRequest(BaseModel):
    sensor_id: int
    variable: str
    model: str

@app.post("/getEval")
async def fn_get_eval(request: evalRequest):
    logger.info(f"START getEval: sensor_id={request.sensor_id}, variable={request.variable}, model={request.model}")
    
    eval_nc_path = "/opt/data/eval/Baynes_5m.nc"
    
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
            model=model,
            sensor_id=request.sensor_id
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
