import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional
import os
import logging
# import uuid
# import pandas as pd

from extractTimeseries import extract_timeseries
from threading import Semaphore

# Limit concurrent extract requests to avoid resource exhaustion (files + DB)
MAX_CONCURRENT_EXTRACTS = int(os.getenv("MAX_CONCURRENT_EXTRACTS", "4"))
_extract_semaphore = Semaphore(MAX_CONCURRENT_EXTRACTS)
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
def read_root():
    # db = TrajectoryDatabase(db_host, db_port, db_name, db_user, db_password)
    # db.connect()
    # db.create_tables()
    # db.disconnect()
    return {"message": "Hello from OAH API!"}

#######################################

@app.get("/variables")
def get_variables():
    """
    Return a list of variables with their min/max datetimes.
    The SQL used can be customized via the VARS_QUERY environment variable.
    Default query expects columns: var, dt and a table named `measurements`.
    """
    try:
        from modules.variables import get_variables as fetch_variables
        variables = fetch_variables(db_host, db_port, db_name, db_user, db_password)
        return variables
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("get_variables failed")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/sensors")
def get_sensors():
    """
    Return a list of sensors with their metadata.
    """
    try:
        import psycopg2
        import psycopg2.extras
    except Exception as exc:
        raise HTTPException(status_code=500, detail="psycopg2 is required for /sensors endpoint") from exc

    query = "SELECT name, latitude, longitude, depths, variables FROM sensors;"
    conn = None
    try:
        conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        
        sensors = []
        for row in rows:
            sensors.append({
                "name": row.get("name"),
                "latitude": row.get("latitude"),
                "longitude": row.get("longitude"),
                "depths": row.get("depths"),
                "variables": row.get("variables"),
            })
        return sensors
    except Exception as exc:
        logger.exception("get_sensors failed")
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

#######################################

@app.get("/metadata/{var}")
def get_metadata(var: str):
    safe_var = os.path.basename(var)
    path = os.path.join(PNG_ROOT, safe_var, "meta.json")
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="Metadata not found")
    return JSONResponse(content=open(path).read())

@app.get("/png/{var}/{dt}/{depth}")
def get_png(var: str, dt: str, depth: str):
    # Serve the PNG file for a specific variable, datetime, and depth with appropriate headers for caching
    safe_var = os.path.basename(var)
    safe_dt = os.path.basename(dt)
    safe_depth = depth.replace('.', 'p')
    path = os.path.join(PNG_ROOT, safe_var, safe_dt)
    filename = f"{safe_depth}.png"
    full_path = os.path.join(path, filename)
    if not os.path.isfile(full_path):
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
def fn_extract_timeseries(request: timeseriesRequest):
    # Reject requests if we are already at concurrency limit to protect the DB and FS
    if not _extract_semaphore.acquire(blocking=False):
        raise HTTPException(status_code=429, detail="Too many concurrent extract requests, try again later")

    try:
        var = request.var
        lat = request.lat
        lon = request.lon
        # use provided depth exactly (float value passed from frontend)
        depth = float(request.depth)

        time, value = extract_timeseries(var=var, lat=lat, lon=lon, depth=depth)
        return {"time": time.tolist(), "value": value.tolist()}
    except Exception as exc:
        logger.exception("extract_timeseries failed")
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        _extract_semaphore.release()

#######################################

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=4000)
