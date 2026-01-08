import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
import os
import logging
# import uuid
# import pandas as pd

from extractTimeseries import extract_timeseries
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
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change to specific origins in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

# @app.get("/getTrajectories")
# def get_trajectories():
#     db = TrajectoryDatabase(db_host, db_port, db_name, db_user, db_password)
#     db.connect()
#     trajectories = db.get_all_trajectories()
#     db.disconnect()
    
#     results = []
#     for record in trajectories:
#         results.append(
#             {
#                 "id": record[0],
#                 "erddap_url": record[1],
#                 "dataset_id": record[2],
#                 "metadata": record[3],
#             }
#         )
#     return results

#######################################

class timeseriesRequest(BaseModel):
    var: str
    from_dt: str
    to_dt: str
    lat: float
    lon: float

@app.post("/extractTimeseries")
def fn_extract_timeseries(request: timeseriesRequest):
    var = request.var
    from_dt = request.from_dt
    to_dt = request.to_dt
    lat = request.lat
    lon = request.lon
    
    time, value = extract_timeseries(var=var, from_dt=from_dt, to_dt=to_dt, lat=lat, lon=lon)
    return {"time": time.tolist(), "value": value.tolist()}

#######################################

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=4000)
