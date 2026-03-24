# OA API - Complete Endpoints Reference

## Summary
**Total Endpoints: 14**
- 8 GET endpoints
- 6 POST endpoints
- **Statistics Support: YES** - Available through two climatology endpoints

---

## Endpoint Details

### 1. **GET** `/`
Root/Health Check Endpoint
- **Parameters**: None
- **Response**: 
  ```json
  {"message": "Hello from OAH API!"}
  ```
- **Description**: Simple health check endpoint

---

### 2. **GET** `/variables`
Get list of all available variables with metadata and available datetimes
- **Parameters**: None (reads from database)
- **Response**: 
  ```json
  [
    {
      "var": "temperature",
      "dts": [ISO datetime strings for each available hour],
      "colormapMin": float,
      "colormapMax": float,
      "depths": [list of available depths],
      "precision": float,
      "colormap": "colormap_name",
      "bounds": {geographic bounds},
      "source": "DataSource"
    },
    ...
  ]
  ```
- **Description**: Returns metadata about all variables, including min/max for colormaps
- **Database Query**: Joins `fields`, `datasets`, and `nc_jobs` tables

---

### 3. **GET** `/sensors`
Get list of all sensor metadata
- **Parameters**: None
- **Response**: 
  ```json
  [
    {
      "id": int,
      "name": "SensorName",
      "latitude": float,
      "longitude": float,
      "depths": [list],
      "variables": [list],
      "device_config": {...},
      "active": bool
    },
    ...
  ]
  ```
- **Description**: Returns sensor station metadata from database

---

### 4. **GET** `/colormaps`
Get all available colormaps
- **Parameters**: None
- **Response**: 
  ```json
  [
    {
      "name": "colormap_name",
      "description": "description",
      "stops": [...],
      "type": "sequential|diverging|categorical",
      "mode": "rgb|lab|...",
      "meta": {}
    },
    ...
  ]
  ```
- **Description**: Returns colormaps for visualization

---

### 5. **POST** `/sensorTimeseries`
Extract sensor telemetry for a given sensor, variable, and datetime (±5 day window)
- **Request Body**: 
  ```json
  {
    "variable": "string (e.g., 'temperature')",
    "sensorId": int,
    "datetime": "ISO datetime string"
  }
  ```
- **Response**: 
  ```json
  {
    "time": ["2026-01-01T00:00:00", ...],
    "value": [12.5, null, 12.4, ...]
  }
  ```
- **Description**: Returns ±5 day window of sensor measurements around the requested datetime
- **Data Source**: Sensors database table

---

### 6. **GET** `/metadata/{var}`
Get metadata JSON file for a specific variable
- **URL Parameters**: 
  - `var`: Variable name (e.g., "temperature")
- **Response**: JSON file content (meta.json from file system)
- **File Location**: `{PNG_ROOT}/{var}/meta.json`

---

### 7. **GET** `/png/{var}/{dt}/{depth}`
Get PNG raster tile for a variable at specific datetime and depth
- **URL Parameters**: 
  - `var`: Variable name
  - `dt`: Datetime (directory)
  - `depth`: Depth value (converted: "1.5" → "1p5")
- **Response**: PNG binary with cache headers
- **File Location**: `{PNG_ROOT}/{var}/{dt}/{depth}.png`
- **Cache**: Immutable (max-age=31536000)

---

### 8. **GET** `/vector/{z}/{x}/{y}.pbf`
Get vector tile (bathymetry)
- **URL Parameters**: 
  - `z`, `x`, `y`: Standard tile coordinates
- **Response**: Protocol Buffer binary (.pbf)
- **File Location**: `{VECTOR_ROOT}/{z}/{x}/{y}.pbf`
- **Cache**: Immutable (max-age=31536000)

---

### 9. **GET** `/raster_tiles/{z}/{x}/{y}.webp`
Get raster tile (bathymetry)
- **URL Parameters**: 
  - `z`, `x`, `y`: Standard tile coordinates
- **Response**: WebP image binary
- **File Location**: `{RASTER_TILES_ROOT}/{z}/{x}/{y}.webp`
- **Cache**: Immutable (max-age=31536000)

---

### 10. **POST** `/extractTimeseries`
Extract time series at a specific coordinate and depth
- **Request Body**: 
  ```json
  {
    "var": "string (variable name)",
    "lat": float (latitude),
    "lon": float (longitude),
    "depth": float (exact depth value)
  }
  ```
- **Response**: 
  ```json
  {
    "time": [numpy datetime array as list],
    "value": [numpy float array as list]
  }
  ```
- **Description**: Extracts values across all available timesteps for the nearest grid point
- **Data Source**: NetCDF files from `/opt/data/nc`
- **Concurrency**: Limited to 4 simultaneous requests (configurable via `MAX_CONCURRENT_EXTRACTS`)

---

### 11. **POST** `/extract_climateTimeseries`
Extract 10-day climatology window (±5 days) with statistical breakdown
- **Request Body**: 
  ```json
  {
    "var": "string (variable name)",
    "lat": float (latitude),
    "lon": float (longitude),
    "depth": "string (e.g., '0p5', '1p0')",
    "dt": "ISO datetime string (or null for current UTC time)"
  }
  ```
- **Response**: 
  ```json
  [
    {
      "requested_date": "2026-01-17T05:30:00",
      "mean": 12.45,
      "median": 12.50,
      "q1": 12.20,
      "q3": 12.75,
      "min": 11.50,
      "max": 13.20
    },
    ...
  ]
  ```
- **Description**: 
  - Extracts a 10-day hourly window (±5 days around requested date)
  - Returns statistics for each hour (mean, median, q1, q3, min, max)
  - Climate statistics are pre-computed from a 5-day climatology for the calendar year 2020
  - Returns 240 records (10 days × 24 hours)
- **Data Source**: `/opt/data/SSC/climatology/5d/{variable}/{variable}_{depth}.nc`
- **Concurrency**: Limited to 4 simultaneous requests

---

### 12. **POST** `/getMonthlyClimatologyAtCoord` ⭐ STATISTICS ENDPOINT
Get monthly climatology statistics AND annual timeseries for a coordinate/depth
- **Request Body**: 
  ```json
  {
    "variable": "string (variable name)",
    "lat": float (latitude),
    "lon": float (longitude),
    "depth": float (depth value)
  }
  ```
- **Response**: 
  ```json
  {
    "timeseries": {
      "by_year": {
        "2007": {
          "time": ["2007-01-01T12:00:00", ...],
          "value": [11.5, 12.3, null, ...]
        },
        "2008": {...},
        ...
      },
      "years": [2007, 2008, 2009, ...]
    },
    "climatology": {
      "month": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
      "virtual_time": ["2020-01-15T00:00:00", "2020-02-15T00:00:00", ...],
      "mean": [11.2, 10.8, 12.5, 14.3, 15.2, 16.1, 17.2, 17.5, 16.3, 13.2, 11.5, 10.3],
      "q1": [10.5, 10.1, 11.8, 13.6, 14.5, 15.4, 16.5, 16.8, 15.6, 12.5, 10.8, 9.6],
      "q3": [11.9, 11.5, 13.2, 15.0, 15.9, 16.8, 17.9, 18.2, 17.0, 14.0, 12.2, 11.0],
      "min": [9.2, 8.9, 10.5, 12.1, 13.0, 13.8, 14.9, 15.2, 14.8, 11.2, 9.5, 8.1],
      "max": [13.5, 13.1, 14.8, 16.8, 17.5, 18.5, 19.8, 20.1, 18.5, 15.2, 13.8, 12.5]
    },
    "nearest_grid_point": {
      "row": 120,
      "col": 45,
      "lat": 49.2134,
      "lon": -123.5678
    }
  }
  ```
- **Description**: 
  - Returns **monthly climatology statistics** (12 months of min/max/mean/q1/q3)
  - Also returns **full annual timeseries by year** extracted from monthly files
  - Merges data from multiple years to calculate monthly statistics
  - Best for understanding typical monthly patterns and historical ranges
- **Data Source**: 
  - Monthly timeseries: `/opt/data/nc/SalishSeaCast/erddap_monthly/{variable}/*.nc`
  - Climatology stats: `/opt/data/nc/SalishSeaCast/monthly_stats/{variable}_monthly_climatology.nc`
- **Concurrency**: Limited to 4 simultaneous requests

---

### 13. **POST** `/getProfile`
Extract vertical profile (multiple depths) at a coordinate and datetime
- **Request Body**: 
  ```json
  {
    "lat": float (latitude),
    "lon": float (longitude),
    "dt": "ISO datetime string",
    "var": "string (optional, defaults to 'temperature')"
  }
  ```
- **Response**: 
  ```json
  {
    "depth": [0.5, 1.0, 2.0, 5.0, 10.0, ...],
    "value": [15.2, 14.8, 14.1, 12.3, 10.5, ...]
  }
  ```
- **Description**: Extracts vertical profile of values at specified datetime
- **Data Source**: NetCDF files
- **Concurrency**: Limited to 4 simultaneous requests

---

### 14. **POST** `/getEval`
Get evaluation data comparing sensor measurements vs model predictions
- **Request Body**: 
  ```json
  {
    "sensor": "string (e.g., 'Baynes_5m')",
    "variable": "string (e.g., 'temperature')",
    "model": "string ('SSC' or 'LiveOcean')"
  }
  ```
- **Response**: 
  ```json
  {
    "time": ["2020-01-01T00:00:00Z", "2020-01-01T01:00:00Z", ...],
    "sensor": [12.5, 12.3, null, 12.4, ...],
    "model": [12.6, 12.2, 12.1, 12.3, ...]
  }
  ```
- **Description**: Compares sensor observations with model outputs
- **Valid Models**: "SSC" (SalishSeaCast) or "LiveOcean"
- **Data Source**: `/opt/data/eval/{sensor}.nc`
- **Concurrency**: Limited to 4 simultaneous requests

---

## Statistics Endpoints Summary

### ⭐ Available Statistics Endpoints:

1. **`/extract_climateTimeseries` (POST)** 
   - **Statistics available**: min, max, mean, median, q1, q3
   - **Time resolution**: Hourly
   - **Time window**: 10-day (±5 days)
   - **Geographic specificity**: Single lat/lon point
   - **Use case**: Short-term variability, 10-day forecasting bounds

2. **`/getMonthlyClimatologyAtCoord` (POST)**
   - **Statistics available**: min, max, mean, q1, q3
   - **Time resolution**: Monthly
   - **Time coverage**: 12 months (climatology) + multi-year timeseries
   - **Geographic specificity**: Single lat/lon point
   - **Use case**: Long-term patterns, seasonal analysis, typical ranges by month

### ❌ No Standalone Stats Endpoints:
- No dedicated `/stats`, `/range`, `/bounds`, `/min`, or `/max` endpoints
- No aggregation by geographic region (e.g., "all points in bounding box")

---

## Data Availability & Limitations

### By Variable Type:

**1. Model Output Variables** (Temperature, Salinity, pH, etc.)
- **Sources**: SalishSeaCast, LiveOcean
- **Available endpoints**: 
  - `/extractTimeseries` - full timeseries
  - `/extract_climateTimeseries` - 10-day climatology window
  - `/getMonthlyClimatologyAtCoord` - monthly climatology
  - `/getProfile` - vertical profiles
- **Spatial coverage**: Full grid
- **Temporal coverage**: Varies by variable (check `/variables` endpoint)

**2. Sensor Measurements**
- **Available endpoints**:
  - `/sensors` - list sensor metadata
  - `/sensorTimeseries` - timeseries at specific sensor location (±5 day window)
- **Spatial coverage**: Limited to installed sensor locations
- **Temporal coverage**: Depends on sensor deployment

**3. Model Evaluation**
- **Available endpoints**:
  - `/getEval` - compare sensor vs model at sensor location
- **Variables**: temperature, salinity, pH, etc. (depends on evaluation file)
- **Models supported**: SalishSeaCast (SSC), LiveOcean

---

## Concurrency & Performance

- **Semaphore limit**: 4 concurrent extraction requests
- **Queue timeout**: 10 seconds (returns 429 if over capacity)
- **Retry strategy**: Client should implement exponential backoff
- **Error response**: `{"detail": "Too many concurrent extract requests, try again later"}`

---

## Data Directories

| Purpose | Path | Env Variable |
|---------|------|--------------|
| PNG rasters | `/opt/data/png` | `PNG_ROOT` |
| Vector tiles (bathymetry) | `/opt/data/bathymetry/NONNA/tiles` | `VECTOR_ROOT` |
| Raster tiles (bathymetry) | `/opt/data/bathymetry/NONNA/raster_tiles` | `RASTER_TILES_ROOT` |
| NetCDF files | `/opt/data/nc` | (hardcoded) |
| SalishSeaCast data | `/opt/data/nc/SalishSeaCast` | (hardcoded) |
| Climatology files | `/opt/data/SSC/climatology/5d` | (hardcoded) |
| Evaluation files | `/opt/data/eval` | (hardcoded) |
| Monthly files | `/opt/data/nc/SalishSeaCast/erddap_monthly` | (hardcoded) |

---

## Database Connection

All database-backed endpoints connect to a PostGIS database with the following tables:
- `grid` - Grid cell indexing (required for lat/lon → grid lookup)
- `sensors` - Sensor metadata and data
- `fields` - Variable metadata
- `datasets` - Dataset information
- `nc_jobs` - Job tracking for PNG generation
- `colormaps` - Colormap definitions

**Connection string**: Configured via environment variables:
- `DB_HOST` (default: "db")
- `DB_PORT` (default: 5432)
- `DB_USER` (default: "postgres")
- `DB_PASSWORD` (default: "postgres")
- `DB_NAME` (default: "oa")

---

## Error Handling

### Common HTTP Status Codes:

| Status | Meaning | Example |
|--------|---------|---------|
| 200 | Success | Data returned |
| 400 | Bad request | Invalid parameter values |
| 404 | Not found | Variable dts don't exist, PNG not available |
| 429 | Too many requests | Max concurrent extractions exceeded |
| 500 | Server error | DB connection failed, NetCDF file error |

---

## Related Modules

| Module | Purpose |
|--------|---------|
| `extractTimeseries.py` | Core timeseries extraction logic |
| `extractProfile.py` | Vertical profile extraction |
| `extract_climate_timeseries.py` | 10-day climatology extraction |
| `modules/monthly_climatology.py` | Monthly climatology statistics |
| `modules/variables.py` | Variable metadata query |
| `modules/eval_extractor.py` | Evaluation data extraction |
| `nc_reader.py` | Thread-safe NetCDF reader with LRU cache |

---

## Recommendations for Statistics Queries

### For Short-term Variability (Next 10 days):
→ Use **`/extract_climateTimeseries`**
- Get hourly min/max bounds for each hour in 10-day window
- Use to show typical range for day-ahead/week-ahead forecasting

### For Long-term Patterns (Seasonal/Annual):
→ Use **`/getMonthlyClimatologyAtCoord`**
- Get monthly min/max statistics across all historical years
- Also get full multi-year timeseries for custom analysis
- Use to show typical ranges for each calendar month

### For Comparative Analysis (Sensor vs Model):
→ Use **`/getEval`**
- Compare sensor observations against model predictions
- Useful for model validation and bias analysis

### For Full Timeseries:
→ Use **`/extractTimeseries`**
- Get complete historical record
- Compute custom statistics as needed on client side
