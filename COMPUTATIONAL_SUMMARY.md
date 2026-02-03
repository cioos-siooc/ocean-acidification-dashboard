# Computational Task Summary

This document summarizes the core computational tasks within the OA service, categorized by their execution pattern and resource requirements.

---

## 1. On-Demand Computations (API Service)
These tasks are triggered by user interactions in the frontend and must return results in real-time.

### Time Series Extraction (`extractTimeseries.py`)
*   **Description**: Pulls historical data for a specific pixel (Lat/Lon) from multiple NetCDF files.
*   **Nature**: 1D interpolation across the time dimension.
*   **Primary Bottleneck**: Disk I/O (random reads from large NC files).

### Vertical Profile Extraction (`extractProfile.py`)
*   **Description**: Generates a surface-to-seabed profile for a specific location.
*   **Nature**: 1D vertical interpolation.
*   **Primary Bottleneck**: Memory/CPU (managing vertical coordinate slices).

### Sensor Data Normalization (`extractSensorTimeseries.py`)
*   **Description**: Fetches external sensor data and aligns it with model timestamps.
*   **Nature**: Time-decoding and format normalization (JSON/NC).
*   **Primary Bottleneck**: Network/API latency (external fetches).

---

## 2. Pipeline Batch Processing (Process Service)
Heavy-duty background tasks that transform raw data into optimized formats for the frontend.

### Derived Variable Calculation (`calc_carbon_grid.py`)
*   **Description**: Computes derived Biogeochemical variables (pH, Omega_Aragonite, DIC) from Temperature and Salinity.
*   **Nature**: High-density floating-point mathematics for every grid point.
*   **Optimization**: Uses **Shared Memory Memmaps** to prevent RAM duplication across multiple CPU cores.
*   **Complexity**: $O(T \times Y \times X)$ where $T$ is time steps.

### Vector Tiling (`nc2tile.py`)
*   **Description**: Converts NetCDF grids into Mapbox-compatible vector tiles.
*   **Nature**: Geometry generation and spatial bucketing.
*   **Primary Bottleneck**: CPU (coordinate transformations).

### Climatology Statistics (`statistics2.py`)
*   **Description**: Calculates multi-year Mean, Median, and Quantiles (Q1, Q3) for every cell.
*   **Nature**: Sorting and grouping across the time axis.
*   **Constraint**: Extremely Memory Intensive (requires $O(N \log N)$ sorting per pixel).
*   **Optimization**: **Spatio-temporal Batching** (processing month-by-month and row-by-row).

### Temporal Smoothing (`smooth_stats.py`)
*   **Description**: Applies a centered rolling mean to filtered statistics.
*   **Nature**: Moving window convolution.
*   **Boundary Handling**: **Circular Wrapping** (Dec-Jan bridge) to ensure a seamless periodic climatology.

---

## 3. Imaging & Visualization
Tasks focused on generating static or interactive visual representations.

### PNG Imaging Pipeline (`png_worker.py`)
*   **Description**: Generates high-resolution diagnostic plots and map overlays.
*   **Nature**: Rasterization and colormap application.
*   **Resource**: CPU/GPU (depending on backend).

---

## Computational Resource Summary

| Task | Pattern | Primary Resource | Scaling Method |
| :--- | :--- | :--- | :--- |
| **Derived Calc** | Batch | CPU (High) | Shared Memory |
| **Statistics** | Batch | RAM (High) | Spatial Batching |
| **Tiling** | Batch | CPU (Medium) | Multiprocessing |
| **Smoothing** | Batch | Disk I/O | Xarray/Dask |
| **API Extractions** | On-Demand | Disk I/O | Metadata Caching |
| **Sensor Sync** | On-Demand | Network | Asynchronous I/O |
