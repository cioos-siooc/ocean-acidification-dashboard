# Computational Resource Analysis for OA Service

This report details the computational requirements of the OA Service pipeline, intended for the assessment of hardware and infrastructure needs.

---

## 1. Biogeochemical (BGC) Derived Variable Calculation
**Task:** `calc_carbon_grid.py` / `calc_carbon_grid_shm_memmap.py`

*   **Function**: Transforms raw physical model outputs (Temperature, Salinity) into critical biogeochemical indicators (pH, Aragonite Saturation/Omega, Dissolved Inorganic Carbon/Total Alkalinity). It solves complex chemical equilibrium equations for every grid point in the model.
*   **Computational Profile**:
    *   **CPU**: **High Intensity**. Requires massive floating-point arithmetic. The derivation involves root-finding algorithms (e.g., iterative Newton-Raphson) to solve for pH at every 3D point.
    *   **RAM**: **Moderate**. We use `shm_memmap` (Shared Memory Memory-Map) to share the massive input temperature/salinity arrays across multiple CPU cores, preventing linear RAM growth during parallel processing.
*   **Scaling**: Scales linearly with the number of hours ($T$) and grid volume ($X \times Y \times Z$).

## 2. Climatology & Advanced Statistics
**Task**: `statistics2.py`

*   **Function**: Computes the "Climatological Normal" (typical state) by aggregating 10+ years of hourly data. For every single pixel, it calculates the Mean, Median, and specific Quantiles (Q1, Q3) to establish standard variation ranges.
*   **Computational Profile**:
    *   **DRAM (Memory)**: **Critical Intensity**. Calculating medians and quantiles requires loading and sorting the entire time-series for a location ($O(N \log N)$ complexity). Without batching, this requires hundreds of GB of RAM.
    *   **I/O**: **High Intensity**. Requires reading thousands of source NetCDF files to construct the time-series.
*   **Optimization**: Currently mitigated using **Spatio-temporal Batching** (processing data in 100-row strips for one month at a time) to cap RAM usage.

## 3. High-Fidelity Circular Smoothing
**Task**: `smooth_stats.py`

*   **Function**: Applies a rolling mean (convolution) to the calculated statistics to remove high-frequency noise. Crucially, it must handle **Circular/Periodic Padding** to ensure the transition from December 31st to January 1st is scientifically seamless.
*   **Computational Profile**:
    *   **I/O & CPU**: **Moderate/High**. Uses large-window convolutions. When using high-level libraries (Xarray/Dask), this creates complex task graphs that can consume significant memory during the "write" phase.
    *   **Complexity**: Performance is bound by the size of the smoothing window and the spatial resolution of the grid.

## 4. Map Tiling & Spatial Indexing
**Task**: `nc2tile.py`

*   **Function**: Converts raw NetCDF data grids into Mapbox Vector Tiles (MVT). This involves re-projecting geographic coordinates into Web Mercator space and "bucketing" data points into specific zoom-level tiles.
*   **Computational Profile**:
    *   **CPU**: **Moderate**. Tasks are highly parallelizable (embarrassingly parallel). Coordinate transformations (lat/lon to tile pixels) are CPU-bound.
    *   **Storage I/O**: **High**. Generates thousands of small tile files (or database entries), requiring fast write speeds and efficient filesystem indexing.

## 5. Dynamic Data Extraction (Real-Time)
**Task**: `API (extractTimeseries / extractProfile)`

*   **Function**: Used by the end-user in the browser. When a user clicks a point on the map, the system must instantly scan multiple NetCDF files to extract and return a localized time-series or depth profile.
*   **Computational Profile**:
    *   **Latency/IO**: **Critical**. Requires extremely fast random access to high-volume NetCDF files.
    *   **CPU**: **Low**. Involves simple linear interpolation of values.

---

## Summary Resource Recommendations

| System Component | CPU Priority | DRAM Priority | Storage I/O | Optimization Goal |
| :--- | :--- | :--- | :--- | :--- |
| **BGC Calculations** | Ultra-High | Moderate | Medium | Throughput |
| **Stats & Climatology** | Low | Ultra-High | High | Memory Stability |
| **Tiling Pipeline** | High | Low | High | Parallel Speed |
| **User API** | Low | Low | Ultra-High | Low Latency |

**Conclusion**: The OA Service is primarily **Memory-bound** during statistical analysis and **CPU-bound** during biogeochemical modelling. For optimal operation, the environment requires high-bandwidth I/O (NVMe storage) to mitigate the massive data read requirements of climatology tasks.
