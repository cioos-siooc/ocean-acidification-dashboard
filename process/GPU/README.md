# GPU-Accelerated Carbonate Chemistry Computation

Experimental GPU-accelerated version of the carbonate chemistry computation using **Numba** + **ROCm** for AMD Radeon GPUs.

## Prerequisites

- **AMD GPU**: Radeon RX 7000 series or newer (tested on RX 9070 XT)
- **ROCm**: Installed on host system (`rocm-smi` must show GPU detected)
- **Docker**: With GPU support enabled
- **docker-compose**: For container orchestration

## Setup

### 1. Install Docker GPU Support

For AMD/ROCm Docker support:

```bash
# Install Docker with GPU support
sudo apt-get install -y gpu-manager
sudo usermod -aG render $(whoami)
sudo usermod -aG video $(whoami)
```

### 2. Build GPU Image

From the project root:

```bash
docker compose -f process/GPU/docker-compose.gpu.yml build compute-gpu
```

### 3. Test GPU Detection in Container

```bash
docker compose -f process/GPU/docker-compose.gpu.yml run --rm compute-gpu rocm-smi
```

You should see your GPU listed.

## Usage

### Option 1: Docker Compose

```bash
cd /path/to/OA
docker compose -f process/GPU/docker-compose.gpu.yml run --rm compute-gpu \
  python3 calc_carbon_gpu.py --date 20260105 --base-dir /opt/data/nc
```

### Option 2: Direct Docker Run

```bash
docker run --rm \
  --device /dev/kfd \
  --device /dev/dri \
  --cap-add SYS_PTRACE \
  -v $(pwd)/data/nc:/opt/data/nc:rw \
  -e HIP_VISIBLE_DEVICES=0 \
  oa-compute-gpu:latest \
  python3 calc_carbon_gpu.py --date 20260105 --base-dir /opt/data/nc
```

### Option 3: Interactive Shell

```bash
docker compose -f process/GPU/docker-compose.gpu.yml run --rm compute-gpu bash
# Inside container:
python3 calc_carbon_gpu.py --date 20260105
```

## Parameters

```bash
python3 calc_carbon_gpu.py --help

Options:
  --date TEXT              Date token in YYYYMMDD format (required) [default: ]
  --base-dir TEXT          Base directory for input NetCDF files [default: /opt/data/nc]
  --output-dir TEXT        Output directory for results [default: /opt/data/nc]
  --workers INTEGER        Number of workers (unused in GPU mode) [default: 1]
  --depth-batch-size INTEGER
                           Depth batch size (unused in GPU mode) [default: 32]
```

## Script Details

### `calc_carbon_gpu.py`

GPU-only computation of carbonate chemistry (ph_total, omega_arag, omega_cal) using:

- **Numba JIT compilation** with ROCm backend for element-wise computations
- **Vectorized operations** on GPU arrays
- **Parallel depth/lat/lon loops** executed on GPU cores
- **No CPU fallback** - fails if GPU unavailable

Input files required (by date token):
- `dissolved_inorganic_carbon/DIC_YYYYMMDD.nc`
- `total_alkalinity/TA_YYYYMMDD.nc`
- `temperature/Temp_YYYYMMDD.nc`
- `salinity/Sal_YYYYMMDD.nc`

Output files created:
- `ph_total_YYYYMMDD.nc`
- `omega_arag_YYYYMMDD.nc`
- `omega_cal_YYYYMMDD.nc`

### `Dockerfile`

ROCm-based image with:
- ROCm terminal as base (includes HIP, ROCm libraries)
- Python 3.11
- Numba (for JIT compilation)
- NumPy, SciPy, xarray (array operations)
- netCDF4, PyCO2SYS (data I/O and chemistry)

## Troubleshooting

### GPU Not Detected in Container

```bash
# Host check
rocm-smi

# Container check
docker compose -f process/GPU/docker-compose.gpu.yml run --rm compute-gpu rocm-smi

# If not visible, verify:
ls -l /dev/kfd /dev/dri
```

### Permission Issues

```bash
sudo usermod -aG render $(whoami)
sudo usermod -aG video $(whoami)
newgrp render
```

### Container Fails to Start

Check ROCm installation:

```bash
cat /opt/rocm/include/rocm_version.h | grep ROCM_VERSION
```

## Performance Notes

- **Expected GPU speedup**: 10-30x over CPU version for carbonate chemistry loops
- **Memory advantage**: GPU VRAM (typically 24-48 GB) allows larger batch processing
- **No multiprocessing overhead**: Single GPU thread, no inter-process communication delays

## Notes

- This is an **experimental GPU-only** implementation
- No error handling or fallback to CPU - **GPU-only**
- PyCO2SYS integration currently uses placeholder kernel; full integration requires additional work
- Designed for rapid prototyping and performance testing

## Future Improvements

1. Full PyCO2SYS vectorization on GPU
2. Batched time-slice processing for large datasets
3. Multi-GPU support
4. Benchmark suite comparing GPU vs CPU performance

## References

- [Numba ROCm Backend](https://numba.readthedocs.io/en/stable/rocm/index.html)
- [ROCm Documentation](https://rocmdocs.amd.com/)
- [Docker ROCm Support](https://hub.docker.com/r/rocm/rocm-terminal)
