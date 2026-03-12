FROM python:3.11-slim

WORKDIR /app

# Install build dependencies and HIP runtime
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    ca-certificates \
    libhdf5-dev \
    libnetcdf-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Install scientific packages
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

RUN pip install --no-cache-dir \
    numpy \
    scipy \
    numba \
    xarray \
    netCDF4 \
    PyCO2SYS

# ROCm environment - relies on host ROCm libraries via device mounts
ENV ROCM_HOME=/opt/rocm
ENV PATH=${ROCM_HOME}/bin:${PATH}
ENV LD_LIBRARY_PATH=${ROCM_HOME}/lib:${LD_LIBRARY_PATH}
ENV HIP_VISIBLE_DEVICES=0

# Copy application code
COPY . .

# Entrypoint
ENTRYPOINT ["python"]
CMD ["calc_carbon_gpu.py"]
