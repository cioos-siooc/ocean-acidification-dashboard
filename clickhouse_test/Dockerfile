FROM clickhouse/clickhouse-server:latest

# Prevent interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Install Python 3, pip, and virtual environment tools
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install the official ClickHouse Python connector
# (Adding pandas as it's highly recommended for data work with ClickHouse)
RUN pip3 install --no-cache-dir \
    clickhouse-connect \
    pandas \
    xarray \
    numpy \
    netcdf4 \
    dask \
    h5netcdf \
    h5py

# COPY main.py /app/main.py
# COPY temperature_20260520.nc /app/temperature_20260520.nc

WORKDIR /app

# COPY clickhouse-entrypoint.sh /usr/local/bin/clickhouse-entrypoint.sh
# RUN chmod +x /usr/local/bin/clickhouse-entrypoint.sh

# ClickHouse default ports:
# 8123 (HTTP interface/BI tools)
# 9000 (Native client interface)
EXPOSE 8123 9000

ENTRYPOINT ["bash", "./clickhouse-entrypoint.sh"]
# The base image's default entrypoint will automatically start the ClickHouse server
# CMD ["python3", "/app/main.py"]