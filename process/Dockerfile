FROM python:3.13-slim

WORKDIR /app

# Install uv
RUN pip install --no-cache-dir uv

# Copy pyproject.toml and uv.lock for dependency installation
COPY process/pyproject.toml process/uv.lock ./

# Install dependencies using uv
# RUN uv sync --frozen --no-dev
RUN uv sync --no-dev

# Copy source code
COPY process . 
COPY shared ./shared

# Allow any user (e.g. host UID passed via docker-compose user:) to run uv and write to /app
RUN chmod -R a+rwX /app

# Entrypoint to allow arguments to be passed to MAIN.py
# ENTRYPOINT ["python", "-Xfrozen_modules=off", "-m", "uv", "run", "MAIN.py"]