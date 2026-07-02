FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

# Point uv cache and venv to /tmp so any UID can write there
# (the container runs as the host user who has no home dir in the image)
ENV UV_CACHE_DIR=/app/.cache/uv \
    UV_NO_SYNC=1

# Create a user matching the host UID/GID passed via docker-compose
# user: "${UID:-1000}:${GID:-1000}" — without a passwd entry uv fails
RUN groupadd -g 1000 appuser \
    && useradd -u 1000 -g 1000 -m -s /bin/bash appuser

COPY sensors/pyproject.toml ./
RUN uv sync --no-dev

COPY sensors/*.py ./

# Allow any UID (e.g. host UID passed via docker-compose user:) to run uv
RUN chmod -R a+rwX /app
