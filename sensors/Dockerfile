FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

# Create a user matching the host UID/GID passed via docker-compose
# user: "${UID:-1000}:${GID:-1000}" — without a passwd entry uv fails
RUN groupadd -g 1000 appuser \
    && useradd -u 1000 -g 1000 -m -s /bin/bash appuser

COPY sensors/pyproject.toml ./
RUN uv sync --no-dev

COPY sensors/*.py ./

# Allow any UID (e.g. host UID passed via docker-compose user:) to run uv
RUN chmod -R a+rwX /app
