FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY sensors/pyproject.toml ./
RUN uv sync --no-dev

COPY sensors/*.py ./
