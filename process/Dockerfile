FROM python:3.13-slim

WORKDIR /app

# Install uv
RUN pip install --no-cache-dir uv

# Copy pyproject.toml and uv.lock for dependency installation
COPY pyproject.toml uv.lock ./

# Install dependencies using uv
# RUN uv sync --frozen --no-dev
RUN uv sync --no-dev

# Copy source code
COPY . .

# Entrypoint to allow arguments to be passed to MAIN.py
# ENTRYPOINT ["python", "-Xfrozen_modules=off", "-m", "uv", "run", "dl.py"]