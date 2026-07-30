FROM python:3.13.13-slim

WORKDIR /app

# compute.py parallelizes PyCO2SYS across time-steps via ProcessPoolExecutor
# (one process per step) — without this, each of those processes' own NumPy
# BLAS backend ALSO tries to use every core, so worker count and BLAS thread
# count multiply instead of adding, and more --workers can get slower, not
# faster, once that compounded oversubscription saturates memory bandwidth.
ENV OMP_NUM_THREADS=1 \
	OPENBLAS_NUM_THREADS=1 \
	MKL_NUM_THREADS=1 \
	NUMEXPR_NUM_THREADS=1

# System packages needed by the sync stage (rsync over SSH, tunneled through
# Cloudflare Access via an ssh ProxyCommand using `cloudflared access ssh`)
RUN apt-get update \
	&& apt-get install -y --no-install-recommends rsync openssh-client curl \
	&& rm -rf /var/lib/apt/lists/* \
	&& arch="$(dpkg --print-architecture)" \
	&& curl -fL -o /usr/local/bin/cloudflared "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${arch}" \
	&& chmod +x /usr/local/bin/cloudflared \
	&& cloudflared --version

# docker-compose passes user: "${UID:-1000}:${GID:-1000}" so file ownership
# matches host-mounted secrets/data, but the base image has no passwd entry
# for that UID — ssh calls getpwuid() unconditionally and refuses to run
# without one ("No user exists for uid 1000"). Bake in a matching entry.
RUN groupadd -g 1000 appuser \
	&& useradd -u 1000 -g 1000 -m -s /bin/bash appuser

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