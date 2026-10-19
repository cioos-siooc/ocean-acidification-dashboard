"""Configuration constants for the LiveOcean pipeline.

Source: UW LiveOcean (ROMS cas7_t2_x11b) daily forecast runs, published as
one `layers.nc` per run at
    {LO_BASE_URL}/f{YYYY.MM.DD}/layers.nc
Each run starts at 00Z on its date and covers 72 h at 4-hourly steps (19
steps: day0 00Z ... day3 00Z), sliced at fixed z-levels. The run is built
around 05:30 UTC.

Unlike SalishSeaCast (an append-only hindcast), every run overlaps the
previous runs' forecast hours. The pipeline keeps one value per valid time —
the newest run's — see db.py for the latest-wins rule.

ClickHouse connection: same CH_* env convention as SSC/config.py.
"""
from __future__ import annotations

import os
from datetime import date

from shared.variable_config import get_source, load_variable_config

SOURCE = 'LiveOcean'
_SRC = get_source(SOURCE)

GRID_TABLE  = _SRC['grid_table']    # grid_LO
DATA_TABLE  = _SRC['data_table']    # LiveOcean_4h
DAILY_TABLE = _SRC['daily_table']   # LiveOcean_daily
STATUS_TABLE = 'LiveOcean_status'

LO_BASE_URL = os.getenv('LO_BASE_URL', 'https://s3.kopah.uw.edu/liveocean-share')
# Optional local mirror laid out like the archive (fYYYY.MM.DD/layers.nc).
# A run found there is read from disk; the rest still come from LO_BASE_URL.
LO_ARCHIVE_DIR = os.getenv('LO_ARCHIVE_DIR', '')

# Earliest run worth asking for: f2025.07.13/14 exist but aren't readable
# netCDF, f2025.07.15 is the first good one.
ARCHIVE_START = date.fromisoformat(os.getenv('LO_ARCHIVE_START', '2025-07-15'))

STEP_HOURS     = int(_SRC['step_hours'])     # 4
STEPS_PER_DAY  = 24 // STEP_HOURS            # 6
STEPS_PER_RUN  = 3 * STEPS_PER_DAY + 1       # 19: day0 00Z .. day3 00Z

# layers.nc variable stem -> canonical variable id (shared/variable_config.yml)
NATIVE_VARIABLES: dict[str, str] = {
    'temp':          'temperature',
    'salt':          'salinity',
    'oxygen':        'dissolved_oxygen',
    'PH':            'ph_total',
    'ARAG':          'omega_arag',
    'NO3':           'nitrate',
    'phytoplankton': 'phytoplankton',
}

# Derived in compute.py from pH + Ω aragonite (+ T/S/pressure) via PyCO2SYS.
COMPUTE_VARIABLES: list[str] = [
    'omega_cal',
    'total_alkalinity',
    'dissolved_inorganic_carbon',
]

ALL_VARIABLES: list[str] = list(_SRC['variables'])
assert set(ALL_VARIABLES) == set(NATIVE_VARIABLES.values()) | set(COMPUTE_VARIABLES), \
    'variable_config.yml LiveOcean.variables out of step with LO/config.py'

# LO_VARIABLES (comma-separated, default all) limits what the pipeline
# downloads, renders and ingests — e.g. `temperature` for a quick backfill.
# Unprocessed variables stay empty in ClickHouse (their columns default to
# NaN); re-running a run later with more variables replaces its days whole.
VARIABLES: list[str] = [v.strip() for v in os.getenv('LO_VARIABLES', '').split(',') if v.strip()] or ALL_VARIABLES
assert set(VARIABLES) <= set(ALL_VARIABLES), f'LO_VARIABLES: unknown variable(s) {set(VARIABLES) - set(ALL_VARIABLES)}'

# Inputs each output needs from layers.nc. Temperature and salinity are always
# read: TEOS-10 needs both, and a row exists where temperature is valid.
_CARBONATE_INPUTS = {'temperature', 'salinity', 'ph_total', 'omega_arag'}
NEEDS_CARBONATE = bool(set(VARIABLES) & set(COMPUTE_VARIABLES))
READ_VARIABLES: list[str] = [
    v for v in NATIVE_VARIABLES.values()
    if v in VARIABLES or v in ('temperature', 'salinity') or (NEEDS_CARBONATE and v in _CARBONATE_INPUTS)
]

# layers.nc level suffix -> stored depth (m). "surface" is 0 and "bottom" the
# -1 sentinel the frontend already knows (stores/main.ts formatDepthLabel).
# Runs before 2025-12-29 only carry surface/10/20/30/50/bottom; whatever a
# file has is what gets ingested.
LEVELS: dict[str, float] = {
    'surface': 0.0, '10': 10.0, '20': 20.0, '30': 30.0, '50': 50.0,
    '100': 100.0, '500': 500.0, '1000': 1000.0, '1500': 1500.0,
    '2000': 2000.0, '2500': 2500.0, 'bottom': -1.0,
}
BOTTOM_DEPTH = -1.0

# LO_IMAGE_DEPTHS (comma-separated level names from LEVELS, default all)
# limits which levels get pre-rendered tiles, e.g. `surface` for a backfill.
# Tiles for the other levels are rendered on demand by GET /png from
# ClickHouse (api/modules/extract_image.py) and cached there.
IMAGE_DEPTHS: list[float] = [
    LEVELS[name.strip()] for name in os.getenv('LO_IMAGE_DEPTHS', '').split(',') if name.strip()
] or list(LEVELS.values())

# Approximate seawater density for TA/DIC unit conversion (µmol/kg -> mmol/m³),
# same constant SSC/compute.py uses in the other direction.
SEAWATER_DENSITY = 1025.0

VARIABLE_PRECISION: dict[str, float] = {
    var: load_variable_config()['variables'][var]['precision'] for var in ALL_VARIABLES
}

# Local working files: one compact NetCDF per run (canonical names, float32,
# (time, depth, eta, xi)), deleted once the run is synced/ingested.
NC_BASE_DIR    = os.getenv('LO_NC_DIR', '/opt/data/LiveOcean/nc')
IMAGE_BASE_DIR = os.getenv('LO_IMAGE_DIR', '/opt/data/LiveOcean/images')
SYNC_STAGING_DIR = os.getenv('LO_SYNC_STAGING_DIR', '/opt/data/LiveOcean/sync_staging')

MAX_RETRY_ATTEMPTS = int(os.getenv('LO_MAX_RETRY_ATTEMPTS', '3'))

# Runs ingested in parallel by a sweep (see ingest.ingest_runs), capped by
# --workers. Each worker holds about a day of one run in memory (~1-2 GB with
# all variables).
INGEST_WORKERS = int(os.getenv('LO_INGEST_WORKERS', '6'))

# ---------------------------------------------------------------------------
# Status — one row per run_date in LiveOcean_status (the run file is the unit)
#   pending_download -> downloading -> success_download
#   -> computing -> success_compute -> imaging -> success_image
#   -> ingesting -> success_ingest -> syncing -> success_sync
# with failed_* for each active step. Each step consumes the previous step's
# success_* rows, so there is no separate promote stage.
# ---------------------------------------------------------------------------
STATUS_PENDING_DOWNLOAD = 'pending_download'
STATUS_DOWNLOADING      = 'downloading'
STATUS_SUCCESS_DOWNLOAD = 'success_download'
STATUS_FAILED_DOWNLOAD  = 'failed_download'
STATUS_COMPUTING        = 'computing'
STATUS_SUCCESS_COMPUTE  = 'success_compute'
STATUS_FAILED_COMPUTE   = 'failed_compute'
STATUS_IMAGING          = 'imaging'
STATUS_SUCCESS_IMAGE    = 'success_image'
STATUS_FAILED_IMAGE     = 'failed_image'
STATUS_INGESTING        = 'ingesting'
STATUS_SUCCESS_INGEST   = 'success_ingest'
STATUS_FAILED_INGEST    = 'failed_ingest'
STATUS_SYNCING          = 'syncing'
STATUS_SUCCESS_SYNC     = 'success_sync'
STATUS_FAILED_SYNC      = 'failed_sync'
# A run all of whose valid days are already owned by newer runs.
STATUS_SUPERSEDED       = 'superseded'

# (input status that makes a row eligible, failed status retried under the cap)
STAGE_INPUTS: dict[str, tuple[str, str]] = {
    'download': (STATUS_PENDING_DOWNLOAD, STATUS_FAILED_DOWNLOAD),
    'compute':  (STATUS_SUCCESS_DOWNLOAD, STATUS_FAILED_COMPUTE),
    'image':    (STATUS_SUCCESS_COMPUTE,  STATUS_FAILED_IMAGE),
    'ingest':   (STATUS_SUCCESS_IMAGE,    STATUS_FAILED_INGEST),
    'sync':     (STATUS_SUCCESS_INGEST,   STATUS_FAILED_SYNC),
}

# ClickHouse connection settings
CH_HOST       = os.getenv('CH_HOST', 'localhost')
CH_PORT       = int(os.getenv('CH_PORT', '8123'))
CH_USER       = os.getenv('CH_USER', 'default')
CH_PASSWORD   = os.getenv('CH_PASSWORD', os.getenv('CLICKHOUSE_PASSWORD', ''))
CH_USE_REMOTE = os.getenv('CH_USE_REMOTE', '').strip().lower() in ('1', 'true', 'yes')
CH_REMOTE_URL = os.getenv('CH_REMOTE_URL', '')

# Sync: same API machine, SSH and token settings as SSC (see SSC/config.py) —
# both pipelines push to the one API.
from SSC.config import (  # noqa: E402
    API_SSH_HOST, API_SSH_KEY_PATH, API_SSH_KNOWN_HOSTS, API_SSH_USER,
    CF_TOKEN_ID, CF_TOKEN_SECRET, SYNC_API_BASE_URL, SYNC_API_DATA_DIR, SYNC_API_TOKEN,
)
