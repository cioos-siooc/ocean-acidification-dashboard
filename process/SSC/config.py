"""Configuration constants for the SalishSeaCast pipeline.

ERDDAP source:  https://salishsea.eos.ubc.ca/erddap  (overrideable via ERDDAP_BASE)

Two griddap datasets are used.  Their IDs are hardcoded here because they are
stable, versioned model identifiers that belong in source control.  Override via
environment variables only if you need to point at a different model version:
  ERDDAP_DATASET_CHEMISTRY  (default: ubcSSg3DChemistryFields1hV21-11)
  ERDDAP_DATASET_PHYSICS    (default: ubcSSg3DPhysicsFields1hV21-11)

ClickHouse connection is configured via:
  CH_HOST, CH_PORT, CH_USER, CH_PASSWORD (or CLICKHOUSE_PASSWORD)
  CH_USE_REMOTE=true + CH_REMOTE_URL for a remote instance.
"""
from __future__ import annotations

import os

# Variables downloaded directly from ERDDAP
DOWNLOAD_VARIABLES: list[str] = [
    'temperature',
    'salinity',
    'total_alkalinity',
    'dissolved_oxygen',
    'dissolved_inorganic_carbon',
]

# Variables derived via PyCO2SYS (all three are computed together in one pass)
COMPUTE_VARIABLES: list[str] = [
    'ph_total',
    'omega_arag',
    'omega_cal',
]

# Inputs required by PyCO2SYS (subset of DOWNLOAD_VARIABLES)
PYCO2SYS_INPUTS: list[str] = [
    'total_alkalinity',
    'dissolved_inorganic_carbon',
    'temperature',
    'salinity',
]

ALL_VARIABLES: list[str] = DOWNLOAD_VARIABLES + COMPUTE_VARIABLES

# ERDDAP griddap dataset IDs.  Each variable is downloaded individually
# (ERDDAP rejects multi-variable requests), but both variables within a
# dataset share the same base URL.
ERDDAP_BASE = os.getenv('ERDDAP_BASE', 'https://salishsea.eos.ubc.ca/erddap')

ERDDAP_DATASET_IDS: dict[str, str] = {
    'chemistry': os.getenv('ERDDAP_DATASET_CHEMISTRY', 'ubcSSg3DChemistryFields1hV21-11'),
    'physics':   os.getenv('ERDDAP_DATASET_PHYSICS',   'ubcSSg3DPhysicsFields1hV21-11'),
}

# Maps each download variable to its dataset key above
VARIABLE_DATASET: dict[str, str] = {
    'total_alkalinity':           'chemistry',
    'dissolved_inorganic_carbon': 'chemistry',
    'dissolved_oxygen':           'chemistry',
    'temperature':                'physics',
    'salinity':                   'physics',
}


def erddap_base_url(variable: str) -> str:
    """Return the full griddap base URL for a download variable."""
    dataset_key = VARIABLE_DATASET[variable]
    dataset_id  = ERDDAP_DATASET_IDS[dataset_key]
    return f'{ERDDAP_BASE}/griddap/{dataset_id}'


# Root directory for NetCDF file storage: {NC_BASE_DIR}/{variable}/{variable}_{YYYYMMDD}.nc
NC_BASE_DIR = os.getenv('SSC_NC_DIR', '/opt/data/SSC/nc')

# Root directory for rendered WebP tiles, read by the API via the same env var.
IMAGE_BASE_DIR = os.getenv('SSC_IMAGE_DIR', '/opt/data/SSC/images')

# Approximate seawater density used for TA/DIC unit conversion (mmol/m³ → µmol/kg)
SEAWATER_DENSITY = 1025.0  # kg/m³

# Depth-to-pressure conversion factor (1 m ≈ 1.019716 dbar)
DEPTH_TO_PRESSURE_FACTOR = 1.019716

# Packing precision for WebP tile generation (controls quantisation in nc2tile)
VARIABLE_PRECISION: dict[str, float] = {
    'temperature':               0.1,
    'salinity':                  0.01,
    'total_alkalinity':          1.0,
    'dissolved_oxygen':          0.1,
    'dissolved_inorganic_carbon': 1.0,
    'ph_total':                  0.01,
    'omega_arag':                0.01,
    'omega_cal':                 0.01,
}

# ---------------------------------------------------------------------------
# Pipeline status values — one row per (date, variable) in SalishSeaCast_status
# ---------------------------------------------------------------------------
# Download variables follow:
#   pending_download → downloading → success_download
#   → pending_image  → imaging     → success_image
#   → pending_ingest → ingesting   → success_ingest
#
# Compute variables follow (skipping the download stage):
#   pending_compute  → computing   → success_compute
#   → pending_image  → imaging     → success_image
#   → pending_ingest → ingesting   → success_ingest
#
# failed_* variants exist for each active (non-pending) step.
# Promotion rules:
#   check_image  (success_download × 5 + success_compute × 3) → pending_image : all 8 together
#   promote      success_image (all 8) → pending_ingest
#   promote      success_ingest (all 8) → pending_sync
#   pending_compute trigger            : all 5 download vars must be past download stage
#
# The sync stage (this process pipeline may run on a remote server) exports the
# date's rows to a Native-format file, rsyncs it plus the WebP images to the
# home server, then calls the home API to import them. See sync.py.

STATUS_PENDING_DOWNLOAD  = 'pending_download'
STATUS_DOWNLOADING       = 'downloading'
STATUS_SUCCESS_DOWNLOAD  = 'success_download'
STATUS_FAILED_DOWNLOAD   = 'failed_download'

STATUS_PENDING_COMPUTE   = 'pending_compute'
STATUS_COMPUTING         = 'computing'
STATUS_SUCCESS_COMPUTE   = 'success_compute'
STATUS_FAILED_COMPUTE    = 'failed_compute'

STATUS_PENDING_IMAGE     = 'pending_image'
STATUS_IMAGING           = 'imaging'
STATUS_SUCCESS_IMAGE     = 'success_image'
STATUS_FAILED_IMAGE      = 'failed_image'

STATUS_PENDING_INGEST    = 'pending_ingest'
STATUS_INGESTING         = 'ingesting'
STATUS_SUCCESS_INGEST    = 'success_ingest'
STATUS_FAILED_INGEST     = 'failed_ingest'

STATUS_PENDING_SYNC      = 'pending_sync'
STATUS_SYNCING           = 'syncing'
STATUS_SUCCESS_SYNC      = 'success_sync'
STATUS_FAILED_SYNC       = 'failed_sync'

# Statuses indicating a download variable has moved past its download stage.
# Used as the compute gate: all 5 download vars must be in this set.
PAST_DOWNLOAD_STATUSES: frozenset[str] = frozenset({
    STATUS_SUCCESS_DOWNLOAD,
    STATUS_PENDING_IMAGE, STATUS_IMAGING, STATUS_SUCCESS_IMAGE, STATUS_FAILED_IMAGE,
    STATUS_PENDING_INGEST, STATUS_INGESTING, STATUS_SUCCESS_INGEST, STATUS_FAILED_INGEST,
})

# Statuses indicating a compute variable has moved past the compute stage.
# Used to make `compute --date` a no-op once a date has already been computed.
PAST_COMPUTE_STATUSES: frozenset[str] = frozenset({
    STATUS_SUCCESS_COMPUTE,
    STATUS_PENDING_IMAGE, STATUS_IMAGING, STATUS_SUCCESS_IMAGE, STATUS_FAILED_IMAGE,
    STATUS_PENDING_INGEST, STATUS_INGESTING, STATUS_SUCCESS_INGEST, STATUS_FAILED_INGEST,
})

# Statuses indicating a variable has moved past the image stage.
# Used to make `image --date` a no-op once a variable has already been imaged.
PAST_IMAGE_STATUSES: frozenset[str] = frozenset({
    STATUS_SUCCESS_IMAGE,
    STATUS_PENDING_INGEST, STATUS_INGESTING, STATUS_SUCCESS_INGEST, STATUS_FAILED_INGEST,
})

# ClickHouse connection settings
CH_HOST       = os.getenv('CH_HOST', 'localhost')
CH_PORT       = int(os.getenv('CH_PORT', '8123'))
CH_USER       = os.getenv('CH_USER', 'default')
CH_PASSWORD   = os.getenv('CH_PASSWORD', os.getenv('CLICKHOUSE_PASSWORD', ''))
CH_USE_REMOTE = os.getenv('CH_USE_REMOTE', '').strip().lower() in ('1', 'true', 'yes')
CH_REMOTE_URL = os.getenv('CH_REMOTE_URL', '')

# ---------------------------------------------------------------------------
# Sync stage — this pipeline runs on the PROCESS machine; sync pushes each
# date's hourly rows + WebP images to the API machine once ingest succeeds.
#
# SSH connectivity goes through `cloudflared access ssh` as a ProxyCommand
# (one subprocess per ssh/rsync invocation) rather than a persistent local
# tunnel container — `cloudflared access tcp`'s listener mode hits a
# "websocket: bad handshake" error against this Access application, while
# `access ssh` works.
# ---------------------------------------------------------------------------

# Staging directory for the Native-format export, on this filesystem. The
# API machine must use the same relative layout under its own data root so
# it can find the file after rsync lands it at the equivalent path there.
SYNC_STAGING_DIR = os.getenv('SSC_SYNC_STAGING_DIR', '/opt/data/SalishSeaCast/sync_staging')

# SSH connection to the API machine, via cloudflared Access (service token auth).
API_SSH_HOST        = os.getenv('API_SSH_HOST', 'ssh.cioospacificlabs.ca')
API_SSH_USER        = os.getenv('API_SSH_USER', 'cioos')
API_SSH_KEY_PATH    = os.getenv('API_SSH_KEY_PATH', '/run/secrets/ONC_OA_rsync')
API_SSH_KNOWN_HOSTS = os.getenv('API_SSH_KNOWN_HOSTS', '/run/secrets/known_hosts')
CF_TOKEN_ID         = os.getenv('CF_TOKEN_ID', '')
CF_TOKEN_SECRET     = os.getenv('CF_TOKEN_SECRET', '')

# API machine's data root as seen on its OWN host filesystem (i.e. the source
# side of its docker-compose bind mount for /opt/data) — this is where rsync
# writes, NOT the in-container /opt/data path. e.g. "/home/user/OA/data".
SYNC_API_DATA_DIR = os.getenv('SYNC_API_DATA_DIR', '')

# API machine base URL and shared bearer token for the import-trigger call.
SYNC_API_BASE_URL = os.getenv('SYNC_API_BASE_URL', '')
SYNC_API_TOKEN    = os.getenv('SYNC_API_TOKEN', '')
