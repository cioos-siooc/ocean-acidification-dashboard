# process

Data pipeline for the OA project. Two ClickHouse-native downloader/compute/imaging/sync
pipelines live here: `process/SSC` for SalishSeaCast and `process/LO` for the LiveOcean
forecast ([below](#liveocean-processlo)). (The older Postgres-backed `nc_jobs` pipeline has
been removed — see git history if you need to look at that code.)

## Overview

- Entry point: `python -m SSC.cli <command>`.
- Every NetCDF file/day-variable combination is tracked as a row in ClickHouse's
  `SalishSeaCast_status` table (see `SSC/db.py`), advancing through:
  ```
  pending_download → downloading → success_download
    → pending_compute → computing → success_compute
    → pending_image → imaging → success_image
    → pending_ingest → success_ingest
    → pending_sync → success_sync
  ```
- Key modules:
  - `SSC/downloader.py` — ERDDAP HTTP fetching with backfill
  - `SSC/compute.py` — biogeochemical derived variables (pH, Ω aragonite) via PyCO2SYS
  - `SSC/imaging.py` — renders WebP tiles via `nc2tile.py` (curvilinear grid sourced from
    ClickHouse's `grid_SSC` table, cached locally to an `.npz` file)
  - `SSC/ingest.py` — inserts hourly rows into `SalishSeaCast_hourly`
  - `SSC/sync.py` — exports a date's hourly rows + WebP images and rsyncs them to the API
    machine (via `cloudflared access ssh` as a ProxyCommand), triggering `POST /admin/syncHourly`

## CLI usage (examples)

```bash
python -m SSC.cli check       [--date YYYY-MM-DD] [--init-days N]   # query ERDDAP, queue new dates
python -m SSC.cli download    [--date YYYY-MM-DD] [--variable VAR] [--limit N]
python -m SSC.cli compute     [--date YYYY-MM-DD] [--limit N] [--workers N]
python -m SSC.cli check_image [--limit N]                            # promote fully-downloaded+computed dates to pending_image
python -m SSC.cli image       [--date YYYY-MM-DD] [--variable VAR] [--limit N] [--workers N]
python -m SSC.cli ingest      [--date YYYY-MM-DD] [--limit N]
python -m SSC.cli promote     [--limit N]                            # advance success_image → pending_ingest, success_ingest → pending_sync
python -m SSC.cli sync        [--date YYYY-MM-DD] [--limit N]
python -m SSC.cli run         [--date YYYY-MM-DD] [--limit N] [--workers N]  # all steps in order
python -m SSC.cli status      [--date YYYY-MM-DD]                    # print pipeline status summary
```

## Scheduling and monitoring (Prefect)

`python -m SSC.flows` serves the `run` pipeline as the `oceaneco-ssc-pipeline` Prefect flow (deployment `OceanECO-SSC`) on a cron
schedule (`RUN_CRON`, default every 3 hours), one task run per step, with each run's logs and a
`oceaneco-ssc-status` artifact in the Prefect UI. The `scheduler` compose service runs it — a
long-running container that polls the Prefect server and executes each due run in-process. Only
one run at a time: one that comes due while another is still going is cancelled, not queued.
It runs only on the prod process machine and reports to the shared server at
https://pipelines.cioospacific.ca; dev has no Prefect services (use the CLI).

```bash
# prod process machine, reporting to the shared Prefect server
# (.env.process.remote: PREFECT_API_URL=https://pipelines.cioospacific.ca/api, PREFECT_AUTH_STRING=user:pass)
docker compose -f docker-compose.prod.process.yml --env-file .env.process.remote --profile tools up -d --build
```

Env: `RUN_CRON`, `RUN_SCHEDULE_PAUSED` (default `false`), `RUN_LIMIT` (default 10),
`RUN_WORKERS` (default 30, matching the old cron's `SSC.cli run --workers 30`),
`PREFECT_API_URL` (required), `PREFECT_AUTH_STRING`. The Prefect scheduler replaces the old
host cron job — don't run both.

The CLI is unaffected and needs no Prefect server. In prod there is no `process` service —
run CLI commands in the scheduler container (`run --rm scheduler ...` while it's stopped): `docker compose -f docker-compose.prod.process.yml
--env-file .env.process.remote exec scheduler uv run python -m SSC.cli status`.

`--force` (where supported) acts on a row regardless of its current status, useful for a
redundant re-download or re-compute.

## LiveOcean (`process/LO`)

UW LiveOcean (ROMS `cas7_t2_x11b`) publishes one `layers.nc` per daily run at
`{LO_BASE_URL}/fYYYY.MM.DD/layers.nc`: a 72 h forecast at 4-hourly steps (19 steps), sliced at
fixed z-levels (surface, 10, 20, 30, 50, 100, 500, 1000, 1500, 2000, 2500 m, plus `bottom`,
stored as −1; runs before 2025-12-29 only carry surface–50 m and bottom). The archive starts at
2025-07-15.

- Entry point: `python -m LO.cli <command>`. The unit is a **run** (`--date` is the run date),
  tracked as one row in `LiveOcean_status`:
  ```
  pending_download → downloading → success_download → computing → success_compute
    → imaging → success_image → ingesting → success_ingest → syncing → success_sync
  ```
  with `failed_*` per step (retried up to `LO_MAX_RETRY_ATTEMPTS`). Each step consumes the
  previous step's `success_*` rows, so there is no promote stage.
- **Latest wins.** Runs overlap, so every row carries the `run_date` that produced it, and
  `LiveOcean_days` records which run owns each valid day. Once a newer run exists, a run keeps
  only its day 0 (6 steps); the newest keeps all 19. A run never replaces a day owned by a newer
  one, so backfills and retries can't overwrite fresher data. Days are swapped atomically with
  `REPLACE PARTITION` (tables are partitioned by day; see `shared/liveocean_schema.py`).
- Key modules:
  - `LO/downloader.py` — probes the archive, seeds runs, populates `grid_LO`, and writes a
    compact per-run NetCDF
  - `LO/compute.py` — derives Ω calcite, total alkalinity and DIC via PyCO2SYS from LO's pH and
    Ω aragonite
  - `LO/imaging.py` — WebP tiles to `LiveOcean/images/{var}/{YYYY-MM-DDTHHMMSS}/{depth}.webp`
  - `LO/ingest.py` — loads `LiveOcean_4h` and `LiveOcean_daily`, one valid day at a time
  - `LO/sync.py` — exports each day the run owns, rsyncs it plus its tiles to the API machine,
    calls `POST /admin/syncLiveOcean`, then drops the local copy and deletes the run's NetCDF
    (unless `LO_KEEP_NC` is set)

```bash
python -m LO.cli check    [--date YYYY-MM-DD] [--init-days N] [--backfill [--from YYYY-MM-DD]]
python -m LO.cli download [--date YYYY-MM-DD] [--limit N] [--force]
python -m LO.cli compute  [--date YYYY-MM-DD] [--limit N] [--workers N] [--force]
python -m LO.cli image    [--date YYYY-MM-DD] [--limit N] [--workers N] [--force]
python -m LO.cli ingest   [--date YYYY-MM-DD] [--limit N] [--workers N] [--force]
python -m LO.cli sync     [--date YYYY-MM-DD] [--limit N] [--force]
python -m LO.cli run      [--date YYYY-MM-DD] [--limit N] [--workers N] [--force] [--init-days N] [--backfill [--from YYYY-MM-DD]]
python -m LO.cli status   [--date YYYY-MM-DD]
```

| Env var | Default | Purpose |
|---|---|---|
| `LO_BASE_URL` | `https://s3.kopah.uw.edu/liveocean-share` | Archive root |
| `LO_ARCHIVE_DIR` | — | Local mirror (`fYYYY.MM.DD/layers.nc`), read before the URL |
| `LO_ARCHIVE_START` | `2025-07-15` | Earliest run `--backfill` asks for |
| `LO_VARIABLES` | all 10 | Subset to process, e.g. `temperature` for a quick backfill. Other columns stay NaN until the run is redone |
| `LO_IMAGE_DEPTHS` | all levels | Levels to pre-render (e.g. `surface`); the rest render on demand via `GET /png` |
| `LO_INGEST_WORKERS` | 6 | Parallel runs per ingest sweep (~1–2 GB RAM each) |
| `LO_MAX_RETRY_ATTEMPTS` | 3 | Retries per failed step |
| `LO_NC_DIR` / `LO_IMAGE_DIR` / `LO_SYNC_STAGING_DIR` | `/opt/data/LiveOcean/{nc,images,sync_staging}` | Working paths |
| `LO_KEEP_NC` | off | Keep the run's NetCDF after sync |

**Not scheduled yet:** there is no Prefect flow or compose service for LiveOcean, so run it from
the CLI.

**Storage** (all 10 variables, every level): about 250 MiB/day in `LiveOcean_4h`, 130 MiB/day in
`LiveOcean_daily` and 380 MB/day of tiles, roughly 275 GB/year on the API machine. The PROCESS
machine only holds runs that haven't synced yet. Limiting `LO_IMAGE_DEPTHS` is the main way to
cut this.

## Other subsystems in this directory

Sensor ingestion (ONC/ERDDAP → ClickHouse) is a separate service — see the top-level `sensors/`
directory, not part of `process/`. An older, Postgres-backed `process/sensors/` existed before
that migration; it's been removed entirely.

- `process/calc_carbon_grid_shm_memmap_year_aware.py`, `process/extract_bottom.py` — standalone
  biogeochemical/bottom-layer calculation scripts, independent of the `SSC/` pipeline above (the
  live pipeline computes pH/Ω via PyCO2SYS in `SSC/compute.py`; these are for ad-hoc, DB-free runs).
