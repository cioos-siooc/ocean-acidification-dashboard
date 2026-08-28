# process

Data pipeline for the OA project. The active pipeline is `process/SSC` — a ClickHouse-native
downloader/compute/imaging/sync pipeline for SalishSeaCast. (The older Postgres-backed
`nc_jobs` pipeline and LiveOcean support have both been removed — see git history if you need
to look at that code.)

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

`--force` (where supported) acts on a row regardless of its current status, useful for a
redundant re-download or re-compute.

## Other subsystems in this directory

Sensor ingestion (ONC/ERDDAP → ClickHouse) is a separate service — see the top-level `sensors/`
directory, not part of `process/`. An older, Postgres-backed `process/sensors/` existed before
that migration; it's been removed entirely.

- `process/calc_carbon_grid_shm_memmap_year_aware.py`, `process/extract_bottom.py` — standalone
  biogeochemical/bottom-layer calculation scripts, independent of the `SSC/` pipeline above (the
  live pipeline computes pH/Ω via PyCO2SYS in `SSC/compute.py`; these are for ad-hoc, DB-free runs).
