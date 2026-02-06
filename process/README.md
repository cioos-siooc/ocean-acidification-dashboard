# process

This package contains the `dl2` downloader utilities used by the OA project.

## Overview

- `process/dl2.py` — thin CLI shim that delegates to `process.dl2pkg.cli`.
- `process/dl2pkg` — library with modular components:
  - `db` — database helpers and `ensure_schema` to create required tables and staged columns.
  - `das` — functions to fetch and parse ERDDAP DAS text.
  - `detector` — detection logic to compute per-day chunks and create `nc_jobs` rows.
  - `downloader` — download worker functions that fetch NetCDF slices and update DB status.
  - `png_worker` / `nc2tile` — generate PNG tiles from downloads.
  - `cli` — command-line wiring that exposes `check`, `download`, `run`, `check_image`, `image`, and `compute` commands.

## Key concepts

- Pipeline stages are tracked in the `nc_jobs` table using a single ENUM column `status` with the following values:
  - `pending_download`, `downloading`, `failed_download`, `success_download`,
    `pending_image`, `imaging`, `failed_image`, `success_image`,
    `pending_compute`, `computing`, `failed_compute`, `success_compute`.
- `process/configs.json` controls behavior (depth indices and compression options).

## Configuration

- `process/configs.json` fields of interest:
  - `compression`: controls NetCDF compression when writing downloaded files. Example:

```json
{
  "compression": {"zlib": true, "complevel": 4, "shuffle": true, "apply_to_downloads": false}
}
```

- Environment variables (defaults):
  - `ERDDAP_BASE` (default: `https://salishsea.eos.ubc.ca/erddap`)
  - `NC_ROOT` (default: `/opt/data/nc`)
  - `PNG_ROOT` (default: `/opt/data/png`)
  - Standard Postgres env vars: `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`.

## CLI usage (examples) ✅

- Check all datasets for new full-day chunks and create pending rows:

```bash
python -m dl2pkg.cli check
```

- Create pending rows for a single UTC date (force existing rows to pending with `--force`):

```bash
python -m dl2pkg.cli check --date 2026-01-05 [--force]
```

- Re-run the full pipeline for a date (create rows for the date and run download+image):

```bash
python -m dl2pkg.cli run --date 2026-01-05 --force
```

- Download worker; optionally requeue failed downloads before starting and set concurrency limits:

```bash
python -m dl2pkg.cli download [--requeue-failed] [--limit 20]
```

- PNG worker (produce PNGs from downloads). The `--workers` number controls the internal `nc2tile` worker count used during tiling:

```bash
python -m dl2pkg.cli image [--limit 10] [--workers 2]
```

## Retry & reprocessing notes 🔄

- Use `--force` when creating rows for a date to reset an existing successful row to `status='pending_download'` so the full pipeline will run again for that date.
- `--requeue-failed` (passed to `download`) resets rows with `status='failed_download'` to `pending_download` so they will be retried by the download worker.
- To re-run only downstream stages you can update `nc_jobs` directly (SQL) and then run the relevant worker:
  - If you need to re-run PNG generation for a specific date/variable: `UPDATE nc_jobs SET status='pending_image', attempts=0 WHERE start_time='2026-01-05 00:30:00' AND end_time='2026-01-05 23:30:00';` then `python -m dl2pkg.cli image`.

  ## Live Ocean downloader

  The Live Ocean workflow downloads a daily `layers.nc` file and splits it into
  per-variable, per-day NetCDFs while merging depth-suffixed variables into a
  single variable with a `depth` dimension.

  Example:

  ```bash
  python process/dl_LO/main.py --url <LAYERS_NC_URL> --input /opt/data/nc/liveOcean/layers.nc --out-dir /opt/data/nc
  ```

  To run the Live Ocean workflow with DB monitoring (separate table) and then feed the PNG worker:

  ```bash
  python -m dl2pkg.cli liveocean_download --liveocean-url <LAYERS_NC_URL> --liveocean-input /opt/data/nc/liveOcean/layers.nc --liveocean-out /opt/data/nc
  python -m dl2pkg.cli liveocean_process --limit 1
  ```

## Testing & CI

- Unit tests are in `process/tests/` and CI runs them on PRs via `.github/workflows/ci.yml`. When running locally inside the container, set `PYTHONPATH=process` (or run from the repo root where tests expect package imports).

---

If you'd like, I can add a small admin subcommand (`requeue` or `reprocess`) to wrap the common SQL requeue/reset operations. Would you like that added? 🔧
