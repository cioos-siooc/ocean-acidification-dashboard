# process

This package contains the `dl2` downloader utilities used by the OA project.

## Overview

- `process/dl2.py` — thin CLI shim that delegates to `process.dl2pkg.cli`.
- `process/dl2pkg` — library with modular components:
  - `db` — database helpers and `ensure_schema` to create required tables and staged columns.
  - `das` — functions to fetch and parse ERDDAP DAS text.
  - `detector` — detection logic to compute per-day chunks and create `nc_files` rows.
  - `downloader` — download worker functions that fetch NetCDF slices and update DB status.
  - `sublevel` — create reduced depth-level NetCDFs (sublevels) from downloads.
  - `png_worker` / `nc2tile` — generate PNG tiles from sublevels.
  - `cli` — command-line wiring that exposes `check`, `download`, `run`, `sublevel`, and `png` commands.

## Key concepts

- Pipeline stages are tracked in the `nc_files` table using a single ENUM column `status` with the following values:
  - `pending_download`, `processing_download`, `failed_download`, `success_download`,
    `pending_image`, `processing_image`, `failed_image`, `success_image`,
    `pending_compute`, `processing_compute`, `failed_compute`, `success_compute`.
- Per-stage metadata and attempt counters (e.g., `attempts_sublevel`, `last_error_png`) live alongside each row to support retries and observability.
- `process/configs.json` controls behavior (depth indices and compression options).

## Configuration

- `process/configs.json` fields of interest:
  - `depth_indices`: list of 0-based depth indices to select for sublevel files (example: `[0,5,10,15,20,25,30,35]`).
  - `compression`: controls NetCDF compression when writing sublevels and (optionally) downloaded files. Example:

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

- Check dataset for new full-day chunks and create pending rows:

```bash
python process/dl2.py check --dataset <DATASET_ID> [--dry-run]
```

- Create pending rows for a single UTC date (force existing rows to pending with `--force`):

```bash
python process/dl2.py check --dataset <DATASET_ID> --date 2026-01-05 [--force] [--dry-run]
```

- Re-run the full pipeline for a date (create rows for the date and run download+sublevel+png):

```bash
python process/dl2.py run --dataset <DATASET_ID> --date 2026-01-05 --force
```

- Download worker; optionally requeue failed downloads before starting and set concurrency limits:

```bash
python process/dl2.py download --dataset <DATASET_ID> [--requeue-failed] [--dry-run] [--limit 20]
```

- Sublevel worker (process downloaded NetCDF -> sublevel files):

```bash
python process/dl2.py sublevel [--dry-run] [--limit 10]
```

- PNG worker (produce PNGs from sublevels). The `--workers` number controls the internal `nc2tile` worker count used during tiling:

```bash
python process/dl2.py png [--dry-run] [--limit 10] [--workers 2]
```

## Retry & reprocessing notes 🔄

- Use `--force` when creating rows for a date to reset an existing successful row to `status_dl='pending'` (also resets attempts and clears last_error) so the full pipeline will run again for that date.
- `--requeue-failed` (passed to `download`) resets rows with `status_dl='failed'` to `pending` so they will be retried by the download worker.
- To re-run only downstream stages you can update `nc_files` directly (SQL) and then run the relevant worker:
  - If you need to re-run PNG generation for a specific date/variable: `UPDATE nc_files SET status_png='pending', attempts_png=0 WHERE start_time='2026-01-05 00:30:00' AND variable='dissolved_oxygen';` then `python process/dl2.py png`.  (Sublevels are deprecated in this workflow.)
  - Reset png: `UPDATE nc_files SET status_png='pending', attempts_png=0 WHERE ...;` then `python process/dl2.py png`.

## Testing & CI

- Unit tests are in `process/tests/` and CI runs them on PRs via `.github/workflows/ci.yml`. When running locally inside the container, set `PYTHONPATH=process` (or run from the repo root where tests expect package imports).

---

If you'd like, I can add a small admin subcommand (`requeue` or `reprocess`) to wrap the common SQL requeue/reset operations so you don't need to run raw SQL for sublevels or PNG re-runs. Would you like that added? 🔧
