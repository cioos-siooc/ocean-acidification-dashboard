OA
===

This repository contains tools for processing oceanographic NetCDF data and producing PNG tiles and metadata. See the individual folders (`process/`, `front/`, `api/`) for service-specific code.

Scripts
-------

The `scripts/` directory contains small helper scripts for managing the local development Postgres database. They are intended for developer convenience and are executable (chmod +x).

- `scripts/db_export.sh` — Export a compressed Postgres dump to `./DB/oa.dump`.
  - Usage: `./scripts/db_export.sh`
  - Produces: `./DB/oa.dump` (custom compressed pg_dump format)
  - Note: Keep dumps out of source control; store them in an artifact store or a secure location.

- `scripts/db_restore.sh` — Restore a dump into the running Postgres container.
  - Usage: `./scripts/db_restore.sh [PATH_TO_DUMP]`
  - Default: `./DB/oa.dump`
  - Example: `./scripts/db_restore.sh ./DB/oa.dump`

- `scripts/migrate_bind_to_volume.sh` — Copy an existing host-bound database directory (`./DB`) into a Docker named volume.
  - Usage: `./scripts/migrate_bind_to_volume.sh [VOLUME_NAME]`
  - Default volume name: `oa_db`
  - This helps avoid permission/ownership issues caused by multiple containers writing to a host bind mount.

Safety notes
------------

- Do not commit large DB dumps or DB directory files into Git. Use the `DB_HISTORY_CLEANUP.md` document for details about why `DB/` was removed from history and how to recover if needed.
- When migrating DB files into a named volume, scripts attempt a best-effort `chown` to the `postgres` user; verify ownership and do a DB integrity check after migration.

If you'd like, I can add example commands to push dumps to an S3 bucket or a small Makefile wrapper to standardize import/export steps.
