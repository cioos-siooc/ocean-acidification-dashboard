#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/migrate_bind_to_volume.sh [VOLUME_NAME]
# Copies the existing host-bound ./DB directory into a newly-created Docker named volume.

VOLUME_NAME=${1:-oa_db}
SRC_DIR="$(pwd)/DB"

if [ ! -d "${SRC_DIR}" ]; then
  echo "Source directory not found: ${SRC_DIR}" >&2
  exit 1
fi

echo "Creating volume: ${VOLUME_NAME}"
docker volume create "${VOLUME_NAME}"

echo "Copying files from ${SRC_DIR} -> volume ${VOLUME_NAME}..."
# Use alpine to copy files; preserve modes/links where possible
docker run --rm -v "${SRC_DIR}":/from -v "${VOLUME_NAME}":/to alpine sh -c "cp -a /from/. /to/"

echo "Fixing ownership inside volume to postgres:postgres (best-effort)"
# Use the PostGIS image to chown properly inside the volume
docker run --rm -v "${VOLUME_NAME}":/var/lib/postgresql/data postgis/postgis:16-master sh -c "chown -R postgres:postgres /var/lib/postgresql/data || true"

echo "Migration complete. Update your docker-compose.yml to use the named volume (already done if you used the helper)."

echo "You can verify the volume contents: docker run --rm -v ${VOLUME_NAME}:/v busybox ls -la /v | head -n 40"