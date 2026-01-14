#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/db_restore.sh [PATH_TO_DUMP]
# Restores a compressed pg dump into the running db container

DUMP=${1:-./DB/oa.dump}
if [ ! -f "${DUMP}" ]; then
  echo "Dump file not found: ${DUMP}"
  exit 1
fi

docker compose up -d db

# copy dump into container
echo "Copying ${DUMP} into container..."
docker cp "${DUMP}" db:/tmp/oa.dump

# run pg_restore as postgres user
echo "Restoring dump into database..."
docker exec -u postgres db pg_restore -d "${PGDATABASE:-oa}" -v /tmp/oa.dump

echo "Restore complete."