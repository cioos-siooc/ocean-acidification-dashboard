#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/db_export.sh
# Exports a compressed pg dump to ./DB/oa.dump

# Ensure docker compose services are up (db)
docker compose up -d db

OUT=./oa.dump
echo "Exporting DB to ${OUT}..."
# Default env fallbacks
PGUSER=${PGUSER:-postgres}
PGDATABASE=${PGDATABASE:-oa}

docker compose exec -t db pg_dump -U "${PGUSER}" -d "${PGDATABASE}" -F c -Z 9 > "${OUT}"

echo "Export complete: ${OUT}"