#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/db_export.sh
# Exports a compressed pg dump to ./DB/oa.dump

# Ensure docker compose services are up (db)
docker compose up -d db

OUT="./oa.dump"
TABLES=""
# Parse args: allow positional OUT and --tables or --tables=val
while [ "$#" -gt 0 ]; do
  case "$1" in
    --tables=*)
      TABLES="${1#--tables=}"
      shift
      ;;
    --tables)
      shift
      if [ "$#" -eq 0 ]; then
        echo "Missing value for --tables"
        exit 1
      fi
      TABLES="$1"
      shift
      ;;
    --help|-h)
      echo "Usage: $0 [OUT_PATH] [--tables=tbl1,tbl2,...]"
      exit 0
      ;;
    -* )
      echo "Unknown option: $1"
      exit 1
      ;;
    *)
      if [ "$OUT" = "./oa.dump" ]; then
        OUT="$1"
      else
        echo "Extra positional argument: $1"
        exit 1
      fi
      shift
      ;;
  esac
done

echo "Exporting DB to ${OUT}..."
# Default env fallbacks
PGUSER=${PGUSER:-postgres}
PGDATABASE=${PGDATABASE:-oa}

# Build table args if requested
TABLE_ARGS=""
if [ -n "${TABLES}" ]; then
  IFS=',' read -r -a _tables <<< "${TABLES}"
  for t in "${_tables[@]}"; do
    # Allow user to specify schema-qualified names or bare table names
    TABLE_ARGS+=" -t ${t}"
  done
  echo "Exporting only tables: ${TABLES}"
fi

# Run pg_dump. Preserve custom-format and compression (-F c -Z 9)
docker compose exec -t db sh -lc "pg_dump -U \"${PGUSER}\" -d \"${PGDATABASE}\" ${TABLE_ARGS} -F c -Z 9" > "${OUT}"

echo "Export complete: ${OUT}"