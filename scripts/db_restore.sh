#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/db_restore.sh [./oa.dump] [--fresh|-f]
# Simplified restore script per project convention:
# - Always uses `docker compose` (no detection)
# - Assumes an oa dump file (default: ./oa.dump)
# - Always restores with pg_restore (custom-format dump)

# Parse arguments: accept flags in any order. Usage: ./scripts/db_restore.sh [PATH_TO_DUMP] [--fresh|-f]
DUMP="./oa.dump"
FRESH=false
for arg in "$@"; do
  case "$arg" in
    --fresh|-f)
      FRESH=true
      ;;
    --help|-h)
      echo "Usage: $0 [PATH_TO_DUMP] [--fresh|-f]"
      exit 0
      ;;
    -*)
      echo "Unknown option: $arg"
      exit 1
      ;;
    *)
      # positional: treat first non-flag as DUMP path
      if [ "$DUMP" = "./oa.dump" ]; then
        DUMP=$arg
      fi
      ;;
  esac
done

DB_SERVICE=db
DB_DEST_PATH="/tmp/oa.dump"
DB_NAME=oa
PG_USER=postgres

if [ ! -f "${DUMP}" ]; then
  echo "Dump file not found: ${DUMP}"
  exit 1
fi

COMPOSE_CMD="docker compose"

echo "Using compose command: ${COMPOSE_CMD}"

# Ensure the db service/container exists
container_id=$(${COMPOSE_CMD} ps -q "${DB_SERVICE}" 2>/dev/null || true)
if [ -z "${container_id}" ]; then
  echo "Database service '${DB_SERVICE}' is not running. Starting it..."
  ${COMPOSE_CMD} up -d "${DB_SERVICE}"
  container_id=$(${COMPOSE_CMD} ps -q "${DB_SERVICE}" 2>/dev/null || true)
  if [ -z "${container_id}" ]; then
    echo "Failed to start database service '${DB_SERVICE}'."
    exit 1
  fi
fi

# Wait for Postgres to be ready
echo "Waiting for Postgres to accept connections..."
for i in $(seq 1 60); do
  if ${COMPOSE_CMD} exec -T "${DB_SERVICE}" pg_isready -U "${PG_USER}" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [ "${ready:-0}" -ne 1 ]; then
  echo "Postgres did not become ready in time. Aborting."
  exit 1
fi

if [ "${FRESH}" = true ]; then
  timestamp=$(date +%Y%m%d_%H%M%S)
  backup_local="./${DB_NAME}_pre_restore_${timestamp}.dump"
  backup_remote="/tmp/${DB_NAME}_pre_restore_${timestamp}.dump"

  echo "Creating backup of current DB '${DB_NAME}' inside container..."
  ${COMPOSE_CMD} exec -T -u "${PG_USER}" "${DB_SERVICE}" pg_dump -U "${PG_USER}" -F c -d "${DB_NAME}" -f "${backup_remote}"

  echo "Copying backup to host: ${backup_local}"
  ${COMPOSE_CMD} cp "${DB_SERVICE}:${backup_remote}" "${backup_local}"

  echo "Removing remote temporary backup file..."
  ${COMPOSE_CMD} exec -T -u "${PG_USER}" "${DB_SERVICE}" rm -f "${backup_remote}" || true

  echo "Dropping and recreating database '${DB_NAME}'..."
  echo "Terminating other connections to '${DB_NAME}'..."
  ${COMPOSE_CMD} exec -T -u "${PG_USER}" "${DB_SERVICE}" psql -v ON_ERROR_STOP=1 -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='${DB_NAME}' AND pid <> pg_backend_pid();"
  ${COMPOSE_CMD} exec -T -u "${PG_USER}" "${DB_SERVICE}" psql -v ON_ERROR_STOP=1 -c "DROP DATABASE IF EXISTS \"${DB_NAME}\";"
  ${COMPOSE_CMD} exec -T -u "${PG_USER}" "${DB_SERVICE}" psql -v ON_ERROR_STOP=1 -c "CREATE DATABASE \"${DB_NAME}\" OWNER \"${PG_USER}\";"

  echo "Fresh DB ready. Backup saved to ${backup_local}"
fi

# Copy dump into container
echo "Copying ${DUMP} into container '${DB_SERVICE}:${DB_DEST_PATH}'..."
${COMPOSE_CMD} cp "${DUMP}" "${DB_SERVICE}:${DB_DEST_PATH}"

# Restore with pg_restore (project dump is always a custom-format dump)
echo "Restoring dump with pg_restore into database '${DB_NAME}'..."
${COMPOSE_CMD} exec -T -u "${PG_USER}" "${DB_SERVICE}" pg_restore --no-owner --no-privileges -d "${DB_NAME}" -v "${DB_DEST_PATH}"

# Cleanup remote dump
echo "Cleaning up temporary dump on container..."
# try as root first (no -u), fall back to postgres user if needed
if ${COMPOSE_CMD} exec -T "${DB_SERVICE}" rm -f "${DB_DEST_PATH}" 2>/dev/null; then
  true
else
  ${COMPOSE_CMD} exec -T -u "${PG_USER}" "${DB_SERVICE}" rm -f "${DB_DEST_PATH}" || true
fi

echo "Restore complete."