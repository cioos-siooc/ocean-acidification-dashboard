if [ -f .updating_sensors ]; then
    echo "Processing is already running. Exiting."
    exit 1
fi

> .updating_sensors
docker compose --env-file .env.prod -f docker-compose.prod.backend.yml run --rm process uv run python sensors/onc_to_nc.py
docker compose --env-file .env.prod -f docker-compose.prod.backend.yml run --rm process uv run python sensors/erddapGrid_to_nc.py
docker compose --env-file .env.prod -f docker-compose.prod.backend.yml run --rm process uv run python sensors/erddapTable_to_nc.py

rm .updating_sensors
