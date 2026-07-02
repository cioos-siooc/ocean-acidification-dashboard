if [ -f .updating_sensors ]; then
    echo "Processing is already running. Exiting."
    exit 1
fi

> .updating_sensors
docker compose --env-file .env.prod -f docker-compose.prod.backend.yml run --rm process uv run python sensors/onc_to_ch.py
docker compose --env-file .env.prod -f docker-compose.prod.backend.yml run --rm process uv run python sensors/erddap_to_ch.py

rm .updating_sensors
