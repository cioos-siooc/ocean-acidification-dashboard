if [ -f .processing ]; then
    echo "Processing is already running. Exiting."
    exit 1
fi

> .processing
docker compose --env-file .env.prod -f docker-compose.prod.backend.yml run --rm process uv run MAIN.py check_download && sleep 5
docker compose --env-file .env.prod -f docker-compose.prod.backend.yml run --rm process uv run MAIN.py download && sleep 5
docker compose --env-file .env.prod -f docker-compose.prod.backend.yml run --rm process uv run MAIN.py compute --workers 6 && sleep 5
docker compose --env-file .env.prod -f docker-compose.prod.backend.yml run --rm process uv run MAIN.py bottom_layer && sleep 5
docker compose --env-file .env.prod -f docker-compose.prod.backend.yml run --rm process uv run MAIN.py check_image && sleep 5
docker compose --env-file .env.prod -f docker-compose.prod.backend.yml run --rm process uv run MAIN.py image --workers 6

rm .processing
