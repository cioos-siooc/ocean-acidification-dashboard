#!/bin/bash
# Runs on the API machine — sensor scripts write directly to the local db-ch.
if [ -f .updating_sensors ]; then
    echo "Sensor update already running. Exiting."
    exit 1
fi

> .updating_sensors
docker compose -f docker-compose.prod.api.yml run --rm sensors python onc_to_ch.py
docker compose -f docker-compose.prod.api.yml run --rm sensors python erddap_to_ch.py

rm .updating_sensors
