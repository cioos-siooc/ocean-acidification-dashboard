-- Sensor metadata.  One row per sensor; ReplacingMergeTree keeps the latest
-- version by updated_at so an UPDATE is just an INSERT with a newer timestamp.
-- Always query with FINAL to collapse un-merged duplicates.
--
-- Apply:
--   docker compose -f docker-compose.dev.yml exec db-ch \
--     clickhouse-client --multiquery < scripts/sensors_schema.sql
CREATE TABLE IF NOT EXISTS default.sensors
(
    id            UUID DEFAULT generateUUIDv4(),
    name          String,
    latitude      Float64,
    longitude     Float64,
    depth         Float32,      -- metres; -1 = variable-depth sensor
    variables     String,       -- JSON: {canonical: {name, unit, conversion_factor}}
    device_config String,       -- JSON: ONC locationCode / codes config
    source        String,       -- JSON: {type: "ERDDAP"|"ONC", link: "..."}
    active        UInt8,
    updated_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;

-- Time-series observations from all sensors.  Narrow/EAV layout:
-- one row per (sensor, timestamp, depth, variable).  Uses LowCardinality on
-- variable so the string column compresses to an integer dictionary — fast
-- WHERE variable = '...' filters and no schema change needed for new variables.
--
-- ReplacingMergeTree deduplicates on the full ORDER BY key so re-ingesting the
-- 1-day overlap window on each run is safe.  Query with FINAL for correctness.
CREATE TABLE IF NOT EXISTS default.sensor_timeseries
(
    sensor_id  UUID                      CODEC(ZSTD(4)),
    time       DateTime                  CODEC(DoubleDelta, ZSTD(4)),
    depth      Float32                   CODEC(Gorilla(4),  ZSTD(4)),
    variable   LowCardinality(String),
    value      Float32                   CODEC(Gorilla(4),  ZSTD(4))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(time)
ORDER BY (sensor_id, variable, depth, time);
