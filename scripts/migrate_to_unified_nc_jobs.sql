-- Migration: unify nc_jobs and live_ocean_data into a single table
-- Run this script on the PostgreSQL instance while both services are stopped.
--
-- Steps:
--   1. Extend nc_file_status enum with LO-specific values
--   2. Add new columns to nc_jobs
--   3. Migrate rows from live_ocean_data into nc_jobs
--   4. Verify migration counts
--   5. Replace 4-column unique index with 3-column
--   6. Drop live_ocean_data (uncomment when satisfied with verification)
-- ---------------------------------------------------------------------------

BEGIN;

-- 1. Extend enum (safe if values already exist in PostgreSQL 9.3+)
DO $$
BEGIN
    ALTER TYPE nc_file_status ADD VALUE IF NOT EXISTS 'extracted';
    ALTER TYPE nc_file_status ADD VALUE IF NOT EXISTS 'imaging_failed';
END $$;

-- Also handle the alternate enum name if present
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'nc_job_status') THEN
        ALTER TYPE nc_job_status ADD VALUE IF NOT EXISTS 'extracted';
        ALTER TYPE nc_job_status ADD VALUE IF NOT EXISTS 'imaging_failed';
    END IF;
END $$;

-- Note: ALTER TYPE cannot run inside a transaction that has already modified data.
-- Commit the enum changes before proceeding.
COMMIT;

BEGIN;

-- 2. Add new columns to nc_jobs (idempotent)
ALTER TABLE nc_jobs ADD COLUMN IF NOT EXISTS error_message TEXT;
ALTER TABLE nc_jobs ADD COLUMN IF NOT EXISTS created_at    TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE nc_jobs ADD COLUMN IF NOT EXISTS updated_at    TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE nc_jobs ADD COLUMN IF NOT EXISTS misc          JSONB;
-- Drop legacy source_date column if present from a prior migration attempt
ALTER TABLE nc_jobs DROP COLUMN IF EXISTS source_date;

-- GIN index for efficient misc key lookups
CREATE INDEX IF NOT EXISTS idx_nc_jobs_misc ON nc_jobs USING GIN (misc);

-- 3. Migrate live_ocean_data rows into nc_jobs
--    source_date is stored in misc as {"source_date": "YYYY-MM-DD"} (text, not DATE type).
--    Map: file_path -> nc_path.
INSERT INTO nc_jobs
    (dataset_id, variable_id, misc, start_time, end_time,
     nc_path, status, error_message, created_at, updated_at)
SELECT
    lod.dataset_id,
    lod.variable_id,
    CASE WHEN lod.source_date IS NOT NULL
         THEN jsonb_build_object('source_date', lod.source_date::text)
         ELSE NULL
    END,
    lod.start_time,
    lod.end_time,
    lod.file_path,
    lod.status::nc_file_status,
    lod.error_message,
    COALESCE(lod.created_at, NOW()),
    COALESCE(lod.updated_at, NOW())
FROM live_ocean_data lod
ON CONFLICT (dataset_id, variable_id, start_time) DO NOTHING;

-- 4. Verification — compare row counts before committing
SELECT 'live_ocean_data rows total'       AS label, COUNT(*) AS count FROM live_ocean_data
UNION ALL
SELECT 'nc_jobs rows for dataset_id=4',            COUNT(*) FROM nc_jobs WHERE dataset_id = 4
UNION ALL
SELECT 'live_ocean_data success_image rows',       COUNT(*) FROM live_ocean_data WHERE status = 'success_image'
UNION ALL
SELECT 'nc_jobs success_image rows (dataset_id=4)', COUNT(*) FROM nc_jobs WHERE dataset_id = 4 AND status = 'success_image';

COMMIT;

-- 5. Replace unique index
--    The old index covers (dataset_id, variable_id, start_time, end_time).
--    The new index covers (dataset_id, variable_id, start_time) only.
--    Do this in its own transaction since index operations can be slow.
BEGIN;

DROP INDEX IF EXISTS ux_nc_jobs_dataset_variable_time;
DROP INDEX IF EXISTS ux_nc_jobs_null_dataset_variable_time;
CREATE UNIQUE INDEX ux_nc_jobs_dataset_variable_time
    ON nc_jobs (dataset_id, variable_id, start_time);


COMMIT;

-- 6. Drop live_ocean_data (run only after verifying step 4 counts match)
-- Uncomment the line below when ready:
-- DROP TABLE IF EXISTS live_ocean_data CASCADE;
