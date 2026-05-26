-- Migration script: Convert nc_jobs.id from SERIAL to UUID
-- Idempotent: Safe to run multiple times
-- Date: 2026-05-21

BEGIN TRANSACTION;

-- Check if id column is already UUID (skip if already migrated)
DO $$
DECLARE
    col_type TEXT;
BEGIN
    SELECT data_type INTO col_type
    FROM information_schema.columns
    WHERE table_name = 'nc_jobs' AND column_name = 'id';
    
    IF col_type LIKE '%uuid%' THEN
        RAISE NOTICE 'nc_jobs.id is already UUID, skipping migration';
        RETURN;
    END IF;
    
    RAISE NOTICE 'Starting migration of nc_jobs.id from % to UUID', col_type;
    
    -- Add temporary uuid column
    ALTER TABLE nc_jobs ADD COLUMN id_uuid UUID DEFAULT gen_random_uuid();
    
    -- Drop primary key constraint
    ALTER TABLE nc_jobs DROP CONSTRAINT nc_jobs_pkey;
    
    -- Drop old id column
    ALTER TABLE nc_jobs DROP COLUMN id;
    
    -- Rename id_uuid to id
    ALTER TABLE nc_jobs RENAME COLUMN id_uuid TO id;
    
    -- Add primary key back on uuid column
    ALTER TABLE nc_jobs ADD PRIMARY KEY (id);
    
    RAISE NOTICE 'Successfully migrated nc_jobs.id to UUID';
END $$;

COMMIT;
