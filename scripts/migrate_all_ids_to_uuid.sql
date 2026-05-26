-- Migration script: Convert all primary/foreign keys to UUID
-- Idempotent: Safe to run multiple times
-- Order: datasets.id → fields (with dataset_id FK) → nc_jobs (with variable_id FK)
-- Date: 2026-05-21

BEGIN TRANSACTION;

-- Helper function to check if column already exists and is UUID
CREATE OR REPLACE FUNCTION is_uuid_column(p_table_name TEXT, p_col_name TEXT)
RETURNS BOOLEAN AS $$
DECLARE
    col_type TEXT;
BEGIN
    SELECT data_type INTO col_type
    FROM information_schema.columns
    WHERE information_schema.columns.table_name = p_table_name 
      AND information_schema.columns.column_name = p_col_name;
    RETURN col_type = 'uuid';
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 1. MIGRATE datasets.id SERIAL → UUID
-- ============================================================================
DO $$
BEGIN
    IF NOT is_uuid_column('datasets', 'id') THEN
        RAISE NOTICE 'Migrating datasets.id to UUID...';
        
        -- Add temporary UUID column
        ALTER TABLE datasets ADD COLUMN id_uuid UUID;
        UPDATE datasets SET id_uuid = gen_random_uuid();
        ALTER TABLE datasets ALTER COLUMN id_uuid SET NOT NULL;
        
        -- Drop the sequence
        DROP SEQUENCE IF EXISTS datasets_id_seq CASCADE;
        
        -- Drop old id column
        ALTER TABLE datasets DROP COLUMN id CASCADE;
        
        -- Rename and make PK
        ALTER TABLE datasets RENAME COLUMN id_uuid TO id;
        ALTER TABLE datasets ADD PRIMARY KEY (id);
        
        RAISE NOTICE 'Successfully migrated datasets.id to UUID';
    ELSE
        RAISE NOTICE 'datasets.id already UUID, skipping';
    END IF;
END $$;

-- ============================================================================
-- 2. MIGRATE fields.id SERIAL → UUID and update dataset_id FK
-- ============================================================================
DO $$
BEGIN
    IF NOT is_uuid_column('fields', 'id') THEN
        RAISE NOTICE 'Migrating fields...';
        
        -- Add temporary UUID column for id
        ALTER TABLE fields ADD COLUMN id_uuid UUID;
        UPDATE fields SET id_uuid = gen_random_uuid();
        ALTER TABLE fields ALTER COLUMN id_uuid SET NOT NULL;
        
        -- Drop sequence
        DROP SEQUENCE IF EXISTS fields_id_seq CASCADE;
        
        -- Drop old id column
        ALTER TABLE fields DROP COLUMN id CASCADE;
        
        -- Rename id
        ALTER TABLE fields RENAME COLUMN id_uuid TO id;
        ALTER TABLE fields ADD PRIMARY KEY (id);
        
        RAISE NOTICE 'Successfully migrated fields.id to UUID';
    ELSE
        RAISE NOTICE 'fields.id already UUID, skipping';
    END IF;
END $$;

-- ============================================================================
-- 3. MIGRATE fields.dataset_id INTEGER FK → UUID FK
-- ============================================================================
DO $$
DECLARE
    fk_exists BOOLEAN;
BEGIN
    -- Check if dataset_id column exists and is still INTEGER
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'fields' AND column_name = 'dataset_id' 
        AND data_type LIKE '%int%'
    ) INTO fk_exists;
    
    IF fk_exists THEN
        RAISE NOTICE 'Migrating fields.dataset_id to UUID FK...';
        
        -- Drop the FK constraint if it exists
        BEGIN
            ALTER TABLE fields DROP CONSTRAINT IF EXISTS fields_dataset_id_fkey;
        EXCEPTION WHEN OTHERS THEN
            NULL;
        END;
        
        -- Add a temporary UUID column
        ALTER TABLE fields ADD COLUMN dataset_id_uuid UUID;
        
        -- Populate it from the integer column (using cascading UUIDs from datasets)
        -- This is a bit tricky - we need to populate based on the updated dataset IDs
        UPDATE fields f SET dataset_id_uuid = d.id 
        FROM datasets d 
        WHERE f.dataset_id::text = d.id::text;
        
        -- Drop old column
        ALTER TABLE fields DROP COLUMN dataset_id;
        
        -- Rename new column
        ALTER TABLE fields RENAME COLUMN dataset_id_uuid TO dataset_id;
        
        -- Recreate FK constraint
        ALTER TABLE fields ADD CONSTRAINT fields_dataset_id_fkey
            FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE SET NULL;
        
        RAISE NOTICE 'Successfully migrated fields.dataset_id FK to UUID';
    ELSE
        RAISE NOTICE 'fields.dataset_id already UUID or FK updated, skipping';
    END IF;
END $$;

-- ============================================================================
-- 4. MIGRATE nc_jobs.dataset_id and nc_jobs.variable_id to UUID FKs
-- ============================================================================
DO $$
DECLARE
    ds_fk_is_int BOOLEAN;
    var_fk_is_int BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'nc_jobs' AND column_name = 'dataset_id' 
        AND data_type LIKE '%int%'
    ) INTO ds_fk_is_int;
    
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'nc_jobs' AND column_name = 'variable_id' 
        AND data_type LIKE '%int%'
    ) INTO var_fk_is_int;
    
    IF ds_fk_is_int OR var_fk_is_int THEN
        RAISE NOTICE 'Migrating nc_jobs FKs to UUID...';
        
        -- Drop FKs if they exist
        BEGIN
            ALTER TABLE nc_jobs DROP CONSTRAINT IF EXISTS nc_jobs_dataset_id_fkey;
        EXCEPTION WHEN OTHERS THEN
            NULL;
        END;
        
        BEGIN
            ALTER TABLE nc_jobs DROP CONSTRAINT IF EXISTS nc_jobs_variable_id_fkey;
        EXCEPTION WHEN OTHERS THEN
            NULL;
        END;
        
        -- Migrate dataset_id
        IF ds_fk_is_int THEN
            RAISE NOTICE 'Converting nc_jobs.dataset_id to UUID...';
            ALTER TABLE nc_jobs ADD COLUMN dataset_id_uuid UUID;
            UPDATE nc_jobs nj SET dataset_id_uuid = d.id 
            FROM datasets d 
            WHERE nj.dataset_id::text = d.id::text;
            ALTER TABLE nc_jobs DROP COLUMN dataset_id;
            ALTER TABLE nc_jobs RENAME COLUMN dataset_id_uuid TO dataset_id;
        END IF;
        
        -- Migrate variable_id
        IF var_fk_is_int THEN
            RAISE NOTICE 'Converting nc_jobs.variable_id to UUID...';
            ALTER TABLE nc_jobs ADD COLUMN variable_id_uuid UUID;
            UPDATE nc_jobs nj SET variable_id_uuid = f.id 
            FROM fields f 
            WHERE nj.variable_id::text = f.id::text;
            ALTER TABLE nc_jobs DROP COLUMN variable_id;
            ALTER TABLE nc_jobs RENAME COLUMN variable_id_uuid TO variable_id;
        END IF;
        
        -- Recreate FK constraints
        ALTER TABLE nc_jobs ADD CONSTRAINT nc_jobs_dataset_id_fkey
            FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE;
        
        ALTER TABLE nc_jobs ADD CONSTRAINT nc_jobs_variable_id_fkey
            FOREIGN KEY (variable_id) REFERENCES fields(id) ON DELETE CASCADE;
        
        RAISE NOTICE 'Successfully migrated nc_jobs FKs to UUID';
    ELSE
        RAISE NOTICE 'nc_jobs FKs already UUID, skipping';
    END IF;
END $$;

DROP FUNCTION IF EXISTS is_uuid_column(TEXT, TEXT);

COMMIT;
