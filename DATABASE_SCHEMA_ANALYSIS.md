# Database Schema Analysis - OA Project

## 1. NC_JOBS TABLE - FULL SCHEMA

### Column Definitions
| Column | Type | Constraints | Description |
|--------|------|-----------|-------------|
| id | SERIAL | PRIMARY KEY | Auto-incrementing unique identifier |
| dataset_id | INT | NOT NULL, FK→datasets(id), ON DELETE CASCADE | Foreign key to datasets table |
| variable_id | INT | NOT NULL, FK→fields(id), ON DELETE CASCADE | Foreign key to fields table |
| start_time | TIMESTAMPTZ | NOT NULL | Pipeline time window start (e.g., YYYY-MM-DD 00:30:00 UTC) |
| end_time | TIMESTAMPTZ | NOT NULL | Pipeline time window end (e.g., YYYY-MM-DD 23:30:00 UTC) |
| status | nc_file_status | DEFAULT 'pending_download' | ENUM with values: 'pending_download', 'downloading', 'failed_download', 'success_download', 'pending_compute', 'computing', 'failed_compute', 'success_compute', 'pending_image', 'imaging', 'failed_image', 'success_image' |
| nc_path | TEXT | NULL | Path to the downloaded/processed NetCDF file |
| checksum | TEXT | NULL | Checksum/hash of the file for verification |
| attempts | INT | DEFAULT 0 | Number of processing attempts for retry tracking |
| last_attempt | TIMESTAMPTZ | NULL | Timestamp of the last attempt (for retry backoff) |

### Indexes
- `idx_nc_jobs_var_start` - ON (variable_id, start_time)
- `idx_nc_jobs_status` - ON (status)
- `ux_nc_jobs_dataset_variable_time` - UNIQUE ON (dataset_id, variable_id, start_time, end_time)
- `ux_nc_jobs_null_dataset_variable_time` - UNIQUE ON (variable_id, start_time, end_time) WHERE dataset_id IS NULL

### Important Notes
- **NO `datetime` column exists** - The nc_jobs table uses `start_time` and `end_time` instead
- Pipeline stages are tracked using the single `status` ENUM column
- Time windows are typically daily chunks: 00:30 UTC to 23:30 UTC

---

## 2. FIELDS TABLE - FULL SCHEMA

### Core Columns (from CREATE TABLE)
| Column | Type | Constraints |
|--------|------|-----------|
| id | SERIAL | PRIMARY KEY |
| dataset_id | INT | FK→datasets(id), ON DELETE SET NULL, allows NULL |
| variable | TEXT | NOT NULL |
| last_downloaded_at | TIMESTAMPTZ | NULL |
| meta | JSONB | NULL |
| UNIQUE(dataset_id, variable) | Composite unique constraint |

### Additional Columns (Added via ALTER TABLE)
| Column | Type | Default | Description |
|--------|------|---------|-------------|
| available_datetimes | timestamptz[] | '{}'::timestamptz[] | **PostgreSQL ARRAY** of timestamps when data is available for this variable |
| type | TEXT | 'download' | Indicates whether variable is 'download' or 'compute' type |
| precision | FLOAT | 0.1 | Precision/packing for PNG generation |

### Missing/Not Auto-Created Columns Referenced in API Query
The following columns are **QUERIED in variables.py but NOT defined in db.py schema initialization**:
- `min` - Colormap minimum value (SHOULD BE FLOAT or NUMERIC)
- `max` - Colormap maximum value (SHOULD BE FLOAT or NUMERIC)
- `depths_image` - Depths to use for PNG generation (SHOULD BE ARRAY or JSONB of integers)
- `colormap` - Colormap name (SHOULD BE TEXT)

**Status**: These columns likely exist in the production database (legacy or manually added), but are not auto-created by the `ensure_schema()` function.

---

## 3. AVAILABLE_DATETIMES FORMAT & STORAGE

### Current Format
- **Type**: `timestamptz[]` (PostgreSQL ARRAY of TIMESTAMPTZ)
- **Default**: `'{}'::timestamptz[]` (empty array)
- **Storage**: Native PostgreSQL array type for efficient storage and querying

### Migration History
The schema migration code handles conversion from older JSONB format:
```python
# If available_datetimes was previously stored as JSONB (ISO string array),
# it's converted to timestamptz[] for efficiency
# Conversion uses: jsonb_array_elements_text() to extract strings, 
# cast to timestamptz, and aggregate with array_agg(DISTINCT ... ORDER BY ...)
```

### Example Values
- Empty: `'{}'::timestamptz[]` or `ARRAY[]::timestamptz[]`
- Example: `ARRAY['2026-01-05 00:30:00+00', '2026-01-06 00:30:00+00', '2026-01-07 00:30:00+00']::timestamptz[]`

---

## 4. HOW AVAILABLE_DATETIMES IS POPULATED/UPDATED

### Current Query Method (API variables.py)
```sql
SELECT 
    ARRAY_AGG(DISTINCT nj.datetime ORDER BY nj.datetime) as available_datetimes
FROM fields f
LEFT JOIN datasets d ON f.dataset_id = d.id
LEFT JOIN nc_jobs nj ON f.variable = nj.variable AND nj.status = 'success_image'
GROUP BY f.variable, ...
```

### ⚠️ CRITICAL BUG IDENTIFIED
**The query references `nj.datetime` but nc_jobs has NO `datetime` column!**

This query will **FAIL** because it's trying to aggregate a non-existent column. The correct column should be:
- `nj.start_time` (beginning of the time window) - **RECOMMENDED**
- OR a computed field combining start_time/end_time

### How It SHOULD Be Populated
Based on code review, `available_datetimes` should be populated by:

1. **Find all nc_jobs rows** where:
   - `status = 'success_image'` (PNG generation completed)
   - `variable_id = fields.id` (matching variable)

2. **Extract datetime values** from those rows:
   - Use `start_time` (most likely) or a computed datetime
   - Could also use `end_time` or a midpoint

3. **Aggregate into array**:
   - Collect DISTINCT values
   - Order chronologically
   - Store in `fields.available_datetimes`

### Current Implementation Status
- **Not explicitly updated** - The query appears to compute it on-the-fly
- **Broken query** - The `nj.datetime` reference will cause query failure
- **Likely intention**: Compute available datetimes at query time by joining with successful nc_jobs

---

## 5. DATABASE SCHEMA FILES & MIGRATIONS

### Schema Definition Location
- **Primary**: `/home/taimazb/Projects/OA/process/modules/db.py`
  - Function: `ensure_schema(conn)` - Idempotent schema initialization
  - Lines 24-157

### No Formal Migration System
- **No Alembic** or other migration framework detected
- **No separate SQL migration files** (.sql files in migrations/ directory)
- **Approach**: Python-based `ensure_schema()` that:
  - Creates tables if they don't exist (`CREATE TABLE IF NOT EXISTS`)
  - Handles column renames (dataset_id → base_url)
  - Converts column types (JSONB → timestamptz[] for available_datetimes)
  - Adds missing columns with `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`

### Idempotent Design
All schema operations check for existence before creating/modifying:
```python
# Examples from ensure_schema():
- "CREATE TABLE IF NOT EXISTS ..."
- "CREATE INDEX IF NOT EXISTS ..."
- "ALTER TABLE fields ADD COLUMN IF NOT EXISTS ..."
- "ALTER TYPE ... ADD VALUE IF NOT EXISTS ..."
```

### Related Files
- [db.py](process/modules/db.py) - Database initialization and helpers
- [variables.py](api/modules/variables.py) - Query that uses available_datetimes (BROKEN)
- [png_worker.py](process/modules/png_worker.py) - Updates nc_jobs status during processing
- [detector.py](process/modules/detector.py) - Creates nc_jobs rows
- [cli.py](process/modules/cli.py) - CLI interface for pipeline stages

---

## 6. RELATED TABLES & FOREIGN KEYS

### DATASETS Table
| Column | Type | Notes |
|--------|------|-------|
| id | SERIAL PRIMARY KEY | |
| base_url | TEXT UNIQUE NOT NULL | Base URL for ERDDAP/data source |
| title | TEXT | Dataset title |
| last_checked_at | TIMESTAMPTZ | Self-tracking |
| last_remote_time | TIMESTAMPTZ | Latest available data timestamp from source |
| last_downloaded_at | TIMESTAMPTZ | Last local download time |
| meta | JSONB | Additional metadata (source, bounds - **not auto schema'd**) |

### NC_FILE_STATUS Enum (PostgreSQL TYPE)
```
ENUM Values:
- pending_download → downloading → {failed_download | success_download}
- pending_compute → computing → {failed_compute | success_compute}
- pending_image → imaging → {failed_image | success_image}
```

### LIVE_OCEAN_RUNS Table (Related)
Separate tracking table for LiveOcean model outputs:
| Column | Type |
|--------|------|
| id | SERIAL PRIMARY KEY |
| run_date | DATE NOT NULL |
| status | live_ocean_status ENUM |
| input_path | TEXT |
| out_dir | TEXT |
| checksum | TEXT |
| attempts | INT |
| last_attempt | TIMESTAMPTZ |
| meta | JSONB |
| UNIQUE(run_date) | Constraint |

---

## 7. SUMMARY OF ISSUES & GAPS

### Issues Found
1. **CRITICAL**: `api/modules/variables.py` references `nj.datetime` column that **does not exist** in nc_jobs
2. **Missing Columns**: `min`, `max`, `depths_image`, `colormap` are queried but not defined in `ensure_schema()`
3. **Missing Columns**: `bounds`, `source` in datasets table are queried but not in schema
4. **No Type Safety**: Fields table allows NULL dataset_id with complex uniqueness handling

### What Datetime Field to Use
For populating `available_datetimes`, the query should use:
- **Most Likely**: `nj.start_time` - the beginning of the processing window
- **Alternative**: A computed datetime (e.g., midpoint: `(start_time + end_time)/2`)
- **NOT**: `end_time` (less intuitive for "datetime available")

### Recommended Fix
Replace the broken query in variables.py:
```sql
-- BROKEN:
ARRAY_AGG(DISTINCT nj.datetime ORDER BY nj.datetime) as available_datetimes

-- FIXED (Option 1 - use start_time):
ARRAY_AGG(DISTINCT nj.start_time ORDER BY nj.start_time) as available_datetimes

-- FIXED (Option 2 - use end_time):
ARRAY_AGG(DISTINCT nj.end_time ORDER BY nj.end_time) as available_datetimes
```

---

## 8. QUERY REFERENCES & USAGE PATTERNS

### How nc_jobs.status is used
- **Update operations**: `UPDATE nc_jobs SET status=%s, attempts=attempts+1, last_attempt=NOW()`
- **Query filtering**: `WHERE status = 'success_image'` (for available data)
- **Status promotion**: Groups promoted to `pending_image` when all variables have `success_download` or `success_compute`

### How available_datetimes is used (Frontend)
```javascript
// From front/app/stores/main.ts
variables: [] as Array<{
    var: string,
    source: string,
    dts: moment.Moment[],  // converted to moment.js
    colormap: string | null,
    colormapMin: number,
    colormapMax: number,
    depths: number[],
    precision: number
}>
```

---

## Schema Initialization Flow

1. **ensure_schema()** called (idempotent)
2. Creates/confirms ENUM types (nc_file_status, live_ocean_status)
3. Creates tables (datasets, fields, nc_jobs, live_ocean_runs)
4. Creates indexes
5. Checks for legacy columns and renames if needed
6. Adds missing columns with ALTER TABLE
7. Handles type conversions (JSONB → timestamptz[])
8. **All operations committed** if successful

---

