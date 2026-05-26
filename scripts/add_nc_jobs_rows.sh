# 077bd12c-5651-4c1b-9095-a4c35cdd3f43      temperature salinity
# 291593b8-93c1-4323-a07d-91d396f8ed8d      dissolved_oxygen dissolved_inorganic_carbon total_alkalinity omega_arag omega_cal ph_total

for m in {01..04}; do
    start_date=$(date -d "2026-${m}-01" +"%Y-%m-%d")
    end_date=$(date -d "2026-${m}-01 +1 month -1 sec" +"%Y-%m-%d")
    for v in dissolved_oxygen dissolved_inorganic_carbon total_alkalinity omega_arag omega_cal ph_total; do
        python scripts/add_nc_jobs_rows.py \
            --dataset-id 291593b8-93c1-4323-a07d-91d396f8ed8d \
            --variable "$v" \
            --start "$start_date 00:00:00" \
            --end "$end_date 23:59:59" \
            --db-host localhost \
            --db-port 9012
    done
done
