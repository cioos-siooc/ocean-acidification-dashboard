## Download daily from 2007-01-01 to 2026-01-01 via ERDDAP

export START_DATE=2007-01-01
export deltaDay=5

function dl() {
    i=$1
    FROM_DATE=$(date -I -d "$START_DATE + $i days")
    TO_DATE=$(date -I -d "$FROM_DATE + $deltaDay days - 1 day")
    if [ -f "temperature_${FROM_DATE}_${TO_DATE}_zip.nc" ]; then
        echo "temperature_${FROM_DATE}_${TO_DATE}_zip.nc already exists, skipping."
        return
    fi
    echo "Downloading temperature for ${FROM_DATE} to ${TO_DATE}..."
    wget -O "temperature_${FROM_DATE}_${TO_DATE}.nc" "https://salishsea.eos.ubc.ca/erddap/griddap/ubcSSg3DPhysicsFields1hV21-11.nc?temperature%5B(${FROM_DATE}T00:30:00Z):1:(${TO_DATE}T23:30:00Z)%5D%5B(0.5000003):1:(10.5047655)%5D%5B(0.0):1:(897.0)%5D%5B(0.0):1:(397)%5D"
    cdo -f nc4 -z zip_4 copy "temperature_${FROM_DATE}_${TO_DATE}.nc" "temperature_${FROM_DATE}_${TO_DATE}_zip.nc" && rm "temperature_${FROM_DATE}_${TO_DATE}.nc"
}
export -f dl


# 2007-01-01 -> 2025-12-31 (6939 days)
parallel -j 2 dl ::: $(seq 0 5 6938)