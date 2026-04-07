### Ocean Acidification Dashboard — nc2tile processing script
# This script processes netCDF files into web tiles using nc2tile.py.
# It iterates over specified variables and precision settings, converting each relevant netCDF file into tiles.

function process {
    V=$1
    P=$2
    
    # for f in `ls ../data/${V}/ | grep -v bottom`; do
	#     uv run nc2tile.py --data ../data/${V}/$f --vars ${V} --depth-indices 0,3,5,10,18,26,30,34 --precision $P --outdir=../webp --grid grid.npz --fields fields.json --workers 20
    # done

    for f in `ls ../data/${V}/*bottom.nc`; do
	    uv run nc2tile.py --data $f --vars ${V} --depth-indices 0 --precision $P --outdir=../webp --grid grid.npz --fields fields.json --workers 24
    done
}


VAR=temperature
PRECISION=0.01
process $VAR $PRECISION

VAR=salinity
PRECISION=0.01
process $VAR $PRECISION

VAR=omega_cal
PRECISION=0.0001
process $VAR $PRECISION

VAR=omega_arag
PRECISION=0.0001
process $VAR $PRECISION

VAR=ph_total
PRECISION=0.001
process $VAR $PRECISION

VAR=dissolved_inorganic_carbon
PRECISION=0.1
process $VAR $PRECISION

VAR=dissolved_oxygen
PRECISION=0.1
process $VAR $PRECISION

VAR=total_alkalinity
PRECISION=0.1
process $VAR $PRECISION
