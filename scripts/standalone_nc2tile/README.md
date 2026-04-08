How to run:
uv run nc2tile.py --data temperature_20260330.nc --vars temperature --depth-indices 0,3,5,10,18,26,30,34 --precision 0.01 --outdir=../webp --grid grid.npz --fields fields.json --workers 2