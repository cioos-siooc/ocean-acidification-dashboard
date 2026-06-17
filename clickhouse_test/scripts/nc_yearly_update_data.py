import netCDF4 as nc
import numpy as np

VAR='omega_cal'
EXISTING = f"/media/taimaz/CIOOS/data/{VAR}/{VAR}_2012.nc"
NEW_DAY  = f"/home/taimaz/{VAR}_20120925.nc"

with nc.Dataset(NEW_DAY) as src, nc.Dataset(EXISTING, 'a') as dst:
    src_t = src.variables['time']
    dst_t = dst.variables['time']
    #
    # Re-encode time from source units → destination units (handles any format mismatch)
    decoded = nc.num2date(src_t[:], units=src_t.units,
                          calendar=getattr(src_t, 'calendar', 'standard'))
    reencoded = nc.date2num(decoded, units=dst_t.units,
                            calendar=getattr(dst_t, 'calendar', 'standard'))
    #
    i0 = dst_t.shape[0]
    n  = len(reencoded)
    dst_t[i0:i0+n] = reencoded
    dst.variables[VAR][i0:i0+n, ...] = src.variables[VAR][:]

print(f"Done. Appended {n} time steps at index {i0}. New total: {i0+n}")
