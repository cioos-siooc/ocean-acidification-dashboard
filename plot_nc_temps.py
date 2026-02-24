#!/usr/bin/env python3
"""Plot temperature variables from a NetCDF file using xarray and matplotlib."""

import xarray as xr
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

# Open the NetCDF file
nc_file = "Baynes_5m.nc"  # Update path as needed
ds = xr.open_dataset(nc_file)

print("Available variables:", list(ds.data_vars))
print("Available dimensions:", list(ds.dims))

# Extract time and temperature variables
time = ds["time_UTC"].values
temp_sensor = ds["temperature_sensor"].values
temp_liveocean = ds["temperature_LiveOcean"].values
temp_ssc = ds["temperature_SSC"].values

# Convert timestamps to datetime for readable x-axis labels
# Assuming time_UTC is in seconds since epoch
time_dt = np.array([datetime.utcfromtimestamp(t) for t in time])

# Create the plot
fig, ax = plt.subplots(figsize=(14, 6))

ax.plot(time_dt, temp_sensor, label="Sensor", linewidth=2, color="black")
ax.plot(time_dt, temp_liveocean, label="LiveOcean", linewidth=2, color="red")
ax.plot(time_dt, temp_ssc, label="SalishSeaCast", linewidth=2, color="blue")

ax.set_xlabel("Time (UTC)", fontsize=12)
ax.set_ylabel("Temperature (°C)", fontsize=12)
ax.set_title("Temperature Comparison: Sensor vs Models", fontsize=14)
ax.legend(fontsize=11)
ax.grid(True, alpha=0.3)

# Format x-axis to show readable dates
fig.autofmt_xdate(rotation=45, ha="right")

plt.tight_layout()
plt.savefig("/opt/data/temperature_comparison.png", dpi=150)
print("Plot saved to /opt/data/temperature_comparison.png")

plt.show()

# Close the dataset
ds.close()
