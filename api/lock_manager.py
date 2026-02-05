import threading

# A process-wide lock to protect non-thread-safe NetCDF4/xarray I/O
io_lock = threading.Lock()
