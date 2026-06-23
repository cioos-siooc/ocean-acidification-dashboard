from datetime import date
import os
from SSC.db import get_client, ensure_schema, mark_success, get_row
from SSC.config import (
    DOWNLOAD_VARIABLES, COMPUTE_VARIABLES, NC_BASE_DIR,
    STATUS_SUCCESS_DOWNLOAD, STATUS_SUCCESS_COMPUTE, STATUS_SUCCESS_SYNC
)

dates = [date(2026, 6, d) for d in range(1, 20)]  # <- fill in your actual dates
nc_dir = os.getenv('SSC_NC_DIR', NC_BASE_DIR)

def nc_path(var, d):
    return os.path.join(nc_dir, var, f'{var}_{d.strftime("%Y%m%d")}.nc')

client = get_client()
ensure_schema(client)

for d in dates:
    for var, status in [(v, STATUS_SUCCESS_DOWNLOAD) for v in DOWNLOAD_VARIABLES] + \
                        [(v, STATUS_SUCCESS_COMPUTE) for v in COMPUTE_VARIABLES]:
        path = nc_path(var, d)
        if not os.path.exists(path):
            print(f'MISSING: {path} — not marking {var} for {d}')
            continue
        row = get_row(client, d, var) or {}
        mark_success(client, d, var, status, nc_path=path, attempts=row.get('attempts', 0))
        print(f'Marked {var} {d} -> {status}')

client.close()