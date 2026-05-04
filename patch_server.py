with open('api/SERVER.py', 'r') as f:
    t = f.read()

# Add _extract_executor
t = t.replace(
    '_extract_semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTS)',
    '_extract_semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTS)\n_extract_executor = ProcessPoolExecutor(max_workers=MAX_CONCURRENT_EXTRACTS)'
)

# replace run_in_threadpool(func, arg1=...) with get_running_loop().run_in_executor(_extract_executor, partial(func, arg1=...))
import re
for func in ['extract_sensor_timeseries', 'extract_timeseries', 'extract_climate_timeseries', 'extract_minmax', 'get_monthly_climatology_profile', 'extract_profile', 'extract_eval_data']:
    t = re.sub(
        rf'await\s+run_in_threadpool\s*\(\s*{func}\s*,',
        rf'await asyncio.get_running_loop().run_in_executor(_extract_executor, partial({func},',
        t
    )

with open('api/SERVER.py', 'w') as f:
    f.write(t)

