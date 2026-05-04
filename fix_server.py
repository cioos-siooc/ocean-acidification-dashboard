import re

with open("api/SERVER.py", "r") as f:
    text = f.read()

# Add _extract_executor
text = text.replace(
    '_extract_semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTS)',
    '_extract_semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTS)\n_extract_executor = ProcessPoolExecutor(max_workers=MAX_CONCURRENT_EXTRACTS)'
)

text = text.replace(
    'from typing import Optional',
    'from typing import Optional\nfrom functools import partial\nimport contextvars'
)

# A safer approach: I will write a custom run_in_process function that wraps it!

text = text.replace(
    'from starlette.concurrency import run_in_threadpool',
    '''from starlette.concurrency import run_in_threadpool

async def run_in_process(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_extract_executor, partial(func, *args, **kwargs))
'''
)

# Now, we just replace `run_in_threadpool(` with `run_in_process(` for the extraction tasks
# Be careful to only replace it for the extractions, not the DB fetches which can remain in threads.

target_funcs = ['extract_timeseries', 'extract_profile', 'extract_eval_data', 
                'extract_climate_timeseries', 'extract_minmax', 'extract_sensor_timeseries']

for func in target_funcs:
    text = text.replace(f'run_in_threadpool({func}', f'run_in_process({func}')
    text = text.replace(f'run_in_threadpool(\n                {func}', f'run_in_process(\n                {func}')

with open("api/SERVER.py", "w") as f:
    f.write(text)

