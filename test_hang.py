import asyncio
import time
from starlette.concurrency import run_in_threadpool

def slow_sync_task(task_id):
    print(f"Task {task_id} starting")
    time.sleep(5)
    print(f"Task {task_id} finished")

async def main():
    print("Main starting")
    try:
        await asyncio.wait_for(run_in_threadpool(slow_sync_task, 1), timeout=2.0)
    except asyncio.TimeoutError:
        print("Task 1 timed out in asyncio")

    # Let's see if Task 1 continues running in the background
    await asyncio.sleep(4)
    print("Main done")

asyncio.run(main())
