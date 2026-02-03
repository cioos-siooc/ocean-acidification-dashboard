import concurrent.futures
import requests
import time

URL = "http://localhost:9011/extract_climateTimeseries"
PAYLOAD = {
    "lat": 49.0468,
    "lon": -123.4174,
    "dt": "2023-01-27T12:00:00Z"
}

def make_request(i):
    start = time.time()
    try:
        resp = requests.post(URL, json=PAYLOAD, timeout=20)
        elapsed = time.time() - start
        print(f"Req {i}: Status {resp.status_code}, Time {elapsed:.2f}s")
        if resp.status_code == 200:
            data = resp.json()
            return f"OK ({len(data)} points)"
        else:
            return f"ERR {resp.text}"
    except Exception as e:
        return f"EXC {str(e)}"

print("Starting 10 concurrent requests...")
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(make_request, i) for i in range(10)]
    for future in concurrent.futures.as_completed(futures):
        print(future.result())
