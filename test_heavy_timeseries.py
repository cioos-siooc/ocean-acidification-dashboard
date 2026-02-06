import concurrent.futures
import requests
import time

URL = "http://localhost:9011/extractTimeseries"
BASE_LAT = 49.2
BASE_LON = -123.7

def make_request(i):
    start = time.time()
    try:
        PAYLOAD = {
            "var": "temperature",
            "lat": BASE_LAT + (i * 0.01),  # Slightly vary lat for each request
            "lon": BASE_LON + (i * 0.01),  # Slightly vary lon for each request
            "depth": 0.5
        }
        resp = requests.post(URL, json=PAYLOAD, timeout=20)
        elapsed = time.time() - start
        print(f"Req {i}: Status {resp.status_code}, Time {elapsed:.2f}s")
        if resp.status_code == 200:
            data = resp.json()
            return f"OK ({len(data['time'])} points)"
        else:
            return f"ERR {resp.text}"
    except Exception as e:
        return f"EXC {str(e)}"

print("Starting 10 concurrent requests for /extractTimeseries...")
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(make_request, i) for i in range(10)]
    for future in concurrent.futures.as_completed(futures):
        print(future.result())
