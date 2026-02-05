import concurrent.futures
import requests
import time

URL1 = "http://localhost:9011/extractTimeseries"
URL2 = "http://localhost:9011/extract_climateTimeseries"
BASE_LAT = 49.0468
BASE_LON = -123.4174

def make_request(i):
    url = URL1 if i % 2 == 0 else URL2
    start = time.time()
    try:
        if url == URL1:
            PAYLOAD = {
                "var": "temperature",
                "lat": BASE_LAT,
                "lon": BASE_LON,
                "depth": 0.5
            }
        else:
            PAYLOAD = {
                "var": "temperature",
                "lat": BASE_LAT,
                "lon": BASE_LON,
                "dt": "2023-08-01T12:00:00"
            }
            
        resp = requests.post(url, json=PAYLOAD, timeout=30)
        elapsed = time.time() - start
        print(f"Req {i} ({url.split('/')[-1]}): Status {resp.status_code}, Time {elapsed:.2f}s")
        return resp.status_code
    except Exception as e:
        print(f"Req {i} ({url.split('/')[-1]}): EXC {str(e)}")
        return "EXC"

print("Starting 10 concurrent mixed requests...")
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(make_request, i) for i in range(30)]
    for future in concurrent.futures.as_completed(futures):
        future.result()
