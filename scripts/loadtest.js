// k6 load test for the ocean-acidification-dashboard API.
//
// Simulates a realistic browsing session (map load -> click a model point ->
// click a sensor -> occasionally open the Analysis Builder) rather than
// hammering one endpoint, so it exercises the real bottleneck: the
// MAX_CONCURRENT_EXTRACTS=4 semaphore/ProcessPoolExecutor gating
// extractTimeseries/sensorTimeseries/getMinMax/getProfile/depthProfile
// (see api/SERVER.py:42-44).
//
// Usage:
//   k6 run -e BASE_URL=http://localhost:9011 scripts/loadtest.js   # local dev
//   k6 run scripts/loadtest.js                                    # defaults to prod
//
// Via Docker instead of a local k6 install (run with -u $(id -u):$(id -g) if
// mounting a directory the container's non-root user can't otherwise read):
//   docker run --rm -i --network host -u $(id -u):$(id -g) \
//     -v $(pwd)/scripts:/scripts grafana/k6 run /scripts/loadtest.js
//
// To run from k6 Cloud instead of your own machine (recommended so the load
// generator isn't competing with the API/DB for the same 8 cores/12GB):
//   k6 login cloud    # one-time, needs a free Grafana Cloud account
//   k6 cloud run scripts/loadtest.js

import http from 'k6/http';
import { check, group, sleep } from 'k6';
import { Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'https://oceaneco-api.cioospacific.ca';

// Per-endpoint latency, tracked separately from k6's aggregate http_req_duration
// so a slow analysis query doesn't hide a queued/timed-out extractTimeseries call.
const extractTimeseriesTrend = new Trend('extract_timeseries_duration');
const sensorTimeseriesTrend = new Trend('sensor_timeseries_duration');
const analysisTrend = new Trend('analysis_duration');
const tileTrend = new Trend('tile_duration');

export const options = {
  stages: [
    { duration: '30s', target: 1 },   // warm-up / sanity check
    { duration: '1m', target: 5 },
    { duration: '2m', target: 10 },   // the number you actually care about
    { duration: '2m', target: 10 },   // hold at 10 to see if it's stable, not just a spike
    { duration: '1m', target: 20 },   // push past it to find the ceiling
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],           // <1% errors overall
    'extract_timeseries_duration': ['p(95)<8000'], // semaphore timeout is 10s server-side
    'sensor_timeseries_duration': ['p(95)<8000'],
    'analysis_duration': ['p(95)<15000'],       // not semaphore-gated, but does its own CH aggregation
  },
};

const DEPTHS = [0.5, 1.5, 2.5, 4.5, 9.5, 18.0];

// Picking uniformly within the model's lat/lon bounding box would land on
// dry coastline a lot of the time (the Salish Sea bbox includes plenty of
// land) and produce legitimate 400s ("no marine grid cell found") that look
// like errors but aren't a capacity signal. Sensor buoy locations are
// guaranteed wet, so jitter around those instead of the raw bbox.
function randomPoint(sensors) {
  const s = pick(sensors);
  const jitter = () => (Math.random() - 0.5) * 0.02; // ~1km wobble
  return { lat: s.latitude + jitter(), lon: s.longitude + jitter() };
}

function pick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function toDaysAgoISO(dateStr, days) {
  const d = new Date(dateStr);
  d.setUTCDate(d.getUTCDate() - days);
  return d.toISOString().slice(0, 10);
}

// Fetched once before the load starts, not per-VU-iteration, so the script
// self-configures against whatever BASE_URL you point it at (dev or prod)
// instead of hardcoding dates/sensors that will go stale.
export function setup() {
  const varsRes = http.get(`${BASE_URL}/variables`);
  if (varsRes.status !== 200) {
    throw new Error(`GET /variables failed during setup: ${varsRes.status} ${varsRes.body}`);
  }
  const varsData = varsRes.json();
  const variableNames = varsData.map((v) => v.var);
  const latestDt = varsData[0].dts[varsData[0].dts.length - 1]; // e.g. "2026-07-27T23:30:00"
  const latestDate = latestDt.slice(0, 10);
  const fromDate = toDaysAgoISO(latestDate, 14);

  const sensorsRes = http.get(`${BASE_URL}/sensors`);
  if (sensorsRes.status !== 200) {
    throw new Error(`GET /sensors failed during setup: ${sensorsRes.status} ${sensorsRes.body}`);
  }
  const sensors = sensorsRes.json().slice(0, 15); // small realistic pool, not all ~50
  const latestYear = new Date(latestDate).getUTCFullYear();

  return { variableNames, latestDt, latestDate, fromDate, sensors, latestYear };
}

export default function (data) {
  const { variableNames, latestDt, latestDate, fromDate, sensors, latestYear } = data;

  group('page load', function () {
    // one map tile fetch to mimic the initial render; the frontend fetches
    // many, this is enough to add realistic file-IO load without spamming
    const depth = pick(DEPTHS);
    const variable = pick(variableNames);
    const tileDt = latestDt.replace(/:/g, ''); // matches useMapAnimator.ts's dayjs 'YYYY-MM-DDTHHmmss'
    const res = http.get(
      `${BASE_URL}/png/SalishSeaCast/${variable}/${tileDt}/${depth}`,
      { tags: { name: 'png_tile' } }
    );
    tileTrend.add(res.timings.duration);
    // 404 is expected if this environment's images aren't synced (e.g. a dev
    // box without the image mount) -- only fail loudly on server errors.
    check(res, { 'tile: no server error': (r) => r.status < 500 });
  });

  sleep(Math.random() * 2 + 1); // think time before clicking the map

  group('extractTimeseries (map point click)', function () {
    const { lat, lon } = randomPoint(sensors);
    const payload = JSON.stringify({
      source: 'SalishSeaCast',
      var: pick(variableNames),
      lat,
      lon,
      depth: pick(DEPTHS),
      fromDate,
      toDate: latestDate,
    });
    const res = http.post(`${BASE_URL}/extractTimeseries`, payload, {
      headers: { 'Content-Type': 'application/json' },
      tags: { name: 'extractTimeseries' },
    });
    extractTimeseriesTrend.add(res.timings.duration);
    check(res, {
      'extractTimeseries: status 200': (r) => r.status === 200,
      'extractTimeseries: not semaphore-timed-out': (r) => r.status !== 503 && r.status !== 504,
    });
  });

  sleep(Math.random() * 2 + 1);

  // ~50% of sessions also check a sensor, like clicking one in the sensor list
  if (Math.random() < 0.5 && sensors.length > 0) {
    group('sensorTimeseries (sensor click)', function () {
      const sensor = pick(sensors);
      const variableKeys = Object.keys(sensor.variables || {});
      const modelVariable = variableKeys.length > 0 ? pick(variableKeys) : pick(variableNames);
      const payload = JSON.stringify({
        sensorId: sensor.id,
        modelVariable,
        fromDate,
        toDate: latestDate,
        source: 'SalishSeaCast',
      });
      const res = http.post(`${BASE_URL}/sensorTimeseries`, payload, {
        headers: { 'Content-Type': 'application/json' },
        tags: { name: 'sensorTimeseries' },
      });
      sensorTimeseriesTrend.add(res.timings.duration);
      check(res, {
        'sensorTimeseries: status 200': (r) => r.status === 200,
        'sensorTimeseries: not semaphore-timed-out': (r) => r.status !== 503 && r.status !== 504,
      });
    });
    sleep(Math.random() * 2 + 1);
  }

  // ~20% of sessions open the Analysis Builder tab (heavier, full-history query)
  if (Math.random() < 0.2) {
    group('analysis/timeseries (Analysis Builder)', function () {
      const { lat, lon } = randomPoint(sensors);
      const payload = JSON.stringify({
        lat,
        lon,
        depth: pick(DEPTHS),
        primaryMetric: { variable: pick(variableNames), stat: 'mean' },
        // year_range is read as request.temporal["yearRange"] server-side (SERVER.py:806)
        // with no pydantic validation on the dict shape -- omitting it 500s.
        temporal: { yearRange: [latestYear, latestYear] },
      });
      const res = http.post(`${BASE_URL}/analysis/timeseries`, payload, {
        headers: { 'Content-Type': 'application/json' },
        tags: { name: 'analysis_timeseries' },
      });
      analysisTrend.add(res.timings.duration);
      check(res, { 'analysis: status 200': (r) => r.status === 200 });
    });
  }

  sleep(Math.random() * 3 + 2); // idle before next simulated action
}
