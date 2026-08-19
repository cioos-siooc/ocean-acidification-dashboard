import axios from 'axios';
import { useRuntimeConfig } from '#app';
import { createRequestCache } from './useRequestCache';
import { useVariableRegistry } from './useVariableRegistry';

// ReplacingMergeTree read via FINAL, revised outside the SSC sync path by the
// separate `sensors/` ingestion service — short TTL bounds staleness, same as
// the backend's `_sensor_cache` (api/modules/response_cache.py).
const cache = createRequestCache<any>(60_000);

export async function getSensorTimeseries(sensorId: string|null, canonicalVariable: string, fromDate: string, toDate: string, depth: number|null = null, source: string|null = null) {
    if (sensorId === null || sensorId === undefined) {
        return null
    }

    const config = useRuntimeConfig();
    const apiBaseUrl = config.public.apiBaseUrl;

    const payload: any = {
        modelVariable: canonicalVariable,  // Model variable name (e.g., "dissolved_oxygen")
        sensorId: sensorId,
        fromDate: fromDate,
        toDate: toDate
    };
    if (depth !== null) {
        payload.depth = depth;
    }
    // Only needed to resolve depth for variable-depth ("profiler") sensors — see
    // extractSensorTimeseries.py's profiler branch.
    if (source !== null) {
        payload.source = source;
    }
    const url = `${apiBaseUrl}/sensorTimeseries`;
    const key = JSON.stringify(payload);
    const raw = await cache.fetch(key, () => axios.post(url, payload));

    // Every caller reads `.data.time`/`.data.value` off the axios-response
    // shape this returns — preserve that, just with `value` converted to the
    // currently-selected display unit. Cached value is canonical (keyed only
    // by request params, not by unit), so build a fresh object rather than
    // mutating `raw` in place.
    if (!raw?.data?.value) return raw;
    const { toDisplayValue } = useVariableRegistry();
    return {
        ...raw,
        data: {
            ...raw.data,
            value: raw.data.value.map((v: number | null) => toDisplayValue(canonicalVariable, v)),
        },
    };
}

export default getSensorTimeseries;
