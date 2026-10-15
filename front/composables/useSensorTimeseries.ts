import axios from 'axios';
import { useRuntimeConfig } from '#app';
import { createRequestCache } from './useRequestCache';
import { useVariableRegistry } from './useVariableRegistry';

// ReplacingMergeTree read via FINAL, revised outside the SSC sync path by the
// separate `sensors/` ingestion service — short TTL bounds staleness, same as
// the backend's `_sensor_cache` (api/modules/response_cache.py).
const cache = createRequestCache<any>(60_000);

export async function getSensorTimeseries(sensorId: string|null, canonicalVariable: string, fromDate: string, toDate: string, depth: number|null = null, source: string|null = null, instrument: string|null = null) {
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
    // One instrument's own series (a `sensorInstruments()` id); without it the
    // API merges a multi-instrument sensor's series by catalog priority.
    if (instrument !== null) {
        payload.instrument = instrument;
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

export interface SensorInstrument {
    /** `sensor_timeseries.source` value, e.g. `CRIP.C1/CTD` — what the API filters on. */
    id: string;
    /** Display label, e.g. `CTD · CRIP.C1`. */
    label: string;
}

/**
 * The instruments that report `variable` at a sensor, highest priority first
 * (`device_config.sources`, from the sensor's catalog entry). Most sensors
 * have one; ONC sites with child locations can have several.
 */
export function sensorInstruments(
    sensor: { device_config?: { sources?: Record<string, Array<Record<string, string>>> } } | null | undefined,
    variable: string,
): SensorInstrument[] {
    return (sensor?.device_config?.sources?.[variable] ?? [])
        .filter(s => s?.locationCode && s?.deviceCategoryCode)
        .map(s => ({ id: `${s.locationCode}/${s.deviceCategoryCode}`, label: `${s.deviceCategoryCode} · ${s.locationCode}` }));
}

export default getSensorTimeseries;
