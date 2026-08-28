import axios from 'axios';
import { useRuntimeConfig } from '#app';
import { createRequestCache } from './useRequestCache';
import { useVariableRegistry } from './useVariableRegistry';

type ModelTimeseriesParams = {
    source: string;
    variable: string;
    depth: number | null;
    lat: number;
    lon: number;
    fromDate: string;
    toDate: string;
};

// Matches the backend's `_model_cache` TTL (api/modules/response_cache.py).
const cache = createRequestCache<{ time: string[]; value: (number | null)[] }>(1_200_000);

/**
 * POSTs to /extractTimeseries — the model's raw hourly series at one point,
 * depth and time window.
 *
 * The daily path (`fetchAnalysisSeries`, off SalishSeaCast_daily) is the right
 * tool for multi-year views, but it cannot show a tidal cycle. This is what the
 * Comparison tab's hourly resolution uses to put model and sensor side by side
 * at their native cadence.
 */
export async function fetchModelTimeseries(params: ModelTimeseriesParams): Promise<{ time: string[]; value: (number | null)[] }> {
    const config = useRuntimeConfig();
    const apiBaseUrl = config.public.apiBaseUrl;

    const key = JSON.stringify(params);
    const raw = await cache.fetch(key, async () => {
        const response = await axios.post(`${apiBaseUrl}/extractTimeseries`, {
            source: params.source,
            var: params.variable,
            depth: params.depth,
            lat: params.lat,
            lon: params.lon,
            fromDate: params.fromDate,
            toDate: params.toDate,
        });

        return {
            time: response.data?.time ?? [],
            // Note the singular key: /extractTimeseries returns `value`, not `values`.
            value: response.data?.value ?? [],
        };
    });

    // Cached value is canonical (keyed only by request params, not by unit) —
    // convert a fresh copy on every call so switching units doesn't require a
    // cache-busting refetch, and never touch the cached array in place.
    const { toDisplayValue } = useVariableRegistry();
    return { time: raw.time, value: raw.value.map((v) => toDisplayValue(params.variable, v)) };
}
