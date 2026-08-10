import axios from 'axios';
import { useRuntimeConfig } from '#app';
import { createRequestCache } from './useRequestCache';

type ClimateTimeseriesParams = {
    variable: string;
    lat: number;
    lon: number;
    depth: number | null;
    fromDate: string;
    toDate: string;
};

// Climatology, always model-backed — matches the backend's `_model_cache` TTL.
const cache = createRequestCache<any>(1_200_000);

/**
 * POSTs to /extract_climateTimeseries — the day-of-year climatological
 * min/mean/max envelope at a point and depth, over a date window.
 *
 * Returns the raw axios response so callers can hand `.data` straight to
 * TimeseriesChart's `plot()`, which expects rows of
 * `{ requested_date, min, mean, max }`.
 */
export function fetchClimateTimeseries(params: ClimateTimeseriesParams) {
    const config = useRuntimeConfig();
    const apiBaseUrl = config.public.apiBaseUrl;

    const key = JSON.stringify(params);
    return cache.fetch(key, () => axios.post(`${apiBaseUrl}/extract_climateTimeseries`, {
        var: params.variable,
        lat: params.lat,
        lon: params.lon,
        depth: params.depth,
        fromDate: params.fromDate,
        toDate: params.toDate,
    }));
}
