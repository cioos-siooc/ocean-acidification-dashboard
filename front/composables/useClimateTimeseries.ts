import axios from 'axios';
import { useRuntimeConfig } from '#app';

/**
 * POSTs to /extract_climateTimeseries — the day-of-year climatological
 * min/mean/max envelope at a point and depth, over a date window.
 *
 * Returns the raw axios response so callers can hand `.data` straight to
 * TimeseriesChart's `plot()`, which expects rows of
 * `{ requested_date, min, mean, max }`.
 */
export function fetchClimateTimeseries(params: {
    variable: string;
    lat: number;
    lon: number;
    depth: number | null;
    fromDate: string;
    toDate: string;
}, signal?: AbortSignal) {
    const config = useRuntimeConfig();
    const apiBaseUrl = config.public.apiBaseUrl;

    return axios.post(`${apiBaseUrl}/extract_climateTimeseries`, {
        var: params.variable,
        lat: params.lat,
        lon: params.lon,
        depth: params.depth,
        fromDate: params.fromDate,
        toDate: params.toDate,
    }, { signal });
}
