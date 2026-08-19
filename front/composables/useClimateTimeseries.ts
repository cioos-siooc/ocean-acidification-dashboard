import axios from 'axios';
import { useRuntimeConfig } from '#app';
import { createRequestCache } from './useRequestCache';
import { useVariableRegistry } from './useVariableRegistry';

type ClimateTimeseriesParams = {
    variable: string;
    lat: number;
    lon: number;
    depth: number | null;
    fromDate: string;
    toDate: string;
    binMode?: 'hourly' | 'daily' | 'monthly';
};

// Climatology, always model-backed — matches the backend's `_model_cache` TTL.
const cache = createRequestCache<any>(1_200_000);

/**
 * POSTs to /extract_climateTimeseries — the climatological min/mean/max
 * envelope at a point and depth, over a date window. `binMode: 'monthly'`
 * requests month-of-year aggregation (12 points/year) instead of the default
 * day-of-year one (365 points/year), so the overlay's resolution matches the
 * monthly view's own coarse bins rather than showing day-level texture
 * stretched across a 20-year window.
 *
 * Returns the raw axios response so callers can hand `.data` straight to
 * TimeseriesChart's `plot()`, which expects rows of
 * `{ requested_date, min, mean, max }` — `mean`/`min`/`max` converted to the
 * currently-selected display unit here, each independently (not `max - min`
 * as a lump sum), so a consumer that re-derives a range from them still gets
 * the right answer under an affine unit like temperature's °F offset.
 */
export async function fetchClimateTimeseries(params: ClimateTimeseriesParams) {
    const config = useRuntimeConfig();
    const apiBaseUrl = config.public.apiBaseUrl;

    const key = JSON.stringify(params);
    const raw = await cache.fetch(key, () => axios.post(`${apiBaseUrl}/extract_climateTimeseries`, {
        var: params.variable,
        lat: params.lat,
        lon: params.lon,
        depth: params.depth,
        fromDate: params.fromDate,
        toDate: params.toDate,
        bin_mode: params.binMode === 'monthly' ? 'monthly' : 'daily',
    }));

    if (!Array.isArray(raw?.data)) return raw;
    const { toDisplayValue } = useVariableRegistry();
    return {
        ...raw,
        data: raw.data.map((row: { requested_date: string; mean: number | null; min: number | null; max: number | null }) => ({
            ...row,
            mean: toDisplayValue(params.variable, row.mean),
            min: toDisplayValue(params.variable, row.min),
            max: toDisplayValue(params.variable, row.max),
        })),
    };
}
