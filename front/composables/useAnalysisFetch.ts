import axios from 'axios';
import { useRuntimeConfig } from '#app';
import { createRequestCache } from './useRequestCache';

export type SeriesPoint = { time: string; value: number | null };

export type AnalysisLocation =
    | { lat: number; lon: number }
    | { polygon: [number, number][] }
    | { sensorId: string };

export type AnalysisFetchParams = {
    variable: string;
    stat: 'min' | 'mean' | 'max';
    depth: number;
    location: AnalysisLocation;
    yearRange: [number, number];
};

// Matches the backend's `_model_cache` TTL (api/modules/response_cache.py) —
// one TTL regardless of whether `location` is a point or a sensorId, same as
// the backend's `analysis/timeseries` handler.
const cache = createRequestCache<SeriesPoint[]>(1_200_000);

/** POSTs to /analysis/timeseries and returns the raw daily series (no aggregation beyond what the API already does). */
export async function fetchAnalysisSeries(params: AnalysisFetchParams): Promise<SeriesPoint[]> {
    const config = useRuntimeConfig();
    const apiBaseUrl = config.public.apiBaseUrl;

    const body = {
        depth: params.depth,
        primaryMetric: { variable: params.variable, stat: params.stat },
        temporal: { yearRange: params.yearRange },
        ...params.location,
    };

    const key = JSON.stringify(params);
    return cache.fetch(key, async () => {
        const response = await axios.post(`${apiBaseUrl}/analysis/timeseries`, body);
        return response.data?.data || [];
    });
}
