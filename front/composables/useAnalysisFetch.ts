import axios from 'axios';
import { useRuntimeConfig } from '#app';

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

/** POSTs to /analysis/timeseries and returns the raw daily series (no aggregation beyond what the API already does). */
export async function fetchAnalysisSeries(params: AnalysisFetchParams, signal?: AbortSignal): Promise<SeriesPoint[]> {
    const config = useRuntimeConfig();
    const apiBaseUrl = config.public.apiBaseUrl;

    const body = {
        depth: params.depth,
        primaryMetric: { variable: params.variable, stat: params.stat },
        temporal: { yearRange: params.yearRange },
        ...params.location,
    };

    const response = await axios.post(`${apiBaseUrl}/analysis/timeseries`, body, { signal });
    return response.data?.data || [];
}
