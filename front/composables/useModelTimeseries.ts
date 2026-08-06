import axios from 'axios';
import { useRuntimeConfig } from '#app';

/**
 * POSTs to /extractTimeseries — the model's raw hourly series at one point,
 * depth and time window.
 *
 * The daily path (`fetchAnalysisSeries`, off SalishSeaCast_daily) is the right
 * tool for multi-year views, but it cannot show a tidal cycle. This is what the
 * Comparison tab's hourly resolution uses to put model and sensor side by side
 * at their native cadence.
 */
export async function fetchModelTimeseries(params: {
    source: string;
    variable: string;
    depth: number | null;
    lat: number;
    lon: number;
    fromDate: string;
    toDate: string;
}, signal?: AbortSignal): Promise<{ time: string[]; value: (number | null)[] }> {
    const config = useRuntimeConfig();
    const apiBaseUrl = config.public.apiBaseUrl;

    const response = await axios.post(`${apiBaseUrl}/extractTimeseries`, {
        source: params.source,
        var: params.variable,
        depth: params.depth,
        lat: params.lat,
        lon: params.lon,
        fromDate: params.fromDate,
        toDate: params.toDate,
    }, { signal });

    return {
        time: response.data?.time ?? [],
        // Note the singular key: /extractTimeseries returns `value`, not `values`.
        value: response.data?.value ?? [],
    };
}
