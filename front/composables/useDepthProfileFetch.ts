import axios from 'axios';
import { useRuntimeConfig } from '#app';

export type DepthProfileResponse = {
    time: string[];               // ISO hourly bin-start strings, ascending
    depths: number[];             // model depth levels (metres), ascending
    model: (number | null)[][];   // [depth][time]
    sensor: (number | null)[][];  // [depth][time] — null where no cast landed in that bin
};

export type DepthProfileFetchParams = {
    source: string;
    var: string;
    sensorId: string;
    lat: number;
    lon: number;
    fromDate: string;  // ISO-8601
    toDate: string;    // ISO-8601
};

/** POSTs to /depthProfile — a profiler sensor's raw casts binned onto the model's own depth levels and hourly time buckets, for one bounded window. */
export async function fetchDepthProfile(params: DepthProfileFetchParams, signal?: AbortSignal): Promise<DepthProfileResponse> {
    const config = useRuntimeConfig();
    const apiBaseUrl = config.public.apiBaseUrl;

    const body = {
        source: params.source,
        var: params.var,
        sensorId: params.sensorId,
        lat: params.lat,
        lon: params.lon,
        fromDate: params.fromDate,
        toDate: params.toDate,
    };

    const response = await axios.post(`${apiBaseUrl}/depthProfile`, body, { signal });
    return response.data;
}
