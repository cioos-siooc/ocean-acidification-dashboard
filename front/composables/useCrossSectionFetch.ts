import axios from 'axios';
import { useRuntimeConfig } from '#app';
import type { BinMode } from './useTimeDepthWindow';
import { createRequestCache } from './useRequestCache';

export type CrossSectionResponse = {
    distances_km: number[];        // cumulative distance (km) at each sample point, ascending
    depths: number[];              // model depth levels (metres), ascending
    model: (number | null)[][];    // [depth][sample]
    vertex_distances_km: number[]; // cumulative distance (km) at each original drawn vertex
    points: [number, number][];    // [lat, lon] of each resampled sample point, same order as distances_km
};

export type CrossSectionFetchParams = {
    source: string;
    var: string;
    vertices: { lat: number; lng: number }[];
    dt: string;   // full timestamp for "hourly", "YYYY-MM-DD" for "daily", "YYYY-MM" for "monthly"
    binMode: BinMode;
};

// Model-only — a cross-section has no sensor overlay in v1, so there's no
// short-TTL half to split off the way useDepthProfileFetch.ts does.
const modelCache = createRequestCache<CrossSectionResponse>(1_200_000);

/**
 * POSTs to /crossSection — the model's depth-vs-distance grid along an
 * arbitrary drawn polyline, at one time snapshot.
 */
export async function fetchCrossSection(params: CrossSectionFetchParams): Promise<CrossSectionResponse> {
    const config = useRuntimeConfig();
    const apiBaseUrl = config.public.apiBaseUrl;

    const body = {
        source: params.source,
        var: params.var,
        vertices: params.vertices.map((p) => [p.lat, p.lng]),
        dt: params.dt,
        binMode: params.binMode,
    };

    const key = JSON.stringify(body);
    return modelCache.fetch(key, async () => {
        const response = await axios.post(`${apiBaseUrl}/crossSection`, body);
        return response.data;
    });
}
