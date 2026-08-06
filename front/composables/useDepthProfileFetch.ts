import axios from 'axios';
import { useRuntimeConfig } from '#app';
import type { BinMode } from './useTimeDepthWindow';

export type DepthProfileResponse = {
    time: string[];               // ISO bin-start strings at the requested resolution, ascending
    depths: number[];             // model depth levels (metres), ascending
    model: (number | null)[][];   // [depth][time]
    // [depth][time] — null where no cast landed in that bin; the whole grid is
    // null when no sensorId was requested (model-only callers).
    sensor: (number | null)[][] | null;
    // How far this grid cell's record reaches in the table the requested
    // resolution reads — hourly and daily are different tables, ~two decades
    // apart, so paging bounds can't be derived client-side. Either end is null
    // when the cell has no rows at all.
    coverage?: { from: string | null; to: string | null };
};

/**
 * Parse a `coverage` bound. The backend emits `datetime.isoformat()`, which
 * yields a bare date ("2007-01-01") for the daily table's Date column and a
 * full timestamp for the hourly table's DateTime — neither carrying a zone.
 * Both are UTC, so normalise before letting Date parse them: appending "Z" to
 * a bare date produces an invalid Date rather than midnight UTC.
 */
export function parseCoverageBound(value: string | null | undefined): Date | null {
    if (!value) return null;
    const iso = value.includes('T') ? value : `${value}T00:00:00`;
    const d = new Date(iso.endsWith('Z') ? iso : `${iso}Z`);
    return Number.isNaN(d.getTime()) ? null : d;
}

export type DepthProfileFetchParams = {
    source: string;
    var: string;
    lat: number;
    lon: number;
    fromDate: string;  // ISO-8601
    toDate: string;    // ISO-8601
    binMode: BinMode;
    /** Omit for a model-only grid. */
    sensorId?: string;
};

/**
 * POSTs to /depthProfile — the model's time-depth grid at the cell nearest
 * (lat, lon) for one bounded window, plus (when `sensorId` is given) that
 * profiler's raw casts binned onto the same grid.
 */
export async function fetchDepthProfile(params: DepthProfileFetchParams, signal?: AbortSignal): Promise<DepthProfileResponse> {
    const config = useRuntimeConfig();
    const apiBaseUrl = config.public.apiBaseUrl;

    const body: Record<string, unknown> = {
        source: params.source,
        var: params.var,
        lat: params.lat,
        lon: params.lon,
        fromDate: params.fromDate,
        toDate: params.toDate,
        binMode: params.binMode,
    };
    if (params.sensorId) body.sensorId = params.sensorId;

    const response = await axios.post(`${apiBaseUrl}/depthProfile`, body, { signal });
    return response.data;
}
