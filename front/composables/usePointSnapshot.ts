import axios from 'axios';
import { useRuntimeConfig } from '#app';
import { createRequestCache } from './useRequestCache';

export type SnapshotSeries = { time: string[]; value: (number | null)[] };

export type PointSnapshot = {
    to: string;
    hours: number;
    model: {
        depth: number | null;
        grid: { lat: number; lon: number; distanceKm: number };
        time: string[];
        values: Record<string, (number | null)[]>;
    } | null;
    /** Set when the point is outside the model domain (same payload as the 400s elsewhere). */
    modelError: { code: string; message: string; distanceKm: number } | null;
    sensor: {
        id: string;
        depth: number | null;
        variables: Record<string, SnapshotSeries & { last: { time: string; value: number | null } | null }>;
    } | null;
};

export type PointSnapshotParams = {
    lat: number;
    lon: number;
    /** The map's model depth. */
    depth: number;
    /** Window end (the map's instant), ISO. */
    to: string;
    hours?: number;
    sensorId?: string | null;
    sensorDepth?: number | null;
};

// Same TTL as the backend's model cache; playback steps revisit instants.
const cache = createRequestCache<PointSnapshot>(1_200_000);

/**
 * POSTs to /pointSnapshot — every variable at one point: the model's eight
 * at the map's depth, plus the selected sensor's own variables, each over the
 * trailing `hours` ending at `to`. Values come back in canonical units.
 */
export async function fetchPointSnapshot(params: PointSnapshotParams, signal?: AbortSignal): Promise<PointSnapshot> {
    const apiBaseUrl = useRuntimeConfig().public.apiBaseUrl;
    const key = JSON.stringify(params);
    return cache.fetch(key, async () => {
        const r = await axios.post(`${apiBaseUrl}/pointSnapshot`, {
            source: 'SalishSeaCast',
            lat: params.lat,
            lon: params.lon,
            depth: params.depth,
            to: params.to,
            hours: params.hours ?? 72,
            sensorId: params.sensorId ?? null,
            sensorDepth: params.sensorDepth ?? null,
        }, { signal });
        return r.data as PointSnapshot;
    });
}
