import axios from 'axios';
import { useRuntimeConfig } from '#app';

export type GridCell = {
    lat: number;
    lon: number;
    distanceKm: number;
    // Water depth (m) of the cell; null for a source without a bottom layer.
    h: number | null;
};

/** POSTs to /gridCell — the model cell nearest (lat, lon) and its water depth. */
export async function fetchGridCell(source: string, lat: number, lon: number): Promise<GridCell> {
    const { apiBaseUrl } = useRuntimeConfig().public;
    const response = await axios.post(`${apiBaseUrl}/gridCell`, { source, lat, lon });
    return response.data;
}

/**
 * The model level to show for a sensor at `sensorDepth`. With a bottom layer
 * (-1 in `levels`) and the cell's water depth known, "bottom" competes as a
 * level at the seabed and fixed levels below the seabed (no data) drop out —
 * so a mooring 60 m above a 1316 m seabed gets "bottom", not 1500 m. Otherwise
 * the numerically nearest fixed level.
 */
export function pickSensorLevel(sensorDepth: number, levels: number[], seabed: number | null): number | null {
    const fixed = levels.filter(d => d >= 0);
    const hasBottom = levels.includes(-1) && seabed != null;
    const candidates: { level: number, at: number }[] = fixed
        .filter(d => !hasBottom || d <= seabed!)
        .map(d => ({ level: d, at: d }));
    // After the fixed levels, so a tie keeps the fixed level.
    if (hasBottom) candidates.push({ level: -1, at: seabed! });
    if (!candidates.length) return null;
    return candidates.reduce((best, c) =>
        Math.abs(c.at - sensorDepth) < Math.abs(best.at - sensorDepth) ? c : best).level;
}
