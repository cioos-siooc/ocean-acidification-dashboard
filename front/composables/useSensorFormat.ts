// Text formatting shared by the sensor list (sensorInfo.vue) and the sensor
// metadata dialog (SensorInfoDialog.vue), so both describe a sensor identically.

export function coordTxt(lat: number, lng: number): string {
    const latStr = `${Math.abs(lat).toFixed(2)}°${lat >= 0 ? 'N' : 'S'}`;
    const lngStr = `${Math.abs(lng).toFixed(2)}°${lng >= 0 ? 'E' : 'W'}`;
    return `${latStr}, ${lngStr}`;
}

export function depth2txt(sensor: { depth: number, depth_min?: number | null, depth_max?: number | null }): string {
    const { depth, depth_min, depth_max } = sensor;
    if (depth == null || depth < 0) {
        if (depth_min != null && depth_max != null) {
            return `Variable depth (${depth_min.toFixed(0)}–${depth_max.toFixed(0)} m)`;
        }
        return 'Variable depth';
    }
    if (depth === 0) return 'Surface';
    return depth.toFixed(0) + ' m';
}

export function formatDataRange(sensor: { first_data_at?: string | null, latest_data_at?: string | null }): string {
    const fmt = (iso: string) => new Date(iso).toLocaleDateString('en-CA', { year: 'numeric', month: 'short' });
    const { first_data_at, latest_data_at } = sensor;
    if (!first_data_at && !latest_data_at) return 'No data';
    if (!first_data_at) return `Up to ${fmt(latest_data_at!)}`;
    if (!latest_data_at) return `From ${fmt(first_data_at)}`;
    const isRecent = Date.now() - new Date(latest_data_at).getTime() < 14 * 86400_000;
    return `${fmt(first_data_at)} – ${isRecent ? 'present' : fmt(latest_data_at)}`;
}
