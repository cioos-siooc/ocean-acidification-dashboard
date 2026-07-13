// Mirrors the color logic in useBuoyLayer.ts / pages/index.vue so non-map
// UI (e.g. the sensor list) can match the map marker colors.
export const REALTIME_THRESHOLD_MS = 14 * 24 * 60 * 60 * 1000; // 14 days

export const SENSOR_STATUS_COLOR = {
    realtime: '#66BB6A', // active, data within last 14 days
    active: '#FFA726',   // active, only historical data
    inactive: '#888888',
} as const;

export function sensorIsRealtime(sensor: { latest_data_at: string | null }): boolean {
    return !!sensor.latest_data_at && (Date.now() - new Date(sensor.latest_data_at).getTime()) < REALTIME_THRESHOLD_MS;
}

export function sensorStatusColor(sensor: { active: boolean; latest_data_at: string | null }): string {
    if (!sensor.active) return SENSOR_STATUS_COLOR.inactive;
    return sensorIsRealtime(sensor) ? SENSOR_STATUS_COLOR.realtime : SENSOR_STATUS_COLOR.active;
}
