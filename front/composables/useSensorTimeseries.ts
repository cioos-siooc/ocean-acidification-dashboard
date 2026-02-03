import axios from 'axios';
import { useRuntimeConfig } from '#app';

export async function getSensorTimeseries(sensorId: number|null, variable: string, opts?: { start?: string; end?: string; limit?: number }) {
    console.log('Fetching sensor timeseries for sensorId:', sensorId, 'variable:', variable, 'options:', opts);
    if (sensorId === null || sensorId === undefined) {
        return null
    }

    const config = useRuntimeConfig();
    const apiBaseUrl = config.public.apiBaseUrl;

    const params: any = { var: variable };
    if (opts?.start) params.start = opts.start;
    if (opts?.end) params.end = opts.end;
    if (opts?.limit) params.limit = opts.limit;

    const url = `${apiBaseUrl}/sensors/${sensorId}/timeseries`;
    const r = await axios.get(url, { params });
    return r;
}

export default getSensorTimeseries;
