import axios from 'axios';
import { useRuntimeConfig } from '#app';

export async function getSensorTimeseries(sensorId: number|null, variable: string) {
    console.log('Fetching sensor timeseries for sensorId:', sensorId, 'variable:', variable);
    if (sensorId === null || sensorId === undefined) {
        return null
    }

    const config = useRuntimeConfig();
    const apiBaseUrl = config.public.apiBaseUrl;

    const payload: any = {
        variable: variable,
        sensorId: sensorId,
        datetime: new Date().toISOString()
    };
    const url = `${apiBaseUrl}/sensorTimeseries`;
    const r = await axios.post(url, payload);
    return r;
}

export default getSensorTimeseries;
