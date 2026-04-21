import axios from 'axios';
import { useRuntimeConfig } from '#app';

export async function getSensorTimeseries(sensorId: number|null, canonicalVariable: string, fromDate: string, toDate: string) {
    if (sensorId === null || sensorId === undefined) {
        return null
    }

    const config = useRuntimeConfig();
    const apiBaseUrl = config.public.apiBaseUrl;

    const payload: any = {
        canonicalVariable: canonicalVariable,  // Model variable name (e.g., "dissolved_oxygen")
        sensorId: sensorId,
        fromDate: fromDate,
        toDate: toDate
    };
    const url = `${apiBaseUrl}/sensorTimeseries`;
    const r = await axios.post(url, payload);
    return r;
}

export default getSensorTimeseries;
