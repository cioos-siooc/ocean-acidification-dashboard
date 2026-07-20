import axios from 'axios';
import { useRuntimeConfig } from '#app';

export async function getSensorTimeseries(sensorId: string|null, canonicalVariable: string, fromDate: string, toDate: string, depth: number|null = null, source: string|null = null) {
    if (sensorId === null || sensorId === undefined) {
        return null
    }

    const config = useRuntimeConfig();
    const apiBaseUrl = config.public.apiBaseUrl;

    const payload: any = {
        modelVariable: canonicalVariable,  // Model variable name (e.g., "dissolved_oxygen")
        sensorId: sensorId,
        fromDate: fromDate,
        toDate: toDate
    };
    if (depth !== null) {
        payload.depth = depth;
    }
    // Only needed to resolve depth for variable-depth ("profiler") sensors — see
    // extractSensorTimeseries.py's profiler branch.
    if (source !== null) {
        payload.source = source;
    }
    const url = `${apiBaseUrl}/sensorTimeseries`;
    const r = await axios.post(url, payload);
    return r;
}

export default getSensorTimeseries;
