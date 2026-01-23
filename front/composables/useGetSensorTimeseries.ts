import axios from 'axios';

export async function getSensorTimeseries() {
    // Resolve runtime config inside the function so this composable can be imported at module scope
    const config = useRuntimeConfig();
    const apiBaseUrl = config.public.apiBaseUrl;

    console.log('Fetching sensor timeseries from:', `${apiBaseUrl}/extractSensorTimeseries`);
    // For now, fetch ${apiBaseUrl}/extractSensorTimeseries and return the result as-is using axios
    const r = await axios.get(`${apiBaseUrl}/extractSensorTimeseries`);
    return r.data;
}