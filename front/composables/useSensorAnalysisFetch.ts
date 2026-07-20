import { getSensorTimeseries } from './useSensorTimeseries';
import type { SeriesPoint } from './useAnalysisFetch';

/** Buckets raw (possibly sub-daily) sensor readings into a daily {time, value} series using the given statistic. */
export function aggregateSensorSeries(times: string[], values: (number | null)[], stat: 'min' | 'mean' | 'max'): SeriesPoint[] {
    const buckets = new Map<string, number[]>();
    for (let i = 0; i < times.length; i++) {
        const v = values[i];
        if (v == null || !isFinite(v)) continue;
        const date = times[i].slice(0, 10);
        const bucket = buckets.get(date);
        if (bucket) bucket.push(v);
        else buckets.set(date, [v]);
    }

    const reduce = (vals: number[]): number => {
        if (stat === 'min') return Math.min(...vals);
        if (stat === 'max') return Math.max(...vals);
        return vals.reduce((s, v) => s + v, 0) / vals.length;
    };

    return Array.from(buckets.entries())
        .map(([time, vals]) => ({ time, value: reduce(vals) }))
        .sort((a, b) => a.time.localeCompare(b.time));
}

/** Fetches a sensor's full raw time series and aggregates it to a daily {time, value} series for the given statistic. */
export async function fetchSensorAnalysisSeries(
    sensorId: string,
    variable: string,
    stat: 'min' | 'mean' | 'max',
    depth: number | null,
    fromDate: string,
    toDate: string,
    source: string | null = null,
): Promise<SeriesPoint[]> {
    const response = await getSensorTimeseries(sensorId, variable, fromDate, toDate, depth, source);
    const times: string[] = response?.data?.time ?? [];
    const values: (number | null)[] = response?.data?.value ?? [];
    if (!times.length) return [];
    return aggregateSensorSeries(times, values, stat);
}
