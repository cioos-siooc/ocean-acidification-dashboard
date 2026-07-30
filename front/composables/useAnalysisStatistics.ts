import type { SeriesPoint } from './useAnalysisFetch';
import type { ECharts } from 'echarts';

export const availableVariables = [
    { id: 'temperature', name: 'Temperature' },
    { id: 'salinity', name: 'Salinity' },
    { id: 'dissolved_oxygen', name: 'Dissolved Oxygen' },
    { id: 'total_alkalinity', name: 'Total Alkalinity' },
    { id: 'dissolved_inorganic_carbon', name: 'Dissolved Inorganic Carbon' },
    { id: 'ph_total', name: 'pH (Total Scale)' },
    { id: 'omega_arag', name: 'Omega Aragonite' },
    { id: 'omega_cal', name: 'Omega Calcite' },
];

export const SEASON_MONTHS: Record<string, number[]> = {
    full_year: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    jja: [6, 7, 8],
    mam: [3, 4, 5],
    son: [9, 10, 11],
    djf: [12, 1, 2],
};

/**
 * Like filterBySeason, but keeps every date slot (nulling out the value instead of
 * dropping the entry). Use this for continuous single-line timeline charts that span
 * multiple years — compacting out off-season dates makes a 'time'-axis line connect
 * e.g. last August straight to next March, drawing a misleading diagonal across the
 * skipped months. A null gap renders as a real break instead.
 */
export function maskBySeason(data: SeriesPoint[], season: string): SeriesPoint[] {
    const months = SEASON_MONTHS[season] || SEASON_MONTHS.full_year;
    return data.map(d => months.includes(new Date(d.time).getUTCMonth() + 1) ? d : { time: d.time, value: null });
}

/**
 * Inserts an explicit null-valued point wherever consecutive entries are more than
 * `maxGapDays` apart. A time-axis line chart connects whatever points are actually in
 * the array regardless of how far apart their dates are — a real multi-day/week outage
 * (common in sensor telemetry, rare in the daily model archive) has no rows at all for
 * the missing dates, so without this the chart silently bridges the outage with a
 * straight diagonal instead of showing a break. Combine with `connectNulls: false`.
 */
export function breakDataGaps(series: SeriesPoint[], maxGapDays = 3): SeriesPoint[] {
    if (series.length < 2) return series;
    const out: SeriesPoint[] = [series[0]];
    for (let i = 1; i < series.length; i++) {
        const prev = series[i - 1];
        const cur = series[i];
        const gapDays = (new Date(cur.time).getTime() - new Date(prev.time).getTime()) / 86_400_000;
        if (gapDays > maxGapDays) {
            const breakTime = new Date(new Date(prev.time).getTime() + 86_400_000).toISOString().slice(0, 10);
            out.push({ time: breakTime, value: null });
        }
        out.push(cur);
    }
    return out;
}

export function filterBySeason(data: SeriesPoint[], season: string): SeriesPoint[] {
    const months = SEASON_MONTHS[season] || SEASON_MONTHS.full_year;
    return data.filter(d => months.includes(new Date(d.time).getUTCMonth() + 1));
}

export function groupByYear(data: SeriesPoint[]): { year: number; data: SeriesPoint[] }[] {
    const byYear = new Map<number, SeriesPoint[]>();
    for (const d of data) {
        const year = new Date(d.time).getUTCFullYear();
        if (!byYear.has(year)) byYear.set(year, []);
        byYear.get(year)!.push(d);
    }
    return Array.from(byYear.entries())
        .sort((a, b) => a[0] - b[0])
        .map(([year, pts]) => ({ year, data: pts }));
}

/** Interpolate a sorted array at the given percentile (0-100) using linear interpolation. */
export function percentileOf(sorted: number[], p: number): number {
    const idx = (p / 100) * (sorted.length - 1);
    const lo = Math.floor(idx), hi = Math.ceil(idx);
    return lo === hi ? sorted[lo]! : sorted[lo]! + (idx - lo) * (sorted[hi]! - sorted[lo]!);
}

export type BandStatsPoint = { ts: number; mean: number; min: number; max: number; p10: number; p90: number };

/**
 * Cross-year statistics per calendar-day slot: pools every year's value at the same overlay
 * timestamp (see callers' OVERLAY_REF_YEAR alignment) and computes mean/min/max/p10/p90 across
 * whichever years have data for that day. Unlike computeClimatologyBaseline this does not pool a
 * +/- day window — each point only compares same-day values, matching what the "Min-Max Range"
 * band and percentile lines actually draw on the overlay chart.
 */
export function computeYearBandStats(perYearPoints: [number, number | null][][]): BandStatsPoint[] {
    const byTs = new Map<number, number[]>();
    for (const points of perYearPoints) {
        for (const [ts, value] of points) {
            if (value == null) continue;
            if (!byTs.has(ts)) byTs.set(ts, []);
            byTs.get(ts)!.push(value);
        }
    }
    const out: BandStatsPoint[] = [];
    for (const [ts, values] of byTs.entries()) {
        const sorted = values.sort((a, b) => a - b);
        const mean = sorted.reduce((s, v) => s + v, 0) / sorted.length;
        out.push({
            ts, mean, min: sorted[0], max: sorted[sorted.length - 1],
            p10: percentileOf(sorted, 10), p90: percentileOf(sorted, 90),
        });
    }
    return out.sort((a, b) => a.ts - b.ts);
}

export type BoxRow = { year: number; box: [number, number, number, number, number]; mean: number; std: number };

export function computeBoxplotData(data: SeriesPoint[]): BoxRow[] {
    return groupByYear(data).map(({ year, data: pts }) => {
        const vals = pts.filter(d => d.value != null).map(d => d.value as number).sort((a, b) => a - b);
        if (vals.length < 4) return null;
        const n = vals.length;
        const mean = vals.reduce((s, v) => s + v, 0) / n;
        const std = Math.sqrt(vals.reduce((s, v) => s + (v - mean) ** 2, 0) / n);
        return {
            year,
            box: [vals[0], percentileOf(vals, 25), percentileOf(vals, 50), percentileOf(vals, 75), vals[n - 1]],
            mean,
            std,
        };
    }).filter(Boolean) as BoxRow[];
}

export function linearRegression(pts: { x: number; y: number }[]): { slope: number; intercept: number } {
    const n = pts.length;
    if (n < 2) return { slope: 0, intercept: pts[0]?.y ?? 0 };
    const sumX = pts.reduce((s, p) => s + p.x, 0);
    const sumY = pts.reduce((s, p) => s + p.y, 0);
    const sumXY = pts.reduce((s, p) => s + p.x * p.y, 0);
    const sumX2 = pts.reduce((s, p) => s + p.x * p.x, 0);
    const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
    return { slope, intercept: (sumY - slope * sumX) / n };
}

/**
 * Maps a year's position in the dataset (0=oldest -> 1=newest) to a colour.
 * Oldest: muted steel-blue. Newest: vivid orange-red, fully opaque.
 * Alpha floor is kept high enough that the oldest years stay legible on a dark background.
 */
export function yearColor(idx: number, total: number): string {
    const t = total <= 1 ? 1 : idx / (total - 1);
    const hue = Math.round(220 - t * 205);
    const sat = Math.round(45 + t * 50);
    const lit = Math.round(65 - t * 7);
    const alpha = (0.5 + t * 0.5).toFixed(2);
    return `hsla(${hue},${sat}%,${lit}%,${alpha})`;
}

/**
 * Replaces ECharts' default legend click behavior (hide the series) with a
 * persistent isolate/fade: clicking a legend entry fades every other series
 * out, until the same entry is clicked again or another one is picked. All
 * series stay visible/selectable in the legend itself.
 *
 * This deliberately does not use ECharts' `highlight`/`downplay` actions (the
 * emphasis/blur state machine behind hover). That state is transient by
 * design — ECharts clears all emphasis/blur globally the moment the pointer
 * crosses any "empty" canvas area (its internal high-down mouseout handling),
 * which happens constantly as the user moves the mouse around the chart. A
 * highlight set that way cannot survive normal mouse movement, so the fade is
 * instead baked directly into each series' own `lineStyle.opacity` via
 * `setOption`, independent of hover state.
 */
export function attachStickyLegendHighlight(chart: ECharts): void {
    let stickyName: string | null = null;

    function applyStickyStyles(): void {
        const series = ((chart.getOption().series as any[]) || []);
        chart.setOption({
            series: series.map(s => {
                const isSticky = s.name === stickyName;
                return {
                    name: s.name,
                    lineStyle: { opacity: stickyName == null || isSticky ? 1 : 0.08 },
                    z: isSticky ? 10 : 0,
                };
            }),
        });
    }

    chart.on('legendselectchanged', (params: any) => {
        // ECharts' own click handling for legend items (dispatchSelectAction in
        // LegendView) always re-highlights/re-toggles the just-clicked series right
        // after this event fires, as part of the same synchronous call. Deferring to
        // a macrotask lets that finish first, so undoing the hide below has the
        // final say instead of being immediately overridden.
        setTimeout(() => {
            // Undo the default click-to-hide — legend selection is repurposed below.
            chart.dispatchAction({ type: 'legendAllSelect' });
            stickyName = params.name === stickyName ? null : params.name;
            applyStickyStyles();
        }, 0);
    });
}

// --- TREND SIGNIFICANCE (Mann-Kendall + Theil-Sen) ---

export type MannKendallResult = {
    S: number;
    varS: number;
    Z: number;
    p: number;
    trend: 'increasing' | 'decreasing' | 'no trend';
};

/** Abramowitz & Stegun 26.2.17 normal-CDF approximation, |error| < 7.5e-8. */
function normalCdf(z: number): number {
    const t = 1 / (1 + 0.2316419 * z);
    const d = 0.3989423 * Math.exp(-z * z / 2);
    const poly = t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
    return 1 - d * poly;
}

/**
 * Mann-Kendall trend test. Intended for small n (e.g. annual means, ~19 points) —
 * the pairwise comparison is O(n^2), so don't run this on raw daily series.
 */
export function mannKendallTest(values: number[]): MannKendallResult {
    const n = values.length;
    let S = 0;
    for (let i = 0; i < n - 1; i++) {
        for (let j = i + 1; j < n; j++) {
            S += Math.sign(values[j] - values[i]);
        }
    }

    const counts = new Map<number, number>();
    for (const v of values) counts.set(v, (counts.get(v) ?? 0) + 1);
    let tieSum = 0;
    for (const g of counts.values()) if (g > 1) tieSum += g * (g - 1) * (2 * g + 5);

    const varS = (n * (n - 1) * (2 * n + 5) - tieSum) / 18;

    let Z: number;
    if (S > 0) Z = (S - 1) / Math.sqrt(varS);
    else if (S < 0) Z = (S + 1) / Math.sqrt(varS);
    else Z = 0;

    const p = 2 * (1 - normalCdf(Math.abs(Z)));
    const trend = p < 0.05 ? (S > 0 ? 'increasing' : 'decreasing') : 'no trend';
    return { S, varS, Z, p, trend };
}

/** Theil-Sen slope estimator: median of all pairwise slopes (robust to outliers/non-normality). */
export function theilSenSlope(pts: { x: number; y: number }[]): { slope: number; intercept: number } {
    const slopes: number[] = [];
    for (let i = 0; i < pts.length - 1; i++) {
        for (let j = i + 1; j < pts.length; j++) {
            const dx = pts[j].x - pts[i].x;
            if (dx !== 0) slopes.push((pts[j].y - pts[i].y) / dx);
        }
    }
    slopes.sort((a, b) => a - b);
    const slope = slopes.length ? percentileOf(slopes, 50) : 0;
    const intercepts = pts.map(p => p.y - slope * p.x).sort((a, b) => a - b);
    const intercept = intercepts.length ? percentileOf(intercepts, 50) : 0;
    return { slope, intercept };
}

// --- CLIMATOLOGY BASELINE (shared by Extreme Events + Climatology Anomaly) ---

export type ClimDay = { doy: number; mean: number; std: number; p10: number; p90: number; n: number };

// Cumulative days before each month, non-leap calendar (Jan=index 0).
const CUM_DAYS_NONLEAP = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];

/**
 * Leap-year-agnostic day-of-year (1-365). Always maps onto a 365-day calendar so every
 * year contributes one consistent bucket — Feb 29 is handled by the caller, not here.
 */
export function dayOfYear365(month: number, day: number): number {
    return CUM_DAYS_NONLEAP[month - 1] + day;
}

/**
 * Day-of-year climatology: for each of the 365 calendar-day buckets, pools all values
 * within +/- windowDays (wrapped across the Dec31->Jan1 boundary) across every year present
 * in `series`, then computes mean/std/p10/p90. Feb 29 is dropped from the pooling entirely
 * (not given its own bucket) so every year contributes the same 365 samples and Feb 29
 * doesn't end up an under-sampled outlier bucket. This is a different concern from the
 * "OVERLAY_REF_YEAR" trick used for the basic chart's display axis — that solves where a
 * date is *drawn*, this solves which statistical bucket a date *belongs to*.
 */
export function computeClimatologyBaseline(series: SeriesPoint[], windowDays = 5): ClimDay[] {
    const dailyValues: { doy: number; value: number }[] = [];
    for (const d of series) {
        if (d.value == null) continue;
        const dt = new Date(d.time);
        const month = dt.getUTCMonth() + 1, day = dt.getUTCDate();
        if (month === 2 && day === 29) continue;
        dailyValues.push({ doy: dayOfYear365(month, day), value: d.value });
    }

    // Bucket raw values by doy first so the windowed pool for each day only scans nearby buckets.
    const byDoy = new Map<number, number[]>();
    for (const { doy, value } of dailyValues) {
        if (!byDoy.has(doy)) byDoy.set(doy, []);
        byDoy.get(doy)!.push(value);
    }

    const climatology: ClimDay[] = [];
    for (let doy = 1; doy <= 365; doy++) {
        const pooled: number[] = [];
        for (let offset = -windowDays; offset <= windowDays; offset++) {
            let d2 = doy + offset;
            if (d2 < 1) d2 += 365;
            if (d2 > 365) d2 -= 365;
            const vals = byDoy.get(d2);
            if (vals) pooled.push(...vals);
        }
        if (pooled.length < 5) {
            climatology.push({ doy, mean: NaN, std: NaN, p10: NaN, p90: NaN, n: pooled.length });
            continue;
        }
        pooled.sort((a, b) => a - b);
        const mean = pooled.reduce((s, v) => s + v, 0) / pooled.length;
        const std = Math.sqrt(pooled.reduce((s, v) => s + (v - mean) ** 2, 0) / pooled.length);
        climatology.push({ doy, mean, std, p10: percentileOf(pooled, 10), p90: percentileOf(pooled, 90), n: pooled.length });
    }
    return climatology;
}

/**
 * Number of distinct calendar years represented in a series. computeClimatologyBaseline pools
 * its per-day-of-year window across every year present in the input — with fewer than ~2 years
 * of data, that pool collapses to a rolling window over a single continuous run rather than
 * independent yearly samples, so the resulting "baseline" is a local statistic, not a true
 * multi-year climatology. Callers use this to decide whether to caveat the baseline in the UI.
 */
export function distinctYearSpan(series: SeriesPoint[]): number {
    const years = new Set<number>();
    for (const d of series) {
        if (d.value == null) continue;
        years.add(new Date(d.time).getUTCFullYear());
    }
    return years.size;
}

/** Looks up the climatology bucket for a given ISO date, reusing Feb 28's bucket for Feb 29. */
export function climatologyForDate(climByDoy: Map<number, ClimDay>, iso: string): ClimDay | undefined {
    const dt = new Date(iso);
    const month = dt.getUTCMonth() + 1;
    const day = dt.getUTCDate();
    const doy = (month === 2 && day === 29) ? dayOfYear365(2, 28) : dayOfYear365(month, day);
    return climByDoy.get(doy);
}

export type ExtremeEvent = {
    startTime: string;
    endTime: string;
    durationDays: number;
    peakValue: number;
    peakAnomaly: number;
    meanIntensity: number;
};

/** Resolves the threshold (and, for anomaly math, the baseline it's measured against) for a given ISO date. */
export type ThresholdLookup = (iso: string) => { threshold: number; baseline: number } | undefined;

/** Threshold lookup backed by a day-of-year climatology — `direction: 'above'` uses p90, `'below'` uses p10. */
export function climatologyThresholdLookup(climatology: ClimDay[], direction: 'above' | 'below'): ThresholdLookup {
    const climByDoy = new Map(climatology.map(c => [c.doy, c]));
    return (iso: string) => {
        const clim = climatologyForDate(climByDoy, iso);
        if (!clim || Number.isNaN(clim.p90)) return undefined;
        return { threshold: direction === 'above' ? clim.p90 : clim.p10, baseline: clim.mean };
    };
}

/** Threshold lookup backed by a single fixed value, constant across every date. */
export function fixedThresholdLookup(value: number): ThresholdLookup {
    return () => ({ threshold: value, baseline: value });
}

/**
 * Marine-heatwave-style event detection (Hobday et al. 2016), adapted for either tail and
 * either a percentile climatology or a fixed threshold via `thresholdLookup`.
 * `direction: 'above'` flags days exceeding the threshold (e.g. temperature highs);
 * `'below'` flags days under it (e.g. pH/omega/oxygen lows).
 */
export function detectExtremeEvents(
    series: SeriesPoint[],
    thresholdLookup: ThresholdLookup,
    direction: 'above' | 'below',
    minDurationDays = 5,
    maxGapDays = 2,
): ExtremeEvent[] {
    type FlaggedDay = { time: string; value: number; threshold: number; baseline: number; isFlagged: boolean };
    const flagged: FlaggedDay[] = [];
    for (const d of series) {
        if (d.value == null) continue;
        const t = thresholdLookup(d.time);
        if (!t) continue;
        const isFlagged = direction === 'above' ? d.value > t.threshold : d.value < t.threshold;
        flagged.push({ time: d.time, value: d.value, threshold: t.threshold, baseline: t.baseline, isFlagged });
    }

    type RawRun = { startIdx: number; endIdx: number };
    const runs: RawRun[] = [];
    let curStart: number | null = null;
    for (let i = 0; i < flagged.length; i++) {
        const prevAdjacent = i > 0 && new Date(flagged[i].time).getTime() - new Date(flagged[i - 1].time).getTime() === 86_400_000;
        if (flagged[i].isFlagged) {
            if (curStart === null) curStart = i;
            else if (!prevAdjacent) { runs.push({ startIdx: curStart, endIdx: i - 1 }); curStart = i; }
        } else if (curStart !== null) {
            runs.push({ startIdx: curStart, endIdx: i - 1 });
            curStart = null;
        }
    }
    if (curStart !== null) runs.push({ startIdx: curStart, endIdx: flagged.length - 1 });

    // Merge runs separated by <= maxGapDays of calendar time.
    const merged: RawRun[] = [];
    for (const run of runs) {
        const last = merged[merged.length - 1];
        if (last) {
            const gapDays = (new Date(flagged[run.startIdx].time).getTime() - new Date(flagged[last.endIdx].time).getTime()) / 86_400_000 - 1;
            if (gapDays <= maxGapDays) { last.endIdx = run.endIdx; continue; }
        }
        merged.push({ ...run });
    }

    return merged
        .map(r => {
            const days = flagged.slice(r.startIdx, r.endIdx + 1);
            const startTime = days[0].time, endTime = days[days.length - 1].time;
            // Full calendar span, including any sub-threshold gap days absorbed by a merge —
            // not just the count of flagged days.
            const durationDays = (new Date(endTime).getTime() - new Date(startTime).getTime()) / 86_400_000 + 1;
            const peakValue = direction === 'above' ? Math.max(...days.map(d => d.value)) : Math.min(...days.map(d => d.value));
            const anomalies = days.map(d => d.value - d.baseline);
            const peakAnomaly = direction === 'above' ? Math.max(...anomalies) : Math.min(...anomalies);
            const meanIntensity = anomalies.reduce((s, v) => s + v, 0) / anomalies.length;
            return { startTime, endTime, durationDays, peakValue, peakAnomaly, meanIntensity };
        })
        .filter(e => e.durationDays >= minDurationDays);
}

export type YearlyEventSummary = { year: number; eventCount: number; totalEventDays: number; meanIntensity: number; maxIntensity: number };

/** Per-year rollup of events, attributed to each event's start year. */
export function summarizeEventsByYear(events: ExtremeEvent[]): YearlyEventSummary[] {
    const byYear = new Map<number, ExtremeEvent[]>();
    for (const e of events) {
        const year = new Date(e.startTime).getUTCFullYear();
        if (!byYear.has(year)) byYear.set(year, []);
        byYear.get(year)!.push(e);
    }
    return Array.from(byYear.entries())
        .sort((a, b) => a[0] - b[0])
        .map(([year, evs]) => ({
            year,
            eventCount: evs.length,
            totalEventDays: evs.reduce((s, e) => s + e.durationDays, 0),
            meanIntensity: evs.reduce((s, e) => s + Math.abs(e.meanIntensity), 0) / evs.length,
            maxIntensity: Math.max(...evs.map(e => Math.abs(e.peakAnomaly))),
        }));
}

// --- CORRELATION ---

export function pearsonCorrelation(xs: number[], ys: number[]): number {
    const n = xs.length;
    if (n < 2) return NaN;
    const meanX = xs.reduce((s, v) => s + v, 0) / n;
    const meanY = ys.reduce((s, v) => s + v, 0) / n;
    let cov = 0, varX = 0, varY = 0;
    for (let i = 0; i < n; i++) {
        const dx = xs[i] - meanX, dy = ys[i] - meanY;
        cov += dx * dy;
        varX += dx * dx;
        varY += dy * dy;
    }
    if (varX === 0 || varY === 0) return NaN;
    return cov / Math.sqrt(varX * varY);
}

/** Inner-joins two series by date, returning aligned non-null value arrays. */
export function joinSeriesByDate(a: SeriesPoint[], b: SeriesPoint[]): { times: string[]; a: number[]; b: number[] } {
    const mapB = new Map<string, number>();
    for (const d of b) if (d.value != null) mapB.set(d.time, d.value);
    const times: string[] = [], av: number[] = [], bv: number[] = [];
    for (const d of a) {
        if (d.value == null) continue;
        const bVal = mapB.get(d.time);
        if (bVal == null) continue;
        times.push(d.time);
        av.push(d.value);
        bv.push(bVal);
    }
    return { times, a: av, b: bv };
}
