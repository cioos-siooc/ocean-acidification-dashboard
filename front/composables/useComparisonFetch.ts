import type { SeriesPoint } from './useAnalysisFetch'

export interface DailyPoint {
    date: string   // YYYY-MM-DD
    value: number
}

export interface ComparisonPoint {
    date: string
    model: number | null
    modelMin: number | null
    modelMax: number | null
    sensor: number | null
}

export interface ComparisonStats {
    bias: number      // mean(model − obs)
    rmse: number
    pearsonR: number
    n: number
}

export type Season = 'all' | 'djf' | 'mam' | 'jja' | 'son'

export interface MonthlyClimatology {
    month: number   // 1–12
    model: number | null
    sensor: number | null
}

export function getPointSeason(date: string): 'djf' | 'mam' | 'jja' | 'son' {
    const m = parseInt(date.slice(5, 7), 10)
    if (m <= 2 || m === 12) return 'djf'
    if (m <= 5) return 'mam'
    if (m <= 8) return 'jja'
    return 'son'
}

export function filterBySeason(points: ComparisonPoint[], season: Season): ComparisonPoint[] {
    if (season === 'all') return points
    return points.filter(p => getPointSeason(p.date) === season)
}

const SEASON_MONTHS: Record<Season, number[]> = {
    all: [1,2,3,4,5,6,7,8,9,10,11,12],
    djf: [12,1,2],
    mam: [3,4,5],
    jja: [6,7,8],
    son: [9,10,11],
}

/**
 * Like filterBySeason, but keeps every date slot and nulls out values outside the
 * season instead of removing them. Use for time-axis line charts — dropping off-season
 * points makes ECharts connect e.g. last August straight to next June, drawing a
 * misleading diagonal across the skipped months. A null gap renders as a real break.
 */
export function maskBySeason(points: ComparisonPoint[], season: Season): ComparisonPoint[] {
    if (season === 'all') return points
    const months = SEASON_MONTHS[season]
    return points.map(p => {
        const m = parseInt(p.date.slice(5, 7), 10)
        if (months.includes(m)) return p
        return { date: p.date, model: null, modelMin: null, modelMax: null, sensor: null }
    })
}

export function computeMonthlyClimatology(points: ComparisonPoint[]): MonthlyClimatology[] {
    const buckets = new Map<number, { model: number[]; sensor: number[] }>()
    for (let m = 1; m <= 12; m++) buckets.set(m, { model: [], sensor: [] })
    for (const p of points) {
        const m = parseInt(p.date.slice(5, 7), 10)
        const b = buckets.get(m)!
        if (p.model != null) b.model.push(p.model)
        if (p.sensor != null) b.sensor.push(p.sensor)
    }
    const avg = (arr: number[]) => arr.length ? arr.reduce((s, v) => s + v, 0) / arr.length : null
    return Array.from(buckets.entries()).map(([month, b]) => ({
        month,
        model: avg(b.model),
        sensor: avg(b.sensor),
    }))
}

/** Bucket hourly sensor readings into daily means. */
export function aggregateSensorToDaily(times: string[], values: (number | null)[]): DailyPoint[] {
    const buckets = new Map<string, { sum: number; count: number }>()
    for (let i = 0; i < times.length; i++) {
        const v = values[i]
        if (v == null || !isFinite(v)) continue
        const date = times[i].slice(0, 10)
        const b = buckets.get(date) ?? { sum: 0, count: 0 }
        b.sum += v
        b.count += 1
        buckets.set(date, b)
    }
    return Array.from(buckets.entries())
        .map(([date, { sum, count }]) => ({ date, value: sum / count }))
        .sort((a, b) => a.date.localeCompare(b.date))
}

/** Combine model min/mean/max series with sensor daily means into one aligned array. */
export function buildComparisonSeries(
    meanSeries: SeriesPoint[],
    minSeries: SeriesPoint[],
    maxSeries: SeriesPoint[],
    sensorDaily: DailyPoint[]
): ComparisonPoint[] {
    const meanMap = new Map(meanSeries.map(p => [p.time.slice(0, 10), p.value]))
    const minMap = new Map(minSeries.map(p => [p.time.slice(0, 10), p.value]))
    const maxMap = new Map(maxSeries.map(p => [p.time.slice(0, 10), p.value]))
    const sensorMap = new Map(sensorDaily.map(p => [p.date, p.value]))

    // Union of all dates so gaps are visible in the chart
    const allDates = new Set([...meanMap.keys(), ...sensorMap.keys()])
    return Array.from(allDates).sort().map(date => ({
        date,
        model: meanMap.get(date) ?? null,
        modelMin: minMap.get(date) ?? null,
        modelMax: maxMap.get(date) ?? null,
        sensor: sensorMap.get(date) ?? null,
    }))
}


// ── ADVANCED STATS ────────────────────────────────────────────────────────────

export interface ScatterStats {
    r2: number
    slope: number      // linear fit: model = slope * obs + intercept
    intercept: number
    n: number
}

/** Linear regression and R² for scatter plot (expects non-null pairs). */
export function computeScatterStats(points: ComparisonPoint[]): ScatterStats {
    const n = points.length
    if (n < 2) return { r2: 0, slope: 1, intercept: 0, n }
    const xs = points.map(p => p.sensor as number)
    const ys = points.map(p => p.model as number)
    const xm = xs.reduce((s, v) => s + v, 0) / n
    const ym = ys.reduce((s, v) => s + v, 0) / n
    const ssxy = xs.reduce((s, x, i) => s + (x - xm) * (ys[i] - ym), 0)
    const ssxx = xs.reduce((s, x) => s + (x - xm) ** 2, 0)
    const slope = ssxx > 0 ? ssxy / ssxx : 1
    const intercept = ym - slope * xm
    const ssTot = ys.reduce((s, y) => s + (y - ym) ** 2, 0)
    const ssRes = ys.reduce((s, y, i) => s + (y - slope * xs[i] - intercept) ** 2, 0)
    const r2 = ssTot > 0 ? 1 - ssRes / ssTot : 0
    return { r2, slope, intercept, n }
}

export interface ResidualStats {
    mean: number
    std: number
    maxOver: number   // largest positive residual (model overestimates)
    maxUnder: number  // most negative residual (model underestimates)
    trend: number     // linear trend in residuals, units per year
    trendIntercept: number  // y-value at t=0 (first date) for drawing the trend line
    spanYears: number       // time span covered by the data
    n: number
}

/** Mean, std, extremes, and temporal trend of daily residuals (expects non-null pairs). */
export function computeResidualStats(points: ComparisonPoint[]): ResidualStats {
    const n = points.length
    if (n === 0) return { mean: 0, std: 0, maxOver: 0, maxUnder: 0, trend: 0, trendIntercept: 0, spanYears: 0, n: 0 }
    const resids = points.map(p => (p.model as number) - (p.sensor as number))
    const mean = resids.reduce((s, v) => s + v, 0) / n
    const std = Math.sqrt(resids.reduce((s, v) => s + (v - mean) ** 2, 0) / n)
    const maxOver = Math.max(...resids)
    const maxUnder = Math.min(...resids)
    const t0 = new Date(points[0].date).getTime()
    const ts = points.map(p => (new Date(p.date).getTime() - t0) / (365.25 * 86400e3))
    const tm = ts.reduce((s, v) => s + v, 0) / n
    const ssxy = ts.reduce((s, t, i) => s + (t - tm) * (resids[i] - mean), 0)
    const ssxx = ts.reduce((s, t) => s + (t - tm) ** 2, 0)
    const trend = ssxx > 0 ? ssxy / ssxx : 0
    const trendIntercept = mean - trend * tm
    const spanYears = ts[ts.length - 1]
    return { mean, std, maxOver, maxUnder, trend, trendIntercept, spanYears, n }
}

export interface SeasonalCycleStats {
    modelPeakMonth: number    // 1–12
    sensorPeakMonth: number
    phaseOffset: number       // sensorPeak − modelPeak; positive = model peaks earlier
    modelAmplitude: number    // max − min of climatological monthly means
    sensorAmplitude: number
    amplitudeRatio: number    // model / sensor
}

/** Peak month, phase offset, and amplitude comparison from monthly climatology. */
export function computeSeasonalCycleStats(clim: MonthlyClimatology[]): SeasonalCycleStats | null {
    const mVals = clim.filter(c => c.model != null).map(c => c.model as number)
    const sVals = clim.filter(c => c.sensor != null).map(c => c.sensor as number)
    if (!mVals.length || !sVals.length) return null
    const mMax = Math.max(...mVals), mMin = Math.min(...mVals)
    const sMax = Math.max(...sVals), sMin = Math.min(...sVals)
    const modelPeakMonth  = (clim.findIndex(c => c.model === mMax) + 1) || 1
    const sensorPeakMonth = (clim.findIndex(c => c.sensor === sMax) + 1) || 1
    let phaseOffset = sensorPeakMonth - modelPeakMonth
    if (phaseOffset > 6) phaseOffset -= 12
    if (phaseOffset < -6) phaseOffset += 12
    const modelAmplitude  = mMax - mMin
    const sensorAmplitude = sMax - sMin
    return {
        modelPeakMonth,
        sensorPeakMonth,
        phaseOffset,
        modelAmplitude,
        sensorAmplitude,
        amplitudeRatio: sensorAmplitude > 0 ? modelAmplitude / sensorAmplitude : 1,
    }
}

/** Compute bias, RMSE, and Pearson R on matched (non-null) daily pairs. */
export function computeComparisonStats(points: ComparisonPoint[]): ComparisonStats {
    const pairs = points.filter(p => p.model != null && p.sensor != null) as
        { date: string; model: number; modelMin: number | null; modelMax: number | null; sensor: number }[]
    const n = pairs.length
    if (n === 0) return { bias: 0, rmse: 0, pearsonR: 0, n: 0 }

    const diffs = pairs.map(p => p.model - p.sensor)
    const bias = diffs.reduce((s, d) => s + d, 0) / n
    const rmse = Math.sqrt(diffs.reduce((s, d) => s + d * d, 0) / n)

    const mMean = pairs.reduce((s, p) => s + p.model, 0) / n
    const sMean = pairs.reduce((s, p) => s + p.sensor, 0) / n
    const cov = pairs.reduce((s, p) => s + (p.model - mMean) * (p.sensor - sMean), 0)
    const sdM = Math.sqrt(pairs.reduce((s, p) => s + (p.model - mMean) ** 2, 0))
    const sdS = Math.sqrt(pairs.reduce((s, p) => s + (p.sensor - sMean) ** 2, 0))
    const pearsonR = sdM > 0 && sdS > 0 ? cov / (sdM * sdS) : 0

    return { bias, rmse, pearsonR, n }
}
