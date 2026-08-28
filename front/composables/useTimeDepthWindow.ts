import { ref, computed, watch, type Ref } from 'vue'

/**
 * Windowing for time-depth (Hovmöller) views — the paging, clamping and
 * jump-to-date logic shared by the Comparison tab's Depth Profile (model vs.
 * one profiler sensor) and the Depth tab's model-only view.
 *
 * Bins are exposed as an explicit array of bin-start Dates (`binStarts`)
 * rather than derived by index arithmetic off a window start. That is what
 * makes `monthly` expressible at all — calendar months have no fixed width,
 * so `windowStart + i * binWidth` cannot land on the 1st of each month, and
 * consumers match cells against the backend's bin-start strings by exact
 * timestamp. A single misaligned bin silently misses every lookup and renders
 * an empty chart against a perfectly valid response, so the alignment is
 * centralised here instead of being re-derived per consumer.
 */

export type BinMode = 'hourly' | 'daily' | 'monthly'

export interface BinModeConfig {
    /** Bins per window. Chosen per mode to keep the count in the same
     *  few-hundred band regardless of bin width — the frontend renders one
     *  rect per (depth, bin) in a single non-progressive pass, so the cell
     *  budget, not the time span, is the real constraint. */
    binCount: number
    /** x-axis gridline/label spacing, in bins. */
    gridlineBins: number
    label: string
    /** Toggle button text. */
    short: string
}

export const BIN_CONFIG: Record<BinMode, BinModeConfig> = {
    hourly: { binCount: 14 * 24, gridlineBins: 24, label: 'Hourly bins · 14-day window', short: '1H' },
    daily: { binCount: 365, gridlineBins: 30, label: 'Daily bins · 1-year window', short: '1D' },
    // 20 years of months covers SalishSeaCast_daily's whole 2007-present reach
    // in one view, at 240 bins — daily bins over that same span would be ~6.8k.
    monthly: { binCount: 240, gridlineBins: 12, label: 'Monthly bins · 20-year window', short: '1M' },
}

/** Advance `d` by `n` bins of `mode` (negative to go back). */
export function addBins(d: Date, n: number, mode: BinMode): Date {
    if (mode === 'monthly') {
        // Date.UTC normalises out-of-range months (including negatives) into
        // the right year, so no explicit year/month carry handling is needed.
        return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + n, 1))
    }
    return new Date(d.getTime() + n * (mode === 'hourly' ? 3600e3 : 86400e3))
}

/** Floor `d` to the start of its bin for `mode`. */
export function floorToBin(d: Date, mode: BinMode): Date {
    if (mode === 'monthly') return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1))
    const ms = mode === 'hourly' ? 3600e3 : 86400e3
    return new Date(Math.floor(d.getTime() / ms) * ms)
}

/** Serialise for the API's ISO-8601 window bounds. */
export function toApiIso(d: Date): string { return d.toISOString().slice(0, 19) + 'Z' }

/**
 * Bin a raw {time, value} series (e.g. sensor observations, which land at
 * whatever cadence the instrument reports on) down to one mean value per
 * `binStarts` bucket — the same buckets the model series is already
 * aggregated to server-side, so overlaying the two on a shared bin mode
 * compares like with like instead of raw readings against pre-aggregated
 * points.
 */
export function binSeries(
    times: string[],
    values: (number | null)[],
    mode: BinMode,
    binStarts: Date[],
): (number | null)[] {
    const index = new Map<number, number>()
    binStarts.forEach((b, i) => index.set(b.getTime(), i))

    const sums = new Array(binStarts.length).fill(0)
    const counts = new Array(binStarts.length).fill(0)

    times.forEach((t, i) => {
        const v = values[i]
        if (v == null || !Number.isFinite(v)) return
        const raw = new Date(t.endsWith('Z') ? t : `${t}Z`)
        const bi = index.get(floorToBin(raw, mode).getTime())
        if (bi == null) return
        sums[bi] += v
        counts[bi]++
    })

    return sums.map((s, i) => (counts[i] > 0 ? s / counts[i] : null))
}

export interface TimeDepthWindowOptions {
    binMode: Ref<BinMode>
    /** Earliest instant with data, or null when unbounded/unknown. */
    dataFloor: Ref<Date | null>
    /** Latest instant with data. */
    dataCeil: Ref<Date>
    /** Extra sources that should snap the window back to latest when they change. */
    resetOn?: Ref<unknown>[]
}

export function useTimeDepthWindow(opts: TimeDepthWindowOptions) {
    const { binMode, dataFloor, dataCeil } = opts

    const binCount = computed(() => BIN_CONFIG[binMode.value].binCount)
    const gridlineBins = computed(() => BIN_CONFIG[binMode.value].gridlineBins)

    const windowEnd = ref<Date>(floorToBin(new Date(), binMode.value))
    const windowStart = computed(() => addBins(windowEnd.value, -binCount.value, binMode.value))

    /** The x-axis domain: one Date per bin, ascending, `windowEnd` exclusive. */
    const binStarts = computed<Date[]>(() => {
        const start = windowStart.value
        const mode = binMode.value
        return Array.from({ length: binCount.value }, (_, i) => addBins(start, i, mode))
    })

    const flooredCeil = computed(() => floorToBin(dataCeil.value, binMode.value))
    const flooredFloor = computed(() => dataFloor.value ? floorToBin(dataFloor.value, binMode.value) : null)

    const canPageBack = computed(() => !flooredFloor.value || windowStart.value.getTime() > flooredFloor.value.getTime())
    const canPageForward = computed(() => windowEnd.value.getTime() < flooredCeil.value.getTime())

    function clampWindowEnd(proposed: Date): Date {
        const ceil = flooredCeil.value.getTime()
        // The earliest legal windowEnd is one full window past the data floor,
        // so paging back never scrolls into a range with nothing in it...
        const floorBound = flooredFloor.value ? addBins(flooredFloor.value, binCount.value, binMode.value).getTime() : -Infinity
        // ...except when the record is *shorter* than one window, where that
        // lower bound lands past the ceiling and the two constraints contradict
        // each other. Left unresolved, a caller that re-clamps on new bounds
        // oscillates between them forever. The ceiling wins: anchor at the
        // latest data and let the window's left edge hang past the floor, where
        // it simply renders as empty bins. This is reachable in every mode —
        // monthly's 20-year window against a ~19-year record, or a sensor
        // deployed less than 14 days ago in hourly mode.
        const floor = Math.min(floorBound, ceil)
        if (proposed.getTime() > ceil) return new Date(ceil)
        if (proposed.getTime() < floor) return new Date(floor)
        return proposed
    }

    function page(dir: number) {
        windowEnd.value = clampWindowEnd(addBins(windowEnd.value, dir * binCount.value, binMode.value))
    }

    // Switching bin mode resets the window to "latest" rather than trying to
    // preserve the prior window's centre — simpler, and consistent with how
    // every other context change (sensor/source/variable) already behaves.
    function resetWindowToLatest() { windowEnd.value = flooredCeil.value }
    watch([binMode, ...(opts.resetOn ?? [])], resetWindowToLatest, { immediate: true })

    // A plain month/day label was unambiguous while the window was always a
    // 14-day span within one year, but the wider modes can span a year boundary
    // (or land almost exactly a year apart), where "Jul 22 – Jul 22" would
    // misread as a zero-length window — include the year once it stops being
    // implied. Monthly drops the day entirely: bins are whole months.
    const rangeLabel = computed(() => {
        const crossesYear = windowStart.value.getUTCFullYear() !== windowEnd.value.getUTCFullYear()
        const opts: Intl.DateTimeFormatOptions = binMode.value === 'monthly'
            ? { month: 'short', year: 'numeric', timeZone: 'UTC' }
            : { month: 'short', day: 'numeric', timeZone: 'UTC' }
        if (crossesYear || binMode.value !== 'hourly') opts.year = 'numeric'
        const fmt = (d: Date) => d.toLocaleDateString('en-US', opts)
        return `${fmt(windowStart.value)} – ${fmt(windowEnd.value)}`
    })

    // ── JUMP-TO-DATE PICKER — pick a day, centre the window on it ───────────
    const dateMenuOpen = ref(false)
    const pickedDate = ref<Date | null>(null)
    const minDateStr = computed(() => flooredFloor.value ? flooredFloor.value.toISOString().slice(0, 10) : undefined)
    const maxDateStr = computed(() => flooredCeil.value.toISOString().slice(0, 10))

    // v-date-picker reads/writes a Date via its LOCAL Y/M/D getters, while every
    // other date here is a raw UTC instant (rangeLabel formats with
    // timeZone:'UTC', toApiIso emits raw ISO). Round-tripping a UTC instant
    // through the picker naively shifts the selected day by the browser's UTC
    // offset. Instead always carry the *calendar* Y/M/D across — UTC-side
    // getters when reading, local-side when talking to the picker — so the
    // visible day never drifts.
    watch(dateMenuOpen, (open) => {
        if (!open) return
        const centre = addBins(windowStart.value, Math.floor(binCount.value / 2), binMode.value)
        pickedDate.value = new Date(centre.getUTCFullYear(), centre.getUTCMonth(), centre.getUTCDate())
    })

    function confirmDatePick() {
        if (pickedDate.value) {
            const d = pickedDate.value
            const centreUTC = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()))
            // Centre by whole bins, then floor — `binStarts` is what consumers
            // match the backend's bin-start strings against, so a windowEnd
            // landing mid-bin makes every lookup miss.
            const proposed = addBins(floorToBin(centreUTC, binMode.value), Math.ceil(binCount.value / 2), binMode.value)
            windowEnd.value = clampWindowEnd(floorToBin(proposed, binMode.value))
        }
        dateMenuOpen.value = false
    }

    return {
        binCount, gridlineBins, windowEnd, windowStart, binStarts,
        canPageBack, canPageForward, page, clampWindowEnd, resetWindowToLatest,
        rangeLabel, dateMenuOpen, pickedDate, minDateStr, maxDateStr, confirmDatePick,
    }
}
