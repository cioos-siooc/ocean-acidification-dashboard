/**
 * CSV export — the one path every downloadable view in the app goes through.
 *
 * Deliberately *not* built on ECharts' `toolbox.feature.dataView`: that renders a
 * read-only tab-separated blob into a modal (no file is ever downloaded), and it
 * degrades to noise for `custom` series — which is exactly what the Hovmöller
 * heatmap and the cross-section are. Charts and tables therefore both serialize
 * here, from their *source* data rather than from chart or table state, so a
 * download is the same thing everywhere and carries full precision (the tables
 * round to 3 decimals for display; that rounding must not reach the file).
 *
 * Two pieces:
 *   - serialization (`buildCsv`/`downloadCsv`) — pure, usable standalone;
 *   - a per-workspace registry (`provideCsvExport`/`useCsvExport`) that lets a
 *     host render ONE download control in its header while the tab components
 *     that actually own the data declare what's downloadable.
 */
import {
    computed, getCurrentScope, inject, onScopeDispose, provide, shallowRef, toValue,
    type ComputedRef, type InjectionKey, type MaybeRefOrGetter,
} from 'vue'
import moment from 'moment-timezone'
import { APP_TIMEZONE } from '@/config/app'

/** Same shape as a Nuxt UI `UTable` column, so `:columns` can be handed straight over. */
export interface CsvColumn {
    header: string
    accessorKey: string
}

export type CsvRow = Record<string, unknown>

/** A `# label: value` line in the file's provenance preamble. Ordered, hence a tuple list. */
export type CsvMetaEntry = [label: string, value: unknown]

export interface CsvDataset {
    /** Menu label when a view offers more than one file, e.g. 'Events'. */
    label: string
    /** Filename fragment, e.g. 'extreme-events'. */
    slug: string
    columns: CsvColumn[]
    rows: CsvRow[]
    /** Provenance preamble, usually built by `csvMeta()`. */
    meta?: CsvMetaEntry[]
    /**
     * Drops the automatic `# dataset:` line. For a view that only ever produces
     * one file the label just restates the filename, so it earns nothing.
     */
    omitDatasetLine?: boolean
    /** Overrides the name derived from `slug` + context. */
    filename?: string
}

/**
 * What the file was a download *of* — the query behind it, not the numbers in it.
 * Hosts (workspaces/panels) build one and provide it; the tab components that
 * register datasets fold it into each file's preamble via `csvMeta()`.
 */
export interface CsvContext {
    /** 'model' | 'sensor' — which side of the app the series came from. */
    source: string
    /** Human name for that source: 'SalishSeaCast model' or the sensor's name. */
    sourceLabel: string
    /** Canonical variable id, e.g. 'ph_total'. */
    variable: string
    variableName: string
    /** Display unit the values are already in (unit preference applied). */
    unit?: string
    depth?: number | null
    /** '49.283N 123.121W' for a map point, or the sensor's name/id. */
    locationLabel?: string
    /** Inclusive ISO date bounds of the underlying query. */
    timeRange?: [string, string] | null
    season?: string
}

const BOM = '\uFEFF'
// RFC 4180 says CRLF. pandas and R both cope either way; Excel prefers this.
const EOL = '\r\n'

/**
 * One CSV field. Numbers keep full precision — formatting is a display concern
 * and stays in the components. Non-finite floats become empty rather than the
 * string 'NaN', which every reader understands as missing.
 */
function csvCell(value: unknown): string {
    if (value == null) return ''
    if (value instanceof Date) return value.toISOString()
    if (typeof value === 'number') return Number.isFinite(value) ? String(value) : ''
    const s = String(value)
    return /[",\r\n]/.test(s) || s !== s.trim() ? `"${s.replace(/"/g, '""')}"` : s
}

/**
 * Timestamp for a CSV cell: ISO 8601 in the app's timezone, offset included.
 *
 * Local rather than UTC because the file should say what the chart said — but
 * with the offset written out, so it still round-trips into pandas/R without
 * anyone having to know which coast this is. Date-only strings (the daily
 * analysis series) are left alone: a calendar day has no instant to place.
 */
export function csvTimestamp(value: Date | string | number | null | undefined): string {
    if (value == null) return ''
    if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value)) return value
    const m = typeof value === 'string' ? moment.utc(value) : moment(value)
    return m.isValid() ? m.tz(APP_TIMEZONE).format() : ''
}

function slugify(part: unknown): string {
    return String(part)
        .normalize('NFKD')
        .replace(/[^\w.-]+/g, '-')
        .replace(/^-+|-+$/g, '')
        .toLowerCase()
}

/** `oceaneco_extreme-events_ph-total_49.283n-123.121w_5m_2007-01-01_2026-12-31.csv` */
export function csvFilename(parts: Array<string | number | null | undefined>): string {
    const body = parts.filter(p => p != null && p !== '').map(slugify).filter(Boolean).join('_')
    return `oceaneco_${body || 'data'}.csv`
}

/**
 * A sensor's depth comes off the instrument's own metadata and can carry a
 * dozen spurious decimals (41.39560317993164 m). Rounded for both the preamble
 * and the filename — nobody needs a mooring depth to the nanometre.
 */
function roundDepth(depth: number): number {
    return Number.isInteger(depth) ? depth : Math.round(depth * 100) / 100
}

function depthLabel(depth: number | null | undefined): string | null {
    if (depth == null) return null
    return depth === -1 ? 'variable (profiler)' : `${roundDepth(depth)} m`
}

/** Calendar-day part of an ISO string — filenames don't need the clock time. */
function datePart(iso: string | undefined): string | undefined {
    return iso?.slice(0, 10)
}

/**
 * Filename for a dataset in a given context, unless the dataset names itself.
 *
 * The window goes in as dates only, and collapses to one when both ends land on
 * the same day — a single-snapshot view (the cross-section) would otherwise
 * repeat its timestamp twice in the name.
 */
export function csvDatasetFilename(dataset: CsvDataset, ctx: CsvContext | null): string {
    if (dataset.filename) return dataset.filename
    const from = datePart(ctx?.timeRange?.[0])
    const to = datePart(ctx?.timeRange?.[1])
    return csvFilename([
        dataset.slug,
        ctx?.variable,
        ctx?.locationLabel,
        ctx?.depth == null ? null : (ctx.depth === -1 ? 'profile' : `${roundDepth(ctx.depth)}m`),
        from,
        to === from ? null : to,
    ])
}

/**
 * Standard provenance preamble for a dataset: the shared context first, then
 * whatever parameters are specific to this file (thresholds, comparison
 * variable, …). Callers pass those as `extra`.
 *
 * An `extra` entry reusing a context label overrides it in place rather than
 * appending a second line — a view whose own controls disagree with the host's
 * (the Overview tab keeps its own season and statistic) states its own value,
 * and no file ends up with two contradictory `# season:` lines.
 */
export function csvMeta(ctx: CsvContext | null, extra: CsvMetaEntry[] = []): CsvMetaEntry[] {
    if (!ctx) return extra
    const base: CsvMetaEntry[] = [
        ['source', ctx.sourceLabel],
        ['variable', ctx.unit ? `${ctx.variableName} (${ctx.unit})` : ctx.variableName],
        ['depth', depthLabel(ctx.depth)],
        ['location', ctx.locationLabel],
        ['time_range', !ctx.timeRange ? null
            : ctx.timeRange[0] === ctx.timeRange[1] ? ctx.timeRange[0]
                : `${ctx.timeRange[0]} .. ${ctx.timeRange[1]}`],
        ['season', ctx.season],
    ]

    const appended: CsvMetaEntry[] = []
    for (const entry of extra) {
        const at = base.findIndex(([label]) => label === entry[0])
        if (at === -1) appended.push(entry)
        else base[at] = entry
    }
    return [...base, ...appended]
}

/**
 * Serializes to RFC 4180 CSV with a `#`-commented provenance preamble.
 *
 * The preamble is the deliberate tradeoff: the numbers alone aren't reproducible
 * for a science audience, so every file states the query that produced it. Cost
 * is that readers need to be told to skip it — `pd.read_csv(f, comment='#')`,
 * `read.csv(f, comment.char='#')` — and Excel shows the lines as rows.
 */
export function buildCsv(dataset: CsvDataset): string {
    const lines: string[] = [
        '# OceanECO — ocean acidification dashboard',
        ...(dataset.omitDatasetLine ? [] : [`# dataset: ${dataset.label}`]),
        `# generated: ${moment().tz(APP_TIMEZONE).format()}`,
    ]
    for (const [label, value] of dataset.meta ?? []) {
        if (value == null || value === '') continue
        lines.push(`# ${label}: ${String(value).replace(/[\r\n]+/g, ' ')}`)
    }
    lines.push('#')

    lines.push(dataset.columns.map(c => csvCell(c.header)).join(','))
    for (const row of dataset.rows) {
        lines.push(dataset.columns.map(c => csvCell(row[c.accessorKey])).join(','))
    }
    return lines.join(EOL) + EOL
}

/** Serializes and hands the file to the browser. No-op outside the client. */
export function downloadCsv(dataset: CsvDataset, ctx: CsvContext | null = null): void {
    if (typeof document === 'undefined') return
    // The BOM is what stops Excel mangling °, μ and Ω in headers and metadata.
    const blob = new Blob([BOM + buildCsv(dataset)], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = csvDatasetFilename(dataset, ctx)
    document.body.appendChild(a)
    a.click()
    a.remove()
    URL.revokeObjectURL(url)
}

// ── REGISTRY ────────────────────────────────────────────────────────────────

export interface CsvExportApi {
    context: ComputedRef<CsvContext | null>
    /** Everything currently registered, in registration order. */
    datasets: ComputedRef<CsvDataset[]>
    /**
     * Declares this component's downloadable data. The getter is called inside a
     * computed, so it tracks reactively; it unregisters with the calling scope.
     *
     * Contract for hosts kept alive with `v-show`: return `[]` while inactive,
     * or an offscreen tab's files show up in the visible tab's menu.
     */
    register(source: () => CsvDataset[]): void
}

const CSV_EXPORT_KEY: InjectionKey<CsvExportApi> = Symbol('csv-export')

/** Called by a workspace/panel that owns the download control in its header. */
export function provideCsvExport(context: MaybeRefOrGetter<CsvContext | null>): CsvExportApi {
    const sources = shallowRef<Array<() => CsvDataset[]>>([])

    const api: CsvExportApi = {
        context: computed(() => toValue(context)),
        datasets: computed(() => sources.value.flatMap(fn => fn())),
        register(source) {
            sources.value = [...sources.value, source]
            if (getCurrentScope()) {
                onScopeDispose(() => { sources.value = sources.value.filter(s => s !== source) })
            }
        },
    }

    provide(CSV_EXPORT_KEY, api)
    return api
}

/** Called by a child that has data worth downloading. Null when no host provides one. */
export function useCsvExport(): CsvExportApi | null {
    return inject(CSV_EXPORT_KEY, null)
}
