import moment from 'moment'
import { useMainStore } from '@/stores/main'
import type { BinMode } from './useTimeDepthWindow'

/**
 * "Share this view" — the whole app state as one link.
 *
 * The dashboard's state is wide (variable/depth/instant/colormap range, which
 * pane is open and its own sub-state, a drawn polyline, a selected sensor, the
 * sensor list's filters, the map camera, view chrome) and almost none of it is
 * derivable from anything else, so a readable query string would run to many
 * hundreds of characters and would need a parser per field. Instead the whole
 * payload is JSON → deflate → base64url in the URL *hash*: opaque, but short,
 * schema-flexible, and never sent to the server (a hash isn't part of the
 * request), which matters because the payload includes a coordinate the user
 * clicked.
 *
 * The keys are deliberately terse — the compressor pays for every byte, and
 * this object is never read by hand. `SHARE_VERSION` is the escape hatch: a
 * payload from a *newer* version is refused outright (`decodeShareState`
 * returns null) rather than half-applied.
 *
 * Capture reads the Pinia store only — no component refs, no map instance — so
 * a button anywhere in the tree can produce a link. That is why a handful of
 * things that used to be local refs now live in the store (`analysisTab`,
 * `comparisonTab`, `mapView`, …): same reasoning as `exploreView`/
 * `exploreBinMode` before them.
 */

export const SHARE_VERSION = 1

/** Hash key the payload travels under: `#s=<marker><base64url>`. */
const HASH_KEY = 's'
/** Payload markers — 'z' deflate-raw, 'u' uncompressed (no CompressionStream). */
const MARK_DEFLATE = 'z'
const MARK_PLAIN = 'u'

export interface SharedState {
    v: number
    /** selected_variable */
    sv?: {
        s?: string          // source
        v?: string          // variable id
        d?: string | null   // depth label ("18.0" / "bottom")
        dn?: number | null  // depth_nc
        cm?: string | null  // colormap name
        cmn?: number | null // colormap min (canonical units)
        cmx?: number | null // colormap max
        cs?: (number | null)[] // colormap stops
        dt?: number | null  // selected instant, epoch ms
    }
    /** midDate — the timeseries chart's window centre, epoch ms */
    md?: number | null
    /** unitPreference */
    up?: Record<string, string>
    /** map camera */
    map?: { c: [number, number], z: number, b?: number, p?: number }
    /** last clicked point, [lat, lng] */
    pt?: [number, number]
    tab?: 'explore' | 'analysis' | 'comparison' | 'crossSection'
    ev?: 'series' | 'model-depth' | 'sensor-depth'
    bin?: BinMode
    /** Explore's time-depth window anchor (windowEnd), epoch ms */
    we?: number
    /** selected sensor, [id, depth] */
    sen?: [string, number]
    as?: 'model' | 'sensor'
    at?: string   // analysis sub-tab
    ase?: string  // analysis season
    ct?: string   // comparison sub-tab
    cse?: string  // comparison season
    /** cross-section polyline, [[lat, lng], …] */
    xs?: [number, number][]
    /** view chrome */
    ui?: {
        cp?: boolean   // control panel open
        cpw?: number   // control panel width
        bc?: boolean   // bathymetry contours
        ml?: boolean   // map labels
        cc?: boolean   // cursor coords
        vp?: boolean   // vertical profile drawer
        qm?: 'point' | 'area'
        ar?: boolean   // autoRangeDisabled
    }
    /** sensor list filters */
    f?: { q?: string, o?: string[], v?: string[] }
    /**
     * Per-view control state, namespaced by owning component — analysis
     * thresholds, chosen secondary variables, chart zoom extents. Opaque here
     * on purpose: the schema lives with each view, via
     * `composables/useViewState.ts`, so adding a control to a tab does not mean
     * touching this file.
     */
    vs?: Record<string, Record<string, unknown>>
}

/** 5 dp ≈ 1 m — finer than any grid cell in this model, and cheaper to encode. */
function round(n: number, dp = 5): number {
    return Number(n.toFixed(dp))
}

/** Drop empty strings/arrays/objects and nulls so they don't cost bytes. */
function prune<T extends Record<string, any>>(obj: T): T {
    for (const k of Object.keys(obj)) {
        const val = obj[k]
        if (val === undefined || val === null) { delete obj[k]; continue }
        if (typeof val === 'string' && val === '') { delete obj[k]; continue }
        if (Array.isArray(val) && val.length === 0) { delete obj[k]; continue }
        if (typeof val === 'object' && !Array.isArray(val) && Object.keys(val).length === 0) delete obj[k]
    }
    return obj
}

/** Deep-copy the view-state bag, dropping scopes that hold nothing. */
function captureViewState(bags: Record<string, Record<string, unknown>>): Record<string, Record<string, unknown>> | undefined {
    const out: Record<string, Record<string, unknown>> = {}
    for (const [scope, bag] of Object.entries(bags ?? {})) {
        if (bag && Object.keys(bag).length) out[scope] = JSON.parse(JSON.stringify(bag))
    }
    return Object.keys(out).length ? out : undefined
}

/** Snapshot the current view. Pure store reads — safe to call from anywhere. */
export function captureShareState(): SharedState {
    const s = useMainStore()
    const sv = s.selected_variable

    const state: SharedState = {
        v: SHARE_VERSION,
        sv: prune({
            s: sv.source,
            v: sv.var,
            d: sv.depth,
            dn: sv.depth_nc,
            cm: sv.colormap,
            cmn: sv.colormapMin,
            cmx: sv.colormapMax,
            // Stops are all-null in the common case; only carry a tuned one.
            cs: sv.colormapStops.some(v => v != null) ? sv.colormapStops : undefined,
            dt: sv.dt ? sv.dt.valueOf() : undefined,
        }),
        md: s.midDate ? s.midDate.valueOf() : undefined,
        up: { ...s.unitPreference },
        map: s.mapView
            ? prune({
                c: [round(s.mapView.center[0]), round(s.mapView.center[1])] as [number, number],
                z: round(s.mapView.zoom, 2),
                b: s.mapView.bearing ? round(s.mapView.bearing, 1) : undefined,
                p: s.mapView.pitch ? round(s.mapView.pitch, 1) : undefined,
            }) as SharedState['map']
            : undefined,
        pt: s.lastClickedMapPoint
            ? [round(s.lastClickedMapPoint.lat), round(s.lastClickedMapPoint.lng)]
            : undefined,
        tab: s.activeBottomTab,
        ev: s.exploreView,
        bin: s.exploreBinMode,
        we: s.exploreWindowEnd ?? undefined,
        sen: s.selectedSensor?.id ? [s.selectedSensor.id, s.selectedSensor.depth] : undefined,
        as: s.analysisSource,
        at: s.analysisTab,
        ase: s.analysisSeason,
        ct: s.comparisonTab,
        cse: s.comparisonSeason,
        xs: s.crossSectionLine?.length
            ? s.crossSectionLine.map(p => [round(p.lat), round(p.lng)] as [number, number])
            : undefined,
        ui: {
            cp: s.isControlPanelOpen,
            cpw: s.controlPanel_width,
            bc: s.showBathymetryContours,
            ml: s.showMapLabels,
            cc: s.showCursorCoords,
            vp: s.isVerticalProfileOpen,
            qm: s.queryMode,
            ar: s.autoRangeDisabled,
        },
        f: prune({
            q: s.sensorSearchQuery,
            o: [...s.sensorOrganizationFilter],
            v: [...s.sensorVariableFilter],
        }),
        // Only scopes a view has actually written exist here, so an untouched
        // tab costs nothing.
        vs: captureViewState(s.viewState),
    }
    return prune(state as Record<string, any>) as SharedState
}

/**
 * Apply everything that does *not* depend on a network response.
 *
 * The selected variable is deliberately left out: it can only be validated
 * against `/variables`, so `app.vue`'s `getVariables()` owns that half (see
 * `applySharedVariable`). Anything a component has to consume at the right
 * moment instead of immediately — the map camera, Explore's window anchor —
 * is parked on the store as a one-shot `pending*` field.
 */
export function applyShareState(state: SharedState): void {
    const s = useMainStore()

    if (state.md) s.setMidDate(moment.utc(state.md))
    if (state.up) for (const [varId, unit] of Object.entries(state.up)) s.setUnitPreference(varId, unit)

    if (state.map) s.pendingMapView = { center: state.map.c, zoom: state.map.z, bearing: state.map.b ?? 0, pitch: state.map.p ?? 0 }
    if (state.pt) s.setLastClickedMapPoint({ lat: state.pt[0], lng: state.pt[1] })
    if (state.we) s.pendingWindowEnd = state.we

    if (state.tab) s.activeBottomTab = state.tab
    if (state.ev) s.exploreView = state.ev
    if (state.bin) s.exploreBinMode = state.bin
    if (state.as) s.analysisSource = state.as
    if (state.at) s.analysisTab = state.at
    if (state.ase) s.analysisSeason = state.ase
    if (state.ct) s.comparisonTab = state.ct
    if (state.cse) s.comparisonSeason = state.cse

    // Set directly rather than through `selectSensor`, which snaps the map's
    // depth to the nearest model level and pushes a snack explaining the move
    // — both wrong here: the shared depth is already what the sender saw.
    if (state.sen) s.setSelectedSensor({ id: state.sen[0], depth: state.sen[1] })

    if (state.xs?.length) s.setCrossSectionLine(state.xs.map(([lat, lng]) => ({ lat, lng })))

    const ui = state.ui
    if (ui) {
        if (ui.cp != null) s.isControlPanelOpen = ui.cp
        if (ui.cpw != null) s.controlPanel_width = ui.cpw
        if (ui.bc != null) s.setShowBathymetryContours(ui.bc)
        if (ui.ml != null) s.setShowMapLabels(ui.ml)
        if (ui.cc != null) s.setShowCursorCoords(ui.cc)
        if (ui.vp != null) s.setIsVerticalProfileOpen(ui.vp)
        if (ui.qm) s.queryMode = ui.qm
        if (ui.ar != null) s.setAutoRangeDisabled(ui.ar)
    }

    // Applied wholesale: each view reads its own scope lazily through
    // `useViewState`, so nothing has to exist yet for this to land.
    if (state.vs && typeof state.vs === 'object') s.setViewState(state.vs)

    const f = state.f
    if (f) {
        if (f.q != null) s.setSensorSearchQuery(f.q)
        if (f.o) s.setSensorOrganizationFilter([...f.o])
        if (f.v) s.setSensorVariableFilter([...f.v])
    }
}

/**
 * Resolve the shared variable selection against the freshly fetched
 * `/variables` list. Every field is validated independently and falls back to
 * the caller's default on its own, so a link that names a retired variable
 * still restores the depth/colormap the sender picked where those still make
 * sense. Returns the fields to apply plus a human-readable list of what could
 * not be honoured.
 */
export function applySharedVariable(
    state: SharedState,
    variables: Array<any>,
): { patch: Record<string, any>, issues: string[] } | null {
    const sv = state.sv
    if (!sv?.v) return null

    const issues: string[] = []
    const meta = variables.find(v => v.var === sv.v && (!sv.s || v.source === sv.s))
        ?? variables.find(v => v.var === sv.v)
    if (!meta) {
        issues.push(`variable "${sv.v}" is no longer available`)
        return { patch: {}, issues }
    }

    const patch: Record<string, any> = {
        var: meta.var,
        source: meta.source,
        precision: meta.precision || 0.1,
        colormap: sv.cm ?? meta.colormap ?? null,
        colormapMin: sv.cmn ?? meta.colormapMin ?? null,
        colormapMax: sv.cmx ?? meta.colormapMax ?? null,
    }
    if (sv.cs) patch.colormapStops = sv.cs

    // Depth: keep the shared level when the variable still has it, otherwise
    // snap to the nearest one (same treatment `selectSensor` gives a mooring
    // depth that falls between model levels).
    const depths: number[] = meta.depths ?? []
    if (sv.dn != null && depths.length) {
        const exact = depths.includes(sv.dn)
        const nearest = exact ? sv.dn : [...depths].sort((a, b) => Math.abs(a - sv.dn!) - Math.abs(b - sv.dn!))[0]!
        if (!exact) issues.push(`depth ${sv.dn} m no longer exists — showing the nearest level`)
        patch.depth_nc = nearest
        patch.depth = nearest === -1 ? 'bottom' : nearest.toFixed(1)
    }

    // Instant: snap to the nearest available timestamp rather than refusing —
    // the model's window rolls forward, so any link older than the retention
    // window would otherwise lose its time entirely.
    const dts: number[] = meta.dts ?? []
    if (sv.dt != null && dts.length) {
        const exact = dts.includes(sv.dt)
        if (exact) {
            patch.dt = moment.utc(sv.dt)
        } else {
            const nearest = dts.reduce((a, b) => Math.abs(b - sv.dt!) < Math.abs(a - sv.dt!) ? b : a)
            patch.dt = moment.utc(nearest)
            if (Math.abs(nearest - sv.dt) > 36e5) issues.push('the shared time is outside the current model window — showing the closest available')
        }
    }

    return { patch, issues }
}

// ── ENCODING ────────────────────────────────────────────────────────────────
// deflate-raw via the platform's own CompressionStream — no dependency, and
// this app already requires a desktop-class modern browser (see
// MobileBlocker.vue). Where it is missing the payload simply travels
// uncompressed under a different marker, so links stay readable both ways.

function bytesToB64Url(bytes: Uint8Array): string {
    let bin = ''
    for (const b of bytes) bin += String.fromCharCode(b)
    return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

function b64UrlToBytes(s: string): Uint8Array {
    const bin = atob(s.replace(/-/g, '+').replace(/_/g, '/'))
    const out = new Uint8Array(bin.length)
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i)
    return out
}

async function streamThrough(data: Uint8Array, stream: ReadableWritablePair<Uint8Array, Uint8Array>): Promise<Uint8Array> {
    const blob = new Blob([data as unknown as BlobPart])
    const piped = blob.stream().pipeThrough(stream as any)
    const buf = await new Response(piped as any).arrayBuffer()
    return new Uint8Array(buf)
}

/** JSON → deflate-raw → base64url, with a one-character format marker. */
export async function encodeShareState(state: SharedState): Promise<string> {
    const json = new TextEncoder().encode(JSON.stringify(state))
    if (typeof CompressionStream === 'undefined') return MARK_PLAIN + bytesToB64Url(json)
    try {
        return MARK_DEFLATE + bytesToB64Url(await streamThrough(json, new CompressionStream('deflate-raw')))
    } catch {
        return MARK_PLAIN + bytesToB64Url(json)
    }
}

/**
 * Decode a payload. Returns null for anything unusable — a truncated link, a
 * hand-edited hash, or a payload from a newer schema version — so a bad link
 * degrades to a normal cold start instead of a broken boot.
 */
export async function decodeShareState(payload: string): Promise<SharedState | null> {
    if (!payload) return null
    try {
        const marker = payload[0]
        const body = b64UrlToBytes(payload.slice(1))
        let json: Uint8Array
        if (marker === MARK_DEFLATE) {
            if (typeof DecompressionStream === 'undefined') return null
            json = await streamThrough(body, new DecompressionStream('deflate-raw'))
        } else if (marker === MARK_PLAIN) {
            json = body
        } else {
            return null
        }
        const parsed = JSON.parse(new TextDecoder().decode(json))
        if (!parsed || typeof parsed !== 'object') return null
        if (typeof parsed.v !== 'number' || parsed.v > SHARE_VERSION) return null
        return parsed as SharedState
    } catch (e) {
        console.warn('Could not read the shared view from this link:', e)
        return null
    }
}

/** The `s` payload in `window.location.hash`, or '' when there isn't one. */
export function readShareHash(): string {
    if (typeof window === 'undefined') return ''
    const hash = window.location.hash.replace(/^#/, '')
    if (!hash) return ''
    return new URLSearchParams(hash).get(HASH_KEY) ?? ''
}

/** An absolute link to the current view. */
export async function buildShareUrl(): Promise<string> {
    const payload = await encodeShareState(captureShareState())
    const { origin, pathname, search } = window.location
    return `${origin}${pathname}${search}#${HASH_KEY}=${payload}`
}
