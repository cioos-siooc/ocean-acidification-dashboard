<template>
  <div class="cross-section-panel flex flex-col h-full p-3">
    <!-- No line drawn yet — the whole view is keyed off the map's drawn polyline. -->
    <div v-if="!line" class="flex flex-col items-center justify-center grow text-center px-6">
      <UIcon name="i-mdi-vector-polyline" class="size-[56px] text-gray-500" />
      <div class="text-gray-500 mt-2">
        Draw a line on the map to see a depth section along it — click to add
        points, double-click (or press Enter) to finish.
      </div>
    </div>

    <template v-else>
      <div class="flex items-center mb-2" style="gap:8px;">
        <SegmentedControl v-model="binMode" :items="binModeItems" size="xs" :disabled="loading"
          aria-label="Time bin resolution" />
        <UIcon name="i-mdi-loading" class="animate-spin size-[14px] text-teal-400" v-if="loading" />
        <div class="grow" />
        <div class="time-controls-row mb-2">
          <TimeControls />
        </div>
        <div class="grow" />
        <UButton variant="subtle" size="xs" leading-icon="i-mdi-vector-line" @click="mainStore.requestCrossSectionRedraw()">
          New line
        </UButton>
      </div>

      <UAlert color="error" variant="subtle" class="mb-2" v-if="loadError">
        {{ loadError }}
      </UAlert>

      <ChartContextBar :items="contextItems" />

      <div class="chart-region grow">
        <div class="hm-slot">
          <TimeDepthHeatmap ref="panel" label="MODEL" :depths="depths" :values="grid" :bin-count="distances.length"
            :color-fn="seqColorFn" :gridline-bins="gridlineBins" :tooltip-formatter="cellTooltip" show-x-axis
            :x-label="xLabel" :mark-cell="selectedCell" @cell-click="onCellClick" />
          <!-- Segment-boundary markers. TimeDepthHeatmap's own gridlineBins is a
               single uniform interval, not arbitrary positions, so the original
               drawn vertices (irregularly spaced in sample-index space) are drawn
               as a plain CSS overlay instead of touching that shared component.
               Numbered 1..N to match the numbered markers index.vue draws at the
               same vertices on the map (see updateCrossSectionVertexLabels) — the
               numbers, not the dashes, are what ties a chart segment back to a
               specific point on the drawn line. -->
          <div v-for="v in vertexMarks" :key="v.num" class="vertex-mark" :class="{ 'vertex-mark--edge': v.isEdge }"
            :style="{ left: `calc(${AXIS_LEFT_PX}px + ${v.frac} * (100% - ${AXIS_LEFT_PX + DATAZOOM_RIGHT_PX}px))` }">
            <span class="vertex-num" :class="`vertex-num--${v.align}`">{{ v.num }}</span>
          </div>
        </div>

        <!-- Per-cell values now show in the heatmap's own ECharts tooltip (see
             `cellTooltip`) — this row is left for the two things that aren't
             hover-dependent: the vertex-marker hint and the empty-cell legend. -->
        <div class="info-row">
          <span class="hint-text">Numbered markers match the drawn line's vertices on the map</span>
          <div class="grow" />
          <div class="legend">
            <span class="swatch-hatch" title="no model data (below seabed)" />
            <span class="legend-label">no model data (below seabed)</span>
          </div>
        </div>

        <div v-if="loading" class="loading-overlay">
          <UIcon name="i-mdi-loading" class="animate-spin size-[42px] text-teal-400" />
        </div>
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import moment from 'moment'
import { useMainStore, formatDepthLabel } from '../../stores/main'
import { fetchCrossSection, type CrossSectionResponse } from '~~/composables/useCrossSectionFetch'
import { useVariableRegistry } from '~~/composables/useVariableRegistry'
import { resolveColormap } from '~~/composables/useColormapResolver'
import { BIN_CONFIG, type BinMode } from '~~/composables/useTimeDepthWindow'
import { utc2pst } from '~~/composables/useUTC2PST'
import TimeDepthHeatmap from '../depth/TimeDepthHeatmap.vue'
import TimeControls from '../TimeControls.vue'
import ChartContextBar from '../ChartContextBar.vue'
import SegmentedControl from '../ui/SegmentedControl.vue'
const binModeItems = computed(() => AVAILABLE_MODES.map(m => ({ value: m, label: BIN_CONFIG[m].short, title: BIN_CONFIG[m].label })))

/**
 * Cross-Section: the spatial counterpart to Explore's Model depth section —
 * a depth-vs-distance heatmap along an arbitrary line drawn on the map,
 * at one time snapshot (the map's own clock, via TimeControls), rather than
 * one grid cell over a time window.
 *
 * Gets its own footer tab (not a mode inside ExplorePanel) because drawing a
 * multi-vertex line is a different interaction than clicking a point, and
 * ExplorePanel's whole shell is built around a single clicked point.
 */

const props = defineProps<{ active?: boolean }>()

const mainStore = useMainStore()
const { toDisplayValue, displayUnit } = useVariableRegistry()

const line = computed(() => mainStore.crossSectionLine)
const source = computed(() => mainStore.selected_variable.source)
const varId = computed(() => mainStore.selected_variable.var)
const varMeta = computed(() => mainStore.variables.find(v => v.source === source.value && v.var === varId.value) ?? null)
const varName = computed(() => varMeta.value?.name || varId.value || 'Variable')

const AVAILABLE_MODES: BinMode[] = ['hourly', 'daily', 'monthly']
// Shared with Explore's own toggle — one "current resolution" for the whole
// app, matching the map's clock the same way TimeControls does.
const binMode = computed<BinMode>({
  get: () => mainStore.exploreBinMode,
  set: (m) => mainStore.setExploreBinMode(m),
})

// dt's shape tells the API which bin mode to read (see extract_cross_section's
// bin_mode branches): a full timestamp for hourly, YYYY-MM-DD for daily,
// YYYY-MM for monthly — same convention index.vue's own PNG layer fetch uses.
const dtStr = computed(() => {
  const dt = mainStore.selected_variable.dt
  if (!dt) return null
  const fmt = binMode.value === 'daily' ? 'YYYY-MM-DD' : binMode.value === 'monthly' ? 'YYYY-MM' : 'YYYY-MM-DDTHH:mm:ss'
  return dt.format(fmt)
})

// ── DATA ──────────────────────────────────────────────────────────────────────
const depths = ref<number[]>([])
const grid = ref<(number | null)[][]>([])
const distances = ref<number[]>([])
const vertexDistances = ref<number[]>([])
const points = ref<[number, number][]>([])
const loading = ref(false)
const loadError = ref<string | null>(null)

function applyResponse(resp: CrossSectionResponse) {
  depths.value = resp.depths
  grid.value = resp.model
  distances.value = resp.distances_km
  vertexDistances.value = resp.vertex_distances_km
  points.value = resp.points
}

let fetchSeq = 0
async function fetchSection() {
  const l = line.value, src = source.value, v = varId.value, dt = dtStr.value
  if (!l || l.length < 2 || !src || !v || !dt) return
  const reqId = ++fetchSeq
  loading.value = true
  loadError.value = null
  try {
    const resp = await fetchCrossSection({ source: src, var: v, vertices: l, dt, binMode: binMode.value })
    if (reqId !== fetchSeq) return
    applyResponse(resp)
  } catch (err: any) {
    if (reqId !== fetchSeq) return
    loadError.value = err?.response?.data?.detail || err?.message || 'Failed to load cross-section.'
    depths.value = []
    grid.value = []
    distances.value = []
    vertexDistances.value = []
  } finally {
    if (reqId === fetchSeq) loading.value = false
  }
}

// Only fetch while the tab is actually on screen — this sits in a v-show'd
// footer pane, and a drawn line/clock change would otherwise refetch here for
// a view nobody is looking at (mirrors ExplorePanel's own `props.active` gate).
// mainStore.unitPreference is included so toggling the display unit re-fetches
// (a cheap cache hit — see useCrossSectionFetch.ts) rather than leaving the
// section showing stale numbers under the old unit.
watch([line, source, varId, dtStr, binMode, () => props.active, () => mainStore.unitPreference[varId.value]], () => {
  if (props.active) fetchSection()
}, { immediate: true })

const totalDistanceLabel = computed(() => {
  const total = distances.value.length ? distances.value[distances.value.length - 1] : null
  return total != null ? `${total.toFixed(1)} km` : ''
})

// Mirrors selectedInfo.vue's own dt formatting: a real instant in hourly mode,
// a calendar-day/month bin start otherwise (see that component's comment).
const formattedDt = computed(() => {
  const dt = mainStore.selected_variable.dt
  if (!dt) return '—'
  if (binMode.value === 'monthly') return moment.utc(dt).format('MMM YYYY')
  if (binMode.value === 'daily') return moment.utc(dt).format('ddd MMM DD, YYYY')
  return utc2pst(moment(dt))
})

// ── CHART CONTEXT — what the section below is plotting: one field at one time
// snapshot, along the whole drawn line — no single coordinate or depth applies
// here the way they do for Explore's point-based views.
const contextItems = computed(() => {
  const items: { label: string; value: string }[] = [
    { label: 'Field', value: varName.value },
    { label: 'Datetime', value: formattedDt.value },
  ]
  if (line.value?.length) items.push({ label: 'Line', value: `${line.value.length} pts · ${totalDistanceLabel.value}` })
  return items
})

// A uniform fallback density (roughly one gridline every ~8 samples) — the
// real segment boundaries are the dashed `.vertex-mark` overlay below.
const gridlineBins = computed(() => Math.max(1, Math.round(distances.value.length / 8)))

function xLabel(binIdx: number) {
  const d = distances.value[Math.round(binIdx)]
  return d != null ? `${d.toFixed(1)}km` : ''
}

// ── SEGMENT-BOUNDARY OVERLAY ─────────────────────────────────────────────────
// Approximates TimeDepthHeatmap's internal `grid.left/right` plot-area inset
// (see its own AXIS_LEFT_PX/DATAZOOM_RIGHT_PX) since that geometry isn't
// exposed outside the component — good enough for a visual segment marker,
// not pixel-exact.
const AXIS_LEFT_PX = 44
const DATAZOOM_RIGHT_PX = 26
// 1-based, matching the numbered map markers (updateCrossSectionVertexLabels
// in index.vue). Endpoints (first/last) get a number badge but no dashed
// line — a line right on the plot's own edge would be redundant — and their
// badge aligns outward (start/end) instead of centering, so it doesn't hang
// off the chart into the axis label gutter.
const vertexMarks = computed(() => {
  const total = distances.value.length ? distances.value[distances.value.length - 1] : 0
  if (!total || !vertexDistances.value.length) return []
  const lastIdx = vertexDistances.value.length - 1
  return vertexDistances.value.map((d, i) => ({
    num: i + 1,
    frac: Math.max(0, Math.min(1, d / total)),
    isEdge: i === 0 || i === lastIdx,
    align: i === 0 ? 'start' : i === lastIdx ? 'end' : 'center',
  }))
})

// ── COLOUR RAMP — reads off the map's own colorbar, same as ExplorePanel's
// Model depth section (see its comment for why: one field, one scale). ───────
function hexToRgb(hex: string): [number, number, number] {
  const n = parseInt(hex.slice(1), 16)
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255]
}
function lerpRgb(a: number[], b: number[], t: number) { return [a[0]! + (b[0]! - a[0]!) * t, a[1]! + (b[1]! - a[1]!) * t, a[2]! + (b[2]! - a[2]!) * t] }
function rgbCss(c: number[]) { return `rgb(${c.map(v => Math.round(v)).join(',')})` }
function colorFromStops(stops: [number, string][], t: number): number[] {
  const clamped = Math.max(0, Math.min(1, t))
  const first = stops[0]!
  if (clamped <= first[0]) return hexToRgb(first[1])
  for (let i = 0; i < stops.length - 1; i++) {
    const [p0, c0] = stops[i]!
    const [p1, c1] = stops[i + 1]!
    if (clamped <= p1) return lerpRgb(hexToRgb(c0), hexToRgb(c1), p1 === p0 ? 0 : (clamped - p0) / (p1 - p0))
  }
  return hexToRgb(stops[stops.length - 1]![1])
}

const SEQ = [hexToRgb('#0d366b'), hexToRgb('#3987e5'), hexToRgb('#cde2fb')]

const resolvedSeqStops = computed(() =>
  resolveColormap(mainStore.colormaps, mainStore.selected_variable.colormap)?.stops ?? null)

// Converted to the current display unit — `grid`'s values are (useCrossSectionFetch.ts
// converts at the source), so these normalization bounds have to match.
const seqMin = computed(() => toDisplayValue(varId.value, mainStore.selected_variable.colormapMin ?? varMeta.value?.colormapMin ?? 0) ?? 0)
const seqMax = computed(() => {
  const hi = toDisplayValue(varId.value, mainStore.selected_variable.colormapMax ?? varMeta.value?.colormapMax ?? 1) ?? 1
  const lo = seqMin.value
  return hi > lo ? hi : lo + 1
})

const seqColorFn = computed(() => {
  const stops = resolvedSeqStops.value
  const lo = seqMin.value, hi = seqMax.value
  return (v: number) => {
    const t = hi === lo ? 0.5 : Math.max(0, Math.min(1, (v - lo) / (hi - lo)))
    if (stops && stops.length) return rgbCss(colorFromStops(stops, t))
    return rgbCss(t < 0.5 ? lerpRgb(SEQ[0]!, SEQ[1]!, t * 2) : lerpRgb(SEQ[1]!, SEQ[2]!, (t - 0.5) * 2))
  }
})

// ── TOOLTIP ───────────────────────────────────────────────────────────────────
const panel = ref<InstanceType<typeof TimeDepthHeatmap> | null>(null)

function cellTooltip(binIdx: number, depthIdx: number, value: number | null): string {
  const depth = depths.value[depthIdx]
  const dist = distances.value[binIdx]
  const distStr = dist != null ? `${dist.toFixed(1)} km` : '—'
  const depthStr = depth != null ? `${depth.toFixed(depth < 10 ? 1 : 0)}m` : '—'
  const valueStr = value == null ? 'no data' : `${value.toFixed(3)} ${displayUnit(varId.value)}`
  return `<div style="opacity:.7;margin-bottom:2px;">${distStr} &middot; ${depthStr}</div><div>${varName.value}: <b>${valueStr}</b></div>`
}

watch(() => props.active, (isActive) => {
  if (isActive) panel.value?.resize()
})

// ── CELL CLICK — this pane has no clicked map point of its own (the whole
// line is one query), but a clicked cell still names a real coordinate/depth
// along it. Feed both into the same universal selection ExplorePanel's own
// onCellClick drives, so the vertical-profile drawer and the map's depth
// layer react here too, same as they do for a clicked point elsewhere. ─────
function nearestLevelIdx(depth: number, levels: number[]) {
  let best = 0, bestDist = Infinity
  levels.forEach((d, i) => { const dist = Math.abs(d - depth); if (dist < bestDist) { bestDist = dist; best = i } })
  return best
}

function onCellClick({ binIdx, depthIdx }: { binIdx: number, depthIdx: number }) {
  const d = depths.value[depthIdx]
  const partial: Partial<typeof mainStore.selected_variable> = {}
  if (d != null) { partial.depth = formatDepthLabel(d); partial.depth_nc = d }
  if (Object.keys(partial).length) mainStore.updateSelectedVariable(partial)
  const p = points.value[binIdx]
  if (p) mainStore.setLastClickedMapPoint({ lat: p[0], lng: p[1] })
}

// Mirrors ExplorePanel's own `selectedCell`: derived from the universal
// selection rather than tracked locally, so it stays in sync no matter what
// moved it (a click here, or the point/depth changing elsewhere). Unlike
// Explore's time axis, distance-along-line has no natural match test, so a
// point selected far from this line (e.g. from Explore) is only highlighted
// if it lands within ~1.5x the resampled points' own spacing — otherwise
// it'd always snap to whichever end of the line happens to be nearest.
const selectedCell = computed<{ binIdx: number, depthIdx: number } | null>(() => {
  const p = mainStore.lastClickedMapPoint
  const d = mainStore.selected_variable.depth_nc
  if (!p || d == null || points.value.length < 2 || !depths.value.length) return null

  let binIdx = 0, bestDistSq = Infinity
  points.value.forEach(([plat, plng]: [number, number], i: number) => {
    const distSq = (plat - p.lat) ** 2 + (plng - p.lng) ** 2
    if (distSq < bestDistSq) { bestDistSq = distSq; binIdx = i }
  })

  const totalKm = distances.value[distances.value.length - 1] ?? 0
  const stepKm = totalKm / (points.value.length - 1)
  const thresholdDeg = (stepKm * 1.5) / 111
  if (Math.sqrt(bestDistSq) > thresholdDeg) return null

  return { binIdx, depthIdx: nearestLevelIdx(d, depths.value) }
})
</script>

<style scoped>
.cross-section-panel {
  min-height: 0;
}

.time-controls-row :deep(.time-controls) {
  margin: 0 !important;
  flex-wrap: nowrap;
  align-items: center;
}

.chart-region {
  position: relative;
  display: flex;
  flex-direction: column;
  min-height: 0;
}

.hm-slot {
  position: relative;
  flex-grow: 1;
  min-height: 0;
}

.loading-overlay {
  position: absolute;
  inset: 0;
  z-index: 20;
  display: flex;
  align-items: center;
  justify-content: center;
  background: rgba(10, 14, 18, 0.4);
  border-radius: 6px;
  pointer-events: none;
}

.vertex-mark {
  position: absolute;
  top: 0;
  bottom: 0;
  width: 0;
  border-left: 1px dashed rgba(255, 255, 255, 0.35);
  pointer-events: none;
}

.vertex-mark--edge {
  border-left: none;
}

.vertex-num {
  position: absolute;
  top: 2px;
  left: 0;
  background: rgba(18, 24, 31, 0.85);
  color: #eef3f7;
  font-size: 9px;
  font-weight: 700;
  line-height: 1;
  padding: 2px 4px;
  border-radius: 3px;
  border: 1px solid rgba(255, 255, 255, 0.25);
  white-space: nowrap;
}

.vertex-num--center {
  transform: translateX(-50%);
}

.vertex-num--start {
  transform: translateX(0);
}

.vertex-num--end {
  transform: translateX(-100%);
}

.info-row {
  display: flex;
  align-items: center;
  gap: 16px;
  margin-top: 6px;
  padding: 4px 10px;
  min-height: 30px;
  background: rgba(255, 255, 255, 0.03);
  border-radius: 4px;
  font-size: 11.5px;
}

.hint-text {
  color: rgba(255, 255, 255, 0.3);
  font-size: 11px;
}

.legend {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-shrink: 0;
}

.legend-label {
  font-size: 9.5px;
  color: rgba(255, 255, 255, 0.4);
}

.swatch-hatch {
  width: 18px;
  height: 11px;
  border-radius: 2px;
  background-color: #1a232c;
  background-image: repeating-linear-gradient(45deg, rgba(255, 255, 255, 0.22) 0, rgba(255, 255, 255, 0.22) 1px, transparent 1px, transparent 6px);
  border: 1px solid rgba(255, 255, 255, 0.09);
}
</style>
