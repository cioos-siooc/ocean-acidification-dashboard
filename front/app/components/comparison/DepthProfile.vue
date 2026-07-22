<template>
  <div class="depth-profile d-flex flex-column flex-grow-1" ref="rootRef">
    <div class="d-flex align-center mb-2" style="gap:8px;">
      <v-btn-toggle v-model="binMode" mandatory variant="tonal" density="compact" :disabled="loading" class="bin-mode-toggle">
        <v-btn value="hourly" size="x-small" :title="BIN_CONFIG.hourly.label">1H</v-btn>
        <v-btn value="6h" size="x-small" :title="BIN_CONFIG['6h'].label">6H</v-btn>
        <v-btn value="daily" size="x-small" :title="BIN_CONFIG.daily.label">1D</v-btn>
      </v-btn-toggle>
      <v-progress-circular v-if="loading" indeterminate size="14" width="2" color="teal" />
      <v-spacer />
      <v-btn icon="mdi-chevron-left" size="x-small" variant="text" :disabled="!canPageBack || loading" @click="page(-1)" />
      <v-menu v-model="dateMenuOpen" :close-on-content-click="false" :disabled="loading" location="bottom">
        <template #activator="{ props: menuProps }">
          <span v-bind="menuProps" class="text-caption range-label range-label--clickable" title="Jump to a date">{{ rangeLabel }}</span>
        </template>
        <v-card>
          <v-date-picker v-model="pickedDate" hide-header show-adjacent-months :min="minDateStr" :max="maxDateStr" />
          <v-card-actions>
            <v-spacer />
            <v-btn variant="text" @click="dateMenuOpen = false">Cancel</v-btn>
            <v-btn color="primary" variant="text" @click="confirmDatePick">OK</v-btn>
          </v-card-actions>
        </v-card>
      </v-menu>
      <v-btn icon="mdi-chevron-right" size="x-small" variant="text" :disabled="!canPageForward || loading" @click="page(1)" />
    </div>

    <v-alert v-if="loadError" type="error" variant="tonal" density="compact" border="start" class="mb-2">
      {{ loadError }}
    </v-alert>

    <div class="chart-region">
      <div class="hm-stack" @mouseleave="hoverInfo = null">
        <div class="hm-panel" :style="{ height: panelH + 'px' }">
          <div ref="mChartRef" class="hm-chart" />
          <span class="panel-label">MODEL</span>
        </div>
        <div class="hm-panel" :style="{ height: panelH + 'px' }">
          <div ref="sChartRef" class="hm-chart" />
          <span class="panel-label">SENSOR &#183; BINNED</span>
        </div>
        <div class="hm-panel" :style="{ height: panelH + 'px' }">
          <div ref="dChartRef" class="hm-chart" />
          <span class="panel-label">DIFF &#183; MODEL &#8722; SENSOR</span>
        </div>
      </div>

      <div class="hover-bar">
        <template v-if="hoverInfo">
          <span class="hover-dt">{{ hoverInfo.dt }} &#183; {{ hoverInfo.depth }}</span>
          <span class="hover-val"><span class="dot" style="background:#3987e5;" />Model <b>{{ hoverInfo.model }}</b></span>
          <span class="hover-val"><span class="dot" style="background:#a5d6a7;" />Sensor <b>{{ hoverInfo.sensor }}</b></span>
          <span class="hover-val"><span class="dot" style="background:#c53030;" />Diff <b>{{ hoverInfo.diff }}</b></span>
        </template>
        <span v-else class="hover-placeholder">Hover a heatmap to inspect values</span>
      </div>

      <div class="legend">
        <v-menu v-model="paletteMenuOpen" :close-on-content-click="false" location="bottom start">
          <template #activator="{ props: menuProps }">
            <v-btn v-bind="menuProps" icon size="x-small" variant="text" density="compact" title="Choose color palette">
              <v-icon size="14">mdi-palette</v-icon>
            </v-btn>
          </template>
          <v-card width="300px" class="pa-3">
            <PalettePicker v-model="seqColormapName" />
          </v-card>
        </v-menu>
        <div class="legend-item">
          <span class="legend-label">Model &#183; Sensor &#8212; {{ varName }}</span>
          <div class="ramp ramp-seq" :style="seqRampStyle" />
          <div class="ramp-ticks"><span>{{ seqMin.toFixed(1) }}</span><span>{{ seqMax.toFixed(1) }}</span></div>
        </div>
        <div class="legend-item">
          <span class="legend-label">Diff &#8212; model &#8722; sensor</span>
          <div class="ramp ramp-div" />
          <div class="ramp-ticks"><span>&#8722;{{ divRange.toFixed(1) }}</span><span>0</span><span>+{{ divRange.toFixed(1) }}</span></div>
        </div>
        <div class="legend-item gap-item">
          <span class="swatch-hatch" />
          <span class="legend-label" style="max-width:110px;">No cast in this bin</span>
        </div>
      </div>

      <div ref="depthHeaderRef">
        <div class="ctrl-label mt-4 mb-1">Single depth &#8212; {{ depthLabel }}, this window</div>
        <div class="d-flex" style="gap:16px; margin-bottom:4px;">
          <span class="chart-legend-item"><span class="swatch" style="background:#ff9800;" />Model</span>
          <span class="chart-legend-item"><span class="swatch" style="background:#a5d6a7;" />Sensor</span>
        </div>
      </div>
      <div ref="lineChartRef" :style="{ height: panelH + 'px' }" />

      <div v-if="loading" class="loading-overlay">
        <v-progress-circular indeterminate size="42" width="3" color="teal" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from 'vue'
import * as echarts from 'echarts'
import { registerEchartsDarkTheme } from '../../../composables/useEchartsTheme'
import { useMainStore } from '../../stores/main'
import { fetchDepthProfile, type DepthProfileResponse } from '../../../composables/useDepthProfileFetch'
import { resolveColormap } from '../../../composables/useColormapResolver'

const props = defineProps<{
  varName: string
}>()

const emit = defineEmits<{
  stats: [{ label: string, value: string, cls?: string }[]]
  'depth-selected': [number]
}>()

const mainStore = useMainStore()

// ── DEPTH LEVELS — real model levels for the current source/variable, with a fallback
// so the panel still renders something sane before /variables has loaded. ─────────────
const FALLBACK_DEPTHS = [0.5, 1.5, 3, 5, 7, 10, 14, 19, 25, 32, 40, 50, 62, 76, 92, 110]
const depths = computed<number[]>(() => {
  const found = mainStore.variables.find(v =>
    v.source === mainStore.selected_variable.source && v.var === mainStore.selected_variable.var
  )?.depths
  // -1 is the "bottom" pseudo-depth pick used elsewhere (see sensorComparison.vue's
  // depthLabel) — not a real numeric level, and NaNs out the sqrt depth scale below.
  const numeric = found?.filter(d => d >= 0)
  return numeric && numeric.length > 1 ? [...numeric].sort((a, b) => a - b) : FALLBACK_DEPTHS
})
const maxDepth = computed(() => depths.value[depths.value.length - 1] ?? 120)

// ── REQUEST CONTEXT — sensor id/coords come from the same store the rest of the app
// already keys off of (see sensorComparison.vue's isVariableDepth / selectedSensor). ──
const sensorId = computed(() => mainStore.selectedSensor?.id ?? null)
const sensorMeta = computed(() => mainStore.sensors.find(s => s.id === sensorId.value) ?? null)
const source = computed(() => mainStore.selected_variable.source)
const varId = computed(() => mainStore.selected_variable.var)

// ── REAL DATA — POST /depthProfile, one bounded window at a time. ──────────────────────
// Each bin resolution carries its own window length so bin *count* stays roughly
// constant (~336-365) across modes regardless of bin width — see the progressive:false
// note in baseOption() for why that invariant matters for render cost.
type BinMode = 'hourly' | '6h' | 'daily'
const BIN_CONFIG: Record<BinMode, { hours: 1 | 6 | 24; windowDays: number; gridlineBins: number; label: string }> = {
  hourly: { hours: 1, windowDays: 14, gridlineBins: 24, label: 'Hourly bins · 14-day window' },
  '6h': { hours: 6, windowDays: 91, gridlineBins: 28, label: '6-hour bins · 3-month window' },
  daily: { hours: 24, windowDays: 365, gridlineBins: 30, label: 'Daily bins · 1-year window' },
}
const binMode = ref<BinMode>('hourly')
const binHours = computed(() => BIN_CONFIG[binMode.value].hours)
const windowHours = computed(() => BIN_CONFIG[binMode.value].windowDays * 24)
const binCount = computed(() => windowHours.value / binHours.value)

function floorToBin(d: Date, hours: number) {
  const ms = hours * 3600e3
  return new Date(Math.floor(d.getTime() / ms) * ms)
}
function toApiIso(d: Date) { return d.toISOString().slice(0, 19) + 'Z' }

interface Grid { model: (number | null)[][]; sensor: (number | null)[][] }
const grid = ref<Grid>({ model: [], sensor: [] })
const loading = ref(false)
const loadError = ref<string | null>(null)

// ── WINDOW PAGING ──────────────────────────────────────────────────────────────────────
const windowEnd = ref<Date>(floorToBin(new Date(), 1))
const windowStart = computed(() => new Date(windowEnd.value.getTime() - windowHours.value * 3600e3))

const dataFloor = computed(() => sensorMeta.value?.first_data_at ? floorToBin(new Date(sensorMeta.value.first_data_at), binHours.value) : null)
const dataCeil = computed(() => floorToBin(sensorMeta.value?.latest_data_at ? new Date(sensorMeta.value.latest_data_at) : new Date(), binHours.value))

const canPageBack = computed(() => !dataFloor.value || windowStart.value.getTime() > dataFloor.value.getTime())
const canPageForward = computed(() => windowEnd.value.getTime() < dataCeil.value.getTime())

function clampWindowEnd(proposed: Date) {
  const ceil = dataCeil.value.getTime()
  const floor = dataFloor.value ? dataFloor.value.getTime() + windowHours.value * 3600e3 : -Infinity
  if (proposed.getTime() > ceil) return new Date(ceil)
  if (proposed.getTime() < floor) return new Date(floor)
  return proposed
}

function page(dir: number) {
  windowEnd.value = clampWindowEnd(new Date(windowEnd.value.getTime() + dir * windowHours.value * 3600e3))
}

// Switching bin mode resets the window to "latest" rather than trying to preserve the
// prior window's center — simpler, and consistent with how every other context change
// here (sensor/source/variable) already behaves.
function resetWindowToLatest() { windowEnd.value = dataCeil.value }
watch([sensorId, source, varId, binMode], resetWindowToLatest, { immediate: true })

// A plain month/day label was unambiguous while the window was always a 14-day span
// within one year, but the 6h/daily modes' 3-month/1-year windows can span a year
// boundary (or land almost exactly a year apart), where "Jul 22 – Jul 22" would
// misread as a zero-length window — include the year once it's no longer implied.
const rangeLabel = computed(() => {
  const crossesYear = windowStart.value.getUTCFullYear() !== windowEnd.value.getUTCFullYear()
  const opts: Intl.DateTimeFormatOptions = { month: 'short', day: 'numeric', timeZone: 'UTC' }
  if (crossesYear || binMode.value !== 'hourly') opts.year = 'numeric'
  const fmt = (d: Date) => d.toLocaleDateString('en-US', opts)
  return `${fmt(windowStart.value)} – ${fmt(windowEnd.value)}`
})

// ── JUMP-TO-DATE PICKER — pick a day, center a 14-day window on it ─────────────────────
const dateMenuOpen = ref(false)
const pickedDate = ref<Date | null>(null)
const minDateStr = computed(() => dataFloor.value ? dataFloor.value.toISOString().slice(0, 10) : undefined)
const maxDateStr = computed(() => dataCeil.value.toISOString().slice(0, 10))

// v-date-picker reads/writes a Date via its LOCAL Y/M/D getters, while every other date in
// this component is a raw UTC instant (rangeLabel formats with timeZone:'UTC', toApiIso emits
// raw ISO). Round-tripping a UTC instant through the picker naively (e.g. re-flooring the
// instant itself) shifts the selected day by the browser's UTC offset. Instead, always convert
// by carrying the *calendar* Y/M/D across — UTC-side getters when reading the center instant,
// local-side getters/constructor when talking to the picker — so the visible day never drifts.
watch(dateMenuOpen, (open) => {
  if (!open) return
  const centerInstant = new Date(windowStart.value.getTime() + (windowHours.value / 2) * 3600e3)
  pickedDate.value = new Date(centerInstant.getUTCFullYear(), centerInstant.getUTCMonth(), centerInstant.getUTCDate())
})

function confirmDatePick() {
  if (pickedDate.value) {
    const d = pickedDate.value
    const centerUTC = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()))
    windowEnd.value = clampWindowEnd(new Date(centerUTC.getTime() + (windowHours.value / 2) * 3600e3))
  }
  dateMenuOpen.value = false
}

// ── FETCH ──────────────────────────────────────────────────────────────────────────────
function nearestLevelIdx(depth: number, levels: number[]) {
  let best = 0, bestDist = Infinity
  levels.forEach((d, i) => { const dist = Math.abs(d - depth); if (dist < bestDist) { bestDist = dist; best = i } })
  return best
}

function applyResponse(resp: DepthProfileResponse) {
  const timeIndex = new Map<number, number>()
  resp.time.forEach((iso, i) => timeIndex.set(new Date(iso.endsWith('Z') ? iso : iso + 'Z').getTime(), i))

  const ds = depths.value
  const model: (number | null)[][] = ds.map(() => Array(binCount.value).fill(null))
  const sensor: (number | null)[][] = ds.map(() => Array(binCount.value).fill(null))
  const winStartMs = windowStart.value.getTime()
  const binMs = binHours.value * 3600e3

  ds.forEach((d, li) => {
    const ri = resp.depths.length ? nearestLevelIdx(d, resp.depths) : null
    if (ri === null) return
    for (let bi = 0; bi < binCount.value; bi++) {
      const ti = timeIndex.get(winStartMs + bi * binMs)
      if (ti == null) continue
      model[li][bi] = resp.model[ri]?.[ti] ?? null
      sensor[li][bi] = resp.sensor[ri]?.[ti] ?? null
    }
  })
  grid.value = { model, sensor }
}

let fetchSeq = 0
async function fetchWindow() {
  const sid = sensorId.value, meta = sensorMeta.value, src = source.value, v = varId.value
  if (!sid || !meta || !src || !v) return
  const reqId = ++fetchSeq
  loading.value = true
  loadError.value = null
  try {
    const resp = await fetchDepthProfile({
      source: src,
      var: v,
      sensorId: sid,
      lat: meta.latitude,
      lon: meta.longitude,
      fromDate: toApiIso(windowStart.value),
      toDate: toApiIso(windowEnd.value),
      binHours: binHours.value,
    })
    if (reqId !== fetchSeq) return
    applyResponse(resp)
  } catch (err: any) {
    if (reqId !== fetchSeq) return
    loadError.value = err?.response?.data?.detail || err?.message || 'Failed to load depth profile.'
    const ds = depths.value
    grid.value = { model: ds.map(() => Array(binCount.value).fill(null)), sensor: ds.map(() => Array(binCount.value).fill(null)) }
  } finally {
    if (reqId === fetchSeq) loading.value = false
    refreshCharts()
  }
}
watch([windowEnd, depths, binMode], fetchWindow, { immediate: true })

// ── SCALES — depth uses a sqrt scale so shallow levels (where most of the interesting
// variability lives) get more vertical resolution than the compressed deep levels. ─────
function depthToFrac(depth: number) { return Math.sqrt(depth / (maxDepth.value || 1)) }
function fracToDepth(f: number) { return f * f * (maxDepth.value || 1) }
const bounds = computed(() => {
  const ds = depths.value
  const b = [0]
  for (let i = 0; i < ds.length - 1; i++) b.push(depthToFrac((ds[i] + ds[i + 1]) / 2))
  b.push(1)
  return b
})

// ── COLOUR RAMPS ───────────────────────────────────────────────────────────────────────
function hexToRgb(hex: string): [number, number, number] {
  const n = parseInt(hex.slice(1), 16)
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255]
}
function lerpRgb(a: number[], b: number[], t: number) { return [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t, a[2] + (b[2] - a[2]) * t] }
function rgbCss(c: number[]) { return `rgb(${c.map(v => Math.round(v)).join(',')})` }

const SEQ = [hexToRgb('#0d366b'), hexToRgb('#3987e5'), hexToRgb('#cde2fb')]
const seqMin = ref(4), seqMax = ref(10.5)

// ── USER-SELECTABLE PALETTE (Model + Sensor panels share one scale/legend, so one picker
// covers both) — falls back to the SEQ default above until the user picks something. ────
const paletteMenuOpen = ref(false)
const seqColormapName = ref<string | null>(null)
const resolvedSeqStops = computed(() => {
  if (!seqColormapName.value) return null
  return resolveColormap(mainStore.colormaps, seqColormapName.value)?.stops ?? null
})
// echarts panels are updated imperatively, not reactively — repaint explicitly when the palette changes
watch(resolvedSeqStops, () => updateChartsData())
const seqRampStyle = computed(() => {
  const stops = resolvedSeqStops.value
  if (!stops || !stops.length) return {}
  const css = stops.map((s: any) => `${s[1]} ${Math.round(s[0] * 100)}%`).join(', ')
  return { background: `linear-gradient(90deg, ${css})` }
})
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
function seqColor(v: number) {
  const t = Math.max(0, Math.min(1, (v - seqMin.value) / (seqMax.value - seqMin.value)))
  const stops = resolvedSeqStops.value
  if (stops && stops.length) return colorFromStops(stops, t)
  return t < 0.5 ? lerpRgb(SEQ[0], SEQ[1], t * 2) : lerpRgb(SEQ[1], SEQ[2], (t - 0.5) * 2)
}

// blue-white-red diverging ramp — model above sensor (the reference/"true" value) reads red, below reads blue
const DIV_NEG = hexToRgb('#2b6cb0'), DIV_MID = hexToRgb('#ffffff'), DIV_POS = hexToRgb('#c53030')
const divRange = ref(0.8)
function divColor(v: number) {
  const t = Math.max(-1, Math.min(1, v / divRange.value))
  return t <= 0 ? lerpRgb(DIV_MID, DIV_NEG, -t) : lerpRgb(DIV_MID, DIV_POS, t)
}

// recompute magnitude/diff ranges whenever the grid changes, so the ramps track real data
watch(grid, (g) => {
  if (!g.model.length) return
  let lo = Infinity, hi = -Infinity, dLo = Infinity, dHi = -Infinity
  g.model.forEach((row, d) => row.forEach((mv, h) => {
    if (mv == null) return
    lo = Math.min(lo, mv); hi = Math.max(hi, mv)
    const sv = g.sensor[d][h]
    if (sv != null) { lo = Math.min(lo, sv); hi = Math.max(hi, sv); dLo = Math.min(dLo, mv - sv); dHi = Math.max(dHi, mv - sv) }
  }))
  if (lo === Infinity) return
  seqMin.value = lo; seqMax.value = hi
  divRange.value = Math.max(0.3, Math.max(-dLo, dHi))
})

// ── HEATMAP PANELS (ECharts, one instance per panel) — a real ECharts chart per panel is
// what lets us use its native `dataZoom` (slider + scroll/drag) for the depth axis, with
// the 3 panels kept in lockstep via `echarts.connect()` rather than a hand-rolled control. ─
type Which = 'model' | 'sensor' | 'diff'
const CHART_GROUP = 'depthProfileHeatmaps'
const AXIS_LEFT_PX = 44
const DATAZOOM_RIGHT_PX = 26

const mChartRef = ref<HTMLDivElement | null>(null)
const sChartRef = ref<HTMLDivElement | null>(null)
const dChartRef = ref<HTMLDivElement | null>(null)
let modelChart: echarts.ECharts | null = null
let sensorChart: echarts.ECharts | null = null
let diffChart: echarts.ECharts | null = null

const panelH = ref(96)
const rowGap = 3

function cellValue(which: Which, d: number, h: number): number | null {
  const mv = grid.value.model[d]?.[h] ?? null
  const sv = grid.value.sensor[d]?.[h] ?? null
  if (which === 'model') return mv
  if (which === 'sensor') return sv
  return (mv == null || sv == null) ? null : mv - sv
}

function colorFor(which: Which, v: number) {
  return rgbCss(which === 'diff' ? divColor(v) : seqColor(v))
}

// one data row per (depth bin, time bin): [binIdx, centerFrac, topFrac, bottomFrac, value, depthIdx]
function buildSeriesData(which: Which) {
  const ds = depths.value
  const b = bounds.value
  const data: (number | null)[][] = []
  ds.forEach((_, d) => {
    const top = b[d]!, bottom = b[d + 1]!
    const center = (top + bottom) / 2
    for (let bi = 0; bi < binCount.value; bi++) {
      data.push([bi, center, top, bottom, cellValue(which, d, bi), d])
    }
  })
  return data
}

function makeRenderItem(which: Which) {
  return (_params: unknown, api: any) => {
    const h = api.value(0) as number
    const top = api.value(2) as number
    const bottom = api.value(3) as number
    const v = api.value(4) as number | null
    const p0 = api.coord([h, top])
    const p1 = api.coord([h + 1, bottom])
    const x = Math.min(p0[0], p1[0])
    const y = Math.min(p0[1], p1[1])
    const width = Math.abs(p1[0] - p0[0]) + 0.6
    const height = Math.abs(p1[1] - p0[1]) + 0.6
    // "no cast" cells are stored as null, but ECharts' api.value() surfaces a null
    // numeric-dimension entry as NaN (not null) — so guard on both, else a NaN would
    // fall through to colorFor() and paint the top of the ramp (highest value).
    // Render nothing for these — the panel's own CSS hatch background (.hm-panel) shows
    // through instead of a per-cell decal. A per-cell decal looks identical but is
    // pathological here: ECharts' CustomView unconditionally marks the decal object dirty
    // on every renderItem call, so createOrUpdatePatternFromDecal regenerates the whole
    // offscreen canvas pattern from scratch on every single "no cast" cell — with casts
    // this sparse, thousands of cells per panel, which freezes the tab for many seconds.
    if (v == null || Number.isNaN(v)) return undefined
    return { type: 'rect', shape: { x, y, width, height }, style: { fill: colorFor(which, v) } }
  }
}

function markLineData() {
  return [{ yAxis: depthToFrac(depths.value[selectedDepthIdx.value] ?? 0) }]
}

function baseOption(which: Which): echarts.EChartsOption {
  const isBottom = which === 'diff'
  return {
    animation: false,
    grid: { left: AXIS_LEFT_PX, right: DATAZOOM_RIGHT_PX, top: 6, bottom: isBottom ? 20 : 6 },
    xAxis: {
      type: 'value', min: 0, max: binCount.value, interval: BIN_CONFIG[binMode.value].gridlineBins,
      axisLine: { lineStyle: { color: 'rgba(255,255,255,0.25)' } },
      axisTick: { show: false },
      splitLine: { show: false },
      axisLabel: isBottom
        ? {
            fontSize: 9.5, color: 'rgba(255,255,255,0.4)',
            formatter: (val: number) => new Date(windowStart.value.getTime() + val * binHours.value * 3600e3).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
          }
        : { show: false },
    },
    yAxis: {
      type: 'value', min: 0, max: 1, inverse: true,
      axisLine: { show: false },
      axisTick: { show: false },
      splitLine: { show: false },
      axisLabel: {
        fontSize: 9.5, color: 'rgba(255,255,255,0.4)',
        formatter: (val: number) => { const d = fracToDepth(val); return d.toFixed(d < 10 ? 1 : 0) + 'm' },
      },
    },
    dataZoom: [
      {
        type: 'slider', yAxisIndex: 0, right: 4, width: 14, start: 0, end: 100,
        showDetail: 'auto',
        labelFormatter: (val: number) => { const d = fracToDepth(val); return d.toFixed(d < 10 ? 1 : 0) + 'm' },
      },
      { type: 'inside', yAxisIndex: 0, zoomOnMouseWheel: true, moveOnMouseWheel: false, moveOnMouseMove: true },
    ],
    // per-cell tooltips were replaced by a single shared hover-info bar below all 3 panels
    // (three floating tooltip boxes moving independently was distracting) — see hoverInfo.
    tooltip: { show: false },
    series: [{
      type: 'custom',
      clip: true,
      // one panel's worth of cells (depths × binCount) comfortably exceeds ECharts'
      // default progressive-rendering threshold for custom series, which otherwise paints
      // only the first chunk (the shallowest depth bins, since data is ordered depth-major)
      // and silently never finishes the rest. A plain rect-per-cell render is cheap enough
      // to just do in one pass — this holds across all 3 bin modes since each mode's window
      // length was chosen specifically to keep binCount capped at ~365, never the "thousands
      // of cells" scale this cliff would actually bite at.
      progressive: false,
      renderItem: makeRenderItem(which),
      dimensions: ['bin', 'centerFrac', 'topFrac', 'bottomFrac', 'value', 'depthIdx'],
      encode: { x: 'bin', y: 'centerFrac' },
      data: buildSeriesData(which),
      markLine: {
        symbol: 'none', silent: true, animation: false,
        lineStyle: { color: '#35c2c9', type: 'dashed', width: 1.5 },
        label: which === 'model'
          ? { show: true, formatter: () => depthLabel.value, position: 'insideStartTop', color: '#06282a', backgroundColor: '#35c2c9', padding: [1, 6], borderRadius: 3, fontSize: 9.5, fontWeight: 700 }
          : { show: false },
        data: markLineData(),
      },
    }],
  }
}

// ── SHARED HOVER-INFO BAR — one row below all 3 panels instead of 3 independent floating
// tooltips (which was distracting to track). Any panel's mousemove updates it; leaving the
// whole stack clears it. Values are read straight out of `grid`, so the 3 panels never need
// to individually cooperate on what's under the cursor. ────────────────────────────────────
interface HoverInfo { dt: string, depth: string, model: string, sensor: string, diff: string }
const hoverInfo = ref<HoverInfo | null>(null)

function formatCellValue(v: number | null) { return v == null ? 'no cast' : v.toFixed(2) }

function handleChartMouseMove(chart: echarts.ECharts, x: number, y: number) {
  const [binVal, fracVal] = chart.convertFromPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [x, y]) as [number, number]
  if (binVal < 0 || binVal >= binCount.value || fracVal < 0 || fracVal > 1) { hoverInfo.value = null; return }
  const bin = Math.floor(binVal)
  const depthIdx = nearestLevelIdx(fracToDepth(fracVal), depths.value)
  const dt = new Date(windowStart.value.getTime() + bin * binHours.value * 3600e3)
  const mv = grid.value.model[depthIdx]?.[bin] ?? null
  const sv = grid.value.sensor[depthIdx]?.[bin] ?? null
  const dv = (mv == null || sv == null) ? null : mv - sv
  const depth = depths.value[depthIdx]
  hoverInfo.value = {
    dt: dt.toLocaleString('en-US', binMode.value === 'daily' ? { month: 'short', day: 'numeric' } : { month: 'short', day: 'numeric', hour: 'numeric' }),
    depth: depth != null ? `${depth.toFixed(depth < 10 ? 1 : 0)}m` : '—',
    model: formatCellValue(mv),
    sensor: formatCellValue(sv),
    diff: dv == null ? 'no cast' : `${dv >= 0 ? '+' : ''}${dv.toFixed(2)}`,
  }
}

function initCharts() {
  const specs: [Which, HTMLDivElement | null][] = [['model', mChartRef.value], ['sensor', sChartRef.value], ['diff', dChartRef.value]]
  const charts: (echarts.ECharts | null)[] = []
  for (const [which, el] of specs) {
    if (!el) { charts.push(null); continue }
    const chart = echarts.init(el, 'dark', { renderer: 'canvas' })
    chart.group = CHART_GROUP
    chart.setOption(baseOption(which))
    chart.on('click', (params: any) => {
      const depthIdx = params?.data?.[5]
      if (depthIdx == null) return
      selectedDepthIdx.value = depthIdx
      emit('depth-selected', depths.value[depthIdx])
    })
    chart.getZr().on('mousemove', (e: any) => handleChartMouseMove(chart, e.offsetX, e.offsetY))
    charts.push(chart)
  }
  modelChart = charts[0] ?? null
  sensorChart = charts[1] ?? null
  diffChart = charts[2] ?? null
  echarts.connect(CHART_GROUP)
}

function updateChartsData() {
  // xAxis.max/interval were baked into each chart's option at initCharts() time from
  // whatever binCount/gridlineBins were current then — re-patch them here (cheap, and
  // ECharts merges partial setOption calls) so a bin-mode switch actually rescales the
  // x-axis instead of clipping the new, differently-sized series to the old range.
  const xAxisPatch = { xAxis: { max: binCount.value, interval: BIN_CONFIG[binMode.value].gridlineBins } }
  modelChart?.setOption({ ...xAxisPatch, series: [{ data: buildSeriesData('model') }] })
  sensorChart?.setOption({ ...xAxisPatch, series: [{ data: buildSeriesData('sensor') }] })
  diffChart?.setOption({ ...xAxisPatch, series: [{ data: buildSeriesData('diff') }] })
}

function updateMarkLines() {
  const data = markLineData()
  modelChart?.setOption({ series: [{ markLine: { data } }] })
  sensorChart?.setOption({ series: [{ markLine: { data } }] })
  diffChart?.setOption({ series: [{ markLine: { data } }] })
}

// the depth zoom is only meaningful for the current variable's depth domain — reset it to
// full whenever the level set changes (e.g. switching variable/source to one with a
// different max depth) so a stale zoom can't point at the wrong depths.
function resetZoomAll() {
  [modelChart, sensorChart, diffChart].forEach(c => c?.dispatchAction({ type: 'dataZoom', start: 0, end: 100 }))
}
watch(depths, resetZoomAll)

function refreshCharts() {
  updateChartsData()
  updateMarkLines()
  updateLineChart()
}

// ── DEPTH SELECTION ────────────────────────────────────────────────────────────────────
const selectedDepthIdx = ref(0)
watch(depths, (ds) => {
  // Prefer whatever depth is already selected elsewhere (map control / a prior pick
  // here) so opening this view reflects it instead of always resetting to ~14m.
  const target = mainStore.selected_variable.depth_nc ?? 14
  let best = 0, bestDist = Infinity
  ds.forEach((d, i) => { const dist = Math.abs(d - target); if (dist < bestDist) { bestDist = dist; best = i } })
  selectedDepthIdx.value = best
}, { immediate: true })

watch(selectedDepthIdx, () => { updateMarkLines(); updateLineChart() })

// Live sync from outside (e.g. the map's depth control changing while this dialog is
// open) — idempotent with onDepthPicked's own writes, since that always writes back a
// value already equal to depths.value[idx], so this just resolves to the same index.
watch(() => mainStore.selected_variable.depth_nc, (d) => {
  if (d == null || !depths.value.length) return
  let best = 0, bestDist = Infinity
  depths.value.forEach((lvl, i) => { const dist = Math.abs(lvl - d); if (dist < bestDist) { bestDist = dist; best = i } })
  selectedDepthIdx.value = best
})

const depthLabel = computed(() => {
  const d = depths.value[selectedDepthIdx.value]
  return d != null ? `${d.toFixed(d < 10 ? 1 : 0)} m` : '—'
})

// ── SINGLE-DEPTH LINE CHART (ECharts, matches the rest of the app) ─────────────────────
const lineChartRef = ref<HTMLDivElement | null>(null)
let lineChart: echarts.ECharts | null = null

function updateLineChart() {
  if (!lineChart) return
  const d = selectedDepthIdx.value
  const model: [number, number | null][] = []
  const sensor: [number, number | null][] = []
  const winStartMs = windowStart.value.getTime()
  const binMs = binHours.value * 3600e3
  for (let bi = 0; bi < binCount.value; bi++) {
    const t = winStartMs + bi * binMs
    model.push([t, grid.value.model[d]?.[bi] ?? null])
    sensor.push([t, grid.value.sensor[d]?.[bi] ?? null])
  }

  let n = 0, sumDiff = 0, sumSqDiff = 0, sm = 0, ss = 0, smm = 0, sss = 0, sms = 0
  for (let bi = 0; bi < binCount.value; bi++) {
    const sv = sensor[bi][1]; const mv = model[bi][1]
    if (sv == null || mv == null) continue
    n++
    const diff = mv - sv
    sumDiff += diff; sumSqDiff += diff * diff
    sm += mv; ss += sv; smm += mv * mv; sss += sv * sv; sms += mv * sv
  }
  const bias = n ? sumDiff / n : 0
  const rmse = n ? Math.sqrt(sumSqDiff / n) : 0
  const cov = n ? sms / n - (sm / n) * (ss / n) : 0
  const varM = n ? smm / n - (sm / n) ** 2 : 0
  const varS = n ? sss / n - (ss / n) ** 2 : 0
  const r = n && varM > 0 && varS > 0 ? cov / Math.sqrt(varM * varS) : 0

  emit('stats', [
    { label: 'Bias', value: `${bias >= 0 ? '+' : ''}${bias.toFixed(3)}`, cls: bias >= 0 ? 'text-red-lighten-2' : 'text-blue-lighten-2' },
    { label: 'Correlation r', value: r.toFixed(3) },
    { label: 'RMSE', value: rmse.toFixed(3) },
    { label: 'Casts (n)', value: String(n) },
  ])

  lineChart.setOption({
    tooltip: {
      trigger: 'axis', confine: true,
      formatter: (params: any) => {
        const items = Array.isArray(params) ? params : [params]
        const date = new Date(items[0].value[0]).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: 'numeric' })
        let s = `<strong>${date}</strong><br/>`
        for (const p of items) if (p.value?.[1] != null) s += `${p.marker} ${p.seriesName}: <strong>${Number(p.value[1]).toFixed(3)}</strong><br/>`
        return s
      },
    },
    // Same fixed left/right pixel margins as the 3 heatmap panels above (AXIS_LEFT_PX /
    // DATAZOOM_RIGHT_PX), and an explicit min/max spanning the exact window bounds rather
    // than the data's min/max timestamp — otherwise this panel's x-axis (percent-based
    // margins, auto-fit to the data extent) drifts out of alignment with the heatmaps'
    // (fixed-pixel margins, axis domain fixed to [0, binCount]) as the plot area widths
    // differ and the last data point sits one bin short of windowEnd.
    grid: { left: AXIS_LEFT_PX, right: DATAZOOM_RIGHT_PX, bottom: 22, top: 10 },
    xAxis: {
      type: 'time', boundaryGap: false,
      min: windowStart.value.getTime(), max: windowEnd.value.getTime(),
      axisLabel: { fontSize: 9, color: '#ccc' },
    },
    // Unrounded axisLabel text (e.g. "20.86674690246582") is far wider than the heatmaps'
    // "159m"-style labels, so it doesn't fit the shared fixed AXIS_LEFT_PX and pushes this
    // panel's plot area right of the heatmaps' — round to keep the label width comparable.
    yAxis: {
      type: 'value', min: 'dataMin', max: 'dataMax',
      axisLabel: { fontSize: 9, color: '#ccc', formatter: (v: number) => v.toFixed(1) },
    },
    series: [
      { name: 'Model', type: 'line', data: model, symbol: 'none', lineStyle: { color: '#ff9800', width: 1.6 } },
      { name: 'Sensor', type: 'line', data: sensor, symbol: 'none', connectNulls: false, lineStyle: { color: '#a5d6a7', width: 1.6 } },
    ],
  }, true)
  lineChart.resize()
}

// ── LAYOUT — 4 equal-height "charts" (3 heatmap panels + the single-depth line chart)
// split the vertical space left over after the fixed chrome (top bar, alert, legend,
// single-depth header) rather than each other. Measured as a span between real rendered
// rects rather than summing each block's own height + a guessed margin constant — Vuetify
// utility margins (mb-2/mt-4/etc.) can CSS-collapse into a parent/sibling and silently
// disappear from getBoundingClientRect().height, which under-counts chrome and can make
// the heatmap block overflow its own box by a few px. Isolating the chrome as
// "everything from root-top through the end of the depth-header block, minus the one
// part we set explicitly in JS (the 3 heatmap panels + their gaps)" sidesteps that
// entirely, since it reads true painted positions instead of guessing box models. ──────
const rootRef = ref<HTMLDivElement | null>(null)
const depthHeaderRef = ref<HTMLDivElement | null>(null)

function computeLayout() {
  if (!rootRef.value || !depthHeaderRef.value) return
  const rootRect = rootRef.value.getBoundingClientRect()
  const depthHeaderRect = depthHeaderRef.value.getBoundingClientRect()
  const consumed = depthHeaderRect.bottom - rootRect.top
  const fixedChrome = consumed - (3 * panelH.value + rowGap * 2)
  const available = rootRect.height - fixedChrome - rowGap * 2
  panelH.value = Math.max(40, Math.floor(available / 4))
}

function resizeAllCharts() {
  modelChart?.resize(); sensorChart?.resize(); diffChart?.resize(); lineChart?.resize()
}

// ── LIFECYCLE ──────────────────────────────────────────────────────────────────────────
let resizeObs: ResizeObserver | null = null

onMounted(async () => {
  registerEchartsDarkTheme()
  await nextTick()
  computeLayout()
  initCharts()
  if (lineChartRef.value) lineChart = echarts.init(lineChartRef.value, 'dark', { renderer: 'canvas' })
  await nextTick()
  refreshCharts()
  resizeAllCharts()
  if (typeof ResizeObserver !== 'undefined') {
    resizeObs = new ResizeObserver(() => { computeLayout(); resizeAllCharts() })
    if (rootRef.value) resizeObs.observe(rootRef.value)
    if (lineChartRef.value) resizeObs.observe(lineChartRef.value)
  }
})

watch(loadError, () => nextTick().then(() => { computeLayout(); resizeAllCharts() }))

onBeforeUnmount(() => {
  resizeObs?.disconnect()
  modelChart?.dispose()
  sensorChart?.dispose()
  diffChart?.dispose()
  lineChart?.dispose()
})
</script>

<style scoped>
.depth-profile { min-height: 0; }

.chart-region { position: relative; }
.loading-overlay {
  position: absolute; inset: 0; z-index: 20;
  display: flex; align-items: center; justify-content: center;
  background: rgba(10, 14, 18, 0.4);
  border-radius: 6px;
  pointer-events: none;
}

.ctrl-label {
  font-size: 0.63rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: rgba(255, 255, 255, 0.38);
}
.range-label { min-width: 118px; text-align: center; color: rgba(255,255,255,0.6); font-variant-numeric: tabular-nums; }
.range-label--clickable { cursor: pointer; border-radius: 3px; }
.range-label--clickable:hover { color: #fff; text-decoration: underline dotted; }

.hm-stack { display: flex; flex-direction: column; }
.hm-panel {
  position: relative; margin-bottom: 3px; border-radius: 3px;
  background-color: #161e26;
  /* shows through "no cast" cells (renderItem returns nothing for them) — same pattern as
     the legend's .swatch-hatch, so unrendered cells and the legend swatch read as one thing */
  background-image: repeating-linear-gradient(45deg, rgba(255,255,255,0.22) 0, rgba(255,255,255,0.22) 1px, transparent 1px, transparent 6px);
}
.hm-chart { width: 100%; height: 100%; cursor: crosshair; }
.panel-label {
  position: absolute; top: 5px; left: 7px; font-size: 9px; font-weight: 700; letter-spacing: 0.06em;
  color: #eef3f7; background: rgba(11,17,22,0.55); padding: 2px 6px; border-radius: 3px; pointer-events: none; z-index: 2;
}

.hover-bar {
  display: flex; align-items: center; justify-content: center; gap: 22px; flex-wrap: wrap;
  margin-top: 8px; padding: 6px 10px; min-height: 28px;
  background: rgba(255,255,255,0.03); border-radius: 4px; font-size: 11.5px;
}
.hover-dt { color: rgba(255,255,255,0.55); font-variant-numeric: tabular-nums; }
.hover-val { display: flex; align-items: center; gap: 6px; color: rgba(255,255,255,0.6); font-variant-numeric: tabular-nums; }
.hover-val b { color: #eef3f7; font-weight: 700; }
.hover-val .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
.hover-placeholder { color: rgba(255,255,255,0.3); font-size: 11px; }

.legend { display: flex; align-items: center; justify-content: center; gap: 26px; flex-wrap: wrap; margin-top: 12px; padding-top: 10px; border-top: 1px solid rgba(255,255,255,0.08); }
.legend-item { display: flex; flex-direction: column; gap: 5px; }
.legend-label { font-size: 9.5px; color: rgba(255,255,255,0.4); }
.ramp { width: 110px; height: 8px; border-radius: 4px; }
.ramp-seq { background: linear-gradient(90deg, #0d366b, #3987e5, #cde2fb); }
.ramp-div { background: linear-gradient(90deg, #2b6cb0, #ffffff, #c53030); }
.ramp-ticks { display: flex; justify-content: space-between; font-size: 9px; color: rgba(255,255,255,0.4); width: 110px; font-variant-numeric: tabular-nums; }
.swatch-hatch {
  width: 20px; height: 12px; border-radius: 2px; background-color: #1a232c;
  background-image: repeating-linear-gradient(45deg, rgba(255,255,255,0.22) 0, rgba(255,255,255,0.22) 1px, transparent 1px, transparent 6px);
  border: 1px solid rgba(255,255,255,0.09);
}
.legend-item.gap-item { flex-direction: row; align-items: center; gap: 8px; }

.chart-legend-item { display: flex; align-items: center; gap: 6px; font-size: 11px; color: rgba(255,255,255,0.6); }
.chart-legend-item .swatch { width: 12px; height: 2.5px; border-radius: 2px; display: inline-block; }
</style>
