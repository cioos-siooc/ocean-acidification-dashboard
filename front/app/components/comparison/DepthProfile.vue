<template>
  <div class="depth-profile d-flex flex-column">
    <div class="d-flex align-center mb-2" style="gap:8px;">
      <span class="ctrl-label">Bounded window &#183; hourly bins</span>
      <v-progress-circular v-if="loading" indeterminate size="14" width="2" color="teal" />
      <v-spacer />
      <v-btn icon="mdi-chevron-left" size="x-small" variant="text" :disabled="!canPageBack || loading" @click="page(-1)" />
      <span class="text-caption range-label">{{ rangeLabel }}</span>
      <v-btn icon="mdi-chevron-right" size="x-small" variant="text" :disabled="!canPageForward || loading" @click="page(1)" />
    </div>

    <v-alert v-if="loadError" type="error" variant="tonal" density="compact" border="start" class="mb-2">
      {{ loadError }}
    </v-alert>

    <div class="hov-wrap">
      <div class="hov-inner" :style="{ width: innerWidth + 'px' }">
        <div class="hov-row">
          <div class="depth-axis" :style="{ height: (panelH * 3 + rowGap * 2) + 'px' }">
            <span v-for="(t, i) in depthTicks" :key="i" :style="{ top: t.top + 'px' }">{{ t.label }}</span>
          </div>
          <div class="panels-col" ref="panelsColRef">
            <div class="panel" :style="{ height: panelH + 'px' }"
              @mousemove="onMove($event, 'model')" @mouseleave="tooltip.show = false" @click="onPick">
              <canvas ref="cvModelRef" />
              <span class="panel-label">MODEL</span>
              <div class="guide-line" :style="guideStyle"><span class="guide-chip">{{ depthLabel }}</span></div>
            </div>
            <div class="panel" :style="{ height: panelH + 'px' }"
              @mousemove="onMove($event, 'sensor')" @mouseleave="tooltip.show = false" @click="onPick">
              <canvas ref="cvSensorRef" />
              <span class="panel-label">SENSOR &#183; BINNED</span>
              <div class="guide-line" :style="guideStyle" />
            </div>
            <div class="panel" :style="{ height: panelH + 'px' }"
              @mousemove="onMove($event, 'diff')" @mouseleave="tooltip.show = false" @click="onPick">
              <canvas ref="cvDiffRef" />
              <span class="panel-label">DIFF &#183; SENSOR &#8722; MODEL</span>
              <div class="guide-line" :style="guideStyle" />
            </div>
          </div>
        </div>
        <div class="time-axis" :style="{ marginLeft: axisW + 'px' }">
          <span v-for="(t, i) in timeTicks" :key="i" :style="{ left: t.left + 'px' }">{{ t.label }}</span>
        </div>
      </div>
      <div class="tooltip" v-show="tooltip.show" :style="{ left: tooltip.x + 'px', top: tooltip.y + 'px' }" v-html="tooltip.html" />
    </div>

    <div class="legend">
      <div class="legend-item">
        <span class="legend-label">Model &#183; Sensor &#8212; {{ varName }}</span>
        <div class="ramp ramp-seq" />
        <div class="ramp-ticks"><span>{{ seqMin.toFixed(1) }}</span><span>{{ seqMax.toFixed(1) }}</span></div>
      </div>
      <div class="legend-item">
        <span class="legend-label">Diff &#8212; sensor &#8722; model</span>
        <div class="ramp ramp-div" />
        <div class="ramp-ticks"><span>&#8722;{{ divRange.toFixed(1) }}</span><span>0</span><span>+{{ divRange.toFixed(1) }}</span></div>
      </div>
      <div class="legend-item gap-item">
        <span class="swatch-hatch" />
        <span class="legend-label" style="max-width:110px;">No cast in this bin</span>
      </div>
    </div>

    <div class="ctrl-label mt-4 mb-1">Single depth &#8212; {{ depthLabel }}, this window</div>
    <div class="d-flex" style="gap:16px; margin-bottom:4px;">
      <span class="chart-legend-item"><span class="swatch" style="background:#ff9800;" />Model</span>
      <span class="chart-legend-item"><span class="swatch" style="background:#a5d6a7;" />Sensor</span>
    </div>
    <div ref="lineChartRef" style="height:170px; flex-shrink:0;" />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from 'vue'
import * as echarts from 'echarts'
import { registerEchartsDarkTheme } from '../../../composables/useEchartsTheme'
import { useMainStore } from '../../stores/main'
import { fetchDepthProfile, type DepthProfileResponse } from '../../../composables/useDepthProfileFetch'

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
const WINDOW_HOURS = 144  // 6-day visible window
const STEP_HOURS = 48     // ~2-day page step

function floorHourUTC(d: Date) { const c = new Date(d); c.setUTCMinutes(0, 0, 0); return c }
function toApiIso(d: Date) { return d.toISOString().slice(0, 19) + 'Z' }

interface Grid { model: (number | null)[][]; sensor: (number | null)[][] }
const grid = ref<Grid>({ model: [], sensor: [] })
const loading = ref(false)
const loadError = ref<string | null>(null)

// ── WINDOW PAGING ──────────────────────────────────────────────────────────────────────
const windowEnd = ref<Date>(floorHourUTC(new Date()))
const windowStart = computed(() => new Date(windowEnd.value.getTime() - WINDOW_HOURS * 3600e3))

const dataFloor = computed(() => sensorMeta.value?.first_data_at ? floorHourUTC(new Date(sensorMeta.value.first_data_at)) : null)
const dataCeil = computed(() => floorHourUTC(sensorMeta.value?.latest_data_at ? new Date(sensorMeta.value.latest_data_at) : new Date()))

const canPageBack = computed(() => !dataFloor.value || windowStart.value.getTime() > dataFloor.value.getTime())
const canPageForward = computed(() => windowEnd.value.getTime() < dataCeil.value.getTime())

function page(dir: number) {
  const proposed = new Date(windowEnd.value.getTime() + dir * STEP_HOURS * 3600e3)
  const ceil = dataCeil.value.getTime()
  windowEnd.value = proposed.getTime() > ceil ? new Date(ceil) : proposed
}

function resetWindowToLatest() { windowEnd.value = dataCeil.value }
watch([sensorId, source, varId], resetWindowToLatest, { immediate: true })

const rangeLabel = computed(() => {
  const fmt = (d: Date) => d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' })
  return `${fmt(windowStart.value)} – ${fmt(windowEnd.value)}`
})

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
  const model: (number | null)[][] = ds.map(() => Array(WINDOW_HOURS).fill(null))
  const sensor: (number | null)[][] = ds.map(() => Array(WINDOW_HOURS).fill(null))
  const winStartMs = windowStart.value.getTime()

  ds.forEach((d, li) => {
    const ri = resp.depths.length ? nearestLevelIdx(d, resp.depths) : null
    if (ri === null) return
    for (let hw = 0; hw < WINDOW_HOURS; hw++) {
      const ti = timeIndex.get(winStartMs + hw * 3600e3)
      if (ti == null) continue
      model[li][hw] = resp.model[ri]?.[ti] ?? null
      sensor[li][hw] = resp.sensor[ri]?.[ti] ?? null
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
    })
    if (reqId !== fetchSeq) return
    applyResponse(resp)
  } catch (err: any) {
    if (reqId !== fetchSeq) return
    loadError.value = err?.response?.data?.detail || err?.message || 'Failed to load depth profile.'
    const ds = depths.value
    grid.value = { model: ds.map(() => Array(WINDOW_HOURS).fill(null)), sensor: ds.map(() => Array(WINDOW_HOURS).fill(null)) }
  } finally {
    if (reqId === fetchSeq) loading.value = false
    drawAll()
  }
}
watch([windowEnd, depths], fetchWindow, { immediate: true })

// ── SCALES ─────────────────────────────────────────────────────────────────────────────
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
function seqColor(v: number) {
  const t = Math.max(0, Math.min(1, (v - seqMin.value) / (seqMax.value - seqMin.value)))
  return t < 0.5 ? lerpRgb(SEQ[0], SEQ[1], t * 2) : lerpRgb(SEQ[1], SEQ[2], (t - 0.5) * 2)
}
const DIV_NEG = hexToRgb('#1c5cab'), DIV_MID = hexToRgb('#33404a'), DIV_POS = hexToRgb('#7a2320')
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
    if (sv != null) { lo = Math.min(lo, sv); hi = Math.max(hi, sv); dLo = Math.min(dLo, sv - mv); dHi = Math.max(dHi, sv - mv) }
  }))
  if (lo === Infinity) return
  seqMin.value = lo; seqMax.value = hi
  divRange.value = Math.max(0.3, Math.max(-dLo, dHi))
})

// ── CANVAS ─────────────────────────────────────────────────────────────────────────────
const cvModelRef = ref<HTMLCanvasElement | null>(null)
const cvSensorRef = ref<HTMLCanvasElement | null>(null)
const cvDiffRef = ref<HTMLCanvasElement | null>(null)
const panelsColRef = ref<HTMLDivElement | null>(null)

const panelH = 96
const rowGap = 3
const axisW = 44
const innerWidth = ref(778)
const DPR = Math.max(1, typeof window !== 'undefined' ? (window.devicePixelRatio || 1) : 1)

function setupCanvas(cv: HTMLCanvasElement, w: number, h: number) {
  cv.width = w * DPR; cv.height = h * DPR
  const ctx = cv.getContext('2d')!
  ctx.setTransform(DPR, 0, 0, DPR, 0, 0)
  return ctx
}

function drawPanel(cv: HTMLCanvasElement | null, w: number, colorFn: (d: number, h: number) => string | null) {
  if (!cv) return
  const ctx = setupCanvas(cv, w, panelH)
  ctx.fillStyle = '#161e26'
  ctx.fillRect(0, 0, w, panelH)
  const colW = w / WINDOW_HOURS
  const b = bounds.value
  depths.value.forEach((_, d) => {
    const y0 = b[d] * panelH, y1 = b[d + 1] * panelH
    for (let hw = 0; hw < WINDOW_HOURS; hw++) {
      const val = colorFn(d, hw)
      if (val === null) {
        ctx.save()
        ctx.beginPath()
        ctx.rect(hw * colW, y0, colW + 0.6, (y1 - y0) + 0.6)
        ctx.clip()
        ctx.strokeStyle = 'rgba(255,255,255,0.16)'
        ctx.lineWidth = 1
        for (let off = -(y1 - y0); off < colW; off += 6) {
          ctx.beginPath()
          ctx.moveTo(hw * colW + off, y1)
          ctx.lineTo(hw * colW + off + (y1 - y0), y0)
          ctx.stroke()
        }
        ctx.restore()
        continue
      }
      ctx.fillStyle = val
      ctx.fillRect(hw * colW, y0, colW + 0.6, (y1 - y0) + 0.6)
    }
  })
}

function drawAll() {
  const w = panelsColRef.value?.clientWidth || (innerWidth.value - axisW)
  drawPanel(cvModelRef.value, w, (d, h) => {
    const v = grid.value.model[d]?.[h]; return v == null ? null : rgbCss(seqColor(v))
  })
  drawPanel(cvSensorRef.value, w, (d, h) => {
    const v = grid.value.sensor[d]?.[h]; return v == null ? null : rgbCss(seqColor(v))
  })
  drawPanel(cvDiffRef.value, w, (d, h) => {
    const sv = grid.value.sensor[d]?.[h]; const mv = grid.value.model[d]?.[h]
    if (sv == null || mv == null) return null
    return rgbCss(divColor(sv - mv))
  })
  updateLineChart()
}

// ── DEPTH & TIME AXIS TICKS ────────────────────────────────────────────────────────────
// The real model's depth levels aren't evenly spaced, and the deepest few can still land
// close together under the sqrt scale (e.g. a 442m-deep column vs. a 110m-deep one bunches
// its 300m+ levels into a small pixel band). Instead of a fixed tick count, walk levels
// top to bottom and only keep one once it's at least MIN_GAP_PX away from the last kept
// tick — self-adapting to however many/however-distributed the levels are — and always
// keep the deepest level as a fixed lower bound.
const MIN_TICK_GAP_PX = 15
const depthTicks = computed(() => {
  const ds = depths.value
  if (!ds.length) return []
  const chosen: number[] = [0]
  let lastFrac = depthToFrac(ds[0])
  for (let i = 1; i < ds.length; i++) {
    const frac = depthToFrac(ds[i])
    if ((frac - lastFrac) * panelH >= MIN_TICK_GAP_PX) { chosen.push(i); lastFrac = frac }
  }
  const lastIdx = ds.length - 1
  if (chosen[chosen.length - 1] !== lastIdx) {
    const finalFrac = depthToFrac(ds[lastIdx])
    if (chosen.length > 1 && (finalFrac - lastFrac) * panelH < MIN_TICK_GAP_PX) chosen.pop()
    chosen.push(lastIdx)
  }
  const rows = [0, panelH + rowGap, 2 * (panelH + rowGap)]
  const out: { label: string, top: number }[] = []
  rows.forEach(rowTop => chosen.forEach(i => out.push({ label: ds[i].toFixed(ds[i] < 10 ? 1 : 0) + 'm', top: rowTop + depthToFrac(ds[i]) * panelH })))
  return out
})

const timeTicks = computed(() => {
  const w = (panelsColRef.value?.clientWidth) || (innerWidth.value - axisW)
  const out: { label: string, left: number }[] = []
  for (let td = 0; td * 24 <= WINDOW_HOURS; td++) {
    const hOff = td * 24
    const dt = new Date(windowStart.value.getTime() + hOff * 3600e3)
    out.push({ label: dt.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }), left: (hOff / WINDOW_HOURS) * w })
  }
  return out
})

// ── DEPTH SELECTION ────────────────────────────────────────────────────────────────────
const selectedDepthIdx = ref(0)
watch(depths, (ds) => {
  // keep pointing at ~14m (or the closest level) whenever the level set changes
  let best = 0, bestDist = Infinity
  ds.forEach((d, i) => { const dist = Math.abs(d - 14); if (dist < bestDist) { bestDist = dist; best = i } })
  selectedDepthIdx.value = best
}, { immediate: true })

const depthLabel = computed(() => {
  const d = depths.value[selectedDepthIdx.value]
  return d != null ? `${d.toFixed(d < 10 ? 1 : 0)} m` : '—'
})
const guideStyle = computed(() => ({ top: depthToFrac(depths.value[selectedDepthIdx.value] ?? 0) * panelH + 'px' }))

function nearestDepthIdx(frac: number) {
  const target = fracToDepth(frac)
  let best = 0, bestDist = Infinity
  depths.value.forEach((d, i) => { const dist = Math.abs(d - target); if (dist < bestDist) { bestDist = dist; best = i } })
  return best
}

function onPick(e: MouseEvent) {
  const el = e.currentTarget as HTMLElement
  const rect = el.getBoundingClientRect()
  const frac = Math.max(0, Math.min(1, (e.clientY - rect.top) / panelH))
  selectedDepthIdx.value = nearestDepthIdx(frac)
  emit('depth-selected', depths.value[selectedDepthIdx.value])
  updateLineChart()
}

// ── TOOLTIP ────────────────────────────────────────────────────────────────────────────
const tooltip = ref({ show: false, x: 0, y: 0, html: '' })
function onMove(e: MouseEvent, which: 'model' | 'sensor' | 'diff') {
  const el = e.currentTarget as HTMLElement
  const rect = el.getBoundingClientRect()
  const x = e.clientX - rect.left, y = e.clientY - rect.top
  const frac = Math.max(0, Math.min(1, y / panelH))
  const di = nearestDepthIdx(frac)
  const w = panelsColRef.value?.clientWidth || (innerWidth.value - axisW)
  const hw = Math.max(0, Math.min(WINDOW_HOURS - 1, Math.floor((x / w) * WINDOW_HOURS)))
  const mv = grid.value.model[di]?.[hw]
  const sv = grid.value.sensor[di]?.[hw]
  let v: number | null = null, label = ''
  if (which === 'model') { v = mv ?? null; label = 'Model' }
  else if (which === 'sensor') { v = sv ?? null; label = 'Sensor' }
  else { v = (sv == null || mv == null) ? null : sv - mv; label = 'Diff' }
  const dt = new Date(windowStart.value.getTime() + hw * 3600e3)
  tooltip.value = {
    show: true,
    x: Math.min(w + axisW - 8, x + axisW + 12),
    y: (el.parentElement ? (el as HTMLElement).offsetTop : 0) + y - 8,
    html: `<b>${dt.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: 'numeric' })}</b> &#183; ${depths.value[di]}m<br>${label}: <b>${v == null ? 'no cast' : v.toFixed(2)}</b>`,
  }
}

// ── SINGLE-DEPTH LINE CHART (ECharts, matches the rest of the app) ─────────────────────
const lineChartRef = ref<HTMLDivElement | null>(null)
let lineChart: echarts.ECharts | null = null

function updateLineChart() {
  if (!lineChart) return
  const d = selectedDepthIdx.value
  const model: [number, number | null][] = []
  const sensor: [number, number | null][] = []
  const winStartMs = windowStart.value.getTime()
  for (let hw = 0; hw < WINDOW_HOURS; hw++) {
    const t = winStartMs + hw * 3600e3
    model.push([t, grid.value.model[d]?.[hw] ?? null])
    sensor.push([t, grid.value.sensor[d]?.[hw] ?? null])
  }

  let n = 0, sumDiff = 0, sumSqDiff = 0, sm = 0, ss = 0, smm = 0, sss = 0, sms = 0
  for (let hw = 0; hw < WINDOW_HOURS; hw++) {
    const sv = sensor[hw][1]; const mv = model[hw][1]
    if (sv == null || mv == null) continue
    n++
    const diff = sv - mv
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
    grid: { left: '8%', right: '2%', bottom: '14%', top: '10%', containLabel: true },
    xAxis: { type: 'time', boundaryGap: false, axisLabel: { fontSize: 9, color: '#ccc' } },
    yAxis: { type: 'value', axisLabel: { fontSize: 9, color: '#ccc' }, min: 'dataMin', max: 'dataMax' },
    series: [
      { name: 'Model', type: 'line', data: model, symbol: 'none', lineStyle: { color: '#ff9800', width: 1.6 } },
      { name: 'Sensor', type: 'line', data: sensor, symbol: 'none', connectNulls: false, lineStyle: { color: '#a5d6a7', width: 1.6 } },
    ],
  }, true)
  lineChart.resize()
}

// ── LIFECYCLE ──────────────────────────────────────────────────────────────────────────
let resizeObs: ResizeObserver | null = null

onMounted(async () => {
  registerEchartsDarkTheme()
  await nextTick()
  if (lineChartRef.value) lineChart = echarts.init(lineChartRef.value, 'dark', { renderer: 'canvas' })
  drawAll()
  if (typeof ResizeObserver !== 'undefined' && panelsColRef.value) {
    resizeObs = new ResizeObserver(() => { drawAll(); lineChart?.resize() })
    resizeObs.observe(panelsColRef.value)
  }
})

onBeforeUnmount(() => {
  resizeObs?.disconnect()
  lineChart?.dispose()
})
</script>

<style scoped>
.depth-profile { min-height: 0; }

.ctrl-label {
  font-size: 0.63rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: rgba(255, 255, 255, 0.38);
}
.range-label { min-width: 118px; text-align: center; color: rgba(255,255,255,0.6); font-variant-numeric: tabular-nums; }

.hov-wrap { position: relative; overflow-x: auto; padding-bottom: 4px; }
.hov-inner { position: relative; }
.hov-row { display: flex; align-items: stretch; position: relative; }
.depth-axis { width: 44px; flex: 0 0 auto; position: relative; }
.depth-axis span {
  position: absolute; right: 8px; transform: translateY(-50%);
  font-size: 9.5px; color: rgba(255,255,255,0.4); font-variant-numeric: tabular-nums;
}
.panels-col { flex: 1 1 auto; position: relative; min-width: 0; }
.panel { position: relative; margin-bottom: 3px; cursor: crosshair; }
.panel canvas { display: block; width: 100%; height: 100%; border-radius: 3px; }
.panel-label {
  position: absolute; top: 5px; left: 7px; font-size: 9px; font-weight: 700; letter-spacing: 0.06em;
  color: #eef3f7; background: rgba(11,17,22,0.55); padding: 2px 6px; border-radius: 3px; pointer-events: none;
}
.guide-line { position: absolute; left: 0; right: 0; height: 0; border-top: 1.5px dashed #35c2c9; pointer-events: none; opacity: 0.9; }
.guide-chip {
  position: absolute; left: -44px; transform: translateY(-50%);
  background: #35c2c9; color: #06282a; font-size: 9.5px; font-weight: 700;
  padding: 1px 6px; border-radius: 3px; white-space: nowrap; font-variant-numeric: tabular-nums; pointer-events: none;
}
.time-axis { position: relative; height: 14px; margin-top: 4px; }
.time-axis span { position: absolute; transform: translateX(-50%); font-size: 9.5px; color: rgba(255,255,255,0.4); }

.tooltip {
  position: absolute; pointer-events: none; background: #212c36; border: 1px solid rgba(255,255,255,0.1);
  border-radius: 6px; padding: 6px 9px; font-size: 11px; line-height: 1.5; color: #eef3f7;
  box-shadow: 0 6px 18px rgba(0,0,0,0.4); white-space: nowrap; z-index: 5;
}
.tooltip :deep(b) { font-variant-numeric: tabular-nums; }

.legend { display: flex; gap: 26px; flex-wrap: wrap; margin-top: 12px; padding-top: 10px; border-top: 1px solid rgba(255,255,255,0.08); }
.legend-item { display: flex; flex-direction: column; gap: 5px; }
.legend-label { font-size: 9.5px; color: rgba(255,255,255,0.4); }
.ramp { width: 110px; height: 8px; border-radius: 4px; }
.ramp-seq { background: linear-gradient(90deg, #0d366b, #3987e5, #cde2fb); }
.ramp-div { background: linear-gradient(90deg, #9ec5f4, #1c5cab, #33404a, #7a2320, #f2a19c); }
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
