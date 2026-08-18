<template>
  <div class="tdh-panel">
    <div ref="chartRef" class="tdh-chart" />
    <span v-if="label" class="tdh-label">{{ label }}</span>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onBeforeUnmount } from 'vue'
import * as echarts from 'echarts'
import { registerEchartsDarkTheme } from '~~/composables/useEchartsTheme'

/**
 * One time-depth (Hovmöller) heatmap panel: bins across x, model depth levels
 * down y, one rect per cell.
 *
 * Owns the ECharts `custom` series and the sqrt depth scale, so every consumer
 * (Comparison tab's model/sensor/diff stack, the Depth tab's model view) gets
 * the same cell geometry, depth zoom and hover behaviour. Cells are addressed
 * by *index* — `values[depthIdx][binIdx]` — and the panel never learns what a
 * bin means in wall-clock terms; `xLabel` turns a bin index into axis text and
 * the parent resolves hover/click indices against its own data.
 */

const props = withDefaults(defineProps<{
  /** Model depth levels in metres, ascending. */
  depths: number[]
  /** Cell values as [depthIdx][binIdx]; null renders as an empty (hatched) cell. */
  values: (number | null)[][]
  binCount: number
  /**
   * Value -> CSS colour. Pass a `computed` that *returns* a function rather
   * than a plain function, so its identity changes when the palette changes —
   * that identity is what triggers a repaint when the data itself is unchanged.
   */
  colorFn: (v: number) => string
  /** x-axis gridline/label spacing, in bins. */
  gridlineBins: number
  /** Bin index -> axis label text. Only consulted when `showXAxis`. */
  xLabel?: (binIdx: number) => string
  showXAxis?: boolean
  /** Shared `echarts.connect` group — panels in one group zoom in lockstep. */
  chartGroup?: string
  /** Depth in metres to mark with a horizontal rule, or null for none. */
  markDepth?: number | null
  markLabel?: string
  /** Last-clicked cell to outline, or null for none. */
  markCell?: { binIdx: number, depthIdx: number } | null
  /** Caption drawn over the panel's top-left corner. */
  label?: string
  /**
   * Enables ECharts' own per-cell tooltip, formatted by the caller from
   * (binIdx, depthIdx, value). Left unset by multi-panel consumers (Comparison's
   * stacked model/sensor view) — see the `tooltip` option below for why.
   */
  tooltipFormatter?: (binIdx: number, depthIdx: number, value: number | null) => string
}>(), {
  showXAxis: false,
  markDepth: null,
  markCell: null,
})

const emit = defineEmits<{
  'cell-click': [payload: { binIdx: number, depthIdx: number }]
  hover: [payload: { binIdx: number, depthIdx: number } | null]
}>()

const AXIS_LEFT_PX = 44
const DATAZOOM_RIGHT_PX = 26

const chartRef = ref<HTMLDivElement | null>(null)
let chart: echarts.ECharts | null = null

// ── DEPTH SCALE — sqrt, so shallow levels (where most of the interesting
// variability lives) get more vertical resolution than the compressed deep
// ones. ───────────────────────────────────────────────────────────────────────
const maxDepth = computed(() => props.depths[props.depths.length - 1] || 1)
function depthToFrac(depth: number) { return Math.sqrt(depth / maxDepth.value) }
function fracToDepth(f: number) { return f * f * maxDepth.value }

/** Cell edges in frac space: one more entry than there are depth levels. */
const bounds = computed(() => {
  const ds = props.depths
  const b = [0]
  for (let i = 0; i < ds.length - 1; i++) b.push(depthToFrac((ds[i]! + ds[i + 1]!) / 2))
  b.push(1)
  return b
})

function nearestDepthIdx(depth: number) {
  let best = 0, bestDist = Infinity
  props.depths.forEach((d, i) => { const dist = Math.abs(d - depth); if (dist < bestDist) { bestDist = dist; best = i } })
  return best
}

// one row per (depth level, bin): [binIdx, centerFrac, topFrac, bottomFrac, value, depthIdx]
function buildSeriesData() {
  const b = bounds.value
  const data: (number | null)[][] = []
  props.depths.forEach((_, d) => {
    const top = b[d]!, bottom = b[d + 1]!
    const center = (top + bottom) / 2
    for (let bi = 0; bi < props.binCount; bi++) {
      data.push([bi, center, top, bottom, props.values[d]?.[bi] ?? null, d])
    }
  })
  return data
}

function renderItem(_params: unknown, api: any) {
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
  // Empty cells are stored as null, but ECharts' api.value() surfaces a null
  // numeric-dimension entry as NaN (not null) — so guard on both, else a NaN
  // would fall through to colorFn() and paint the top of the ramp (highest
  // value). Render nothing for these — the panel's own CSS hatch background
  // shows through instead of a per-cell decal. A per-cell decal looks identical
  // but is pathological here: ECharts' CustomView unconditionally marks the
  // decal object dirty on every renderItem call, so
  // createOrUpdatePatternFromDecal regenerates the whole offscreen canvas
  // pattern from scratch for every single empty cell — thousands of them per
  // panel when data is sparse, which freezes the tab for many seconds.
  if (v == null || Number.isNaN(v)) return undefined
  return { type: 'rect', shape: { x, y, width, height }, style: { fill: props.colorFn(v) } }
}

function markLineData() {
  return props.markDepth == null ? [] : [{ yAxis: depthToFrac(props.markDepth) }]
}

// ── HOVER/CLICK CROSSHAIRS ───────────────────────────────────────────────────
// Drawn as raw zrender Line graphics, positioned directly with .attr() rather
// than routed through chart.setOption(). The hover crosshair repositions on
// every mousemove-driven cell change — a custom series has no fine-grained
// diffing, so a setOption there re-runs renderItem for every cell (hundreds
// to low thousands) on each call, which reads as visible lag trailing the
// cursor. Mutating standalone graphic elements skips ECharts' update pipeline
// entirely, so it's effectively free regardless of how many cells the panel
// has. The click crosshair works the same way for consistency, though it
// changes far less often.
const hoverCell = ref<{ binIdx: number, depthIdx: number } | null>(null)
let hoverLineX: any = null, hoverLineY: any = null
let clickLineX: any = null, clickLineY: any = null

/** Cell centre in [x, yFrac] chart coordinates. */
function cellCenter(binIdx: number, depthIdx: number): [number, number] {
  const b = bounds.value
  const top = b[depthIdx] ?? 0, bottom = b[depthIdx + 1] ?? 1
  return [binIdx + 0.5, (top + bottom) / 2]
}

function makeCrosshairLine(color: string, width: number, dashed: boolean) {
  return new echarts.graphic.Line({
    shape: { x1: 0, y1: 0, x2: 0, y2: 0 },
    style: { stroke: color, lineWidth: width, lineDash: dashed ? [4, 4] : undefined },
    silent: true, // must not steal hit-testing from the series' own click handler
    invisible: true,
    z: 10,
  })
}

function toPixel(x: number, yFrac: number): [number, number] {
  return chart!.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [x, yFrac]) as [number, number]
}

/** Repositions one crosshair pair to a cell centre, in current pixel space —
 *  called on hover/click changes but also on zoom/pan/resize, since those
 *  change the data->pixel mapping without changing which cell is marked. */
function positionCrosshair(lineX: any, lineY: any, cell: { binIdx: number, depthIdx: number } | null) {
  if (!chart || !lineX || !lineY) return
  if (!cell) { lineX.attr({ invisible: true }); lineY.attr({ invisible: true }); return }
  const [x, yFrac] = cellCenter(cell.binIdx, cell.depthIdx)
  const [px, pyTop] = toPixel(x, 0)
  const [, pyBottom] = toPixel(x, 1)
  const [pxLeft, py] = toPixel(0, yFrac)
  const [pxRight] = toPixel(props.binCount, yFrac)
  lineX.attr({ invisible: false, shape: { x1: px, y1: pyTop, x2: px, y2: pyBottom } })
  lineY.attr({ invisible: false, shape: { x1: pxLeft, y1: py, x2: pxRight, y2: py } })
}

function repositionCrosshairs() {
  positionCrosshair(hoverLineX, hoverLineY, hoverCell.value)
  positionCrosshair(clickLineX, clickLineY, props.markCell ?? null)
}

/** Plot-area margins, shared by init and the showXAxis re-patch below. */
function gridConfig() {
  return { left: AXIS_LEFT_PX, right: DATAZOOM_RIGHT_PX, top: 6 }
}

function baseOption(): echarts.EChartsOption {
  return {
    animation: false,
    grid: { ...gridConfig(), bottom: props.showXAxis ? 20 : 6 },
    xAxis: {
      type: 'value', min: 0, max: props.binCount, interval: props.gridlineBins,
      axisLine: { lineStyle: { color: 'rgba(255,255,255,0.25)' } },
      axisTick: { show: false },
      splitLine: { show: false },
      axisLabel: props.showXAxis
        ? { fontSize: 9.5, color: 'rgba(255,255,255,0.4)', formatter: (val: number) => props.xLabel?.(val) ?? '' }
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
    // Per-cell tooltips are opt-in via `tooltipFormatter`. Left off by default:
    // consumers stacking several panels (Comparison's model/sensor view) render
    // one shared hover-info bar instead, since several floating tooltip boxes
    // moving independently was distracting — a single-panel consumer has no
    // such conflict, so it can just use ECharts' own tooltip directly.
    tooltip: props.tooltipFormatter
      ? {
          trigger: 'item',
          confine: true,
          backgroundColor: 'rgba(18,24,31,0.92)',
          borderColor: 'rgba(255,255,255,0.18)',
          borderWidth: 1,
          padding: 8,
          textStyle: { color: '#eef3f7', fontSize: 11 },
          formatter: (params: any) => {
            const d = params.data as (number | null)[]
            const bin = d[0] as number
            const depthIdx = d[5] as number
            const v = d[4] as number | null
            return props.tooltipFormatter!(bin, depthIdx, v == null ? null : v)
          },
        }
      : { show: false },
    series: [{
      type: 'custom',
      clip: true,
      // One panel's worth of cells (depths x binCount) comfortably exceeds
      // ECharts' default progressive-rendering threshold for custom series,
      // which otherwise paints only the first chunk (the shallowest depths,
      // since data is ordered depth-major) and silently never finishes the
      // rest. A plain rect-per-cell render is cheap enough to do in one pass —
      // this holds across all bin modes because each mode's window length was
      // chosen to cap binCount in the low hundreds (see BIN_CONFIG), never the
      // "thousands of cells" scale this cliff would actually bite at.
      progressive: false,
      renderItem,
      dimensions: ['bin', 'centerFrac', 'topFrac', 'bottomFrac', 'value', 'depthIdx'],
      encode: { x: 'bin', y: 'centerFrac' },
      data: buildSeriesData(),
      markLine: {
        symbol: 'none', silent: true, animation: false,
        lineStyle: { color: '#35c2c9', type: 'dashed', width: 1.5 },
        label: props.markLabel
          ? { show: true, formatter: () => props.markLabel!, position: 'insideStartTop', color: '#06282a', backgroundColor: '#35c2c9', padding: [1, 6], borderRadius: 3, fontSize: 9.5, fontWeight: 700 }
          : { show: false },
        data: markLineData(),
      },
    }],
  }
}

function setHoverCell(next: { binIdx: number, depthIdx: number } | null) {
  const prev = hoverCell.value
  if (prev?.binIdx === next?.binIdx && prev?.depthIdx === next?.depthIdx) return
  hoverCell.value = next
  positionCrosshair(hoverLineX, hoverLineY, next)
}

function handleMouseMove(x: number, y: number) {
  if (!chart) return
  const [binVal, fracVal] = chart.convertFromPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [x, y]) as [number, number]
  if (binVal < 0 || binVal >= props.binCount || fracVal < 0 || fracVal > 1) {
    emit('hover', null)
    setHoverCell(null)
    return
  }
  const cell = { binIdx: Math.floor(binVal), depthIdx: nearestDepthIdx(fracToDepth(fracVal)) }
  emit('hover', cell)
  setHoverCell(cell)
}

// xAxis.max/interval are baked in at init time from whatever binCount/
// gridlineBins were current then — re-patch them alongside the data (cheap, and
// ECharts merges partial setOption calls) so a bin-mode switch actually
// rescales the x-axis instead of clipping the new, differently-sized series to
// the old range.
function updateData() {
  // The hovered cell may no longer exist under new data (e.g. a smaller
  // binCount) — drop it rather than leaving a stale crosshair behind.
  hoverCell.value = null
  chart?.setOption({
    xAxis: { max: props.binCount, interval: props.gridlineBins },
    series: [{ data: buildSeriesData(), markLine: { data: markLineData() } }],
  })
  repositionCrosshairs()
}

// `showXAxis` is baked into the option at init, but which panel is *last* in a
// stack changes at runtime — a model-only view grows sensor and difference
// panels beneath it the moment a profiler is selected. Without this the panel
// that used to be last keeps drawing a now-duplicated axis.
watch(() => props.showXAxis, (show) => {
  chart?.setOption({
    grid: { ...gridConfig(), bottom: show ? 20 : 6 },
    xAxis: {
      axisLabel: show
        ? { show: true, fontSize: 9.5, color: 'rgba(255,255,255,0.4)', formatter: (val: number) => props.xLabel?.(val) ?? '' }
        : { show: false },
    },
  })
})

function updateMarkLine() {
  chart?.setOption({ series: [{ markLine: { data: markLineData() } }] })
}

function resize() {
  chart?.resize()
  repositionCrosshairs()
}

watch([() => props.values, () => props.colorFn, () => props.binCount, () => props.gridlineBins], updateData)
watch(() => props.markDepth, updateMarkLine)
watch(() => props.markCell, (cell) => positionCrosshair(clickLineX, clickLineY, cell ?? null))

// The depth zoom is only meaningful for the current variable's depth domain —
// reset it to full whenever the level set changes (e.g. switching to a variable
// with a different max depth) so a stale zoom can't point at the wrong depths.
watch(() => props.depths, () => {
  chart?.dispatchAction({ type: 'dataZoom', start: 0, end: 100 })
  updateData()
})

let resizeObs: ResizeObserver | null = null

onMounted(() => {
  registerEchartsDarkTheme()
  if (!chartRef.value) return
  chart = echarts.init(chartRef.value, 'dark', { renderer: 'canvas' })
  if (props.chartGroup) {
    chart.group = props.chartGroup
    echarts.connect(props.chartGroup)
  }
  chart.setOption(baseOption())
  chart.on('click', (params: any) => {
    const binIdx = params?.data?.[0]
    const depthIdx = params?.data?.[5]
    if (binIdx != null && depthIdx != null) emit('cell-click', { binIdx, depthIdx })
  })
  hoverLineX = makeCrosshairLine('rgba(255,255,255,0.45)', 1, true)
  hoverLineY = makeCrosshairLine('rgba(255,255,255,0.45)', 1, true)
  clickLineX = makeCrosshairLine('#ffb300', 1.5, false)
  clickLineY = makeCrosshairLine('#ffb300', 1.5, false)
  chart.getZr().add(hoverLineX)
  chart.getZr().add(hoverLineY)
  chart.getZr().add(clickLineX)
  chart.getZr().add(clickLineY)
  positionCrosshair(clickLineX, clickLineY, props.markCell ?? null)
  chart.getZr().on('mousemove', (e: any) => handleMouseMove(e.offsetX, e.offsetY))
  // Pointer left the canvas entirely (not just the plot area) — mousemove
  // stops firing, so the hover crosshair would otherwise freeze in place.
  chart.getZr().on('globalout', () => { emit('hover', null); setHoverCell(null) })
  // The depth axis has its own zoom/pan (slider + wheel) — the crosshairs'
  // pixel positions depend on the current view, so they need to follow it.
  chart.on('dataZoom', repositionCrosshairs)
  if (typeof ResizeObserver !== 'undefined') {
    resizeObs = new ResizeObserver(() => resize())
    resizeObs.observe(chartRef.value)
  }
})

onBeforeUnmount(() => {
  resizeObs?.disconnect()
  chart?.dispose()
  chart = null
})

defineExpose({ resize })
</script>

<style scoped>
.tdh-panel {
  position: relative;
  height: 100%;
  border-radius: 3px;
  background-color: #161e26;
  /* Shows through empty cells (renderItem draws nothing for them) — same
     pattern the consumers' legend swatches use, so unrendered cells and the
     legend read as one thing. */
  background-image: repeating-linear-gradient(45deg, rgba(255, 255, 255, 0.22) 0, rgba(255, 255, 255, 0.22) 1px, transparent 1px, transparent 6px);
}

.tdh-chart { width: 100%; height: 100%; cursor: crosshair; }

.tdh-label {
  position: absolute; top: 5px; left: 7px;
  font-size: 9px; font-weight: 700; letter-spacing: 0.06em;
  color: #eef3f7; background: rgba(11, 17, 22, 0.55);
  padding: 2px 6px; border-radius: 3px; pointer-events: none; z-index: 2;
}
</style>
