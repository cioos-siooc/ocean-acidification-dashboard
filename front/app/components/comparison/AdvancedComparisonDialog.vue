<template>
  <v-dialog v-model="isOpen" fullscreen transition="dialog-bottom-transition" :scrim="false">
    <v-card class="d-flex flex-column" style="height:100vh;">

      <v-toolbar color="grey-darken-4">
        <v-toolbar-title class="text-body-2">
          Comparison Analysis — {{ varName }} · {{ depthLabel }}
          <v-chip v-if="sensorName" size="x-small" color="warning" variant="tonal" class="ml-2">
            {{ sensorName }}
          </v-chip>
        </v-toolbar-title>
        <v-spacer />
        <template v-if="activeTab !== 'depth'">
          <span class="ctrl-label mr-2">Season</span>
          <v-btn-toggle v-model="selectedSeason" mandatory variant="tonal" class="mr-4">
            <v-btn value="all" size="x-small">All</v-btn>
            <v-btn value="mam" size="x-small">MAM</v-btn>
            <v-btn value="jja" size="x-small">JJA</v-btn>
            <v-btn value="son" size="x-small">SON</v-btn>
            <v-btn value="djf" size="x-small">DJF</v-btn>
          </v-btn-toggle>
        </template>
        <v-btn icon="mdi-close" variant="text" @click="isOpen = false" title="Close" />
      </v-toolbar>

      <v-tabs v-model="activeTab" color="warning" class="flex-shrink-0">
        <v-tab v-if="variableDepth" value="depth">Depth Profile</v-tab>
        <v-tab value="scatter">Scatter</v-tab>
        <v-tab value="residuals">Residuals</v-tab>
        <v-tab value="seasonal">Seasonal Cycle</v-tab>
      </v-tabs>

      <!-- Content row: chart + sidebar -->
      <div class="flex-grow-1 d-flex" style="min-height:0; overflow:hidden;">

        <!-- Chart area -->
        <div class="flex-grow-1 d-flex flex-column pa-2" style="min-width:0; min-height:0; overflow-y:auto;">
          <!-- v-show (not v-if) on the tab content keeps both branches mounted across tab
               switches, so Depth Profile's fetched window/depth and the scatter/residuals/
               seasonal chart state survive navigating away and back instead of refetching
               and resetting every time. Depth Profile itself is still gated by variableDepth
               (v-if) — it fetches immediately on mount, and fixed-depth sensors never have
               this tab, so it should never mount for them at all. -->
          <template v-if="variableDepth">
            <DepthProfile v-show="activeTab === 'depth'" :var-name="varName"
              @stats="depthProfileStats = $event" @depth-selected="emit('depth-selected', $event)" />
          </template>
          <div v-show="activeTab !== 'depth'" class="d-flex flex-column flex-grow-1" style="min-height:0;">
            <div v-if="!hasChartData"
              class="d-flex flex-column align-center justify-center flex-grow-1 text-center">
              <v-icon size="48" color="grey-darken-1">mdi-chart-scatter-plot</v-icon>
              <div class="text-caption text-grey-darken-1 mt-2">No matched pairs for this season.</div>
            </div>
            <div v-else ref="chartRef" class="flex-grow-1" style="min-height:0;" />
          </div>
        </div>

        <!-- Right sidebar: description + statistics -->
        <div class="adv-sidebar pa-3 d-flex flex-column"
          style="width:240px; min-width:240px; border-left:1px solid rgba(255,255,255,0.08); overflow-y:auto;">

          <div class="ctrl-label mb-1">About</div>
          <p class="text-caption mb-4" style="line-height:1.6; color:rgba(255,255,255,0.6);">
            {{ tabDescription }}
          </p>

          <template v-if="activeTab === 'depth'">
            <div v-if="depthProfileStats.length" class="ctrl-label mb-2">Statistics</div>
            <div v-for="stat in depthProfileStats" :key="stat.label" class="stat-row">
              <span class="stat-label">{{ stat.label }}</span>
              <span class="stat-value" :class="stat.cls || ''">{{ stat.value }}</span>
            </div>
          </template>
          <template v-else-if="hasChartData && currentStats.length">
            <div class="ctrl-label mb-2">Statistics</div>
            <div v-for="stat in currentStats" :key="stat.label" class="stat-row">
              <span class="stat-label">{{ stat.label }}</span>
              <span class="stat-value" :class="stat.cls || ''">{{ stat.value }}</span>
            </div>
          </template>

        </div>
      </div>

    </v-card>
  </v-dialog>
</template>


<script setup lang="ts">
import { ref, computed, watch, nextTick, onBeforeUnmount } from 'vue'
import * as echarts from 'echarts'
import { registerEchartsDarkTheme } from '~~/composables/useEchartsTheme'
import DepthProfile from './DepthProfile.vue'
import {
  filterBySeason,
  maskBySeason,
  computeMonthlyClimatology,
  computeScatterStats,
  computeResidualStats,
  computeSeasonalCycleStats,
  type ComparisonPoint,
  type Season,
} from '~~/composables/useComparisonFetch'

type Tab = 'scatter' | 'residuals' | 'seasonal' | 'depth'

const props = defineProps<{
  data: ComparisonPoint[]
  sensorName: string
  varName: string
  depthLabel: string
  initialSeason?: Season
  variableDepth?: boolean
  initialTab?: Tab
}>()

const emit = defineEmits<{ 'depth-selected': [number] }>()

const isOpen = defineModel<boolean>()

const activeTab = ref<Tab>(props.initialTab ?? 'scatter')
const selectedSeason = ref<Season>(props.initialSeason ?? 'all')
const depthProfileStats = ref<{ label: string, value: string, cls?: string }[]>([])
const chartRef = ref<HTMLDivElement | null>(null)
let chart: echarts.ECharts | null = null

const MONTH_LABELS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

// Season-filtered matched pairs for scatter / residuals
const filteredData = computed(() =>
  filterBySeason(props.data, selectedSeason.value)
    .filter(p => p.model != null && p.sensor != null)
)

// Seasonal cycle always uses all matched pairs across all months
const allMatchedData = computed(() =>
  props.data.filter(p => p.model != null && p.sensor != null)
)

// Masked matched data: keeps all date slots but nulls out off-season values so the
// residuals time-series renders real gaps instead of connecting across skipped months.
const maskedMatchedData = computed(() =>
  maskBySeason(allMatchedData.value, selectedSeason.value)
)

const hasChartData = computed(() =>
  activeTab.value === 'seasonal'
    ? allMatchedData.value.length > 0
    : filteredData.value.length > 0
)

// ── PER-TAB STATS ─────────────────────────────────────────────────────────────

const scatterStats  = computed(() => computeScatterStats(filteredData.value))
const residualStats = computed(() => computeResidualStats(filteredData.value))
const monthlyClim   = computed(() => computeMonthlyClimatology(allMatchedData.value))
const seasCycStats  = computed(() => computeSeasonalCycleStats(monthlyClim.value))

const fmt = (v: number, d = 3) => v.toFixed(d)
const sign = (v: number, d = 3) => `${v >= 0 ? '+' : ''}${v.toFixed(d)}`

const tabDescription = computed(() => {
  if (activeTab.value === 'scatter')
    return 'Each point is a matched model–observation daily pair. The dashed line is the 1:1 reference; the solid line is the linear best fit. Points above the 1:1 line indicate model overestimation.'
  if (activeTab.value === 'residuals')
    return 'Daily (model − obs) over time. Positive values mean the model overestimates. The dashed trend line reveals whether model bias is drifting systematically over the sensor record.'
  if (activeTab.value === 'depth')
    return 'Sensor casts are binned onto the model\'s own depth levels within the visible window. Click a depth band to inspect that level\'s timeseries and stats.'
  return 'Climatological monthly means over the full sensor record. Reveals whether the model correctly captures the amplitude and timing of the annual cycle — regardless of the season filter above.'
})

interface StatRow { label: string; value: string; cls?: string }

const currentStats = computed((): StatRow[] => {
  if (activeTab.value === 'scatter') {
    const s = scatterStats.value
    if (!s.n) return []
    return [
      { label: 'R²',         value: fmt(s.r2) },
      { label: 'Slope',      value: fmt(s.slope),     cls: Math.abs(s.slope - 1) > 0.1 ? 'text-amber-lighten-2' : '' },
      { label: 'Intercept',  value: sign(s.intercept) },
      { label: 'Pairs',      value: String(s.n) },
    ]
  }

  if (activeTab.value === 'residuals') {
    const s = residualStats.value
    if (!s.n) return []
    return [
      { label: 'Mean residual', value: sign(s.mean), cls: s.mean > 0 ? 'text-red-lighten-2' : 'text-blue-lighten-2' },
      { label: 'Std dev',       value: fmt(s.std) },
      { label: 'Max overest.',  value: sign(s.maxOver) },
      { label: 'Max underest.', value: sign(s.maxUnder) },
      { label: 'Trend',         value: `${sign(s.trend, 4)} /yr`, cls: Math.abs(s.trend) > 0.05 ? 'text-amber-lighten-2' : '' },
      { label: 'Pairs',         value: String(s.n) },
    ]
  }

  // Seasonal cycle
  const s = seasCycStats.value
  if (!s) return []
  const phaseLabel = s.phaseOffset === 0
    ? 'In phase'
    : `${sign(s.phaseOffset, 0)} mo`
  return [
    { label: 'Model peak',       value: MONTH_LABELS[s.modelPeakMonth - 1]  },
    { label: 'Sensor peak',      value: MONTH_LABELS[s.sensorPeakMonth - 1] },
    { label: 'Phase offset',     value: phaseLabel, cls: Math.abs(s.phaseOffset) >= 2 ? 'text-amber-lighten-2' : '' },
    { label: 'Model amplitude',  value: fmt(s.modelAmplitude) },
    { label: 'Sensor amplitude', value: fmt(s.sensorAmplitude) },
    { label: 'Ampl. ratio',      value: fmt(s.amplitudeRatio), cls: Math.abs(s.amplitudeRatio - 1) > 0.2 ? 'text-amber-lighten-2' : '' },
  ]
})

// ── SCATTER ───────────────────────────────────────────────────────────────────
function renderScatter() {
  if (!chart) return
  const data = filteredData.value
  const pairs = data.map(p => [p.sensor as number, p.model as number])
  if (!pairs.length) return

  const allVals = pairs.flatMap(p => p) as number[]
  const lo = Math.min(...allVals)
  const hi = Math.max(...allVals)
  const pad = (hi - lo) * 0.05 || 0.1
  const s = scatterStats.value
  const regLine = [
    [lo - pad, s.slope * (lo - pad) + s.intercept],
    [hi + pad, s.slope * (hi + pad) + s.intercept],
  ]

  chart.setOption({
    tooltip: {
      trigger: 'item',
      formatter: (p: any) =>
        `Obs: <strong>${Number(p.value[0]).toFixed(3)}</strong><br/>Model: <strong>${Number(p.value[1]).toFixed(3)}</strong>`,
    },
    legend: { data: ['1:1', 'Best fit'], top: 4, textStyle: { fontSize: 10 } },
    grid: { left: '8%', right: '4%', bottom: '12%', top: '14%', containLabel: true },
    xAxis: {
      type: 'value',
      name: `Observed ${props.varName}`,
      nameLocation: 'middle',
      nameGap: 28,
      min: lo - pad,
      max: hi + pad,
      axisLabel: { fontSize: 10, color: '#ccc' },
    },
    yAxis: {
      type: 'value',
      name: `Model ${props.varName}`,
      nameLocation: 'middle',
      nameGap: 44,
      min: lo - pad,
      max: hi + pad,
      axisLabel: { fontSize: 10, color: '#ccc' },
    },
    series: [
      {
        name: '1:1',
        type: 'line',
        data: [[lo - pad, lo - pad], [hi + pad, hi + pad]],
        symbol: 'none',
        lineStyle: { color: 'rgba(255,255,255,0.22)', type: 'dashed', width: 1 },
        silent: true,
      },
      {
        name: 'Best fit',
        type: 'line',
        data: regLine,
        symbol: 'none',
        lineStyle: { color: '#ff9800', width: 1.5 },
        silent: true,
      },
      {
        name: 'pairs',
        type: 'scatter',
        data: pairs,
        symbolSize: 4,
        itemStyle: { color: '#ff9800', opacity: 0.4 },
        legendHoverLink: false,
      },
    ],
  }, true)
  chart.resize()
}

// ── RESIDUALS ─────────────────────────────────────────────────────────────────
function renderResiduals() {
  if (!chart) return
  // Use masked data so off-season months appear as gaps, not connected diagonals
  const masked = maskedMatchedData.value
  const resData = masked.map(p => [
    p.date,
    p.model != null && p.sensor != null ? (p.model - p.sensor) : null,
  ])
  if (!resData.length) return

  chart.setOption({
    tooltip: {
      trigger: 'axis',
      confine: true,
      formatter: (params: any) => {
        const p = Array.isArray(params) ? params.find((x: any) => x.seriesName === 'Residual') : params
        if (!p?.value) return ''
        const date = String(p.value[0]).slice(0, 10)
        const val = Number(p.value[1])
        return `<strong>${date}</strong><br/>${p.marker} Model−Obs: <strong>${val >= 0 ? '+' : ''}${val.toFixed(3)}</strong>`
      },
    },
    legend: { show: false },
    grid: { left: '3%', right: '2%', bottom: '10%', top: '14%', containLabel: true },
    xAxis: {
      type: 'time',
      boundaryGap: false,
      axisLabel: { rotate: 30, fontSize: 9, color: '#ccc' },
    },
    yAxis: {
      type: 'value',
      name: 'Model − Obs',
      nameLocation: 'middle',
      nameGap: 50,
      axisLabel: { fontSize: 10, color: '#ccc' },
      min: 'dataMin',
      max: 'dataMax',
      splitLine: { lineStyle: { color: 'rgba(255,255,255,0.07)' } },
    },
    dataZoom: [{ type: 'inside' }, { type: 'slider', bottom: 4, height: 16 }],
    series: [
      {
        name: 'zero',
        type: 'line',
        data: resData.length ? [[resData[0][0], 0], [resData[resData.length - 1][0], 0]] : [],
        symbol: 'none',
        lineStyle: { color: 'rgba(255,255,255,0.2)', type: 'dashed', width: 1 },
        silent: true,
        legendHoverLink: false,
      },
      {
        name: 'Residual',
        type: 'line',
        data: resData,
        symbol: 'none',
        connectNulls: false,
        lineStyle: { color: '#ff9800', width: 1 },
        itemStyle: { color: '#ff9800' },
        legendHoverLink: false,
        areaStyle: {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: 'rgba(255,152,0,0.15)' },
            { offset: 1, color: 'rgba(255,152,0,0.01)' },
          ]),
        },
      },
    ],
  }, true)
  chart.resize()
}

// ── SEASONAL CYCLE ────────────────────────────────────────────────────────────
function renderSeasonalCycle() {
  if (!chart) return
  const clim = monthlyClim.value

  chart.setOption({
    tooltip: {
      trigger: 'axis',
      confine: true,
      formatter: (params: any) => {
        const items = Array.isArray(params) ? params : [params]
        const label = MONTH_LABELS[(items[0]?.dataIndex ?? 0)]
        let s = `<strong>${label}</strong><br/>`
        for (const p of items) {
          if (p.value != null)
            s += `${p.marker} ${p.seriesName}: <strong>${Number(p.value).toFixed(3)}</strong><br/>`
        }
        return s
      },
    },
    legend: { data: ['Model (mean)', 'Sensor (mean)'], top: 4, textStyle: { fontSize: 10 } },
    grid: { left: '3%', right: '3%', bottom: '8%', top: '16%', containLabel: true },
    xAxis: {
      type: 'category',
      data: MONTH_LABELS,
      axisLabel: { fontSize: 10, color: '#ccc' },
    },
    yAxis: {
      type: 'value',
      name: props.varName,
      nameLocation: 'middle',
      nameGap: 50,
      axisLabel: { fontSize: 10, color: '#ccc' },
      min: 'dataMin',
      max: 'dataMax',
    },
    series: [
      {
        name: 'Model (mean)',
        type: 'line',
        data: clim.map(c => c.model),
        symbol: 'circle',
        symbolSize: 5,
        lineStyle: { color: '#ff9800', width: 2 },
        itemStyle: { color: '#ff9800' },
      },
      {
        name: 'Sensor (mean)',
        type: 'line',
        data: clim.map(c => c.sensor),
        symbol: 'circle',
        symbolSize: 5,
        lineStyle: { color: '#a5d6a7', width: 2 },
        itemStyle: { color: '#a5d6a7' },
      },
    ],
  }, true)
  chart.resize()
}

// ── RENDER DISPATCH ───────────────────────────────────────────────────────────
function renderChart() {
  if (!chart || !hasChartData.value) return
  if (activeTab.value === 'scatter') renderScatter()
  else if (activeTab.value === 'residuals') renderResiduals()
  else renderSeasonalCycle()
}

function initAndRender() {
  registerEchartsDarkTheme()
  if (chart) { chart.dispose(); chart = null }
  if (chartRef.value)
    chart = echarts.init(chartRef.value, 'dark', { renderer: 'canvas' })
  renderChart()
}

watch([activeTab, selectedSeason, () => props.data], async () => {
  await nextTick()
  if (!chart && chartRef.value) initAndRender()
  else renderChart()
})

watch(isOpen, async (open) => {
  if (!open) return
  // The dialog instance persists across sensor changes, so re-apply the caller's
  // requested tab each time it opens rather than only at component creation.
  activeTab.value = props.initialTab ?? 'scatter'
  await nextTick()
  initAndRender()
})

watch(() => props.initialSeason, (s) => { if (s) selectedSeason.value = s })

let resizeObs: ResizeObserver | null = null
watch(chartRef, async (el) => {
  if (resizeObs) { resizeObs.disconnect(); resizeObs = null }
  if (el) {
    if (typeof ResizeObserver !== 'undefined') {
      resizeObs = new ResizeObserver(() => { if (chart) chart.resize() })
      resizeObs.observe(el)
    }
    await nextTick()
    initAndRender()
  } else {
    if (chart) { chart.dispose(); chart = null }
  }
})

onBeforeUnmount(() => {
  if (resizeObs) { resizeObs.disconnect(); resizeObs = null }
  if (chart) { chart.dispose(); chart = null }
})
</script>


<style scoped>
.ctrl-label {
  font-size: 0.63rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: rgba(255, 255, 255, 0.38);
}

.adv-sidebar {
  background: rgba(255, 255, 255, 0.02);
}

.stat-row {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  margin-bottom: 5px;
}

.stat-label {
  font-size: 0.68rem;
  color: rgba(255, 255, 255, 0.5);
}

.stat-value {
  font-size: 0.72rem;
  font-weight: 600;
  font-variant-numeric: tabular-nums;
}
</style>
