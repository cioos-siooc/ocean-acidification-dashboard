<template>
  <v-dialog v-model="isOpen" fullscreen transition="dialog-bottom-transition" :scrim="false">
    <v-card class="d-flex flex-column" style="height:100vh;">
      <v-toolbar color="grey-darken-4" density="compact">
        <v-toolbar-title class="text-body-2">
          Comparison — {{ varName }}
          <v-chip v-if="sensorName" size="x-small" color="warning" variant="tonal" class="ml-2">{{ sensorName }}</v-chip>
        </v-toolbar-title>
        <v-spacer />
        <v-btn icon="mdi-close" variant="text" title="Close (Esc)" @click="isOpen = false" />
      </v-toolbar>

      <v-tabs v-model="activeTab" color="warning" density="compact" class="flex-shrink-0">
        <v-tab value="overview">Timeseries</v-tab>
        <v-tab v-if="isVariableDepth" value="sections">Depth sections</v-tab>
        <v-tab value="scatter">Scatter</v-tab>
        <v-tab value="residuals">Residuals</v-tab>
        <v-tab value="seasonal">Seasonal Cycle</v-tab>
      </v-tabs>

      <div class="flex-grow-1" style="min-height:0; overflow:hidden;">
        <!-- The pane owns the model/sensor fetch and the timeseries chart; the
             statistical views are derived from the same matched pairs, so they
             live behind tabs on this one surface instead of a second dialog. -->
        <div v-show="activeTab === 'overview'" style="height:100%;">
          <SensorComparison :active="isOpen && activeTab === 'overview'" @data="onComparisonData" />
        </div>

        <div v-show="activeTab === 'sections'" style="height:100%;">
          <ComparisonSections v-if="isVariableDepth" :active="isOpen && activeTab === 'sections'" />
        </div>

        <div v-if="isStatsTab" class="h-100 d-flex" style="overflow:hidden;">
          <div class="flex-grow-1 d-flex flex-column pa-2" style="min-width:0; min-height:0;">
            <div class="d-flex align-center mb-2" style="gap:10px;">
              <span class="ctrl-label">Season</span>
              <v-btn-toggle v-model="selectedSeason" mandatory variant="tonal" density="compact">
                <v-btn value="all" size="x-small">All</v-btn>
                <v-btn value="mam" size="x-small">MAM</v-btn>
                <v-btn value="jja" size="x-small">JJA</v-btn>
                <v-btn value="son" size="x-small">SON</v-btn>
                <v-btn value="djf" size="x-small">DJF</v-btn>
              </v-btn-toggle>
            </div>
            <div v-if="!hasChartData" class="d-flex flex-column align-center justify-center flex-grow-1 text-center">
              <v-icon size="48" color="grey-darken-1">mdi-chart-scatter-plot</v-icon>
              <div class="text-caption text-grey-darken-1 mt-2">No matched pairs for this season.</div>
            </div>
            <div v-else ref="chartRef" class="flex-grow-1" style="min-height:0;" />
          </div>

          <div class="adv-sidebar pa-3 d-flex flex-column"
            style="width:240px; min-width:240px; border-left:1px solid rgba(255,255,255,0.08); overflow-y:auto;">
            <div class="ctrl-label mb-1">About</div>
            <p class="text-caption mb-4" style="line-height:1.6; color:rgba(255,255,255,0.6);">{{ tabDescription }}</p>
            <template v-if="hasChartData && currentStats.length">
              <div class="ctrl-label mb-2">Statistics</div>
              <div v-for="stat in currentStats" :key="stat.label" class="stat-row">
                <span class="stat-label">{{ stat.label }}</span>
                <span class="stat-value" :class="stat.cls || ''">{{ stat.value }}</span>
              </div>
            </template>
          </div>
        </div>
      </div>
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import { ref, computed, watch, nextTick, onBeforeUnmount } from 'vue'
import * as echarts from 'echarts'
import { registerEchartsDarkTheme } from '~~/composables/useEchartsTheme'
import { useMainStore } from '../stores/main'
import { availableVariables } from '~~/composables/useAnalysisStatistics'
import { useVariableRegistry } from '~~/composables/useVariableRegistry'
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
import SensorComparison from './sensorComparison.vue'
import ComparisonSections from './comparison/ComparisonSections.vue'

/**
 * Comparison workspace — fullscreen for the same reason Analysis is: it takes a
 * sensor and a coordinate as input and then has nothing further to say to the
 * map. Being fullscreen is also what finally makes room for the two depth
 * sections, which never fit in a footer strip.
 */

const isOpen = defineModel<boolean>()

const mainStore = useMainStore()
const { displayUnit } = useVariableRegistry()
const varUnit = computed(() => displayUnit(mainStore.selected_variable.var))

const sensorInfo = computed(() => mainStore.sensors.find(s => s.id === mainStore.selectedSensor?.id) ?? null)
const sensorName = computed(() => sensorInfo.value?.name ?? '')
const isVariableDepth = computed(() => sensorInfo.value?.depth === -1)
const varName = computed(() =>
  availableVariables.find(v => v.id === mainStore.selected_variable.var)?.name || mainStore.selected_variable.var || 'Variable')

type Tab = 'overview' | 'sections' | 'scatter' | 'residuals' | 'seasonal'
const activeTab = ref<Tab>('overview')
const isStatsTab = computed(() => ['scatter', 'residuals', 'seasonal'].includes(activeTab.value))

// A profiler losing its sections tab would strand the view on a blank pane.
watch(isVariableDepth, (v) => { if (!v && activeTab.value === 'sections') activeTab.value = 'overview' })

// ── MATCHED PAIRS — produced by the overview pane's own fetch and handed up,
// so the statistical tabs never duplicate that request. ──────────────────────
const rawData = ref<ComparisonPoint[]>([])
function onComparisonData(points: ComparisonPoint[]) { rawData.value = points }

const selectedSeason = ref<Season>('all')
const MONTH_LABELS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

const filteredData = computed(() =>
  filterBySeason(rawData.value, selectedSeason.value).filter(p => p.model != null && p.sensor != null))
const allMatchedData = computed(() => rawData.value.filter(p => p.model != null && p.sensor != null))
const maskedMatchedData = computed(() => maskBySeason(allMatchedData.value, selectedSeason.value))

const hasChartData = computed(() =>
  activeTab.value === 'seasonal' ? allMatchedData.value.length > 0 : filteredData.value.length > 0)

const scatterStats = computed(() => computeScatterStats(filteredData.value))
const residualStats = computed(() => computeResidualStats(filteredData.value))
const monthlyClim = computed(() => computeMonthlyClimatology(allMatchedData.value))
const seasCycStats = computed(() => computeSeasonalCycleStats(monthlyClim.value))

const fmt = (v: number, d = 3) => v.toFixed(d)
const sign = (v: number, d = 3) => `${v >= 0 ? '+' : ''}${v.toFixed(d)}`

const tabDescription = computed(() => {
  if (activeTab.value === 'scatter')
    return 'Each point is a matched model–observation daily pair. The dashed line is the 1:1 reference; the solid line is the linear best fit. Points above the 1:1 line indicate model overestimation.'
  if (activeTab.value === 'residuals')
    return 'Daily (model − obs) over time. Positive values mean the model overestimates. The dashed trend line reveals whether model bias is drifting systematically over the sensor record.'
  return 'Climatological monthly means over the full sensor record. Reveals whether the model correctly captures the amplitude and timing of the annual cycle — regardless of the season filter above.'
})

interface StatRow { label: string; value: string; cls?: string }

const currentStats = computed((): StatRow[] => {
  if (activeTab.value === 'scatter') {
    const s = scatterStats.value
    if (!s.n) return []
    return [
      { label: 'R²', value: fmt(s.r2) },
      { label: 'Slope', value: fmt(s.slope), cls: Math.abs(s.slope - 1) > 0.1 ? 'text-amber-lighten-2' : '' },
      { label: 'Intercept', value: sign(s.intercept) },
      { label: 'Pairs', value: String(s.n) },
    ]
  }
  if (activeTab.value === 'residuals') {
    const s = residualStats.value
    if (!s.n) return []
    return [
      { label: 'Mean residual', value: sign(s.mean), cls: s.mean > 0 ? 'text-red-lighten-2' : 'text-blue-lighten-2' },
      { label: 'Std dev', value: fmt(s.std) },
      { label: 'Max overest.', value: sign(s.maxOver) },
      { label: 'Max underest.', value: sign(s.maxUnder) },
      { label: 'Trend', value: `${sign(s.trend, 4)} /yr`, cls: Math.abs(s.trend) > 0.05 ? 'text-amber-lighten-2' : '' },
      { label: 'Pairs', value: String(s.n) },
    ]
  }
  const s = seasCycStats.value
  if (!s) return []
  return [
    { label: 'Model peak', value: MONTH_LABELS[s.modelPeakMonth - 1]! },
    { label: 'Sensor peak', value: MONTH_LABELS[s.sensorPeakMonth - 1]! },
    { label: 'Phase', value: s.phaseOffset === 0 ? 'In phase' : `${sign(s.phaseOffset, 0)} mo` },
    { label: 'Model amplitude', value: fmt(s.modelAmplitude) },
    { label: 'Sensor amplitude', value: fmt(s.sensorAmplitude) },
  ]
})

// ── CHART ────────────────────────────────────────────────────────────────────
const chartRef = ref<HTMLDivElement | null>(null)
let chart: echarts.ECharts | null = null

function ensureChart() {
  if (chart || !chartRef.value) return
  registerEchartsDarkTheme()
  chart = echarts.init(chartRef.value, 'dark', { renderer: 'canvas' })
}

function renderChart() {
  ensureChart()
  if (!chart || !hasChartData.value) return

  if (activeTab.value === 'scatter') {
    const pts = filteredData.value.map(p => [p.sensor as number, p.model as number])
    const lo = Math.min(...pts.map(p => Math.min(p[0]!, p[1]!)))
    const hi = Math.max(...pts.map(p => Math.max(p[0]!, p[1]!)))
    const s = scatterStats.value
    chart.setOption({
      tooltip: { trigger: 'item', formatter: (p: any) => `obs ${p.value[0].toFixed(3)}<br/>model ${p.value[1].toFixed(3)}` },
      grid: { left: 60, right: 30, top: 20, bottom: 50 },
      xAxis: { type: 'value', name: varUnit.value ? `Observed (${varUnit.value})` : 'Observed', nameLocation: 'middle', nameGap: 28, min: lo, max: hi, axisLabel: { fontSize: 10 } },
      yAxis: { type: 'value', name: varUnit.value ? `Model (${varUnit.value})` : 'Model', nameLocation: 'middle', nameGap: 42, min: lo, max: hi, axisLabel: { fontSize: 10 } },
      series: [
        { type: 'scatter', data: pts, symbolSize: 4, itemStyle: { color: 'rgba(255,152,0,0.55)' } },
        { type: 'line', data: [[lo, lo], [hi, hi]], symbol: 'none', lineStyle: { type: 'dashed', color: 'rgba(255,255,255,0.4)' } },
        { type: 'line', symbol: 'none', lineStyle: { color: '#35c2c9', width: 2 },
          data: [[lo, s.slope * lo + s.intercept], [hi, s.slope * hi + s.intercept]] },
      ],
    }, true)
  } else if (activeTab.value === 'residuals') {
    const pts = maskedMatchedData.value.map(p => [p.date, p.model != null && p.sensor != null ? p.model - p.sensor : null])
    chart.setOption({
      tooltip: { trigger: 'axis' },
      grid: { left: 60, right: 30, top: 20, bottom: 50 },
      xAxis: { type: 'time', axisLabel: { fontSize: 10 } },
      yAxis: { type: 'value', name: varUnit.value ? `Model − obs (${varUnit.value})` : 'Model − obs', nameLocation: 'middle', nameGap: 42, axisLabel: { fontSize: 10 } },
      series: [
        { type: 'line', data: pts, symbol: 'none', connectNulls: false, lineStyle: { color: '#ff9800', width: 1.2 } },
        { type: 'line', markLine: { symbol: 'none', silent: true, data: [{ yAxis: 0 }], lineStyle: { color: 'rgba(255,255,255,0.35)', type: 'dashed' } }, data: [] },
      ],
    }, true)
  } else {
    const clim = monthlyClim.value
    chart.setOption({
      tooltip: { trigger: 'axis' },
      legend: { data: ['Model', 'Sensor'], top: 0, textStyle: { fontSize: 10 } },
      grid: { left: 60, right: 30, top: 30, bottom: 50 },
      xAxis: { type: 'category', data: MONTH_LABELS, axisLabel: { fontSize: 10 } },
      yAxis: { type: 'value', name: varUnit.value, nameLocation: 'middle', nameGap: 42, axisLabel: { fontSize: 10 } },
      series: [
        { name: 'Model', type: 'line', data: clim.map(m => m.model), symbol: 'circle', symbolSize: 5, lineStyle: { color: '#ff9800' }, itemStyle: { color: '#ff9800' } },
        { name: 'Sensor', type: 'line', data: clim.map(m => m.sensor), symbol: 'circle', symbolSize: 5, lineStyle: { color: '#a5d6a7' }, itemStyle: { color: '#a5d6a7' } },
      ],
    }, true)
  }
  chart.resize()
}

watch([activeTab, selectedSeason, rawData, isOpen], () => {
  if (!isOpen.value || !isStatsTab.value) return
  nextTick().then(renderChart)
})

onBeforeUnmount(() => { chart?.dispose(); chart = null })
</script>

<style scoped>
.ctrl-label {
  font-size: 0.63rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: rgba(255, 255, 255, 0.6);
}

.stat-row {
  display: flex;
  justify-content: space-between;
  gap: 8px;
  padding: 3px 0;
  font-size: 11.5px;
  border-bottom: 1px solid rgba(255, 255, 255, 0.05);
}

.stat-label { color: rgba(255, 255, 255, 0.55); }
.stat-value { font-weight: 700; font-variant-numeric: tabular-nums; }
</style>
