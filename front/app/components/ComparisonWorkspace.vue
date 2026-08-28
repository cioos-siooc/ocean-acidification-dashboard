<template>
  <UModal v-model:open="isOpen" fullscreen :overlay="false">
    <template #content>
    <div class="flex flex-col bg-default" style="height:100vh;">
      <div class="flex items-center gap-2 px-3 h-12 bg-elevated shrink-0">
        <div class="font-medium truncate">
          Comparison — {{ varName }}
          <UBadge size="xs" color="warning" variant="subtle" class="ml-2 rounded-full" v-if="sensorName">{{ sensorName }}</UBadge>
        </div>
        <div class="grow" />
        <DownloadButton :datasets="csvDatasets" class="shrink-0" />
        <UButton variant="ghost" icon="i-mdi-close" class="shrink-0" title="Close (Esc)" @click="isOpen = false" />
      </div>

      <UTabs v-model="activeTab" :items="tabItems" :content="false" class="shrink-0" />

      <div class="grow" style="min-height:0; overflow:hidden;">
        <!-- The pane owns the model/sensor fetch and the timeseries chart; the
             statistical views are derived from the same matched pairs, so they
             live behind tabs on this one surface instead of a second dialog. -->
        <div v-show="activeTab === 'overview'" style="height:100%;">
          <SensorComparison :active="isOpen && activeTab === 'overview'" @data="onComparisonData" />
        </div>

        <div v-show="activeTab === 'sections'" style="height:100%;">
          <ComparisonSections v-if="isVariableDepth" :active="isOpen && activeTab === 'sections'" />
        </div>

        <div v-if="isStatsTab" class="h-full flex" style="overflow:hidden;">
          <div class="grow flex flex-col p-2" style="min-width:0; min-height:0;">
            <div class="flex items-center mb-2" style="gap:10px;">
              <span class="ctrl-label">Season</span>
              <SegmentedControl v-model="selectedSeason" :items="seasonItems" size="xs" aria-label="Season" />
            </div>
            <div v-if="!hasChartData" class="flex flex-col items-center justify-center grow text-center">
              <UIcon name="i-mdi-chart-scatter-plot" class="size-[48px] text-gray-500" />
              <div class="text-gray-500 mt-2">No matched pairs for this season.</div>
            </div>
            <div v-else ref="chartRef" class="grow" style="min-height:0;" />
          </div>

          <div class="adv-sidebar p-3 flex flex-col"
            style="width:240px; min-width:240px; border-left:1px solid rgba(255,255,255,0.08); overflow-y:auto;">
            <div class="ctrl-label mb-1">About</div>
            <p class="mb-4" style="line-height:1.6; color:rgba(255,255,255,0.6);">{{ tabDescription }}</p>
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
    </div>
    </template>
  </UModal>
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
import SegmentedControl from './ui/SegmentedControl.vue'
import DownloadButton from './ui/DownloadButton.vue'
import { csvMeta, provideCsvExport, type CsvContext, type CsvDataset } from '~~/composables/useCsvExport'
const seasonItems = [{ value: 'all', label: 'All' }, { value: 'mam', label: 'MAM' }, { value: 'jja', label: 'JJA' }, { value: 'son', label: 'SON' }, { value: 'djf', label: 'DJF' }]

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
// "Depth sections" only applies to variable-depth sensors, so the tab list is
// computed rather than static (v-tabs used a v-if on the tab itself).
const tabItems = computed(() => [
    { value: 'overview', label: 'Timeseries' },
    ...(isVariableDepth.value ? [{ value: 'sections', label: 'Depth sections' }] : []),
    { value: 'scatter', label: 'Scatter' },
    { value: 'residuals', label: 'Residuals' },
    { value: 'seasonal', label: 'Seasonal Cycle' },
])
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

// ── CSV EXPORT ──────────────────────────────────────────────────────────────
// Every tab here reads the same matched model/observation pairs, so the export
// follows the tab rather than the chart: the pairs as plotted, plus whichever
// derived table the current tab is actually about. The sidebar's statistics go
// into the preamble — they're one result per file, not per row, and `currentStats`
// is already exactly the label/value list the sidebar shows.
const csvContext = computed<CsvContext | null>(() => {
  if (!sensorInfo.value || !mainStore.selected_variable.var) return null
  const dates = rawData.value.map(p => p.date)
  return {
    source: 'comparison',
    sourceLabel: `SalishSeaCast model vs sensor — ${sensorName.value}`,
    variable: mainStore.selected_variable.var,
    variableName: varName.value,
    unit: varUnit.value,
    depth: isVariableDepth.value ? -1 : (mainStore.selectedSensor?.depth ?? null),
    locationLabel: sensorName.value,
    timeRange: dates.length ? [dates[0]!, dates[dates.length - 1]!] : null,
    season: isStatsTab.value ? selectedSeason.value : undefined,
  }
})

const csvExport = provideCsvExport(csvContext)
const csvDatasets = csvExport.datasets

const statsMeta = computed(() => currentStats.value.map(s => [s.label, s.value] as [string, unknown]))

csvExport.register((): CsvDataset[] => {
  const u = varUnit.value ? ` (${varUnit.value})` : ''
  const pairColumns = [
    { header: 'date', accessorKey: 'date' },
    { header: `model${u}`, accessorKey: 'model' },
    { header: `sensor${u}`, accessorKey: 'sensor' },
    { header: `difference${u}`, accessorKey: 'difference' },
  ]
  const withDifference = (points: typeof rawData.value) => points.map(p => ({
    date: p.date,
    model: p.model,
    modelMin: p.modelMin,
    modelMax: p.modelMax,
    sensor: p.sensor,
    difference: (p.model != null && p.sensor != null) ? p.model - p.sensor : null,
  }))

  if (activeTab.value === 'overview') {
    if (!rawData.value.length) return []
    return [{
      label: 'Model vs sensor (daily)',
      slug: 'comparison-daily',
      columns: [
        ...pairColumns.slice(0, 2),
        { header: `model_min${u}`, accessorKey: 'modelMin' },
        { header: `model_max${u}`, accessorKey: 'modelMax' },
        ...pairColumns.slice(2),
      ],
      rows: withDifference(rawData.value),
      meta: csvMeta(csvContext.value, [
        ['note', 'the full daily record, including days where only one of the two has a value'],
      ]),
    }]
  }

  if (activeTab.value === 'seasonal') {
    if (!monthlyClim.value.length) return []
    return [{
      label: 'Monthly climatology',
      slug: 'comparison-monthly-climatology',
      columns: [
        { header: 'month', accessorKey: 'month' },
        { header: `model${u}`, accessorKey: 'model' },
        { header: `sensor${u}`, accessorKey: 'sensor' },
        { header: `difference${u}`, accessorKey: 'difference' },
      ],
      rows: monthlyClim.value.map(m => ({
        month: MONTH_LABELS[m.month - 1],
        model: m.model,
        sensor: m.sensor,
        difference: (m.model != null && m.sensor != null) ? m.model - m.sensor : null,
      })),
      meta: csvMeta(csvContext.value, [
        ['season', 'full record — the seasonal cycle deliberately ignores the season filter'],
        ...statsMeta.value,
      ]),
    }]
  }

  if (activeTab.value === 'scatter' || activeTab.value === 'residuals') {
    if (!filteredData.value.length) return []
    return [{
      label: 'Matched pairs',
      slug: activeTab.value === 'scatter' ? 'comparison-scatter' : 'comparison-residuals',
      columns: pairColumns,
      rows: withDifference(filteredData.value),
      meta: csvMeta(csvContext.value, [
        ['note', 'days where both the model and the sensor have a value'],
        ...statsMeta.value,
      ]),
    }]
  }

  return []
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
