<template>
  <div class="comparison-panel flex flex-col h-full" style="overflow:hidden;">

    <!-- HEADER STRIP -->
    <div class="flex items-center px-3 shrink-0"
      style="height:30px; border-bottom:1px solid rgba(255,255,255,0.08); gap:8px;">
      <span class="font-medium truncate" style="min-width:0;">
        {{ sensorInfo?.name || '—' }}
        <span class="text-gray-500"> · {{ varName }} · {{ variableDepthLabel }}</span>
      </span>
      <div class="grow" />
      <!-- Daily spans the whole record; hourly trades that reach for a fortnight
           at native cadence, where tidal and diurnal structure is visible. -->
      <SegmentedControl v-model="resolution" :items="resolutionItems" size="xs" :disabled="isLoading"
        class="shrink-0 mr-2" aria-label="Time resolution" />
      <UIcon name="i-mdi-loading" class="animate-spin size-[14px] text-warning shrink-0" v-if="isLoading" />
      <span v-if="isLoading" class="text-gray-500 shrink-0">{{ loadingStep }}</span>
      <UBadge variant="subtle" class="rounded-full"> 0" size="x-small" color="teal" variant="tonal" class="shrink-0">
        {{ stats.n }} days
      </UBadge>
    </div>

    <!-- MAIN ROW: chart + stats -->
    <div class="flex grow" style="min-height:0; overflow:hidden;">

      <!-- TIME SERIES CHART -->
      <div class="grow flex flex-col" style="min-width:0; overflow:hidden; position:relative;">

        <div v-show="hasData" ref="timeseriesContainerRef" class="w-full h-full"
          :class="{ 'chart-loading': isLoading && hasData }" />

        <div v-if="isLoading && !hasData"
          class="flex flex-col items-center justify-center h-full">
          <UIcon name="i-mdi-loading" class="animate-spin size-[36px] text-warning mb-2" />
          <div class="text-warning">{{ loadingStep }}</div>
        </div>

        <UAlert color="error" variant="subtle" class="m-3" v-else-if="!hasData && errorMessage">
          {{ errorMessage }}
        </UAlert>

        <div v-else-if="isVariableDepth && sensorInfo && !hasData"
          class="flex flex-col items-center justify-center h-full text-center px-6">
          <UIcon name="i-mdi-chart-timeline-variant" class="size-[48px] text-teal-400" />
          <div class="text-gray-400 mt-2" style="max-width:280px;">
            This sensor profiles the water column instead of sitting at one depth. Pick a depth
            via the map's depth control to compare it here, or open the Depth sections tab
            above to see its casts against the model at every depth.
          </div>
        </div>

        <div v-else-if="!hasData && !isLoading"
          class="flex flex-col items-center justify-center h-full text-center px-6">
          <UIcon name="i-mdi-compare-horizontal" class="size-[48px] text-gray-500" />
          <div class="text-gray-500 mt-2">Select a sensor to load the comparison</div>
        </div>
      </div>

      <!-- RIGHT: Season filter + Stats -->
      <div class="comparison-stats p-2 flex flex-col"
        style="width:210px; min-width:210px; border-left:1px solid rgba(255,255,255,0.08); overflow:hidden;">

        <div class="ctrl-label mb-1">Season</div>
        <SegmentedControl v-model="selectedSeason" :items="seasonItems" size="xs" block
          item-class="px-1 min-w-0" class="mb-3" aria-label="Season" />

        <template v-if="stats && stats.n > 0">
          <div class="ctrl-label mb-2">Summary Stats</div>

          <div class="stat-row">
            <span class="stat-label">Bias (Model−Obs)</span>
            <span class="stat-value" :class="biasColorClass">
              {{ stats.bias >= 0 ? '+' : '' }}{{ stats.bias.toFixed(3) }}
            </span>
          </div>
          <div class="stat-row">
            <span class="stat-label">RMSE</span>
            <span class="stat-value">{{ stats.rmse.toFixed(3) }}</span>
          </div>
          <div class="stat-row">
            <span class="stat-label">Pearson R</span>
            <span class="stat-value">{{ stats.pearsonR.toFixed(3) }}</span>
          </div>
          <div class="stat-row">
            <span class="stat-label">Matched days</span>
            <span class="stat-value">{{ stats.n }}</span>
          </div>
        </template>

        <div v-else-if="!hasData && !isLoading"
          class="flex items-center justify-center grow">
          <div class="text-gray-500 text-center">Stats appear<br>after loading</div>
        </div>

        <div v-else-if="hasData && stats?.n === 0" class="text-gray-500 mt-2">
          No matched pairs for this season.
        </div>
      </div>
    </div>

  </div>

</template>


<script setup lang="ts">
import { ref, computed, watch, nextTick, onMounted, onBeforeUnmount } from 'vue'
import * as echarts from 'echarts'
import moment from 'moment-timezone'
import { registerEchartsDarkTheme } from '~~/composables/useEchartsTheme'
import { trackEvent } from '~~/composables/useAnalytics'
import { useMainStore, formatDepthLabel } from '../stores/main'
import { fetchAnalysisSeries } from '~~/composables/useAnalysisFetch'
import { getSensorTimeseries } from '~~/composables/useSensorTimeseries'
import { fetchModelTimeseries } from '~~/composables/useModelTimeseries'
import { availableVariables } from '~~/composables/useAnalysisStatistics'
import { useVariableRegistry } from '~~/composables/useVariableRegistry'
import SegmentedControl from './ui/SegmentedControl.vue'
import {

  aggregateSensorToDaily,
  buildComparisonSeries,
  filterBySeason,
  computeComparisonStats,
  type ComparisonPoint,
  type Season,
} from '~~/composables/useComparisonFetch'

const resolutionItems = [{ value: 'hourly', label: '1H', title: 'Hourly · 14-day window' }, { value: 'daily', label: '1D', title: 'Daily · full record' }]
const seasonItems = [{ value: 'all', label: 'All' }, { value: 'djf', label: 'DJF' }, { value: 'mam', label: 'MAM' }, { value: 'jja', label: 'JJA' }, { value: 'son', label: 'SON' }]

const props = defineProps<{ active?: boolean }>()
// The workspace's Scatter/Residuals/Seasonal tabs are derived from exactly these
// pairs — emitting them keeps this component the single fetch owner.
const emit = defineEmits<{ data: [ComparisonPoint[]] }>()
const mainStore = useMainStore()
const { displayUnit } = useVariableRegistry()

// --- STORE-DERIVED STATE ---
const variable = computed(() => mainStore.selected_variable.var)
const depth = computed(() => mainStore.selected_variable.depth_nc)
const selectedSensor = computed(() => mainStore.selectedSensor)

const sensorInfo = computed(() => {
  if (!selectedSensor.value?.id) return null
  return mainStore.sensors.find(s => s.id === selectedSensor.value!.id) ?? null
})

// Registry convention (see process/sensors/erddapTable_to_nc.py): depth === -1 means the
// sensor profiles the water column instead of sitting at one fixed depth. The daily
// single-depth chart below can't represent that — see the Depth Profile tab instead.
const isVariableDepth = computed(() => sensorInfo.value?.depth === -1)

const variableDepthLabel = computed(() => {
  if (!isVariableDepth.value) return depthLabel.value
  return depth.value != null ? `variable depth · ${depth.value}m picked` : 'variable depth'
})

const varName = computed(() =>
  availableVariables.find(v => v.id === variable.value)?.name || variable.value || 'Variable'
)

const depthLabel = computed(() => {
  if (depth.value == null) return '—'
  return depth.value === -1 ? 'bottom' : `${depth.value}m`
})

// --- SEASON FILTER ---
const selectedSeason = ref<Season>('all')

const seasonalData = computed(() => filterBySeason(rawComparisonData.value, selectedSeason.value))

const stats = computed(() =>
  rawComparisonData.value.length ? computeComparisonStats(seasonalData.value) : null
)

const biasColorClass = computed(() => {
  if (!stats.value) return ''
  return stats.value.bias > 0 ? 'text-red-lighten-2' : 'text-blue-lighten-2'
})

// --- STATE ---
const isLoading = ref(false)
const loadingStep = ref('')
const hasData = ref(false)
const errorMessage = ref<string | null>(null)


const rawComparisonData = ref<ComparisonPoint[]>([])

// ── RESOLUTION ────────────────────────────────────────────────────────────────
// `rawComparisonData` stays daily and full-record no matter what: the stats
// sidebar and the Advanced dialog's scatter/residuals/seasonal views are all
// defined on matched *daily* pairs, and re-basing them on a fortnight of hourly
// samples would quietly change what those numbers mean. Hourly is a second,
// display-only series that only the chart reads.
const resolution = ref<'hourly' | 'daily'>('daily')
const hourlyData = ref<ComparisonPoint[]>([])
const chartData = computed(() => resolution.value === 'hourly' ? hourlyData.value : rawComparisonData.value)

const HOURLY_WINDOW_DAYS = 14

/** Model and sensor at native cadence over the most recent window with data. */
async function loadHourly() {
  if (!sensorInfo.value || !selectedSensor.value || depth.value == null) return
  hourlyData.value = []

  const latest = sensorInfo.value.latest_data_at ? moment.utc(sensorInfo.value.latest_data_at) : moment.utc()
  const from = latest.clone().subtract(HOURLY_WINDOW_DAYS, 'days')
  const fromStr = from.format('YYYY-MM-DDTHHmmss')
  const toStr = latest.format('YYYY-MM-DDTHHmmss')

  const [modelResp, sensorResp] = await Promise.all([
    fetchModelTimeseries({
      source: mainStore.selected_variable.source,
      variable: variable.value,
      depth: depth.value,
      lat: sensorInfo.value.latitude,
      lon: sensorInfo.value.longitude,
      fromDate: fromStr,
      toDate: toStr,
    }),
    getSensorTimeseries(
      selectedSensor.value.id, variable.value, fromStr, toStr, depth.value,
      isVariableDepth.value ? mainStore.selected_variable.source : null,
    ),
  ])

  // Key both series by timestamp so the chart's two lines share an x-axis even
  // where one has samples the other lacks (a gappy sensor, an unmodelled hour).
  const byTime = new Map<string, ComparisonPoint>()
  const touch = (t: string) => {
    let p = byTime.get(t)
    if (!p) { p = { date: t, model: null, modelMin: null, modelMax: null, sensor: null }; byTime.set(t, p) }
    return p
  }
  modelResp.time.forEach((t, i) => { touch(t).model = modelResp.value[i] ?? null })
  const sTimes: string[] = sensorResp?.data?.time ?? []
  const sValues: (number | null)[] = sensorResp?.data?.value ?? []
  sTimes.forEach((t, i) => { touch(t).sensor = sValues[i] ?? null })

  hourlyData.value = Array.from(byTime.values()).sort((a, b) => a.date.localeCompare(b.date))
}

async function refreshHourlyIfNeeded() {
  if (resolution.value === 'hourly' && !hourlyData.value.length) {
    isLoading.value = true
    loadingStep.value = 'Fetching hourly data…'
    try {
      await loadHourly()
    } catch (err: any) {
      errorMessage.value = err?.response?.data?.detail || err?.message || 'Failed to load hourly data.'
    } finally {
      isLoading.value = false
      loadingStep.value = ''
    }
  }
  await nextTick()
  renderTimeseriesChart()
}

watch(resolution, refreshHourlyIfNeeded)

// A different sensor/variable/depth invalidates the cached hourly window.
// mainStore.unitPreference is included so toggling the display unit
// invalidates it too and, if hourly is the active resolution, re-fetches
// (a cheap cache hit — see useModelTimeseries.ts/useSensorTimeseries.ts)
// rather than leaving the chart showing stale numbers under the old unit.
watch([selectedSensor, variable, depth, () => mainStore.unitPreference[variable.value]], () => {
  hourlyData.value = []
  refreshHourlyIfNeeded()
})

// --- CHART REFS ---
const timeseriesContainerRef = ref<HTMLDivElement | null>(null)
let tsChart: echarts.ECharts | null = null

// --- SIGNATURE (prevents redundant reloads) ---
function currentSignature(): string {
  return JSON.stringify({ id: selectedSensor.value?.id, variable: variable.value, depth: depth.value })
}
let lastLoadedSig: string | null = null

// --- CHART ---
function initChart() {
  registerEchartsDarkTheme()
  if (tsChart) { tsChart.dispose(); tsChart = null }
  if (timeseriesContainerRef.value)
    tsChart = echarts.init(timeseriesContainerRef.value, 'dark', { renderer: 'canvas' })
}

function renderTimeseriesChart() {
  if (!tsChart || !chartData.value.length) return

  const data = chartData.value
  const modelMean = data.map(p => [p.date, p.model])
  const modelMin  = data.map(p => [p.date, p.modelMin])
  const modelMax  = data.map(p => [p.date, p.modelMax])
  const sensor    = data.map(p => [p.date, p.sensor])

  tsChart.setOption({
    tooltip: {
      trigger: 'axis',
      confine: true,
      formatter: (params: any) => {
        const items = (Array.isArray(params) ? params : [params])
          .filter((p: any) => !['Model min', 'Model max'].includes(p.seriesName) && p.value?.[1] != null)
        if (!items.length) return ''
        const rawDate = items[0]?.axisValue ?? items[0]?.value?.[0]
        const date = rawDate != null ? new Date(rawDate).toISOString().slice(0, 10) : ''
        let s = `<strong>${date}</strong><br/>`
        for (const p of items) {
          s += `${p.marker} ${p.seriesName}: <strong>${Number(p.value[1]).toFixed(3)}</strong><br/>`
        }
        return s
      },
    },
    legend: { data: ['Model (mean)', 'Sensor'], top: 4, textStyle: { fontSize: 10 } },
    grid: { left: '3%', right: '2%', bottom: '10%', top: '22%', containLabel: true },
    xAxis: {
      type: 'time',
      boundaryGap: false,
      axisLabel: { rotate: 45, fontSize: 9, color: '#ccc' },
    },
    yAxis: {
      type: 'value',
      name: displayUnit(variable.value) ? `${varName.value} (${displayUnit(variable.value)})` : varName.value,
      nameLocation: 'middle',
      nameGap: 50,
      axisLabel: { fontSize: 10, color: '#ccc' },
      min: 'dataMin',
      max: 'dataMax',
    },
    dataZoom: [{ type: 'inside' }, { type: 'slider', bottom: 4, height: 16 }],
    series: [
      {
        name: 'Model min',
        type: 'line',
        data: modelMin,
        symbol: 'none',
        lineStyle: { color: '#ff9800', width: 0.8, type: 'dashed', opacity: 0.45 },
        itemStyle: { color: '#ff9800' },
        emphasis: { disabled: true },
        legendHoverLink: false,
        silent: true,
      },
      {
        name: 'Model max',
        type: 'line',
        data: modelMax,
        symbol: 'none',
        lineStyle: { color: '#ff9800', width: 0.8, type: 'dashed', opacity: 0.45 },
        itemStyle: { color: '#ff9800' },
        emphasis: { disabled: true },
        legendHoverLink: false,
        silent: true,
      },
      {
        name: 'Model (mean)',
        type: 'line',
        data: modelMean,
        symbol: 'none',
        lineStyle: { color: '#ff9800', width: 1.5 },
        itemStyle: { color: '#ff9800' },
      },
      {
        name: 'Sensor',
        type: 'line',
        data: sensor,
        // Daily pairs are dense enough to read as a line, but hourly casts are
        // sparse and land on their own timestamps — with no neighbour to draw a
        // segment to, a symbol-less point renders as nothing at all. Show the
        // markers at hourly resolution so isolated casts are actually visible.
        symbol: resolution.value === 'hourly' ? 'circle' : 'none',
        symbolSize: 3,
        showSymbol: resolution.value === 'hourly',
        connectNulls: false,
        lineStyle: { color: '#a5d6a7', width: 1.5 },
        itemStyle: { color: '#a5d6a7' },
      },
    ],
  }, true)
  tsChart.resize()
}

// --- DATA LOAD ---
async function loadData() {
  if (!sensorInfo.value || !selectedSensor.value || depth.value == null) return

  const sig = currentSignature()
  isLoading.value = true
  errorMessage.value = null

  try {
    loadingStep.value = 'Fetching sensor data…'
    const toDateStr = moment.utc().format('YYYY-MM-DDTHHmmss')
    // Profilers have no fixed depth of their own (selectedSensor.depth === -1) — use the
    // shared global depth instead, same as Timeseries/Sensor Analysis, so this stays in
    // sync with whatever depth the user picked via the map control or Depth Profile.
    const sensorResp = await getSensorTimeseries(
      selectedSensor.value.id,
      variable.value,
      '2000-01-01T000000',
      toDateStr,
      depth.value,
      isVariableDepth.value ? mainStore.selected_variable.source : null
    )

    const sensorTimes: string[] = sensorResp?.data?.time ?? []
    const sensorValues: (number | null)[] = sensorResp?.data?.value ?? []

    if (!sensorTimes.length) throw new Error('No sensor data found for this variable.')

    const firstYear = parseInt(sensorTimes[0].slice(0, 4), 10)
    const lastYear  = parseInt(sensorTimes[sensorTimes.length - 1].slice(0, 4), 10)

    loadingStep.value = 'Fetching model data…'
    const location = { lat: sensorInfo.value.latitude, lon: sensorInfo.value.longitude }
    const base = { variable: variable.value, depth: depth.value, location, yearRange: [firstYear, lastYear] as [number, number] }

    const [meanData, minData, maxData] = await Promise.all([
      fetchAnalysisSeries({ ...base, stat: 'mean' }),
      fetchAnalysisSeries({ ...base, stat: 'min' }),
      fetchAnalysisSeries({ ...base, stat: 'max' }),
    ])

    loadingStep.value = 'Processing…'
    const sensorDaily = aggregateSensorToDaily(sensorTimes, sensorValues)
    rawComparisonData.value = buildComparisonSeries(meanData, minData, maxData, sensorDaily)
    emit('data', rawComparisonData.value)
    hasData.value = true
    lastLoadedSig = sig

    await nextTick()
    initChart()
    await nextTick()
    renderTimeseriesChart()

  } catch (err: any) {
    errorMessage.value = err?.response?.data?.detail || err?.message || 'Failed to load comparison data.'
    hasData.value = false
  } finally {
    isLoading.value = false
    loadingStep.value = ''
  }
}

// --- WATCHERS ---
// mainStore.unitPreference is included so toggling the display unit
// re-fetches (a cheap cache hit — see useAnalysisFetch.ts/useSensorTimeseries.ts)
// rather than leaving the chart/stats showing stale numbers under the old unit.
watch([selectedSensor, variable, depth, () => mainStore.unitPreference[variable.value]], () => {
  // The old guard against reloading while the Advanced dialog was open is gone
  // with the dialog: `props.active` now covers it, since the workspace only
  // marks this tab active while it is the visible one.
  hasData.value = false
  rawComparisonData.value = []
  errorMessage.value = null
  if (tsChart) { tsChart.dispose(); tsChart = null }

  if (props.active && sensorInfo.value && depth.value != null) {
    loadData()
  }
})

// immediate: true also covers first mount — the dialog only creates this component once
// opened, so `active` is already true by then and a plain watch would never see it change.
watch(() => props.active, (active) => {
  if (!active) return
  if (!sensorInfo.value || depth.value == null) return
  const sig = currentSignature()
  if (sig !== lastLoadedSig && !isLoading.value) loadData()
}, { immediate: true })


let resizeObserver: ResizeObserver | null = null
watch(timeseriesContainerRef, (el) => {
  if (resizeObserver) { resizeObserver.disconnect(); resizeObserver = null }
  if (el && typeof ResizeObserver !== 'undefined') {
    resizeObserver = new ResizeObserver(() => { if (tsChart) tsChart.resize() })
    resizeObserver.observe(el)
  }
})

function handleResize() { if (tsChart) tsChart.resize() }

onMounted(() => {
  registerEchartsDarkTheme()
  window.addEventListener('resize', handleResize)
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleResize)
  if (resizeObserver) { resizeObserver.disconnect(); resizeObserver = null }
  if (tsChart) { tsChart.dispose(); tsChart = null }
})
</script>


<style scoped>
.comparison-stats {
  background: rgba(255, 255, 255, 0.02);
}

.ctrl-label {
  font-size: 0.63rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: rgba(255, 255, 255, 0.38);
}

.comparison-stats :deep(button) {
  font-size: 0.65rem !important;
  letter-spacing: 0.01em !important;
  min-width: 0 !important;
  padding: 0 6px !important;
}

.stat-row {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  margin-bottom: 4px;
}

.stat-label {
  font-size: 0.68rem;
  color: rgba(255, 255, 255, 0.55);
}

.stat-value {
  font-size: 0.72rem;
  font-weight: 600;
  font-variant-numeric: tabular-nums;
}

.chart-loading {
  filter: grayscale(1);
  opacity: 0.45;
  transition: filter 0.4s ease, opacity 0.4s ease;
}
</style>
