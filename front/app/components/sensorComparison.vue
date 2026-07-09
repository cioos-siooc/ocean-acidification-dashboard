<template>
  <div class="comparison-panel d-flex flex-column h-100" style="overflow:hidden;">

    <!-- HEADER STRIP -->
    <div class="d-flex align-center px-3 flex-shrink-0"
      style="height:30px; border-bottom:1px solid rgba(255,255,255,0.08); gap:8px;">
      <span class="text-caption font-weight-medium text-truncate" style="min-width:0;">
        {{ sensorInfo?.name || '—' }}
        <span class="text-grey"> · {{ varName }} · {{ depthLabel }}</span>
      </span>
      <v-spacer />
      <v-progress-circular v-if="isLoading" indeterminate color="warning" size="14" width="2" class="flex-shrink-0" />
      <span v-if="isLoading" class="text-caption text-grey flex-shrink-0">{{ loadingStep }}</span>
      <v-chip v-if="!isLoading && stats && stats.n > 0" size="x-small" color="teal" variant="tonal" class="flex-shrink-0">
        {{ stats.n }} days
      </v-chip>
      <v-btn icon="mdi-fullscreen" size="x-small" variant="text" :disabled="!hasData"
        title="Advanced Analysis" @click="advancedOpen = true" />
    </div>

    <!-- MAIN ROW: chart + stats -->
    <div class="d-flex flex-grow-1" style="min-height:0; overflow:hidden;">

      <!-- TIME SERIES CHART -->
      <div class="flex-grow-1 d-flex flex-column" style="min-width:0; overflow:hidden; position:relative;">

        <div v-show="hasData" ref="timeseriesContainerRef" class="w-100 h-100"
          :class="{ 'chart-loading': isLoading && hasData }" />

        <div v-if="isLoading && !hasData"
          class="d-flex flex-column align-center justify-center fill-height">
          <v-progress-circular indeterminate color="warning" size="36" class="mb-2" />
          <div class="text-caption text-warning">{{ loadingStep }}</div>
        </div>

        <v-alert v-else-if="!hasData && errorMessage" type="error" variant="tonal" border="start"
          class="ma-3" density="compact">
          {{ errorMessage }}
        </v-alert>

        <div v-else-if="!hasData && !isLoading"
          class="d-flex flex-column align-center justify-center h-100 text-center px-6">
          <v-icon size="48" color="grey-darken-1">mdi-compare-horizontal</v-icon>
          <div class="text-caption text-grey-darken-1 mt-2">Select a sensor to load the comparison</div>
        </div>
      </div>

      <!-- RIGHT: Season filter + Stats -->
      <div class="comparison-stats pa-2 d-flex flex-column"
        style="width:210px; min-width:210px; border-left:1px solid rgba(255,255,255,0.08); overflow:hidden;">

        <div class="ctrl-label mb-1">Season</div>
        <v-btn-toggle v-model="selectedSeason" mandatory variant="tonal" density="compact" class="w-100 mb-3 flex-wrap">
          <v-btn value="all" size="x-small">All</v-btn>
          <v-btn value="djf" size="x-small">DJF</v-btn>
          <v-btn value="mam" size="x-small">MAM</v-btn>
          <v-btn value="jja" size="x-small">JJA</v-btn>
          <v-btn value="son" size="x-small">SON</v-btn>
        </v-btn-toggle>

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
          class="d-flex align-center justify-center flex-grow-1">
          <div class="text-caption text-grey text-center">Stats appear<br>after loading</div>
        </div>

        <div v-else-if="hasData && stats?.n === 0" class="text-caption text-grey mt-2">
          No matched pairs for this season.
        </div>
      </div>
    </div>

  </div>

  <AdvancedComparisonDialog
    v-model="advancedOpen"
    :data="rawComparisonData"
    :sensor-name="sensorInfo?.name || ''"
    :var-name="varName"
    :depth-label="depthLabel"
    :initial-season="selectedSeason"
  />
</template>


<script setup lang="ts">
import { ref, computed, watch, nextTick, onMounted, onBeforeUnmount } from 'vue'
import * as echarts from 'echarts'
import moment from 'moment-timezone'
import { registerEchartsDarkTheme } from '../../composables/useEchartsTheme'
import { useMainStore } from '../stores/main'
import { fetchAnalysisSeries } from '../../composables/useAnalysisFetch'
import { getSensorTimeseries } from '../../composables/useSensorTimeseries'
import { availableVariables } from '../../composables/useAnalysisStatistics'
import {
  aggregateSensorToDaily,
  buildComparisonSeries,
  filterBySeason,
  computeComparisonStats,
  type ComparisonPoint,
  type Season,
} from '../../composables/useComparisonFetch'
import AdvancedComparisonDialog from './comparison/AdvancedComparisonDialog.vue'

const props = defineProps<{ active?: boolean }>()
const mainStore = useMainStore()

// --- STORE-DERIVED STATE ---
const variable = computed(() => mainStore.selected_variable.var)
const depth = computed(() => mainStore.selected_variable.depth_nc)
const selectedSensor = computed(() => mainStore.selectedSensor)

const sensorInfo = computed(() => {
  if (!selectedSensor.value?.id) return null
  return mainStore.sensors.find(s => s.id === selectedSensor.value!.id) ?? null
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
const advancedOpen = ref(false)

const rawComparisonData = ref<ComparisonPoint[]>([])

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
  if (!tsChart || !rawComparisonData.value.length) return

  const data = rawComparisonData.value
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
        const date = String(items[0]?.axisValue ?? items[0]?.value?.[0] ?? '').slice(0, 10)
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
      name: varName.value,
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
        symbol: 'none',
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
    const sensorResp = await getSensorTimeseries(
      selectedSensor.value.id,
      variable.value,
      '2000-01-01T000000',
      toDateStr,
      selectedSensor.value.depth
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
watch([selectedSensor, variable, depth], () => {
  hasData.value = false
  rawComparisonData.value = []
  errorMessage.value = null
  if (tsChart) { tsChart.dispose(); tsChart = null }

  if (props.active && sensorInfo.value && depth.value != null) {
    loadData()
  }
})

watch(() => props.active, (active) => {
  if (!active) return
  if (!sensorInfo.value || depth.value == null) return
  const sig = currentSignature()
  if (sig !== lastLoadedSig && !isLoading.value) loadData()
})

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

.comparison-stats :deep(.v-btn) {
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
