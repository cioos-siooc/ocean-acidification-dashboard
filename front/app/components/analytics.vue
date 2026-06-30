<template>
  <div class="analytics-panel d-flex h-100" style="overflow:hidden;">

    <!-- LEFT: Controls -->
    <div class="analytics-sidebar pa-2 d-flex flex-column"
      style="width:190px; min-width:190px; overflow-y:auto; border-right:1px solid rgba(255,255,255,0.08);">

      <div class="d-flex align-center justify-space-between mb-2">
        <span class="ctrl-label" style="margin-bottom:0;">ANALYSIS</span>
        <div>
          <v-btn icon="mdi-fullscreen" size="x-small" variant="text" @click="advancedOpen = true" title="Advanced Mode" />
          <v-btn icon="mdi-refresh" size="x-small" variant="text" @click="resetParameters" title="Reset" />
        </div>
      </div>

      <div class="ctrl-label">View</div>
      <v-btn-toggle v-model="chartView" mandatory direction="vertical" variant="tonal" density="compact"
        class="w-100 mb-3">
        <v-btn value="overlay" size="small">All Years Overlaid</v-btn>
        <v-btn value="annual" size="small">Annual Summary</v-btn>
      </v-btn-toggle>

      <div class="ctrl-label">Season</div>
      <v-btn-toggle v-model="selectedSeason" mandatory variant="tonal" density="compact" class="flex-wrap w-100 mb-3">
        <v-btn value="full_year" size="x-small" class="season-btn">All</v-btn>
        <v-btn value="mam" size="x-small" class="season-btn">MAM</v-btn>
        <v-btn value="jja" size="x-small" class="season-btn">JJA</v-btn>
        <v-btn value="son" size="x-small" class="season-btn">SON</v-btn>
        <v-btn value="djf" size="x-small" class="season-btn">DJF</v-btn>
      </v-btn-toggle>

      <div class="ctrl-label">Statistic</div>
      <v-btn-toggle v-model="primaryStat" mandatory variant="outlined" density="compact" class="w-100 mb-3">
        <v-btn value="min" size="small">Min</v-btn>
        <v-btn value="mean" size="small">Mean</v-btn>
        <v-btn value="max" size="small">Max</v-btn>
      </v-btn-toggle>

      <v-btn block color="warning" size="small" prepend-icon="mdi-chart-line" :loading="isGenerating"
        :disabled="!lastClicked || !variable || depth == null" @click="runAnalysis">
        Run Analysis
      </v-btn>
    </div>

    <!-- CENTER: Chart -->
    <div class="flex-grow-1 d-flex flex-column" style="min-width:0; overflow:hidden;">

      <div class="d-flex align-center px-2 flex-shrink-0"
        style="height:28px; border-bottom:1px solid rgba(255,255,255,0.06);">
        <span class="text-caption font-weight-medium text-truncate">{{ chartTitle }}</span>
        <v-spacer />
        <v-chip size="x-small" color="warning" variant="tonal" class="ml-1 flex-shrink-0">{{ seasonLabel }}</v-chip>
      </div>

      <div class="flex-grow-1" style="position:relative; min-height:0;">

        <!-- Chart — stays mounted once first render; greyscale while reloading -->
        <div v-show="hasActivePlot" ref="chartContainerRef" class="w-100 h-100"
          :class="{ 'chart-loading': isGenerating }" />

        <!-- First-time load spinner (no chart yet) -->
        <div v-if="isGenerating && !hasActivePlot"
          class="d-flex flex-column align-center justify-center fill-height">
          <v-progress-circular indeterminate color="warning" size="36" class="mb-2" />
          <div class="text-caption text-warning">Querying ClickHouse...</div>
        </div>

        <!-- Reload badge shown over greyscale chart -->
        <div v-if="isGenerating && hasActivePlot" class="chart-reload-badge">
          <v-progress-circular indeterminate color="warning" size="14" width="2" class="mr-1" />
          <span>Updating…</span>
        </div>

        <v-alert v-else-if="!hasActivePlot && plotErrorMessage" type="error" variant="tonal" border="start"
          class="ma-3" density="compact">
          {{ plotErrorMessage }}
        </v-alert>

        <div v-else-if="!hasActivePlot && !isGenerating && !plotErrorMessage"
          class="d-flex flex-column align-center justify-center h-100 text-center px-6">
          <v-icon size="56" color="grey-darken-1">mdi-poll</v-icon>
          <div class="text-caption text-grey-darken-1 mt-2">Select a point on the map — analysis loads
            automatically</div>
        </div>
      </div>
    </div>

    <!-- RIGHT: Stats panel -->
    <div class="analytics-stats pa-2 d-flex flex-column"
      style="width:215px; min-width:215px; overflow:hidden; border-left:1px solid rgba(255,255,255,0.08);">

      <template v-if="extremeRecords">
        <div class="ctrl-label">All-time Records</div>
        <div class="d-flex align-center mb-1">
          <v-icon size="13" color="red-lighten-2">mdi-arrow-up-bold</v-icon>
          <span class="text-caption font-weight-medium ml-1">{{ Number(extremeRecords.max.value).toFixed(3) }}</span>
          <span class="text-grey ml-auto" style="font-size:0.63rem;">{{ String(extremeRecords.max.time).slice(0,
            10) }}</span>
        </div>
        <div class="d-flex align-center mb-3">
          <v-icon size="13" color="blue-lighten-2">mdi-arrow-down-bold</v-icon>
          <span class="text-caption font-weight-medium ml-1">{{ Number(extremeRecords.min.value).toFixed(3) }}</span>
          <span class="text-grey ml-auto" style="font-size:0.63rem;">{{ String(extremeRecords.min.time).slice(0,
            10) }}</span>
        </div>
      </template>

      <div class="ctrl-label">Threshold</div>
      <div class="d-flex align-center mb-3" style="gap:4px;">
        <v-btn-toggle v-model="thresholdDirection" mandatory direction="vertical" variant="tonal" density="compact"
          style="flex: none;">
          <v-btn value=">" size="x-small" class="threshold-btn">Above</v-btn>
          <v-btn value="<" size="x-small" class="threshold-btn">Below</v-btn>
        </v-btn-toggle>
        <v-text-field v-model.number="thresholdValue" type="number" variant="outlined" density="compact" hide-details
          style="flex: none; width: 120px;" />
      </div>

      <div class="ctrl-label">Per Year · {{ thresholdDirection === '>' ? 'Above' : 'Below' }} {{ thresholdValue }}</div>
      <div style="flex:1; overflow-y:auto; min-height:0;" @mouseover="onStatsRowMouseOver"
        @mouseleave="onStatsRowMouseLeave">
        <v-data-table v-if="yearlyStats.length" :headers="statsHeaders" :items="yearlyStats" density="compact"
          hide-default-footer :items-per-page="-1" :sort-by="statsSortBy" class="stats-table">
          <template #item.year="{ item }">
            <span :data-year="item.year">{{ item.year }}</span>
          </template>
          <template #item.days="{ item }">
            <span :data-year="item.year" :style="statCellStyle(item.days, maxDays)">{{ item.days }}</span>
          </template>
          <template #item.streak="{ item }">
            <span :data-year="item.year" :style="statCellStyle(item.streak, maxStreak)">{{ item.streak }}</span>
          </template>
        </v-data-table>
        <div v-else class="text-caption text-grey text-center mt-6">
          Per-year stats appear once analysis loads
        </div>
      </div>
    </div>

  </div>

  <AdvancedAnalysisDialog v-model="advancedOpen" />
</template>


<script setup lang="ts">
import { ref, computed, watch, nextTick, onMounted, onBeforeUnmount } from 'vue'
import * as echarts from 'echarts'
import { registerEchartsDarkTheme } from '../../composables/useEchartsTheme'
import { useMainStore } from '../stores/main'
import { fetchAnalysisSeries, type SeriesPoint } from '../../composables/useAnalysisFetch'
import {
  availableVariables, filterBySeason, groupByYear,
  computeBoxplotData, type BoxRow, linearRegression, yearColor,
} from '../../composables/useAnalysisStatistics'
import AdvancedAnalysisDialog from './analysis/AdvancedAnalysisDialog.vue'

const mainStore = useMainStore()

// Only fetch while the Analysis Builder tab is actually visible — the parent
// page keeps this component mounted (v-show) even when another tab is shown.
const props = defineProps<{ active?: boolean }>()

const advancedOpen = ref(false)

// --- STORE-DERIVED STATE ---
const variable = computed(() => mainStore.selected_variable.var)
const lastClicked = computed(() => mainStore.lastClickedMapPoint)
const queryMode = computed(() => mainStore.queryMode)
const depth = computed(() => mainStore.selected_variable.depth_nc)

// --- CONSTANTS ---
const minYear = 2007
const maxYear = 2026

// --- REACTIVE STATE ---
const chartView = ref<'overlay' | 'annual'>('overlay')
const selectedSeason = ref('full_year')
const primaryStat = ref('mean')
const thresholdValue = ref(0)
const thresholdDirection = ref('>')

const isGenerating = ref(false)
const hasActivePlot = ref(false)
const plotErrorMessage = ref<string | null>(null)

let autoRunTimer: ReturnType<typeof setTimeout> | null = null
let activeRequestId = 0
let lastFetchSignature: string | null = null
let analysisRequestController: AbortController | null = null

const chartContainerRef = ref<HTMLDivElement | null>(null)
let chartInstance: echarts.ECharts | null = null

// Per-series base color/width for the overlay chart, captured at render time so
// table-row hover can dim/restore series directly (the legend itself fades others
// natively via ECharts' hover focus/blur, which only kicks in for real hover events).
let overlaySeriesStyles: { name: string; color: string; width: number }[] = []

const rawAllData = ref<SeriesPoint[]>([])
const rawSeasonalData = ref<SeriesPoint[]>([])

// --- COMPUTED ---
const polygonFromClick = computed<[number, number][]>(() => {
  const pt = lastClicked.value
  if (!pt) return []
  const h = 0.05
  return [
    [pt.lng - h, pt.lat - h],
    [pt.lng + h, pt.lat - h],
    [pt.lng + h, pt.lat + h],
    [pt.lng - h, pt.lat + h],
    [pt.lng - h, pt.lat - h],
  ]
})

const varName = computed(() =>
  availableVariables.find(v => v.id === variable.value)?.name || variable.value || 'Variable'
)

const seasonLabel = computed(() => {
  const labels: Record<string, string> = {
    full_year: 'Full Year', jja: 'Summer (JJA)', mam: 'Spring (MAM)', son: 'Autumn (SON)', djf: 'Winter (DJF)'
  }
  return labels[selectedSeason.value] || 'Full Year'
})

const chartTitle = computed(() =>
  `${chartView.value === 'overlay' ? 'All Years Overlaid' : 'Annual Summary'} — ${varName.value} (${primaryStat.value.toUpperCase()})`
)

const extremeRecords = computed(() => {
  const data = rawSeasonalData.value
  if (!data.length) return null
  let max: SeriesPoint | null = null
  let min: SeriesPoint | null = null
  for (const d of data) {
    if (d.value == null) continue
    if (!max || d.value > (max.value as number)) max = d
    if (!min || d.value < (min.value as number)) min = d
  }
  return (max && min) ? { max, min } : null
})

const statsHeaders = [
  { title: 'Year', key: 'year' },
  { title: 'Days', key: 'days' },
  { title: 'Streak', key: 'streak' }
]

const statsSortBy = [{ key: 'year', order: 'desc' as const }]

const yearlyStats = computed(() => {
  const data = rawSeasonalData.value
  if (!data.length) return []
  const t = thresholdValue.value
  const dir = thresholdDirection.value
  const match = (v: number) => dir === '>' ? v > t : v < t

  return groupByYear(data).map(({ year, data: pts }) => {
    const days = pts.filter(d => d.value != null && match(d.value as number)).length

    let maxStreak = 0, cur = 0
    for (let i = 0; i < pts.length; i++) {
      const { time, value } = pts[i]
      const prev = pts[i - 1]
      const adjacent = i > 0 && prev != null &&
        new Date(time).getTime() - new Date(prev.time).getTime() === 86_400_000
      if (value != null && match(value as number)) {
        cur = adjacent ? cur + 1 : 1
      } else {
        cur = 0
      }
      if (cur > maxStreak) maxStreak = cur
    }

    return { year, days, streak: maxStreak }
  })
})

const maxDays = computed(() => Math.max(0, ...yearlyStats.value.map(r => r.days)))
const maxStreak = computed(() => Math.max(0, ...yearlyStats.value.map(r => r.streak)))

function statCellStyle(val: number, max: number): Record<string, string> {
  if (max === 0) return { color: 'rgba(180,180,180,0.4)' }
  const t = Math.min(1, val / max)
  const r = Math.round(180 + t * 75)   // 180 → 255
  const g = Math.round(180 - t * 28)   // 180 → 152
  const b = Math.round(180 - t * 180)  // 180 → 0
  const a = (0.4 + t * 0.6).toFixed(2) // 0.40 → 1.00
  return {
    color: `rgba(${r},${g},${b},${a})`,
    fontWeight: t > 0.6 ? '600' : 'normal',
  }
}

// --- HELPERS ---
const MONTH_ABBR = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

// Reference year the "All Years Overlaid" chart maps every point's month/day
// onto, so a partial year (e.g. data only through June) lands at its real
// calendar position on the x-axis instead of sliding to the start. Must be a
// leap year so a real Feb 29 always has a slot. DJF's December points are
// placed in the year before this one so Dec sits immediately before Jan on
// the axis, matching how a single winter actually runs Dec → Jan → Feb.
const OVERLAY_REF_YEAR = 2000

function overlayTimestamp(iso: string): number {
  const month = parseInt(iso.slice(5, 7), 10)
  const day = parseInt(iso.slice(8, 10), 10)
  const year = (selectedSeason.value === 'djf' && month === 12) ? OVERLAY_REF_YEAR - 1 : OVERLAY_REF_YEAR
  return Date.UTC(year, month - 1, day)
}

function fmtOverlayDate(ts: number): string {
  const d = new Date(ts)
  return `${MONTH_ABBR[d.getUTCMonth()]} ${String(d.getUTCDate()).padStart(2, '0')}`
}

function thresholdMarkLine() {
  return {
    silent: true,
    symbol: 'none',
    lineStyle: { color: '##33cccc', type: 'dashed', width: 1.5 },
    label: { formatter: String(thresholdValue.value), position: 'insideEndTop', fontSize: 9, color: '#33cccc' },
    data: [{ yAxis: thresholdValue.value }]
  }
}

/** Highlights a single year's series (overlay chart only) by dimming all others; null restores normal view. */
function applyYearHighlight(year: string | null) {
  if (!chartInstance || chartView.value !== 'overlay' || !overlaySeriesStyles.length) return
  chartInstance.setOption({
    series: overlaySeriesStyles.map(s => {
      const focused = year != null && s.name === year
      const dimmed = year != null && !focused
      return {
        name: s.name,
        z: focused ? 10 : 5,
        lineStyle: { color: s.color, width: focused ? s.width + 1.5 : s.width, opacity: dimmed ? 0.07 : 1 },
        itemStyle: { color: s.color, opacity: dimmed ? 0.07 : 1 },
      }
    })
  })
}

function onStatsRowMouseOver(e: MouseEvent) {
  const target = (e.target as HTMLElement).closest('[data-year]') as HTMLElement | null
  if (!target?.dataset.year) return
  applyYearHighlight(target.dataset.year)
}

function onStatsRowMouseLeave() {
  applyYearHighlight(null)
}

function initChart() {
  registerEchartsDarkTheme()
  if (chartInstance) { chartInstance.dispose(); chartInstance = null }
  if (!chartContainerRef.value) return
  chartInstance = echarts.init(chartContainerRef.value, 'dark', { renderer: 'canvas' })
}

// --- CHART RENDERERS ---
function renderOverlayChart(series: { year: number; data: SeriesPoint[] }[]) {
  initChart()
  if (!chartInstance) return
  overlaySeriesStyles = []

  const total = series.length

  const echartsSeries: any[] = series.map((s, idx) => {
    const t = total <= 1 ? 1 : idx / (total - 1)
    const color = yearColor(idx, total)
    const width = 1 + t * 1.5   // 1px (oldest) → 2.5px (newest)
    overlaySeriesStyles.push({ name: String(s.year), color, width })

    // Points placed by actual calendar day on a shared synthetic-year axis
    // (see overlayTimestamp) rather than by array index, so a partial year
    // lands at its real position instead of sliding to the start. Sorted
    // because DJF's Dec points get mapped to the year before Jan/Feb, which
    // is out of order relative to the original Jan→Dec array order.
    const points = s.data
      .map(d => [overlayTimestamp(d.time), d.value] as [number, number | null])
      .sort((a, b) => a[0] - b[0])

    return {
      name: String(s.year),
      type: 'line',
      smooth: true,
      symbol: 'none',
      lineStyle: { width, color },
      itemStyle: { color },
      data: points,
      emphasis: {
        focus: 'series',
        lineStyle: { width: width + 1.5, opacity: 1, color },
      },
      blur: {
        lineStyle: { opacity: 0.07 },
      },
    }
  })

  if (echartsSeries.length > 0) echartsSeries[0].markLine = thresholdMarkLine()

  chartInstance.setOption({
    tooltip: {
      trigger: 'axis',
      formatter: (params: any) => {
        const items = (Array.isArray(params) ? params : [params])
          .filter((p: any) => p.value?.[1] != null)
          .sort((a: any, b: any) => (b.value[1] ?? 0) - (a.value[1] ?? 0))
        if (!items.length) return ''
        let s = `<strong>${fmtOverlayDate(items[0].value[0])}</strong><br/>`
        items.forEach((p: any) => {
          s += `${p.marker} ${p.seriesName}: <strong>${Number(p.value[1]).toFixed(3)}</strong><br/>`
        })
        return s
      }
    },
    legend: { top: 4, type: 'scroll', textStyle: { fontSize: 10 } },
    grid: { left: '3%', right: '2%', bottom: '10%', top: '22%', containLabel: true },
    xAxis: {
      type: 'time',
      boundaryGap: false,
      axisLabel: { rotate: 45, fontSize: 9, color: '#ccc', formatter: (value: number) => fmtOverlayDate(value) },
    },
    yAxis: { type: 'value', name: `${varName.value} (${primaryStat.value})`, nameLocation: 'middle', nameGap: 50, axisLabel: { fontSize: 10, color: '#ccc' }, min: 'dataMin', max: 'dataMax' },
    dataZoom: [{ type: 'inside' }, { type: 'slider', bottom: 4, height: 16 }],
    series: echartsSeries
  }, true)

  chartInstance.resize()
}

function renderAnnualSummaryChart(rows: BoxRow[]) {
  initChart()
  if (!chartInstance) return

  const years = rows.map(r => String(r.year))
  const boxData = rows.map(r => r.box)
  const meanData = rows.map(r => r.mean)

  // Linear trend on annual means
  const pts = rows.map((r, i) => ({ x: i, y: r.mean }))
  const { slope, intercept } = linearRegression(pts)
  const trendData = rows.map((_, i) => Number((slope * i + intercept).toFixed(4)))

  chartInstance.setOption({
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      formatter: (params: any) => {
        const items = Array.isArray(params) ? params : [params]
        if (!items.length) return ''
        const idx = items[0].dataIndex
        const r = rows[idx]
        if (!r) return ''
        return `<strong>${r.year}</strong><br/>
          Max: <strong>${r.box[4].toFixed(3)}</strong><br/>
          Q3: <strong>${r.box[3].toFixed(3)}</strong><br/>
          Median: <strong>${r.box[2].toFixed(3)}</strong><br/>
          Q1: <strong>${r.box[1].toFixed(3)}</strong><br/>
          Min: <strong>${r.box[0].toFixed(3)}</strong><br/>
          Mean: <strong>${r.mean.toFixed(3)}</strong><br/>
          Std: <strong>${r.std.toFixed(3)}</strong>`
      }
    },
    legend: { data: ['Distribution', 'Mean', 'Trend'], top: 4, textStyle: { fontSize: 10 } },
    grid: { left: '3%', right: '2%', bottom: '10%', top: '22%', containLabel: true },
    xAxis: { type: 'category', data: years, axisLabel: { rotate: 45, fontSize: 9, color: '#ccc' } },
    yAxis: { type: 'value', name: `${varName.value} (${primaryStat.value})`, nameLocation: 'middle', nameGap: 50, axisLabel: { fontSize: 10, color: '#ccc' }, min: 'dataMin', max: 'dataMax' },
    dataZoom: [{ type: 'inside' }, { type: 'slider', bottom: 4, height: 16 }],
    series: [
      {
        name: 'Distribution',
        type: 'boxplot',
        data: boxData,
        itemStyle: { color: 'rgba(255,152,0,0.25)', borderColor: '#ff9800', borderWidth: 1.5 },
        boxWidth: ['20%', '50%'],
        markLine: thresholdMarkLine()
      },
      {
        name: 'Mean',
        type: 'scatter',
        data: meanData,
        symbolSize: 9,
        symbol: 'diamond',
        itemStyle: { color: '#ff9800' },
        z: 10
      },
      {
        name: 'Trend',
        type: 'line',
        data: trendData,
        smooth: false,
        symbol: 'none',
        lineStyle: { color: 'rgba(255,255,255,0.7)', width: 1.5, type: 'dashed' },
        itemStyle: { color: 'rgba(255,255,255,0.7)' }
      }
    ]
  }, true)
  chartInstance.resize()
}

function renderChart(seasonal: SeriesPoint[]) {
  if (!seasonal.length) return
  if (chartView.value === 'overlay') {
    renderOverlayChart(groupByYear(seasonal))
  } else {
    renderAnnualSummaryChart(computeBoxplotData(seasonal))
  }
}

function reRenderChart() {
  if (!rawSeasonalData.value.length || !hasActivePlot.value) return
  nextTick(() => setTimeout(() => renderChart(rawSeasonalData.value), 50))
}

// --- WATCHERS ---
watch(chartView, reRenderChart)

watch(thresholdValue, () => {
  if (!chartInstance || !hasActivePlot.value) return
  chartInstance.setOption({ series: [{ markLine: thresholdMarkLine() }] })
})

watch(selectedSeason, () => {
  if (!rawAllData.value.length || isGenerating.value) return
  rawSeasonalData.value = filterBySeason(rawAllData.value, selectedSeason.value)
  reRenderChart()
})

// Auto-fetch on point/area, variable, or depth change — but only while this tab is visible.
// primaryStat is intentionally excluded: switching Min/Mean/Max stays manual via the Run button.
watch([lastClicked, variable, depth, queryMode], scheduleAutoRun)

// Switching into this tab fetches fresh data for whatever changed while it was hidden.
watch(() => props.active, (active: boolean | undefined) => {
  if (active) scheduleAutoRun()
})

let resizeObserver: ResizeObserver | null = null
watch(chartContainerRef, (el) => {
  if (resizeObserver) { resizeObserver.disconnect(); resizeObserver = null }
  if (el && typeof ResizeObserver !== 'undefined') {
    resizeObserver = new ResizeObserver(() => { if (chartInstance) chartInstance.resize() })
    resizeObserver.observe(el)
  }
})

function handleResize() { if (chartInstance) chartInstance.resize() }

onMounted(() => {
  registerEchartsDarkTheme()
  window.addEventListener('resize', handleResize)
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleResize)
  if (resizeObserver) { resizeObserver.disconnect(); resizeObserver = null }
  if (chartInstance) { chartInstance.dispose(); chartInstance = null }
})

// --- DATA FETCH ---
async function fetchRegionTimeseries(): Promise<SeriesPoint[]> {
  const pt = lastClicked.value
  if (!pt) throw new Error('No location selected. Click on the map first.')
  if (depth.value == null) throw new Error('No depth selected.')
  const location = queryMode.value === 'area'
    ? { polygon: polygonFromClick.value }
    : { lat: pt.lat, lon: pt.lng }
  // Abort any in-flight request so a stale response can't clobber a newer one
  if (analysisRequestController) analysisRequestController.abort()
  analysisRequestController = new AbortController()
  return fetchAnalysisSeries(
    { variable: variable.value, stat: primaryStat.value as 'min' | 'mean' | 'max', depth: depth.value, location, yearRange: [minYear, maxYear] },
    analysisRequestController.signal
  )
}

// --- ACTIONS ---
// Identifies the inputs that should trigger an automatic refetch (point/area, variable, depth).
// Deliberately excludes primaryStat — switching Min/Mean/Max stays manual via the Run button.
function currentSignature(): string {
  const pt = lastClicked.value
  return JSON.stringify({ lat: pt?.lat, lng: pt?.lng, mode: queryMode.value, variable: variable.value, depth: depth.value })
}

function scheduleAutoRun() {
  if (!props.active) return
  if (!lastClicked.value || !variable.value || depth.value == null) return
  const sig = currentSignature()
  if (sig === lastFetchSignature) return
  if (autoRunTimer) clearTimeout(autoRunTimer)
  autoRunTimer = setTimeout(runAnalysis, 300)
}

const runAnalysis = async () => {
  const requestId = ++activeRequestId
  lastFetchSignature = currentSignature()
  plotErrorMessage.value = null
  isGenerating.value = true
  // Keep hasActivePlot as-is so old chart remains visible (greyscaled) while fetching
  rawAllData.value = []
  rawSeasonalData.value = []

  try {
    const rawData = await fetchRegionTimeseries()
    if (requestId !== activeRequestId) return // superseded by a newer request
    rawAllData.value = rawData
    rawSeasonalData.value = filterBySeason(rawData, selectedSeason.value)

    hasActivePlot.value = true
    await nextTick()
    // Keep isGenerating true until new chart is rendered, then clear it
    await new Promise<void>(resolve => setTimeout(() => {
      if (chartContainerRef.value) {
        renderChart(rawSeasonalData.value)
      } else {
        plotErrorMessage.value = 'Chart container not found.'
        hasActivePlot.value = false
      }
      resolve()
    }, 100))
  } catch (error: any) {
    if (requestId !== activeRequestId || error?.code === 'ERR_CANCELED') return
    plotErrorMessage.value = error?.response?.data?.detail || error?.message || 'Failed to generate analysis.'
    hasActivePlot.value = false
  } finally {
    if (requestId === activeRequestId) isGenerating.value = false
  }
}

function resetParameters() {
  chartView.value = 'overlay'
  selectedSeason.value = 'full_year'
  primaryStat.value = 'mean'
  thresholdValue.value = 0
  thresholdDirection.value = '>'
  hasActivePlot.value = false
  plotErrorMessage.value = null
  rawAllData.value = []
  rawSeasonalData.value = []
  if (chartInstance) { chartInstance.dispose(); chartInstance = null }
}
</script>


<style scoped>
.analytics-panel {
  height: 100%;
}

.analytics-sidebar {
  background: rgba(255, 255, 255, 0.02);
}

.analytics-stats {
  background: rgba(255, 255, 255, 0.02);
}

.ctrl-label {
  font-size: 0.63rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: rgba(255, 255, 255, 0.38);
  margin-bottom: 4px;
}

.analytics-sidebar :deep(.v-btn) {
  font-size: 0.7rem !important;
  letter-spacing: 0.02em !important;
}

.analytics-sidebar :deep(.v-btn__content) {
  font-size: 0.7rem !important;
}

.season-btn {
  min-width: 34px !important;
  flex: 1 !important;
  padding: 0 4px !important;
}

.threshold-btn {
  max-width: 80px !important;
  flex: 1 !important;
  padding: 0 4px !important;
  font-size: 0.7rem !important;
}

.chart-loading {
  filter: grayscale(1);
  opacity: 0.45;
  transition: filter 0.4s ease, opacity 0.4s ease;
}

.chart-reload-badge {
  position: absolute;
  top: 8px;
  right: 8px;
  display: flex;
  align-items: center;
  gap: 4px;
  background: rgba(0, 0, 0, 0.55);
  border-radius: 4px;
  padding: 3px 8px;
  font-size: 0.65rem;
  color: rgba(255, 193, 7, 0.9);
  pointer-events: none;
}

.stats-table :deep(.v-data-table__td),
.stats-table :deep(.v-data-table__th) {
  font-size: 0.72rem !important;
  padding: 2px 6px !important;
}

.stats-table :deep(.v-data-table__th) {
  font-weight: 600 !important;
}
</style>
