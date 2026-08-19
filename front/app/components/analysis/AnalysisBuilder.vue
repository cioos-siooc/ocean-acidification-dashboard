<template>
  <div class="analytics-panel d-flex h-100" style="overflow:hidden;">

    <!-- LEFT: Controls -->
    <div class="analytics-sidebar pa-2 d-flex flex-column"
      style="width:190px; min-width:190px; overflow-y:auto; border-right:1px solid rgba(255,255,255,0.08);">

      <div class="d-flex align-center justify-space-between mb-2">
        <span class="ctrl-label" style="margin-bottom:0;">ANALYSIS</span>
      </div>

      <div class="ctrl-label">Season</div>
      <v-btn-toggle v-model="selectedSeason" mandatory variant="tonal" class="flex-wrap w-100 mb-3">
        <v-btn value="full_year" size="x-small" class="season-btn">All</v-btn>
        <v-btn value="mam" size="x-small" class="season-btn">MAM</v-btn>
        <v-btn value="jja" size="x-small" class="season-btn">JJA</v-btn>
        <v-btn value="son" size="x-small" class="season-btn">SON</v-btn>
        <v-btn value="djf" size="x-small" class="season-btn">DJF</v-btn>
      </v-btn-toggle>

      <div class="ctrl-label">Statistic</div>
      <v-btn-toggle v-model="primaryStat" mandatory variant="outlined" class="w-100 mb-3">
        <v-btn value="min" size="small">Min</v-btn>
        <v-btn value="mean" size="small">Mean</v-btn>
        <v-btn value="max" size="small">Max</v-btn>
      </v-btn-toggle>

      <v-spacer />
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
          class="ma-3">
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
      <div v-else class="text-caption text-grey text-center mt-6">
        Records appear once analysis loads
      </div>
    </div>

  </div>

</template>


<script setup lang="ts">
import { ref, computed, watch, nextTick, onMounted, onBeforeUnmount } from 'vue'
import * as echarts from 'echarts'
import { registerEchartsDarkTheme } from '~~/composables/useEchartsTheme'
import { useVariableRegistry } from '~~/composables/useVariableRegistry'
import { useMainStore } from '../../stores/main'
import { fetchAnalysisSeries, type SeriesPoint } from '~~/composables/useAnalysisFetch'
import { fetchSensorAnalysisSeries } from '~~/composables/useSensorAnalysisFetch'
import {
  availableVariables, filterBySeason, groupByYear, breakDataGaps, yearColor, computeYearBandStats,
} from '~~/composables/useAnalysisStatistics'

const props = defineProps<{ active?: boolean; source?: 'model' | 'sensor' }>()
const analysisSource = computed(() => props.source ?? 'model')
const isSensor = computed(() => analysisSource.value === 'sensor')

const mainStore = useMainStore()
const { displayUnit } = useVariableRegistry()

// ── SENSOR CONTEXT (unused in model mode) ────────────────────────────────────
const selectedSensor = computed(() => mainStore.selectedSensor)
const sensorInfo = computed(() => {
  if (!selectedSensor.value?.id) return null
  return mainStore.sensors.find(s => s.id === selectedSensor.value!.id) ?? null
})
// A profiler has no depth of its own, so it borrows the map's selected depth —
// the same one driving the raster layer.
const isVariableDepth = computed(() => sensorInfo.value?.depth === -1)

// Only fetch while the Analysis Builder tab is actually visible — the parent
// page keeps this component mounted (v-show) even when another tab is shown.


// --- STORE-DERIVED STATE ---
const variable = computed(() => mainStore.selected_variable.var)
const lastClicked = computed(() => mainStore.lastClickedMapPoint)
const queryMode = computed(() => mainStore.queryMode)
const depth = computed(() => {
  if (!isSensor.value || isVariableDepth.value) return mainStore.selected_variable.depth_nc
  return selectedSensor.value?.depth ?? null
})

// --- CONSTANTS ---
const MODEL_MIN_YEAR = 2007
const MODEL_MAX_YEAR = 2026
const currentYear = new Date().getFullYear()
const minYear = computed(() => !isSensor.value ? MODEL_MIN_YEAR
  : (sensorInfo.value?.first_data_at ? parseInt(sensorInfo.value.first_data_at.slice(0, 4), 10) : currentYear))
const maxYear = computed(() => !isSensor.value ? MODEL_MAX_YEAR
  : (sensorInfo.value?.latest_data_at ? parseInt(sensorInfo.value.latest_data_at.slice(0, 4), 10) : currentYear))

// --- REACTIVE STATE ---
const selectedSeason = ref('full_year')
const primaryStat = ref('mean')

const isGenerating = ref(false)
const hasActivePlot = ref(false)
const plotErrorMessage = ref<string | null>(null)

let autoRunTimer: ReturnType<typeof setTimeout> | null = null
let activeRequestId = 0
let lastFetchSignature: string | null = null

const chartContainerRef = ref<HTMLDivElement | null>(null)
let chartInstance: echarts.ECharts | null = null

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
const varUnit = computed(() => variable.value ? displayUnit(variable.value) : '')

const pointLabel = computed(() => {
  if (isSensor.value) return sensorInfo.value?.name ?? ''
  const pt = lastClicked.value
  if (!pt) return ''
  return queryMode.value === 'area' ? `~${pt.lat.toFixed(2)}, ${pt.lng.toFixed(2)} (area)` : `${pt.lat.toFixed(3)}, ${pt.lng.toFixed(3)}`
})


const seasonLabel = computed(() => {
  const labels: Record<string, string> = {
    full_year: 'Full Year', jja: 'Summer (JJA)', mam: 'Spring (MAM)', son: 'Autumn (SON)', djf: 'Winter (DJF)'
  }
  return labels[selectedSeason.value] || 'Full Year'
})

const chartTitle = computed(() => {
  // A profiler's depth is not implied by the sensor, so name it.
  const depthSuffix = isSensor.value && isVariableDepth.value && depth.value != null ? ` @ ${depth.value}m` : ''
  return `Historical Range — ${varName.value}${depthSuffix} (${primaryStat.value.toUpperCase()})`
})

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

// --- HELPERS ---
const MONTH_ABBR = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

// Reference year the "All Years Overlaid" chart maps every point's month/day
// onto, so a partial year (e.g. data only through June) lands at its real
// calendar position on the x-axis instead of sliding to the start. Must be a
// leap year so a real Feb 29 always has a slot. DJF's December points are
// placed in the year before this one so Dec sits immediately before Jan on
// the axis, matching how a single winter actually runs Dec → Jan → Feb.
const OVERLAY_REF_YEAR = 2000

// Fixed baseline cutoff for the historical band/mean/percentile stats — years after this
// are excluded regardless of the current calendar year (so e.g. in 2027, 2026 still isn't
// pooled into the baseline), since only years through 2025 are treated as complete/settled.
const STATS_BASELINE_MAX_YEAR = 2025

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

  const total = series.length
  const perYearPoints: [number, number | null][][] = []

  const yearSeries: any[] = series.map((s, idx) => {
    const color = yearColor(idx, total)
    // Uniform width — with most years hidden by default, there's no clutter left to
    // de-emphasize older years for; only the color gradient still distinguishes them.
    const width = 2

    // Points placed by actual calendar day on a shared synthetic-year axis
    // (see overlayTimestamp) rather than by array index, so a partial year
    // lands at its real position instead of sliding to the start. Sorted
    // because DJF's Dec points get mapped to the year before Jan/Feb, which
    // is out of order relative to the original Jan→Dec array order. Gap-broken
    // first so a real multi-day data outage within the year renders as a break
    // instead of ECharts bridging it with a straight diagonal.
    const points = breakDataGaps(s.data)
      .map(d => [overlayTimestamp(d.time), d.value] as [number, number | null])
      .sort((a, b) => a[0] - b[0])
    // Years after STATS_BASELINE_MAX_YEAR are excluded from the baseline stats
    // (see below) but still get their own toggleable line.
    if (s.year <= STATS_BASELINE_MAX_YEAR) perYearPoints.push(points)

    return {
      name: String(s.year),
      type: 'line',
      smooth: true,
      symbol: 'none',
      connectNulls: false,
      lineStyle: { width, color },
      itemStyle: { color },
      data: points,
      // Only a handful of years are ever visible at once (the rest are toggled off by
      // default), so there's nothing left to isolate — hovering the legend shouldn't
      // fade the few lines that are already shown.
      legendHoverLink: false,
      emphasis: {
        focus: 'series',
        lineStyle: { width: width + 1.5, opacity: 1, color },
      },
      blur: {
        lineStyle: { opacity: 0.07 },
      },
    }
  })

  // Cross-year mean/min/max/p10/p90 per calendar day — computed once from every year
  // present, regardless of which individual year lines are toggled on in the legend.
  const bandStats = computeYearBandStats(perYearPoints)
  const bandStatsByTs = new Map(bandStats.map(b => [b.ts, b]))

  // Shared between lineStyle/areaStyle and itemStyle below so the legend swatch (which
  // reads itemStyle.color, not lineStyle.color) can't drift out of sync with the actual
  // line/fill color.
  const BAND_FILL_COLOR = 'rgba(200,200,200,0.16)'
  const PCTL_COLOR = 'rgba(220,220,220,0.6)'
  const MEAN_COLOR = '#eaeaea'

  const statsSeries: any[] = [
    {
      name: 'Min-Max Range', type: 'line', stack: 'band', symbol: 'none', silent: true, z: 1,
      lineStyle: { opacity: 0 }, areaStyle: { opacity: 0 }, itemStyle: { color: BAND_FILL_COLOR }, legendHoverLink: false,
      data: bandStats.map(b => [b.ts, b.min]),
    },
    {
      name: 'Min-Max Range', type: 'line', stack: 'band', symbol: 'none', silent: true, z: 1,
      lineStyle: { opacity: 0 }, areaStyle: { color: BAND_FILL_COLOR }, itemStyle: { color: BAND_FILL_COLOR }, legendHoverLink: false,
      data: bandStats.map(b => [b.ts, b.max - b.min]),
    },
    {
      name: 'P10', type: 'line', symbol: 'none', z: 2, legendHoverLink: false,
      lineStyle: { width: 1, type: 'dashed', color: PCTL_COLOR }, itemStyle: { color: PCTL_COLOR },
      data: bandStats.map(b => [b.ts, b.p10]),
    },
    {
      name: 'P90', type: 'line', symbol: 'none', z: 2, legendHoverLink: false,
      lineStyle: { width: 1, type: 'dashed', color: PCTL_COLOR }, itemStyle: { color: PCTL_COLOR },
      data: bandStats.map(b => [b.ts, b.p90]),
    },
    {
      name: 'Mean', type: 'line', symbol: 'none', z: 3, legendHoverLink: false,
      lineStyle: { width: 2.2, color: MEAN_COLOR }, itemStyle: { color: MEAN_COLOR },
      data: bandStats.map(b => [b.ts, b.mean]),
    },
  ]

  // Only the most recent year is overlaid by default; clicking a legend entry
  // adds/removes that year's line (native ECharts legend toggle behavior).
  const defaultYear = series.length ? series[series.length - 1].year : null

  chartInstance.setOption({
    tooltip: {
      trigger: 'axis',
      confine: true,
      formatter: (params: any) => {
        const rawItems = Array.isArray(params) ? params : [params]
        const items = rawItems
          .filter((p: any) => p.value?.[1] != null && p.seriesName !== 'Min-Max Range')
          .sort((a: any, b: any) => (b.value[1] ?? 0) - (a.value[1] ?? 0))
        if (!items.length) return ''
        const ts = items[0].value[0]
        const band = bandStatsByTs.get(ts)
        const header = `<strong>${fmtOverlayDate(ts)}</strong><br/>`
        const rangeLine = band ? `Range: <strong>${band.min.toFixed(3)} – ${band.max.toFixed(3)}</strong><br/>` : ''
        const cols = items.length > 15 ? 3 : items.length > 8 ? 2 : 1
        if (cols === 1) {
          let s = header + rangeLine
          items.forEach((p: any) => {
            s += `${p.marker} ${p.seriesName}: <strong>${Number(p.value[1]).toFixed(3)}</strong><br/>`
          })
          return s
        }
        const rows = Math.ceil(items.length / cols)
        let table = `${header}${rangeLine}<table style="border-collapse:collapse;line-height:1.4;">`
        for (let r = 0; r < rows; r++) {
          table += '<tr>'
          for (let c = 0; c < cols; c++) {
            const p = items[c * rows + r]
            if (p) {
              table += `<td style="padding:0 10px 0 0;white-space:nowrap;">${p.marker} ${p.seriesName}: <strong>${Number(p.value[1]).toFixed(3)}</strong></td>`
            }
          }
          table += '</tr>'
        }
        table += '</table>'
        return table
      }
    },
    legend: {
      top: 4, type: 'scroll', textStyle: { fontSize: 10 },
      // A thin rect reads as a little line swatch — the default line+circle combo icon
      // implied a per-point marker that these lines (all symbol:'none') don't actually have.
      icon: 'rect', itemWidth: 14, itemHeight: 2,
      selected: {
        'Min-Max Range': true, P10: true, P90: true, Mean: true,
        ...Object.fromEntries(series.map(s => [String(s.year), s.year === defaultYear])),
      },
    },
    grid: { left: '3%', right: '2%', bottom: '10%', top: '22%', containLabel: true },
    xAxis: {
      type: 'time',
      boundaryGap: false,
      axisLabel: { rotate: 45, fontSize: 9, color: '#ccc', formatter: (value: number) => fmtOverlayDate(value) },
    },
    yAxis: {
      type: 'value',
      name: `${varName.value} (${primaryStat.value})${varUnit.value ? ` [${varUnit.value}]` : ''}`,
      nameLocation: 'middle', nameGap: 50, axisLabel: { fontSize: 10, color: '#ccc' }, scale: true,
    },
    dataZoom: [{ type: 'inside' }, { type: 'slider', bottom: 4, height: 16 }],
    series: [...statsSeries, ...yearSeries]
  }, true)

  chartInstance.resize()
}

function renderChart(seasonal: SeriesPoint[]) {
  if (!seasonal.length) return
  renderOverlayChart(groupByYear(seasonal))
}

function reRenderChart() {
  if (!rawSeasonalData.value.length || !hasActivePlot.value) return
  nextTick(() => setTimeout(() => renderChart(rawSeasonalData.value), 50))
}

// --- WATCHERS ---
watch(selectedSeason, () => {
  if (!rawAllData.value.length || isGenerating.value) return
  rawSeasonalData.value = filterBySeason(rawAllData.value, selectedSeason.value)
  reRenderChart()
})

// Auto-fetch on point/area, variable, depth, or statistic change — but only while this tab is visible.
// mainStore.unitPreference is included so toggling the display unit re-fetches
// (a cheap cache hit — see useAnalysisFetch.ts/useSensorAnalysisFetch.ts)
// rather than the chart showing stale numbers under the old unit.
watch([lastClicked, variable, depth, queryMode, primaryStat, analysisSource, selectedSensor, () => mainStore.unitPreference[variable.value]], scheduleAutoRun)

// Switching into this tab fetches fresh data for whatever changed while it was hidden.
// immediate: true also covers first mount — the dialog only creates this component once
// opened, so `active` is already true by then and a plain watch would never see it change.
watch(() => props.active, (active: boolean | undefined) => {
  if (active) scheduleAutoRun()
}, { immediate: true })

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
  if (depth.value == null) throw new Error('No depth selected.')

  if (isSensor.value) {
    if (!sensorInfo.value) throw new Error('No sensor selected.')
    const from = `${minYear.value}-01-01T000000`
    const to = `${maxYear.value}-12-31T235959`
    return fetchSensorAnalysisSeries(
      sensorInfo.value.id, variable.value, primaryStat.value as 'min' | 'mean' | 'max', depth.value, from, to,
      isVariableDepth.value ? mainStore.selected_variable.source : null,
    )
  }

  const pt = lastClicked.value
  if (!pt) throw new Error('No location selected. Click on the map first.')
  const location = queryMode.value === 'area'
    ? { polygon: polygonFromClick.value }
    : { lat: pt.lat, lon: pt.lng }
  // Stale responses are discarded by runAnalysis's own requestId guard, not
  // cancellation — fetchAnalysisSeries's cache can be shared across callers,
  // so aborting it here would cancel other callers waiting on the same request.
  return fetchAnalysisSeries(
    { variable: variable.value, stat: primaryStat.value as 'min' | 'mean' | 'max', depth: depth.value, location, yearRange: [minYear.value, maxYear.value] },
  )
}

// --- ACTIONS ---
// Identifies the inputs that should trigger an automatic refetch.
function currentSignature(): string {
  const pt = lastClicked.value
  return JSON.stringify({ source: analysisSource.value, sensorId: selectedSensor.value?.id, lat: pt?.lat, lng: pt?.lng, mode: queryMode.value, variable: variable.value, depth: depth.value, stat: primaryStat.value })
}

function scheduleAutoRun() {
  if (!props.active) return
  if (!variable.value || depth.value == null) return
  if (isSensor.value ? !sensorInfo.value : !lastClicked.value) return
  const sig = currentSignature()
  if (sig === lastFetchSignature) return
  if (autoRunTimer) clearTimeout(autoRunTimer)
  autoRunTimer = setTimeout(runAnalysis, 300)
}

// A hoisted function declaration, not `const runAnalysis = async () =>`: scheduleAutoRun's
// setTimeout(runAnalysis, ...) can now fire from the immediate:true watcher above during
// setup, before a const this far down the file would be initialized (TDZ).
async function runAnalysis() {
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
</style>
