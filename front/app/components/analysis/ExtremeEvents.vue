<template>
  <div class="d-flex h-100" style="overflow:hidden;">
    <div class="pa-2 d-flex flex-column flex-shrink-0" style="width:200px; overflow-y:auto; border-right:1px solid rgba(255,255,255,0.08);">
      <div class="ctrl-label">Threshold Mode</div>
      <v-btn-toggle v-model="thresholdMode" mandatory variant="tonal" class="w-100 mb-3">
        <v-btn value="percentile" size="small">Percentile</v-btn>
        <v-btn value="fixed" size="small">Fixed value</v-btn>
      </v-btn-toggle>

      <div class="ctrl-label">Direction</div>
      <v-btn-toggle v-model="direction" mandatory variant="tonal" class="w-100 mb-3">
        <v-btn value="above" size="small">Above (high)</v-btn>
        <v-btn value="below" size="small">Below (low)</v-btn>
      </v-btn-toggle>

      <template v-if="thresholdMode === 'percentile'">
        <div class="ctrl-label">Baseline window (± days)</div>
        <v-text-field v-model.number="windowDays" type="number" hide-details class="mb-3" min="1" max="30" />
      </template>
      <template v-else>
        <div class="ctrl-label">Fixed threshold</div>
        <v-text-field v-model.number="fixedThreshold" type="number" hide-details class="mb-3" />
      </template>

      <div class="ctrl-label">Min duration (days)</div>
      <v-text-field v-model.number="minDurationDays" type="number" hide-details class="mb-3" min="1" />

      <div class="ctrl-label">Max merge gap (days)</div>
      <v-text-field v-model.number="maxGapDays" type="number" hide-details class="mb-3" min="0" />

      <div class="text-caption text-grey mt-2">
        <template v-if="thresholdMode === 'percentile'">
          Flags days crossing the {{ direction === 'above' ? '90th' : '10th' }} percentile of a day-of-year
          climatological baseline (marine-heatwave-style methodology), merges nearby runs, and keeps events
          lasting at least the minimum duration.
        </template>
        <template v-else>
          Flags days {{ direction === 'above' ? 'above' : 'below' }} the fixed value of {{ fixedThreshold }},
          merges nearby runs, and keeps events lasting at least the minimum duration.
        </template>
      </div>

      <v-alert v-if="thresholdMode === 'percentile' && isShortHistory" type="warning" variant="tonal" icon="mdi-alert-outline"
        class="mt-3 text-caption">
        Only {{ yearSpan }} year{{ yearSpan === 1 ? '' : 's' }} of data available. The dashed threshold isn't a
        true multi-year climatology here — it's a local ±{{ windowDays }}-day rolling percentile of this same
        record, so it will track short-term swings rather than a stable "normal for this time of year."
      </v-alert>
    </div>

    <div class="flex-grow-1 d-flex flex-column" style="min-width:0;">
      <div ref="chartContainerRef" class="w-100" style="height:55%; flex-shrink:0;" />
      <div class="flex-grow-1 d-flex" style="min-height:0; overflow:hidden;">
        <div class="flex-grow-1 pa-2" style="overflow-y:auto;">
          <div class="ctrl-label mb-1">Events ({{ events.length }})</div>
          <v-data-table :headers="eventHeaders" :items="eventRows" hide-default-footer
            :items-per-page="-1" :sort-by="[{ key: 'peakAnomaly', order: 'desc' as const }]" class="stats-table" />
        </div>
        <div style="width:260px; border-left:1px solid rgba(255,255,255,0.08); overflow-y:auto;" class="pa-2 flex-shrink-0">
          <div class="ctrl-label mb-1">Per-year summary</div>
          <v-data-table :headers="yearHeaders" :items="yearRows" hide-default-footer
            :items-per-page="-1" :sort-by="[{ key: 'year', order: 'desc' as const }]" class="stats-table" />
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from 'vue'
import * as echarts from 'echarts'
import { registerEchartsDarkTheme } from '../../../composables/useEchartsTheme'
import type { SeriesPoint } from '../../../composables/useAnalysisFetch'
import {
  filterBySeason, maskBySeason, breakDataGaps, computeClimatologyBaseline, detectExtremeEvents, summarizeEventsByYear,
  distinctYearSpan, climatologyThresholdLookup, fixedThresholdLookup, type ThresholdLookup,
} from '../../../composables/useAnalysisStatistics'

const props = defineProps<{ series: SeriesPoint[]; season: string; variable: string }>()

const yearSpan = computed(() => distinctYearSpan(props.series))
const isShortHistory = computed(() => yearSpan.value < 2)

// Direction defaults: low extremes matter for OA-relevant variables, high extremes for temperature.
const LOW_EXTREME_VARS = new Set(['ph_total', 'omega_arag', 'omega_cal', 'dissolved_oxygen'])
const direction = ref<'above' | 'below'>(LOW_EXTREME_VARS.has(props.variable) ? 'below' : 'above')

const thresholdMode = ref<'percentile' | 'fixed'>('percentile')
const fixedThreshold = ref(0)
const windowDays = ref(5)
const minDurationDays = ref(5)
const maxGapDays = ref(2)

const climatology = computed(() => computeClimatologyBaseline(props.series, windowDays.value))
const thresholdLookup = computed<ThresholdLookup>(() => thresholdMode.value === 'percentile'
  ? climatologyThresholdLookup(climatology.value, direction.value)
  : fixedThresholdLookup(fixedThreshold.value))
const seasonalSeries = computed(() => filterBySeason(props.series, props.season))
const events = computed(() => detectExtremeEvents(seasonalSeries.value, thresholdLookup.value, direction.value, minDurationDays.value, maxGapDays.value))
const yearlySummary = computed(() => summarizeEventsByYear(events.value))

const eventHeaders = [
  { title: 'Start', key: 'startTime' },
  { title: 'End', key: 'endTime' },
  { title: 'Days', key: 'durationDays' },
  { title: 'Peak', key: 'peakValue' },
  { title: 'Peak Δ', key: 'peakAnomaly' },
]
const eventRows = computed(() => events.value.map(e => ({
  startTime: e.startTime, endTime: e.endTime, durationDays: e.durationDays,
  peakValue: e.peakValue.toFixed(3), peakAnomaly: e.peakAnomaly.toFixed(3),
})))

const yearHeaders = [
  { title: 'Year', key: 'year' },
  { title: 'Events', key: 'eventCount' },
  { title: 'Days', key: 'totalEventDays' },
  { title: 'Max Δ', key: 'maxIntensity' },
]
const yearRows = computed(() => yearlySummary.value.map(y => ({
  year: y.year, eventCount: y.eventCount, totalEventDays: y.totalEventDays, maxIntensity: y.maxIntensity.toFixed(3),
})))

// --- CHART ---
const chartContainerRef = ref<HTMLDivElement | null>(null)
let chartInstance: echarts.ECharts | null = null
let resizeObserver: ResizeObserver | null = null
// Preserved across re-renders so changing a param on the left doesn't reset the user's zoom.
let zoomRange = { start: 0, end: 100 }

function render() {
  if (!chartContainerRef.value) return
  registerEchartsDarkTheme()
  if (!chartInstance) {
    chartInstance = echarts.init(chartContainerRef.value, 'dark', { renderer: 'canvas' })
    chartInstance.on('datazoom', () => {
      const dz = (chartInstance!.getOption().dataZoom as any[]) || []
      if (dz[0]) zoomRange = { start: dz[0].start, end: dz[0].end }
    })
  }

  // Masked (not compacted) so off-season months render as a real gap in the line
  // instead of a straight diagonal connecting e.g. last August to next March. Gap-broken
  // first so a genuine multi-day data outage (common in sensor telemetry) also renders
  // as a break instead of bridging straight across, on both the value and threshold lines.
  const maskedSeries = maskBySeason(breakDataGaps(props.series), props.season)
  const valuePoints = maskedSeries.map(d => [d.time, d.value] as [string, number | null])
  const baselinePoints = maskedSeries
    .map(d => {
      if (d.value == null) return [d.time, null] as [string, number | null]
      const t = thresholdLookup.value(d.time)
      return [d.time, t ? t.threshold : null] as [string, number | null]
    })

  const markAreaData = events.value.map(e => [{ xAxis: e.startTime }, { xAxis: e.endTime }])

  chartInstance.setOption({
    tooltip: { trigger: 'axis' },
    grid: { left: '4%', right: '3%', bottom: '12%', top: '8%', containLabel: true },
    xAxis: { type: 'time', axisLabel: { fontSize: 9, color: '#ccc' } },
    yAxis: { type: 'value', axisLabel: { fontSize: 10, color: '#ccc' }, scale: true },
    dataZoom: [
      { type: 'inside', start: zoomRange.start, end: zoomRange.end },
      { type: 'slider', bottom: 4, height: 14, start: zoomRange.start, end: zoomRange.end },
    ],
    series: [
      {
        name: 'Value', type: 'line', showSymbol: false, data: valuePoints, connectNulls: false,
        lineStyle: { width: 1.2, color: '#58d9f9' }, itemStyle: { color: '#58d9f9' },
        markArea: { itemStyle: { color: direction.value === 'above' ? 'rgba(255,110,118,0.18)' : 'rgba(73,146,255,0.18)' }, data: markAreaData },
      },
      { name: 'Threshold', type: 'line', showSymbol: false, data: baselinePoints, connectNulls: false, lineStyle: { width: 1, color: '#ff9800', type: 'dashed' } },
    ],
  }, true)
  chartInstance.resize()
}

watch([events, direction, thresholdMode, fixedThreshold], () => nextTick(render))

onMounted(() => {
  registerEchartsDarkTheme()
  nextTick(render)
  if (chartContainerRef.value && typeof ResizeObserver !== 'undefined') {
    resizeObserver = new ResizeObserver(() => chartInstance?.resize())
    resizeObserver.observe(chartContainerRef.value)
  }
})

onBeforeUnmount(() => {
  resizeObserver?.disconnect()
  chartInstance?.dispose()
  chartInstance = null
})
</script>

<style scoped>
.ctrl-label {
  font-size: 0.63rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: rgba(255, 255, 255, 0.38);
  margin-bottom: 4px;
}

.stats-table :deep(.v-data-table__td),
.stats-table :deep(.v-data-table__th) {
  font-size: 0.72rem !important;
  padding: 2px 6px !important;
}

:deep(.v-text-field) { flex: none; }
</style>
