<template>
  <div class="flex h-full" style="overflow:hidden;">
    <div class="p-2 flex flex-col shrink-0" style="width:200px; overflow-y:auto; border-right:1px solid rgba(255,255,255,0.08);">
      <div class="ctrl-label">Threshold Mode</div>
      <SegmentedControl v-model="thresholdMode" :items="thresholdModeItems" size="sm" block
        class="mb-3" aria-label="Threshold mode" />

      <div class="ctrl-label">Direction</div>
      <SegmentedControl v-model="direction" :items="directionItems" size="sm" block
        class="mb-3" aria-label="Event direction" />

      <template v-if="thresholdMode === 'percentile'">
        <div class="ctrl-label">Baseline window (± days)</div>
        <UInput v-model.number="windowDays" type="number" class="mb-3" min="1" max="30" />
      </template>
      <template v-else>
        <div class="ctrl-label">Fixed threshold</div>
        <UInput v-model.number="fixedThreshold" type="number" class="mb-3" />
      </template>

      <div class="ctrl-label">Min duration (days)</div>
      <UInput v-model.number="minDurationDays" type="number" class="mb-3" min="1" />

      <div class="ctrl-label">Max merge gap (days)</div>
      <UInput v-model.number="maxGapDays" type="number" class="mb-3" min="0" />

      <div class="text-gray-500 mt-2">
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

      <UAlert
        color="warning"
        variant="subtle"
        icon="i-mdi-alert-outline"
        class="mt-3"
        v-if="thresholdMode === 'percentile' && isShortHistory"
        :description="shortHistoryWarning"
      />
    </div>

    <div class="grow flex flex-col" style="min-width:0;">
      <div ref="chartContainerRef" class="w-full" style="height:55%; flex-shrink:0;" />
      <div class="grow flex" style="min-height:0; overflow:hidden;">
        <div class="grow p-2" style="overflow-y:auto;">
          <div class="ctrl-label mb-1">Events ({{ events.length }})</div>
          <UTable v-model:sorting="sorting1" :columns="eventHeaders" :data="eventRows" class="stats-table" />
        </div>
        <div style="width:260px; border-left:1px solid rgba(255,255,255,0.08); overflow-y:auto;" class="p-2 shrink-0">
          <div class="ctrl-label mb-1">Per-year summary</div>
          <UTable v-model:sorting="sorting2" :columns="yearHeaders" :data="yearRows" class="stats-table" />
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from 'vue'
import * as echarts from 'echarts'
import { registerEchartsDarkTheme } from '~~/composables/useEchartsTheme'
import { useVariableRegistry } from '~~/composables/useVariableRegistry'
import type { SeriesPoint } from '~~/composables/useAnalysisFetch'
import SegmentedControl from '../ui/SegmentedControl.vue'
import { csvMeta, useCsvExport, type CsvDataset } from '~~/composables/useCsvExport'
import { useViewState, useChartZoom } from '~~/composables/useViewState'
import {

  filterBySeason, maskBySeason, breakDataGaps, computeClimatologyBaseline, detectExtremeEvents, summarizeEventsByYear,
  distinctYearSpan, climatologyThresholdLookup, fixedThresholdLookup, type ThresholdLookup,
} from '~~/composables/useAnalysisStatistics'

const thresholdModeItems = [{ value: 'percentile', label: 'Percentile' }, { value: 'fixed', label: 'Fixed value' }]
const directionItems = [{ value: 'above', label: 'Above (high)' }, { value: 'below', label: 'Below (low)' }]

const VIEW_SCOPE = 'analysis.extremes'

const props = defineProps<{ series: SeriesPoint[]; season: string; variable: string }>()

const { displayUnit } = useVariableRegistry()
const unit = computed(() => displayUnit(props.variable))
const unitSuffix = computed(() => unit.value ? ` (${unit.value})` : '')

const yearSpan = computed(() => distinctYearSpan(props.series))
const isShortHistory = computed(() => yearSpan.value < 2)
const shortHistoryWarning = computed(() =>
  `Only ${yearSpan.value} year${yearSpan.value === 1 ? '' : 's'} of data available. The dashed threshold isn't a `
  + `true multi-year climatology here — it's a local ±${windowDays.value}-day rolling percentile of this same `
  + `record, so it will track short-term swings rather than a stable "normal for this time of year."`)

// Every control here is store-backed so a shared link reopens this tab with the
// sender's parameters, not the defaults — see composables/useViewState.ts.
const field = useViewState(VIEW_SCOPE)

// Direction defaults: low extremes matter for OA-relevant variables, high extremes for temperature.
const LOW_EXTREME_VARS = new Set(['ph_total', 'omega_arag', 'omega_cal', 'dissolved_oxygen'])
const direction = field<'above' | 'below'>('direction', LOW_EXTREME_VARS.has(props.variable) ? 'below' : 'above')

const thresholdMode = field<'percentile' | 'fixed'>('thresholdMode', 'percentile')
const fixedThreshold = field('fixedThreshold', 0)
const windowDays = field('windowDays', 5)
const minDurationDays = field('minDurationDays', 5)
const maxGapDays = field('maxGapDays', 2)

const climatology = computed(() => computeClimatologyBaseline(props.series, windowDays.value))
const thresholdLookup = computed<ThresholdLookup>(() => thresholdMode.value === 'percentile'
  ? climatologyThresholdLookup(climatology.value, direction.value)
  : fixedThresholdLookup(fixedThreshold.value))
const seasonalSeries = computed(() => filterBySeason(props.series, props.season))
const events = computed(() => detectExtremeEvents(seasonalSeries.value, thresholdLookup.value, direction.value, minDurationDays.value, maxGapDays.value))
const yearlySummary = computed(() => summarizeEventsByYear(events.value))

// v-data-table sorted by default; TanStack's table needs the initial state given explicitly.
const sorting1 = field('sorting1', [{ id: 'peakAnomaly', desc: true }])
// v-data-table sorted by default; TanStack's table needs the initial state given explicitly.
const sorting2 = field('sorting2', [{ id: 'year', desc: true }])

const eventHeaders = computed(() => [
  { header: 'Start', accessorKey: 'startTime' },
  { header: 'End', accessorKey: 'endTime' },
  { header: 'Days', accessorKey: 'durationDays' },
  { header: `Peak${unitSuffix.value}`, accessorKey: 'peakValue' },
  { header: `Peak Δ${unitSuffix.value}`, accessorKey: 'peakAnomaly' },
])
const eventRows = computed(() => events.value.map(e => ({
  startTime: e.startTime, endTime: e.endTime, durationDays: e.durationDays,
  peakValue: e.peakValue.toFixed(3), peakAnomaly: e.peakAnomaly.toFixed(3),
})))

const yearHeaders = computed(() => [
  { header: 'Year', accessorKey: 'year' },
  { header: 'Events', accessorKey: 'eventCount' },
  { header: 'Days', accessorKey: 'totalEventDays' },
  { header: `Max Δ${unitSuffix.value}`, accessorKey: 'maxIntensity' },
])
const yearRows = computed(() => yearlySummary.value.map(y => ({
  year: y.year, eventCount: y.eventCount, totalEventDays: y.totalEventDays, maxIntensity: y.maxIntensity.toFixed(3),
})))

// ── CSV EXPORT ──────────────────────────────────────────────────────────────
// Built from `events`/`yearlySummary` rather than the table rows above: those
// carry `.toFixed(3)` strings for display, and that rounding must not be what
// lands in a file someone re-analyses. `meanIntensity` rides along too — it's
// already computed and useful, it just has no column to spare on screen.
const csv = useCsvExport()

const csvParams = computed(() => thresholdMode.value === 'percentile'
  ? [
      ['threshold', `${direction.value === 'above' ? '90th' : '10th'} percentile of a day-of-year climatology`] as [string, unknown],
      ['baseline_window_days', `±${windowDays.value}`] as [string, unknown],
    ]
  : [['threshold', `fixed ${direction.value === 'above' ? '>' : '<'} ${fixedThreshold.value}${unitSuffix.value}`] as [string, unknown]])

const csvCommonMeta = computed(() => [
  ...csvParams.value,
  ['direction', direction.value] as [string, unknown],
  ['min_duration_days', minDurationDays.value] as [string, unknown],
  ['max_merge_gap_days', maxGapDays.value] as [string, unknown],
])

if (csv) csv.register((): CsvDataset[] => [
  {
    label: 'Extreme events',
    slug: 'extreme-events',
    columns: [
      { header: 'start_time', accessorKey: 'startTime' },
      { header: 'end_time', accessorKey: 'endTime' },
      { header: 'duration_days', accessorKey: 'durationDays' },
      { header: `peak_value${unitSuffix.value}`, accessorKey: 'peakValue' },
      { header: `peak_anomaly${unitSuffix.value}`, accessorKey: 'peakAnomaly' },
      { header: `mean_intensity${unitSuffix.value}`, accessorKey: 'meanIntensity' },
    ],
    rows: events.value as unknown as Record<string, unknown>[],
    meta: csvMeta(csv.context.value, csvCommonMeta.value),
  },
  {
    label: 'Per-year summary',
    slug: 'extreme-events-by-year',
    columns: [
      { header: 'year', accessorKey: 'year' },
      { header: 'event_count', accessorKey: 'eventCount' },
      { header: 'total_event_days', accessorKey: 'totalEventDays' },
      { header: `mean_intensity${unitSuffix.value}`, accessorKey: 'meanIntensity' },
      { header: `max_intensity${unitSuffix.value}`, accessorKey: 'maxIntensity' },
    ],
    rows: yearlySummary.value as unknown as Record<string, unknown>[],
    meta: csvMeta(csv.context.value, [
      ...csvCommonMeta.value,
      ['note', 'events are attributed to the year they start in'],
    ]),
  },
])

// --- CHART ---
const chartContainerRef = ref<HTMLDivElement | null>(null)
let chartInstance: echarts.ECharts | null = null
let resizeObserver: ResizeObserver | null = null
// Preserved across re-renders so changing a param on the left doesn't reset the
// user's zoom — and stored, so a shared link reopens at the same zoom.
const zoom = useChartZoom(VIEW_SCOPE)

function render() {
  if (!chartContainerRef.value) return
  registerEchartsDarkTheme()
  if (!chartInstance) {
    chartInstance = echarts.init(chartContainerRef.value, 'dark', { renderer: 'canvas' })
    zoom.track(chartInstance)
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
    yAxis: {
      type: 'value', name: unit.value, nameLocation: 'middle', nameGap: 38,
      axisLabel: { fontSize: 10, color: '#ccc' }, scale: true,
    },
    dataZoom: [
      { type: 'inside', ...zoom.current() },
      { type: 'slider', bottom: 4, height: 14, ...zoom.current() },
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

.stats-table :deep(td),
.stats-table :deep(th) {
  font-size: 0.72rem !important;
  padding: 2px 6px !important;
}

:deep(.v-text-field) { flex: none; }
</style>
