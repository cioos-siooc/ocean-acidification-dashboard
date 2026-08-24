<template>
  <div class="flex h-full" style="overflow:hidden;">
    <div class="p-2 flex flex-col shrink-0" style="width:200px; overflow-y:auto; border-right:1px solid rgba(255,255,255,0.08);">
      <div class="ctrl-label">Baseline window (± days)</div>
      <UInput v-model.number="windowDays" type="number" class="mb-3" min="1" max="30" />
      <div class="text-gray-500">
        Each year's daily values are plotted as their deviation from a day-of-year climatological mean
        (pooled across all years, smoothed over the baseline window), so it's easy to see which years ran
        warmer/cooler, more/less acidic, etc. relative to the long-term average for that calendar day.
      </div>

      <UAlert color="warning" variant="subtle" icon="i-mdi-alert-outline" class="mt-3" v-if="isShortHistory">
        Only {{ yearSpan }} year{{ yearSpan === 1 ? '' : 's' }} of data available. The "mean" here isn't a true
        multi-year climatology — it's a local ±{{ windowDays }}-day rolling average of this same record, so
        anomalies reflect short-term swings rather than deviation from a stable long-term normal.
      </UAlert>
    </div>

    <div class="grow" style="min-width:0;">
      <div ref="chartContainerRef" class="w-full h-full" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from 'vue'
import * as echarts from 'echarts'
import { registerEchartsDarkTheme } from '~~/composables/useEchartsTheme'
import { useVariableRegistry } from '~~/composables/useVariableRegistry'
import type { SeriesPoint } from '~~/composables/useAnalysisFetch'
import {
  filterBySeason, groupByYear, breakDataGaps, computeClimatologyBaseline, climatologyForDate, yearColor,
  distinctYearSpan, attachStickyLegendHighlight,
} from '~~/composables/useAnalysisStatistics'
import { csvMeta, useCsvExport, type CsvDataset } from '~~/composables/useCsvExport'

const props = defineProps<{ series: SeriesPoint[]; season: string; variable?: string }>()

const { displayUnit } = useVariableRegistry()
const unit = computed(() => props.variable ? displayUnit(props.variable) : '')

const windowDays = ref(5)
const climatology = computed(() => computeClimatologyBaseline(props.series, windowDays.value))
const yearSpan = computed(() => distinctYearSpan(props.series))
const isShortHistory = computed(() => yearSpan.value < 2)
const seasonalSeries = computed(() => filterBySeason(props.series, props.season))

// Same synthetic-year-axis trick as the basic tab's overlay chart, so partial years
// and DJF's Dec->Jan->Feb ordering display correctly.
const REF_YEAR = 2000
function overlayTimestamp(iso: string, season: string): number {
  const month = parseInt(iso.slice(5, 7), 10)
  const day = parseInt(iso.slice(8, 10), 10)
  const year = (season === 'djf' && month === 12) ? REF_YEAR - 1 : REF_YEAR
  return Date.UTC(year, month - 1, day)
}

const anomalySeries = computed(() => {
  const climByDoy = new Map(climatology.value.map(c => [c.doy, c]))
  return groupByYear(seasonalSeries.value).map(({ year, data }) => {
    // Gap-broken (not compacted) so a real multi-day data outage within the year renders
    // as a break instead of ECharts bridging it with a straight diagonal.
    const points = breakDataGaps(data).map(d => {
      const x = overlayTimestamp(d.time, props.season)
      if (d.value == null) return [x, null] as [number, number | null]
      const clim = climatologyForDate(climByDoy, d.time)
      if (!clim || Number.isNaN(clim.mean)) return [x, null] as [number, number | null]
      return [x, (d.value as number) - clim.mean] as [number, number | null]
    })
    return { year, points: points.sort((a, b) => a[0] - b[0]) }
  }).filter(s => s.points.some(p => p[1] != null))
})

// ── CSV EXPORT ──────────────────────────────────────────────────────────────
// The chart plots one line per year against a synthetic reference year, which is
// a display trick — the file keeps the real dates and carries the climatological
// mean each anomaly was measured against, so the subtraction is checkable.
const csv = useCsvExport()

const csvAnomalyRows = computed(() => {
  const climByDoy = new Map(climatology.value.map(c => [c.doy, c]))
  const out: Record<string, unknown>[] = []
  for (const d of seasonalSeries.value) {
    if (d.value == null) continue
    const clim = climatologyForDate(climByDoy, d.time)
    if (!clim || Number.isNaN(clim.mean)) continue
    out.push({
      time: d.time,
      year: Number(d.time.slice(0, 4)),
      value: d.value,
      climatology_mean: clim.mean,
      anomaly: (d.value as number) - clim.mean,
    })
  }
  return out
})

if (csv) csv.register((): CsvDataset[] => {
  if (!csvAnomalyRows.value.length) return []
  const u = unit.value ? ` (${unit.value})` : ''
  const meta = csvMeta(csv.context.value, [
    ['baseline_window_days', `±${windowDays.value}`],
    ...(isShortHistory.value
      ? [['caveat', `only ${yearSpan.value} year(s) of data — the baseline is a local rolling mean, not a stable climatology`] as [string, unknown]]
      : []),
  ])
  return [
    {
      label: 'Anomalies (daily)',
      slug: 'climatology-anomalies',
      columns: [
        { header: 'time', accessorKey: 'time' },
        { header: 'year', accessorKey: 'year' },
        { header: `value${u}`, accessorKey: 'value' },
        { header: `climatology_mean${u}`, accessorKey: 'climatology_mean' },
        { header: `anomaly${u}`, accessorKey: 'anomaly' },
      ],
      rows: csvAnomalyRows.value,
      meta,
    },
    {
      label: 'Day-of-year climatology',
      slug: 'climatology-baseline',
      columns: [
        { header: 'day_of_year', accessorKey: 'doy' },
        { header: `mean${u}`, accessorKey: 'mean' },
        { header: `std${u}`, accessorKey: 'std' },
        { header: `p10${u}`, accessorKey: 'p10' },
        { header: `p90${u}`, accessorKey: 'p90' },
        { header: 'n_observations', accessorKey: 'n' },
      ],
      rows: climatology.value as unknown as Record<string, unknown>[],
      meta,
    },
  ]
})

const chartContainerRef = ref<HTMLDivElement | null>(null)
let chartInstance: echarts.ECharts | null = null
let resizeObserver: ResizeObserver | null = null

function render() {
  if (!chartContainerRef.value) return
  registerEchartsDarkTheme()
  if (!chartInstance) {
    chartInstance = echarts.init(chartContainerRef.value, 'dark', { renderer: 'canvas' })
    attachStickyLegendHighlight(chartInstance)
  }

  const total = anomalySeries.value.length
  const series = anomalySeries.value.map((s, idx) => ({
    name: String(s.year),
    type: 'line',
    smooth: true,
    symbol: 'none',
    connectNulls: false,
    lineStyle: { width: 1 + (total <= 1 ? 1 : idx / (total - 1)) * 1.5, color: yearColor(idx, total) },
    itemStyle: { color: yearColor(idx, total) },
    data: s.points,
    // Disables the legend's own mouseover/mouseout highlight so it doesn't fight with
    // attachStickyLegendHighlight's click-driven, persistent highlight below.
    legendHoverLink: false,
    emphasis: { focus: 'series' },
  }))
  if (series.length) (series[0] as any).markLine = { silent: true, symbol: 'none', lineStyle: { color: '#fff', opacity: 0.4, type: 'solid', width: 1 }, data: [{ yAxis: 0 }] }

  chartInstance.setOption({
    tooltip: { trigger: 'axis' },
    legend: { top: 4, type: 'scroll', textStyle: { fontSize: 10 } },
    grid: { left: '4%', right: '3%', bottom: '12%', top: '18%', containLabel: true },
    xAxis: { type: 'time', axisLabel: { fontSize: 9, color: '#ccc' } },
    yAxis: {
      type: 'value', name: unit.value ? `Anomaly (${unit.value})` : 'Anomaly',
      nameLocation: 'middle', nameGap: 40, axisLabel: { fontSize: 10, color: '#ccc' }, scale: true,
    },
    dataZoom: [{ type: 'inside' }, { type: 'slider', bottom: 4, height: 14 }],
    series,
  }, true)
  chartInstance.resize()
}

watch(anomalySeries, () => nextTick(render))

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
</style>
