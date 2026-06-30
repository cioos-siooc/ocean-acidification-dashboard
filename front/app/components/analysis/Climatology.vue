<template>
  <div class="d-flex h-100" style="overflow:hidden;">
    <div class="pa-2 d-flex flex-column flex-shrink-0" style="width:200px; overflow-y:auto; border-right:1px solid rgba(255,255,255,0.08);">
      <div class="ctrl-label">Baseline window (± days)</div>
      <v-text-field v-model.number="windowDays" type="number" variant="outlined" density="compact" hide-details class="mb-3" min="1" max="30" />
      <div class="text-caption text-grey">
        Each year's daily values are plotted as their deviation from a day-of-year climatological mean
        (pooled across all years, smoothed over the baseline window), so it's easy to see which years ran
        warmer/cooler, more/less acidic, etc. relative to the long-term average for that calendar day.
      </div>
    </div>

    <div class="flex-grow-1" style="min-width:0;">
      <div ref="chartContainerRef" class="w-100 h-100" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from 'vue'
import * as echarts from 'echarts'
import { registerEchartsDarkTheme } from '../../../composables/useEchartsTheme'
import type { SeriesPoint } from '../../../composables/useAnalysisFetch'
import { filterBySeason, groupByYear, computeClimatologyBaseline, climatologyForDate, yearColor } from '../../../composables/useAnalysisStatistics'

const props = defineProps<{ series: SeriesPoint[]; season: string }>()

const windowDays = ref(5)
const climatology = computed(() => computeClimatologyBaseline(props.series, windowDays.value))
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
    const points = data
      .filter(d => d.value != null)
      .map(d => {
        const clim = climatologyForDate(climByDoy, d.time)
        if (!clim || Number.isNaN(clim.mean)) return null
        return [overlayTimestamp(d.time, props.season), (d.value as number) - clim.mean] as [number, number]
      })
      .filter(Boolean) as [number, number][]
    return { year, points: points.sort((a, b) => a[0] - b[0]) }
  }).filter(s => s.points.length > 0)
})

const chartContainerRef = ref<HTMLDivElement | null>(null)
let chartInstance: echarts.ECharts | null = null
let resizeObserver: ResizeObserver | null = null

function render() {
  if (!chartContainerRef.value) return
  registerEchartsDarkTheme()
  if (!chartInstance) chartInstance = echarts.init(chartContainerRef.value, 'dark', { renderer: 'canvas' })

  const total = anomalySeries.value.length
  const series = anomalySeries.value.map((s, idx) => ({
    name: String(s.year),
    type: 'line',
    smooth: true,
    symbol: 'none',
    lineStyle: { width: 1 + (total <= 1 ? 1 : idx / (total - 1)) * 1.5, color: yearColor(idx, total) },
    itemStyle: { color: yearColor(idx, total) },
    data: s.points,
    emphasis: { focus: 'series' },
  }))
  if (series.length) (series[0] as any).markLine = { silent: true, symbol: 'none', lineStyle: { color: '#fff', opacity: 0.4, type: 'solid', width: 1 }, data: [{ yAxis: 0 }] }

  chartInstance.setOption({
    tooltip: { trigger: 'axis' },
    legend: { top: 4, type: 'scroll', textStyle: { fontSize: 10 } },
    grid: { left: '4%', right: '3%', bottom: '12%', top: '18%', containLabel: true },
    xAxis: { type: 'time', axisLabel: { fontSize: 9, color: '#ccc' } },
    yAxis: { type: 'value', name: 'Anomaly', nameLocation: 'middle', nameGap: 40, axisLabel: { fontSize: 10, color: '#ccc' }, scale: true },
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
