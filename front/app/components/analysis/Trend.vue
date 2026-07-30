<template>
  <div class="d-flex h-100" style="overflow:hidden;">
    <div class="pa-3 d-flex flex-column flex-shrink-0" style="width:240px; overflow-y:auto; border-right:1px solid rgba(255,255,255,0.08);">
      <div class="ctrl-label">Theil-Sen slope</div>
      <div class="text-h6 mb-3">{{ result ? result.theilSen.slope.toFixed(4) : '—' }} <span class="text-caption text-grey">/ year</span></div>

      <div class="ctrl-label">Mann-Kendall significance</div>
      <div class="text-body-2 mb-1">
        <v-chip size="small" :color="trendColor" variant="tonal">{{ result?.mk.trend ?? '—' }}</v-chip>
      </div>
      <div class="text-caption text-grey mb-3">p = {{ result ? result.mk.p.toFixed(4) : '—' }} (α = 0.05)</div>

      <div class="ctrl-label">Detail</div>
      <div class="text-caption text-grey">S = {{ result?.mk.S ?? '—' }}</div>
      <div class="text-caption text-grey">Z = {{ result ? result.mk.Z.toFixed(3) : '—' }}</div>
      <div class="text-caption text-grey mb-3">n (years) = {{ annualMeans.length }}</div>

      <div class="text-caption text-grey mt-2">
        Computed on season-filtered annual means (not raw daily values) — Mann-Kendall tests for a
        monotonic trend, Theil-Sen estimates its robust magnitude.
      </div>
    </div>

    <div class="flex-grow-1" style="min-width:0;">
      <div v-if="annualMeans.length < 3" class="d-flex align-center justify-center h-100 text-caption text-grey">
        Not enough years of data to compute a trend.
      </div>
      <!-- Stays mounted via v-show (not v-else) once shown once, so the ECharts instance
           never gets orphaned by a destroyed/recreated container — see the same bug fixed
           in CompoundStress.vue and Correlation.vue. -->
      <div v-show="annualMeans.length >= 3" ref="chartContainerRef" class="w-100 h-100" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from 'vue'
import * as echarts from 'echarts'
import { registerEchartsDarkTheme } from '../../../composables/useEchartsTheme'
import type { SeriesPoint } from '../../../composables/useAnalysisFetch'
import { filterBySeason, groupByYear, mannKendallTest, theilSenSlope } from '../../../composables/useAnalysisStatistics'

const props = defineProps<{ series: SeriesPoint[]; season: string }>()

const annualMeans = computed(() => {
  const seasonal = filterBySeason(props.series, props.season)
  return groupByYear(seasonal).map(({ year, data }) => {
    const vals = data.filter(d => d.value != null).map(d => d.value as number)
    return vals.length ? { year, mean: vals.reduce((s, v) => s + v, 0) / vals.length } : null
  }).filter((r): r is { year: number; mean: number } => r != null)
})

const result = computed(() => {
  if (annualMeans.value.length < 3) return null
  const mk = mannKendallTest(annualMeans.value.map(r => r.mean))
  const theilSen = theilSenSlope(annualMeans.value.map(r => ({ x: r.year, y: r.mean })))
  return { mk, theilSen }
})

const trendColor = computed(() => {
  const t = result.value?.mk.trend
  if (t === 'increasing') return 'red-lighten-2'
  if (t === 'decreasing') return 'blue-lighten-2'
  return 'grey'
})

const chartContainerRef = ref<HTMLDivElement | null>(null)
let chartInstance: echarts.ECharts | null = null
let resizeObserver: ResizeObserver | null = null

function render() {
  if (!chartContainerRef.value || !result.value) return
  registerEchartsDarkTheme()
  if (!chartInstance) chartInstance = echarts.init(chartContainerRef.value, 'dark', { renderer: 'canvas' })

  const years = annualMeans.value.map(r => r.year)
  const means = annualMeans.value.map(r => r.mean)
  const { slope, intercept } = result.value.theilSen
  const trendLine = years.map(y => slope * y + intercept)

  chartInstance.setOption({
    tooltip: { trigger: 'axis' },
    legend: { data: ['Annual Mean', 'Theil-Sen Trend'], top: 4, textStyle: { fontSize: 10 } },
    grid: { left: '4%', right: '3%', bottom: '8%', top: '18%', containLabel: true },
    xAxis: { type: 'category', data: years, axisLabel: { fontSize: 9, color: '#ccc' } },
    yAxis: { type: 'value', axisLabel: { fontSize: 10, color: '#ccc' }, scale: true },
    series: [
      { name: 'Annual Mean', type: 'line', data: means, symbol: 'circle', symbolSize: 6, itemStyle: { color: '#58d9f9' }, lineStyle: { color: '#58d9f9' } },
      { name: 'Theil-Sen Trend', type: 'line', data: trendLine, symbol: 'none', lineStyle: { color: '#ff9800', type: 'dashed', width: 1.5 } },
    ],
  }, true)
  chartInstance.resize()
}

watch(result, () => nextTick(render))

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
