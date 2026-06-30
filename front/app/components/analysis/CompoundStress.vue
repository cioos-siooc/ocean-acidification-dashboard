<template>
  <div class="d-flex h-100" style="overflow:hidden;">
    <div class="pa-2 d-flex flex-column flex-shrink-0" style="width:220px; overflow-y:auto; border-right:1px solid rgba(255,255,255,0.08);">
      <div class="ctrl-label">Primary variable</div>
      <div class="text-caption mb-3">{{ varName(primaryVariable) }} {{ primaryDirection === '>' ? '>' : '<' }} {{ primaryThreshold }}</div>
      <v-btn-toggle v-model="primaryDirection" mandatory direction="vertical" variant="tonal" density="compact" class="mb-1">
        <v-btn value=">" size="x-small">Above</v-btn>
        <v-btn value="<" size="x-small">Below</v-btn>
      </v-btn-toggle>
      <v-text-field v-model.number="primaryThreshold" type="number" variant="outlined" density="compact" hide-details class="mb-3" />

      <div class="ctrl-label">Compare against</div>
      <v-select v-model="secondaryVariable" :items="otherVariables" item-title="name" item-value="id"
        density="compact" variant="outlined" hide-details class="mb-3" />
      <v-btn-toggle v-model="secondaryDirection" mandatory direction="vertical" variant="tonal" density="compact" class="mb-1">
        <v-btn value=">" size="x-small">Above</v-btn>
        <v-btn value="<" size="x-small">Below</v-btn>
      </v-btn-toggle>
      <v-text-field v-model.number="secondaryThreshold" type="number" variant="outlined" density="compact" hide-details class="mb-3" />

      <v-alert v-if="secondaryError" type="error" variant="tonal" density="compact" class="mt-2">{{ secondaryError }}</v-alert>
    </div>

    <div class="flex-grow-1 d-flex flex-column" style="min-width:0; position:relative;">
      <div v-if="secondaryLoading" class="d-flex align-center justify-center"
        style="position:absolute; inset:0; z-index:1; background:rgba(0,0,0,0.45);">
        <v-progress-circular indeterminate color="warning" size="40" />
      </div>
      <!-- Chart container stays mounted across loading toggles — destroying/recreating it
           would orphan the ECharts instance, which keeps a reference to the old DOM node
           and silently stops updating (see CompoundStress chart-disappears bug). -->
      <div ref="chartContainerRef" class="w-100" style="height:55%; flex-shrink:0;" />
      <div class="flex-grow-1 pa-2" style="overflow-y:auto;">
        <div class="ctrl-label mb-1">Per-year days both conditions hold</div>
        <v-data-table :headers="yearHeaders" :items="yearRows" density="compact" hide-default-footer
          :items-per-page="-1" :sort-by="[{ key: 'year', order: 'desc' as const }]" class="stats-table" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from 'vue'
import * as echarts from 'echarts'
import { registerEchartsDarkTheme } from '../../../composables/useEchartsTheme'
import type { SeriesPoint, AnalysisLocation } from '../../../composables/useAnalysisFetch'
import { availableVariables, filterBySeason, maskBySeason, groupByYear } from '../../../composables/useAnalysisStatistics'

const props = defineProps<{
  primarySeries: SeriesPoint[]
  primaryVariable: string
  season: string
  depth: number
  location: AnalysisLocation
  yearRange: [number, number]
  fetchSeries: (variableId: string, depth: number, location: AnalysisLocation) => Promise<SeriesPoint[]>
}>()

function varName(id: string) { return availableVariables.find(v => v.id === id)?.name || id }

const otherVariables = computed(() => availableVariables.filter(v => v.id !== props.primaryVariable))
const secondaryVariable = ref(otherVariables.value[0]?.id || '')

const primaryThreshold = ref(0)
const primaryDirection = ref<'>' | '<'>('>')
const secondaryThreshold = ref(0)
const secondaryDirection = ref<'>' | '<'>('>')

const secondarySeries = ref<SeriesPoint[]>([])
const secondaryLoading = ref(false)
const secondaryError = ref<string | null>(null)

async function loadSecondary() {
  if (!secondaryVariable.value) return
  secondaryLoading.value = true
  secondaryError.value = null
  try {
    secondarySeries.value = await props.fetchSeries(secondaryVariable.value, props.depth, props.location)
  } catch (err: any) {
    secondaryError.value = err?.response?.data?.detail || err?.message || 'Failed to load comparison variable.'
  } finally {
    secondaryLoading.value = false
  }
}
watch(secondaryVariable, loadSecondary)
onMounted(loadSecondary)

const seasonalPrimary = computed(() => filterBySeason(props.primarySeries, props.season))
const seasonalSecondary = computed(() => filterBySeason(secondarySeries.value, props.season))

const joined = computed(() => {
  const mapB = new Map<string, number>()
  for (const d of seasonalSecondary.value) if (d.value != null) mapB.set(d.time, d.value)
  const out: { time: string; a: number; b: number }[] = []
  for (const d of seasonalPrimary.value) {
    if (d.value == null) continue
    const bVal = mapB.get(d.time)
    if (bVal == null) continue
    out.push({ time: d.time, a: d.value, b: bVal })
  }
  return out
})

function matchA(v: number) { return primaryDirection.value === '>' ? v > primaryThreshold.value : v < primaryThreshold.value }
function matchB(v: number) { return secondaryDirection.value === '>' ? v > secondaryThreshold.value : v < secondaryThreshold.value }

const compoundDays = computed(() => joined.value.filter(d => matchA(d.a) && matchB(d.b)))

const yearRows = computed(() => {
  const byYear = groupByYear(joined.value.map(d => ({ time: d.time, value: 1 })))
  return byYear.map(({ year, data: pts }) => {
    const yearTimes = new Set(pts.map(p => p.time))
    const yearCompound = compoundDays.value.filter(d => yearTimes.has(d.time)).map(d => d.time).sort()

    let maxStreak = 0, cur = 0, prevT: number | null = null
    for (const t of yearCompound) {
      const ts = new Date(t).getTime()
      cur = (prevT != null && ts - prevT === 86_400_000) ? cur + 1 : 1
      if (cur > maxStreak) maxStreak = cur
      prevT = ts
    }
    return { year, days: yearCompound.length, streak: maxStreak }
  })
})

const yearHeaders = [
  { title: 'Year', key: 'year' },
  { title: 'Days', key: 'days' },
  { title: 'Streak', key: 'streak' },
]

// --- CHART ---
const chartContainerRef = ref<HTMLDivElement | null>(null)
let chartInstance: echarts.ECharts | null = null
let resizeObserver: ResizeObserver | null = null

function render() {
  if (!chartContainerRef.value) return
  registerEchartsDarkTheme()
  if (!chartInstance) chartInstance = echarts.init(chartContainerRef.value, 'dark', { renderer: 'canvas' })

  const compoundSet = new Set(compoundDays.value.map(d => d.time))
  const markAreaData = buildMarkAreas(Array.from(compoundSet).sort())

  chartInstance.setOption({
    tooltip: { trigger: 'axis' },
    legend: { top: 4, textStyle: { fontSize: 10 } },
    grid: { left: '4%', right: '3%', bottom: '12%', top: '18%', containLabel: true },
    xAxis: { type: 'time', axisLabel: { fontSize: 9, color: '#ccc' } },
    yAxis: [
      { type: 'value', name: varName(props.primaryVariable), nameTextStyle: { fontSize: 9 }, axisLabel: { fontSize: 9, color: '#ccc' }, scale: true },
      { type: 'value', name: varName(secondaryVariable.value), nameTextStyle: { fontSize: 9 }, axisLabel: { fontSize: 9, color: '#ccc' }, scale: true },
    ],
    dataZoom: [{ type: 'inside' }, { type: 'slider', bottom: 4, height: 14 }],
    series: [
      {
        // Masked (not compacted) so off-season months render as a real gap instead
        // of a straight diagonal connecting e.g. last August to next March.
        name: varName(props.primaryVariable), type: 'line', showSymbol: false, yAxisIndex: 0, connectNulls: false,
        data: maskBySeason(props.primarySeries, props.season).map(d => [d.time, d.value]),
        lineStyle: { width: 1.2, color: '#58d9f9' },
        markArea: { itemStyle: { color: 'rgba(255,110,118,0.18)' }, data: markAreaData },
      },
      {
        name: varName(secondaryVariable.value), type: 'line', showSymbol: false, yAxisIndex: 1, connectNulls: false,
        data: maskBySeason(secondarySeries.value, props.season).map(d => [d.time, d.value]),
        lineStyle: { width: 1.2, color: '#ff9800' },
      },
    ],
  }, true)
  chartInstance.resize()
}

/** Groups consecutive (calendar-adjacent) compound days into markArea [start,end] pairs. */
function buildMarkAreas(sortedTimes: string[]): [{ xAxis: string }, { xAxis: string }][] {
  const areas: [{ xAxis: string }, { xAxis: string }][] = []
  let start: string | null = null, prevTs: number | null = null
  for (const t of sortedTimes) {
    const ts = new Date(t).getTime()
    if (start === null) { start = t; prevTs = ts; continue }
    if (ts - (prevTs as number) !== 86_400_000) {
      areas.push([{ xAxis: start }, { xAxis: sortedTimes[sortedTimes.indexOf(t) - 1] }])
      start = t
    }
    prevTs = ts
  }
  if (start !== null) areas.push([{ xAxis: start }, { xAxis: sortedTimes[sortedTimes.length - 1] }])
  return areas
}

watch([joined, primaryThreshold, primaryDirection, secondaryThreshold, secondaryDirection], () => nextTick(render))
watch(secondarySeries, () => nextTick(render))

onMounted(() => {
  registerEchartsDarkTheme()
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
</style>
