<template>
  <div class="flex h-full" style="overflow:hidden;">
    <div class="p-2 flex flex-col shrink-0" style="width:220px; overflow-y:auto; border-right:1px solid rgba(255,255,255,0.08);">
      <div class="ctrl-label">Variables (2-4)</div>
      <USelectMenu v-model="selectedVariables" :items="selectableVariables" label-key="name" value-key="id" class="mb-3 w-full" :return-object="false" multiple />

      <template v-for="id in selectedVariables" :key="id">
        <UAlert color="error" variant="subtle" class="mb-2" v-if="errorByVar[id]" :description="`${varName(id)}: ${errorByVar[id]}`" />
      </template>

      <div class="text-gray-500 mt-2">
        Pearson correlation across the date-aligned overlap of each variable pair, at the selected point/depth.
        Click a matrix cell to see the underlying scatter.
      </div>
    </div>

    <div class="grow flex" style="min-width:0;">
      <div style="width:50%; min-width:0; position:relative;" class="flex flex-col">
        <div v-if="anyLoading" class="flex items-center justify-center"
          style="position:absolute; inset:0; z-index:1; background:rgba(0,0,0,0.45);">
          <UIcon name="i-mdi-loading" class="animate-spin size-[40px] text-warning" />
        </div>
        <div v-else-if="readyVariables.length < 2" class="flex items-center justify-center h-full text-gray-500">
          Select at least one more variable to compare
        </div>
        <!-- Container stays mounted across loading toggles — destroying/recreating it would
             orphan the ECharts instance, which keeps a reference to the old DOM node and
             silently stops updating (see Correlation matrix-frozen-at-one-cell bug). -->
        <div v-show="!anyLoading && readyVariables.length >= 2" ref="matrixContainerRef" class="w-full h-full" />
      </div>
      <div style="width:50%; min-width:0; border-left:1px solid rgba(255,255,255,0.08);">
        <div v-if="!selectedPair" class="flex items-center justify-center h-full text-gray-500">
          Click a matrix cell to inspect a pair
        </div>
        <div v-show="selectedPair" ref="scatterContainerRef" class="w-full h-full" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, computed, watch, onMounted, onBeforeUnmount, nextTick } from 'vue'
import * as echarts from 'echarts'
import { registerEchartsDarkTheme } from '~~/composables/useEchartsTheme'
import { useVariableRegistry } from '~~/composables/useVariableRegistry'
import type { SeriesPoint, AnalysisLocation } from '~~/composables/useAnalysisFetch'
import { availableVariables, filterBySeason, pearsonCorrelation, joinSeriesByDate, linearRegression } from '~~/composables/useAnalysisStatistics'
import { csvMeta, useCsvExport, type CsvDataset } from '~~/composables/useCsvExport'

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
const { displayUnit } = useVariableRegistry()
function axisName(id: string) { const u = displayUnit(id); return u ? `${varName(id)} (${u})` : varName(id) }

const selectableVariables = availableVariables
const MAX_VARS = 4
const selectedVariables = ref<string[]>([props.primaryVariable])

function onSelectionChange(vals: string[]) {
  if (vals.length > MAX_VARS) selectedVariables.value = vals.slice(0, MAX_VARS)
  if (!vals.includes(props.primaryVariable) && vals.length === 0) selectedVariables.value = [props.primaryVariable]
}

const seriesByVar = reactive<Record<string, SeriesPoint[]>>({ [props.primaryVariable]: props.primarySeries })
const loadingByVar = reactive<Record<string, boolean>>({})
const errorByVar = reactive<Record<string, string | null>>({})

async function ensureLoaded(id: string) {
  if (id === props.primaryVariable || seriesByVar[id] || loadingByVar[id]) return
  loadingByVar[id] = true
  errorByVar[id] = null
  try {
    seriesByVar[id] = await props.fetchSeries(id, props.depth, props.location)
  } catch (err: any) {
    errorByVar[id] = err?.response?.data?.detail || err?.message || 'Failed to load.'
  } finally {
    loadingByVar[id] = false
  }
}

watch(selectedVariables, (vals) => { for (const id of vals) ensureLoaded(id) }, { immediate: true })

const anyLoading = computed(() => selectedVariables.value.some(id => loadingByVar[id]))

const seasonalByVar = computed(() => {
  const out: Record<string, SeriesPoint[]> = {}
  for (const id of selectedVariables.value) {
    if (seriesByVar[id]) out[id] = filterBySeason(seriesByVar[id], props.season)
  }
  return out
})

const readyVariables = computed(() => selectedVariables.value.filter(id => seasonalByVar.value[id]))

const matrix = computed(() => {
  const vars = readyVariables.value
  const cells: { i: number; j: number; r: number }[] = []
  for (let i = 0; i < vars.length; i++) {
    for (let j = 0; j < vars.length; j++) {
      if (i === j) { cells.push({ i, j, r: 1 }); continue }
      const { a, b } = joinSeriesByDate(seasonalByVar.value[vars[i]], seasonalByVar.value[vars[j]])
      cells.push({ i, j, r: a.length >= 2 ? pearsonCorrelation(a, b) : NaN })
    }
  }
  return { vars, cells }
})

const selectedPair = ref<[string, string] | null>(null)

// ── CSV EXPORT ──────────────────────────────────────────────────────────────
// The matrix goes out long (one row per pair) rather than as a grid — a square
// of r values is awkward to join against anything, and this way the pair count
// behind each coefficient can travel with it. The joined series is the scatter's
// data, widened so every selected variable is one column against a shared date.
const csv = useCsvExport()

const csvMatrixRows = computed(() => {
  const vars = readyVariables.value
  const out: Record<string, unknown>[] = []
  for (let i = 0; i < vars.length; i++) {
    for (let j = 0; j < vars.length; j++) {
      if (i === j) { out.push({ x: vars[i], y: vars[j], r: 1, n: null }); continue }
      const { a, b } = joinSeriesByDate(seasonalByVar.value[vars[i]], seasonalByVar.value[vars[j]])
      out.push({ x: vars[i], y: vars[j], r: a.length >= 2 ? pearsonCorrelation(a, b) : null, n: a.length })
    }
  }
  return out
})

/** Union of dates across the selected variables, one column each. */
const csvJoinedRows = computed(() => {
  const vars = readyVariables.value
  const byDate = new Map<string, Record<string, unknown>>()
  for (const id of vars) {
    for (const d of seasonalByVar.value[id] ?? []) {
      if (d.value == null) continue
      let row = byDate.get(d.time)
      if (!row) { row = { time: d.time }; byDate.set(d.time, row) }
      row[id] = d.value
    }
  }
  return Array.from(byDate.values()).sort((a, b) => String(a.time).localeCompare(String(b.time)))
})

if (csv) csv.register((): CsvDataset[] => {
  const vars = readyVariables.value
  if (!vars.length) return []
  const meta = csvMeta(csv.context.value, [
    ['variables', vars.map(id => varName(id)).join(', ')],
    ['method', 'Pearson correlation over dates where both variables have a value'],
  ])
  return [
    {
      label: 'Correlation matrix',
      slug: 'correlation-matrix',
      columns: [
        { header: 'variable_x', accessorKey: 'x' },
        { header: 'variable_y', accessorKey: 'y' },
        { header: 'pearson_r', accessorKey: 'r' },
        { header: 'n_days', accessorKey: 'n' },
      ],
      rows: csvMatrixRows.value,
      meta,
    },
    {
      label: 'Joined daily series',
      slug: 'correlation-series',
      columns: [
        { header: 'time', accessorKey: 'time' },
        ...vars.map(id => ({ header: axisName(id), accessorKey: id })),
      ],
      rows: csvJoinedRows.value,
      meta,
    },
  ]
})

// --- MATRIX HEATMAP ---
const matrixContainerRef = ref<HTMLDivElement | null>(null)
let matrixInstance: echarts.ECharts | null = null
let matrixResizeObserver: ResizeObserver | null = null

function renderMatrix() {
  if (!matrixContainerRef.value || readyVariables.value.length < 2) return
  registerEchartsDarkTheme()
  if (!matrixInstance) matrixInstance = echarts.init(matrixContainerRef.value, 'dark', { renderer: 'canvas' })

  const { vars, cells } = matrix.value
  const labels = vars.map(varName)
  const data = cells.map(c => [c.j, c.i, Number.isNaN(c.r) ? null : Number(c.r.toFixed(3))])

  matrixInstance.off('click')
  matrixInstance.setOption({
    tooltip: {
      formatter: (p: any) => `${labels[p.value[1]]} vs ${labels[p.value[0]]}<br/>r = ${p.value[2] ?? 'n/a'}`,
    },
    grid: { left: '20%', right: '5%', bottom: '20%', top: '5%' },
    xAxis: { type: 'category', data: labels, axisLabel: { fontSize: 9, color: '#ccc', rotate: 30 }, splitArea: { show: true } },
    yAxis: { type: 'category', data: labels, axisLabel: { fontSize: 9, color: '#ccc' }, splitArea: { show: true } },
    visualMap: {
      min: -1, max: 1, show: true, orient: 'horizontal', left: 'center', bottom: 0,
      textStyle: { color: '#ccc', fontSize: 9 },
      inRange: { color: ['#4992ff', '#222', '#ff6e76'] },
    },
    series: [{
      type: 'heatmap',
      data,
      label: { show: true, fontSize: 9, formatter: (p: any) => p.value[2] ?? '' },
      emphasis: { itemStyle: { borderColor: '#fff', borderWidth: 1 } },
    }],
  }, true)
  matrixInstance.resize()

  matrixInstance.on('click', (p: any) => {
    if (p.value[1] === p.value[0]) return
    selectedPair.value = [vars[p.value[1]], vars[p.value[0]]]
    nextTick(renderScatter)
  })
}

watch(matrix, () => nextTick(renderMatrix))

// --- SCATTER DETAIL ---
const scatterContainerRef = ref<HTMLDivElement | null>(null)
let scatterInstance: echarts.ECharts | null = null
let scatterResizeObserver: ResizeObserver | null = null

function renderScatter() {
  if (!scatterContainerRef.value || !selectedPair.value) return
  registerEchartsDarkTheme()
  if (!scatterInstance) scatterInstance = echarts.init(scatterContainerRef.value, 'dark', { renderer: 'canvas' })

  const [xVar, yVar] = selectedPair.value
  const { a: xs, b: ys, times } = joinSeriesByDate(seasonalByVar.value[xVar], seasonalByVar.value[yVar])
  const points = xs.map((x, i) => [x, ys[i], new Date(times[i]).getUTCFullYear()])

  const { slope, intercept } = linearRegression(xs.map((x, i) => ({ x, y: ys[i] })))
  const xMin = Math.min(...xs), xMax = Math.max(...xs)
  const trendLine = [[xMin, slope * xMin + intercept], [xMax, slope * xMax + intercept]]

  scatterInstance.setOption({
    tooltip: { formatter: (p: any) => Array.isArray(p.value) ? `${varName(xVar)}: ${p.value[0]}<br/>${varName(yVar)}: ${p.value[1]}<br/>Year: ${p.value[2]}` : '' },
    grid: { left: '10%', right: '5%', bottom: '12%', top: '8%', containLabel: true },
    xAxis: { type: 'value', name: axisName(xVar), nameLocation: 'middle', nameGap: 28, axisLabel: { fontSize: 9, color: '#ccc' }, scale: true },
    yAxis: { type: 'value', name: axisName(yVar), nameLocation: 'middle', nameGap: 40, axisLabel: { fontSize: 9, color: '#ccc' }, scale: true },
    series: [
      { type: 'scatter', symbolSize: 4, data: points, itemStyle: { color: '#58d9f9', opacity: 0.5 } },
      { type: 'line', data: trendLine, showSymbol: false, lineStyle: { color: '#ff9800', width: 1.5, type: 'dashed' } },
    ],
  }, true)
  scatterInstance.resize()
}

watch(selectedPair, () => nextTick(renderScatter))
watch(seasonalByVar, () => { if (selectedPair.value) nextTick(renderScatter) })

onMounted(() => {
  registerEchartsDarkTheme()
  nextTick(renderMatrix)
  if (matrixContainerRef.value && typeof ResizeObserver !== 'undefined') {
    matrixResizeObserver = new ResizeObserver(() => matrixInstance?.resize())
    matrixResizeObserver.observe(matrixContainerRef.value)
  }
})

watch(scatterContainerRef, (el) => {
  scatterResizeObserver?.disconnect()
  scatterResizeObserver = null
  if (el && typeof ResizeObserver !== 'undefined') {
    scatterResizeObserver = new ResizeObserver(() => scatterInstance?.resize())
    scatterResizeObserver.observe(el)
  }
})

onBeforeUnmount(() => {
  matrixResizeObserver?.disconnect()
  scatterResizeObserver?.disconnect()
  matrixInstance?.dispose()
  scatterInstance?.dispose()
  matrixInstance = null
  scatterInstance = null
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

:deep(.v-select) { flex: none; }
</style>
