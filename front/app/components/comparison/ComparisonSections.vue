<template>
  <div class="sections flex flex-col h-full p-3" ref="rootRef">
    <div class="flex items-center mb-2" style="gap:8px;">
      <SegmentedControl v-model="binMode" :items="binModeItems" size="xs" :disabled="loading"
        aria-label="Time bin resolution" />
      <UIcon name="i-mdi-loading" class="animate-spin size-[14px] text-teal-400" v-if="loading" />
      <span v-if="stats" class="stat-strip">
        bias {{ stats.bias }} &#183; r {{ stats.r }} &#183; RMSE {{ stats.rmse }} &#183; n {{ stats.n }}
        <span class="text-gray-500"> at {{ depthLabel }}</span>
      </span>
      <div class="grow" />
      <UButton variant="ghost" size="xs" icon="i-mdi-chevron-left" class="shrink-0" :disabled="!canPageBack || loading" @click="page(-1)" />
      <span class="range-label">{{ rangeLabel }}</span>
      <UButton variant="ghost" size="xs" icon="i-mdi-chevron-right" class="shrink-0" :disabled="!canPageForward || loading" @click="page(1)" />
    </div>

    <UAlert color="error" variant="subtle" class="mb-2 shrink-0" v-if="loadError" :description="loadError" />

    <!-- Same distinction Explore draws: a sensor moored outside the model
         domain has nothing to compare against, which is a fact about the
         location, not a failed request. -->
    <UAlert
      v-else-if="outOfDomain && !outOfDomainDismissed"
      color="info"
      variant="subtle"
      icon="i-mdi-map-marker-off-outline"
      class="mb-2 shrink-0"
      close
      @update:open="outOfDomainDismissed = true"
      title="Outside the SalishSeaCast model domain"
      :description="`This sensor sits ${outOfDomain.distanceKm.toFixed(0)} km from the nearest model cell, so there is no model section to compare against.`"
    />

    <div class="grow" style="min-height:0;" @mouseleave="hoverCell = null">
      <div class="hm-slot" :style="{ height: panelH + 'px' }">
        <TimeDepthHeatmap ref="modelPanel" label="MODEL" :depths="depths" :values="modelGrid"
          :bin-count="binCount" :color-fn="colorFn" :gridline-bins="gridlineBins"
          :chart-group="CHART_GROUP" :mark-depth="markDepth" :mark-label="depthLabel"
          @cell-click="onCellClick" @hover="hoverCell = $event" />
      </div>
      <div class="hm-slot" :style="{ height: panelH + 'px' }">
        <TimeDepthHeatmap ref="sensorPanel" label="SENSOR &#183; BINNED" :depths="depths" :values="sensorGrid"
          :bin-count="binCount" :color-fn="colorFn" :gridline-bins="gridlineBins"
          :chart-group="CHART_GROUP" :mark-depth="markDepth"
          show-x-axis :x-label="xLabel"
          @cell-click="onCellClick" @hover="hoverCell = $event" />
      </div>

      <div class="info-row">
        <template v-if="hoverInfo">
          <span class="hover-dt">{{ hoverInfo.dt }} &#183; {{ hoverInfo.depth }}</span>
          <span class="hover-val"><span class="dot" style="background:#3987e5;" />Model <b>{{ hoverInfo.model }}</b></span>
          <span class="hover-val"><span class="dot" style="background:#a5d6a7;" />Sensor <b>{{ hoverInfo.sensor }}</b></span>
          <span class="hover-val"><span class="dot" style="background:#c53030;" />Diff <b>{{ hoverInfo.diff }}</b></span>
        </template>
        <span v-else class="hover-placeholder">
          Both panels share the map's colour scale, so the same value reads the same in each &#183; hover to inspect
        </span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from 'vue'
import { useMainStore, formatDepthLabel } from '../../stores/main'
import { fetchDepthProfile, parseCoverageBound, asOutsideDomainError, type DepthProfileResponse, type OutsideDomainError } from '~~/composables/useDepthProfileFetch'
import { useVariableRegistry } from '~~/composables/useVariableRegistry'
import { resolveColormap } from '~~/composables/useColormapResolver'
import { BIN_CONFIG, useTimeDepthWindow, toApiIso, type BinMode } from '~~/composables/useTimeDepthWindow'
import TimeDepthHeatmap from '../depth/TimeDepthHeatmap.vue'
import { csvFilename, csvMeta, csvTimestamp, useCsvExport, type CsvDataset } from '~~/composables/useCsvExport'
import SegmentedControl from '../ui/SegmentedControl.vue'
import { useViewState } from '~~/composables/useViewState'
const binModeItems = computed(() => AVAILABLE_MODES.map(m => ({ value: m, label: BIN_CONFIG[m].short, title: BIN_CONFIG[m].label })))

/**
 * Model and sensor water columns side by side, for a profiler.
 *
 * No difference panel: with both sections on one shared colour scale the eye
 * does that comparison directly, and the numeric agreement (bias/r/RMSE) is
 * more useful stated than painted. The scale is the map's own, so a value here
 * reads the same colour as it does on the map.
 */

const props = defineProps<{ active?: boolean }>()

const mainStore = useMainStore()
const { toDisplayValue, displayUnit } = useVariableRegistry()

const sensorInfo = computed(() => mainStore.sensors.find(s => s.id === mainStore.selectedSensor?.id) ?? null)
const modelSource = computed(() => mainStore.selected_variable.source)
const varId = computed(() => mainStore.selected_variable.var)
const varMeta = computed(() => mainStore.variables.find(v => v.source === modelSource.value && v.var === varId.value) ?? null)

const FALLBACK_DEPTHS = [0.5, 1.5, 3, 5, 7, 10, 14, 19, 25, 32, 40, 50, 62, 76, 92, 110]
const depths = computed<number[]>(() => {
  const numeric = varMeta.value?.depths?.filter(d => d >= 0)
  return numeric && numeric.length > 1 ? [...numeric].sort((a, b) => a - b) : FALLBACK_DEPTHS
})

// Monthly is omitted: no deployed profiler has anything like a 20-year cast record.
const AVAILABLE_MODES: BinMode[] = ['hourly', 'daily']
// Store-backed so a shared link reopens this tab as the sender left it.
const field = useViewState('comparison.sections')
const binMode = field<BinMode>('binMode', 'daily')

const dataFloor = computed(() => sensorInfo.value?.first_data_at ? new Date(sensorInfo.value.first_data_at) : null)
const dataCeil = computed(() => sensorInfo.value?.latest_data_at ? new Date(sensorInfo.value.latest_data_at) : new Date())

const {
  binCount, gridlineBins, windowEnd, windowStart, binStarts,
  canPageBack, canPageForward, page, rangeLabel,
} = useTimeDepthWindow({
  binMode, dataFloor, dataCeil,
  resetOn: [computed(() => mainStore.selectedSensor?.id ?? ''), varId],
})

const modelGrid = ref<(number | null)[][]>([])
const sensorGrid = ref<(number | null)[][]>([])
const loading = ref(false)
const loadError = ref<string | null>(null)

// Kept out of `loadError` so it renders informationally rather than as a
// failure — see the alert in the template.
const outOfDomain = ref<OutsideDomainError | null>(null)

// Dismissible like Explore's copy of this banner, and scoped the same way: to
// the thing being explained (here the sensor) rather than to the fetch, so
// paging the window does not bring back a banner the user just closed.
const outOfDomainDismissed = ref(false)
watch(() => mainStore.selectedSensor?.id, () => { outOfDomainDismissed.value = false })

function nearestLevelIdx(depth: number, levels: number[]) {
  let best = 0, bestDist = Infinity
  levels.forEach((d, i) => { const dist = Math.abs(d - depth); if (dist < bestDist) { bestDist = dist; best = i } })
  return best
}

function applyResponse(resp: DepthProfileResponse) {
  const timeIndex = new Map<number, number>()
  resp.time.forEach((iso, i) => timeIndex.set(new Date(iso.endsWith('Z') ? iso : iso + 'Z').getTime(), i))

  const ds = depths.value
  const starts = binStarts.value
  const model: (number | null)[][] = ds.map(() => Array(binCount.value).fill(null))
  const sensor: (number | null)[][] = ds.map(() => Array(binCount.value).fill(null))

  ds.forEach((d, li) => {
    const ri = resp.depths.length ? nearestLevelIdx(d, resp.depths) : null
    if (ri === null) return
    for (let bi = 0; bi < binCount.value; bi++) {
      const ti = timeIndex.get(starts[bi]!.getTime())
      if (ti == null) continue
      model[li]![bi] = resp.model[ri]?.[ti] ?? null
      sensor[li]![bi] = resp.sensor?.[ri]?.[ti] ?? null
    }
  })
  modelGrid.value = model
  sensorGrid.value = sensor
}

let fetchSeq = 0
async function fetchWindow() {
  const meta = sensorInfo.value
  if (!meta || meta.depth !== -1 || !varId.value) return
  const reqId = ++fetchSeq
  loading.value = true
  loadError.value = null
  outOfDomain.value = null
  try {
    const resp = await fetchDepthProfile({
      source: modelSource.value,
      var: varId.value,
      sensorId: meta.id,
      lat: meta.latitude,
      lon: meta.longitude,
      fromDate: toApiIso(windowStart.value),
      toDate: toApiIso(windowEnd.value),
      binMode: binMode.value,
    })
    if (reqId !== fetchSeq) return
    applyResponse(resp)
  } catch (err: any) {
    if (reqId !== fetchSeq) return
    const domain = asOutsideDomainError(err)
    if (domain) outOfDomain.value = domain
    else loadError.value = err?.response?.data?.detail || err?.message || 'Failed to load depth sections.'
    modelGrid.value = depths.value.map(() => Array(binCount.value).fill(null))
    sensorGrid.value = depths.value.map(() => Array(binCount.value).fill(null))
  } finally {
    if (reqId === fetchSeq) loading.value = false
  }
}

// mainStore.unitPreference is included so toggling the display unit re-fetches
// (a cheap cache hit — see useDepthProfileFetch.ts) rather than leaving the
// section showing stale numbers under the old unit.
watch([windowEnd, depths, binMode, () => mainStore.selectedSensor?.id, () => props.active, () => mainStore.unitPreference[varId.value]], () => {
  if (props.active) fetchWindow()
}, { immediate: true })

// ── COLOUR — the map's scale, so both panels and the map agree. ──────────────
function hexToRgb(hex: string): [number, number, number] {
  const n = parseInt(hex.slice(1), 16)
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255]
}
function lerpRgb(a: number[], b: number[], t: number) { return [a[0]! + (b[0]! - a[0]!) * t, a[1]! + (b[1]! - a[1]!) * t, a[2]! + (b[2]! - a[2]!) * t] }
function rgbCss(c: number[]) { return `rgb(${c.map(v => Math.round(v)).join(',')})` }
function colorFromStops(stops: [number, string][], t: number): number[] {
  const clamped = Math.max(0, Math.min(1, t))
  const first = stops[0]!
  if (clamped <= first[0]) return hexToRgb(first[1])
  for (let i = 0; i < stops.length - 1; i++) {
    const [p0, c0] = stops[i]!
    const [p1, c1] = stops[i + 1]!
    if (clamped <= p1) return lerpRgb(hexToRgb(c0), hexToRgb(c1), p1 === p0 ? 0 : (clamped - p0) / (p1 - p0))
  }
  return hexToRgb(stops[stops.length - 1]![1])
}
const FALLBACK = [hexToRgb('#0d366b'), hexToRgb('#3987e5'), hexToRgb('#cde2fb')]
const stops = computed(() => resolveColormap(mainStore.colormaps, mainStore.selected_variable.colormap)?.stops ?? null)
// Converted to the current display unit — `modelGrid`/`sensorGrid` values are
// (useDepthProfileFetch.ts converts at the source), so these normalization
// bounds have to match.
const lo = computed(() => toDisplayValue(varId.value, mainStore.selected_variable.colormapMin ?? varMeta.value?.colormapMin ?? 0) ?? 0)
const hi = computed(() => {
  const v = toDisplayValue(varId.value, mainStore.selected_variable.colormapMax ?? varMeta.value?.colormapMax ?? 1) ?? 1
  return v > lo.value ? v : lo.value + 1
})
const colorFn = computed(() => {
  const s = stops.value, min = lo.value, max = hi.value
  return (v: number) => {
    const t = Math.max(0, Math.min(1, (v - min) / (max - min)))
    if (s && s.length) return rgbCss(colorFromStops(s, t))
    return rgbCss(t < 0.5 ? lerpRgb(FALLBACK[0]!, FALLBACK[1]!, t * 2) : lerpRgb(FALLBACK[1]!, FALLBACK[2]!, (t - 0.5) * 2))
  }
})

// ── DEPTH / HOVER / STATS ────────────────────────────────────────────────────
const selectedDepthIdx = field('selectedDepthIdx', 0)
const markDepth = computed(() => depths.value[selectedDepthIdx.value] ?? null)
// ── CSV EXPORT ──────────────────────────────────────────────────────────────
// One file, not two: the whole point of this tab is the two panels side by side,
// and splitting them into separate downloads would leave the reader to re-join
// them on (time, depth) to get back what they were looking at. Rows where both
// are empty are dropped — below the seabed that's most of the grid.
const csv = useCsvExport()

const csvSectionRows = computed(() => {
  const ds = depths.value
  const starts = binStarts.value
  const rows: Record<string, unknown>[] = []
  for (let bi = 0; bi < binCount.value; bi++) {
    const t = csvTimestamp(starts[bi])
    for (let li = 0; li < ds.length; li++) {
      const m = modelGrid.value[li]?.[bi] ?? null
      const sv = sensorGrid.value[li]?.[bi] ?? null
      if (m == null && sv == null) continue
      rows.push({
        time: t,
        depth: ds[li],
        model: m,
        sensor: sv,
        difference: (m != null && sv != null) ? m - sv : null,
      })
    }
  }
  return rows
})

if (csv) csv.register((): CsvDataset[] => {
  if (!props.active || !csvSectionRows.value.length) return []
  const u = displayUnit(varId.value) ? ` (${displayUnit(varId.value)})` : ''
  return [{
    label: 'Depth sections (model & sensor)',
    slug: 'comparison-depth-sections',
    // Named for the paged window it actually holds, not the sensor's whole
    // record — the shared context spans the latter, and a file called
    // 2010–2026 holding one year of it would be a lie.
    filename: csvFilename([
      'comparison-depth-sections',
      varId.value,
      sensorInfo.value?.name,
      csvTimestamp(windowStart.value).slice(0, 10),
      csvTimestamp(windowEnd.value).slice(0, 10),
    ]),
    columns: [
      { header: 'time', accessorKey: 'time' },
      { header: 'depth_m', accessorKey: 'depth' },
      { header: `model${u}`, accessorKey: 'model' },
      { header: `sensor${u}`, accessorKey: 'sensor' },
      { header: `difference${u}`, accessorKey: 'difference' },
    ],
    rows: csvSectionRows.value,
    meta: csvMeta(csv.context.value, [
      ['bin_mode', BIN_CONFIG[binMode.value].label],
      ['time_range', `${csvTimestamp(windowStart.value)} .. ${csvTimestamp(windowEnd.value)}`],
      ['note', 'sensor casts binned onto the model time/depth grid; cells empty in both are omitted'],
    ]),
  }]
})

const depthLabel = computed(() => {
  const d = depths.value[selectedDepthIdx.value]
  return d != null ? `${d.toFixed(d < 10 ? 1 : 0)} m` : '—'
})
watch(depths, (ds) => { selectedDepthIdx.value = nearestLevelIdx(mainStore.selected_variable.depth_nc ?? 14, ds) }, { immediate: true })
watch(() => mainStore.selected_variable.depth_nc, (d) => {
  if (d == null || d < 0 || !depths.value.length) return
  selectedDepthIdx.value = nearestLevelIdx(d, depths.value)
})

function onCellClick({ depthIdx }: { binIdx: number, depthIdx: number }) {
  selectedDepthIdx.value = depthIdx
  const d = depths.value[depthIdx]
  if (d != null) mainStore.updateSelectedVariable({ depth: formatDepthLabel(d), depth_nc: d })
}

const hoverCell = ref<{ binIdx: number, depthIdx: number } | null>(null)
const hoverInfo = computed(() => {
  const cell = hoverCell.value
  if (!cell) return null
  const mv = modelGrid.value[cell.depthIdx]?.[cell.binIdx] ?? null
  const sv = sensorGrid.value[cell.depthIdx]?.[cell.binIdx] ?? null
  const dv = (mv == null || sv == null) ? null : mv - sv
  const depth = depths.value[cell.depthIdx]
  const dt = binStarts.value[cell.binIdx]
  return {
    dt: dt ? dt.toLocaleString('en-US', binMode.value === 'hourly'
      ? { month: 'short', day: 'numeric', hour: 'numeric', timeZone: 'UTC' }
      : { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' }) : '—',
    depth: depth != null ? `${depth.toFixed(depth < 10 ? 1 : 0)}m` : '—',
    model: mv == null ? 'no data' : mv.toFixed(2),
    sensor: sv == null ? 'no cast' : sv.toFixed(2),
    diff: dv == null ? '—' : `${dv >= 0 ? '+' : ''}${dv.toFixed(2)}`,
  }
})

const stats = computed(() => {
  const d = selectedDepthIdx.value
  let n = 0, sumDiff = 0, sumSq = 0, sm = 0, ss = 0, smm = 0, sss = 0, sms = 0
  for (let bi = 0; bi < binCount.value; bi++) {
    const mv = modelGrid.value[d]?.[bi] ?? null
    const sv = sensorGrid.value[d]?.[bi] ?? null
    if (mv == null || sv == null) continue
    n++
    const diff = mv - sv
    sumDiff += diff; sumSq += diff * diff
    sm += mv; ss += sv; smm += mv * mv; sss += sv * sv; sms += mv * sv
  }
  if (!n) return null
  const cov = sms / n - (sm / n) * (ss / n)
  const varM = smm / n - (sm / n) ** 2
  const varS = sss / n - (ss / n) ** 2
  const bias = sumDiff / n
  return {
    bias: `${bias >= 0 ? '+' : ''}${bias.toFixed(3)}`,
    r: (varM > 0 && varS > 0 ? cov / Math.sqrt(varM * varS) : 0).toFixed(3),
    rmse: Math.sqrt(sumSq / n).toFixed(3),
    n,
  }
})

function xLabel(binIdx: number) {
  const d = binStarts.value[Math.round(binIdx)]
  return d ? d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' }) : ''
}

// ── LAYOUT ───────────────────────────────────────────────────────────────────
const CHART_GROUP = 'comparisonSections'
const rootRef = ref<HTMLDivElement | null>(null)
const modelPanel = ref<InstanceType<typeof TimeDepthHeatmap> | null>(null)
const sensorPanel = ref<InstanceType<typeof TimeDepthHeatmap> | null>(null)
const panelH = ref(200)

const INFO_ROW_PX = 40

function computeLayout() {
  if (!rootRef.value) return
  const el = rootRef.value.querySelector('.flex-grow-1') as HTMLElement | null
  if (!el) return
  panelH.value = Math.max(80, Math.floor((el.getBoundingClientRect().height - INFO_ROW_PX) / 2))
}

function resizeAll() { modelPanel.value?.resize(); sensorPanel.value?.resize() }

let resizeObs: ResizeObserver | null = null
onMounted(async () => {
  await nextTick()
  computeLayout(); resizeAll()
  if (typeof ResizeObserver !== 'undefined') {
    resizeObs = new ResizeObserver(() => { computeLayout(); resizeAll() })
    if (rootRef.value) resizeObs.observe(rootRef.value)
  }
})
watch(() => props.active, async (a) => { if (a) { await nextTick(); computeLayout(); resizeAll() } })
onBeforeUnmount(() => resizeObs?.disconnect())
</script>

<style scoped>
.sections { min-height: 0; }
.hm-slot { margin-bottom: 3px; }
.range-label { min-width: 150px; text-align: center; color: rgba(255,255,255,0.6); font-variant-numeric: tabular-nums; }
.stat-strip { color: rgba(255,255,255,0.6); font-variant-numeric: tabular-nums; }

.info-row {
  display: flex; align-items: center; justify-content: center; gap: 22px; flex-wrap: wrap;
  margin-top: 6px; padding: 4px 10px; min-height: 30px;
  background: rgba(255,255,255,0.03); border-radius: 4px; font-size: 11.5px;
}
.hover-dt { color: rgba(255,255,255,0.55); font-variant-numeric: tabular-nums; }
.hover-val { display: flex; align-items: center; gap: 6px; color: rgba(255,255,255,0.6); font-variant-numeric: tabular-nums; }
.hover-val b { color: #eef3f7; font-weight: 700; }
.hover-val .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
.hover-placeholder { color: rgba(255,255,255,0.3); font-size: 11px; }
</style>
