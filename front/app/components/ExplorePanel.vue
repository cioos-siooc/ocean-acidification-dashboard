<template>
  <div class="model-depth-profile flex flex-col h-full p-3" ref="rootRef">
    <!-- No point picked yet — the whole view is keyed off one map coordinate. -->
    <div v-if="!point" class="flex flex-col items-center justify-center grow text-center px-6">
      <UIcon name="i-mdi-layers-search" class="size-[56px] text-gray-500" />
      <div class="text-gray-500 mt-2">
        Click a point on the map to see its full water-column section over time.
      </div>
    </div>

    <template v-else>
      <div class="flex items-center mb-2" style="gap:8px;">
        <SegmentedControl
          v-model="binMode"
          :items="binModeItems"
          size="xs"
          :disabled="loading"
          aria-label="Time bin resolution"
        />
        <!-- Which sub-view (Timeseries / Model depth / Sensor depth) is showing
             is picked from the footer's nav rail now, not a toggle in here. -->
        <UIcon name="i-mdi-loading" class="animate-spin size-[14px] text-teal-400" v-if="loading" />

        <div class="grow" />

        <!-- Model clock: steps/animates which timestamp the raster layer paints.
           Shared across all three sub-views — the depth sections' "MAP" marker
           and the map's own raster layer both read `selected_variable.dt`. -->
        <div class="time-controls-row mb-2">
          <TimeControls hide-date-picker />
        </div>

        <div class="grow" />
        <UButton variant="ghost" size="xs" icon="i-mdi-chevron-left" class="shrink-0" :disabled="!canPageBack || loading" @click="pageWindow(-1)" />
        <UPopover v-model:open="dateMenuOpen" arrow :content="{ side: 'bottom' }" :disabled="loading">
  <span class="range-label range-label--clickable" title="Jump to a date">{{
                rangeLabel }}</span>
  <template #content>
    <div class="bg-elevated rounded-lg">
                <UCalendar v-model="pickedCalendarDate" :min-value="minCalendarDate" :max-value="maxCalendarDate" />
                <div class="flex items-center gap-2 px-2 py-2">
                  <div class="grow" />
                  <UButton variant="ghost" @click="dateMenuOpen = false">Cancel</UButton>
                  <UButton variant="ghost" color="primary" @click="confirmDatePickAndUnpin">OK</UButton>
                </div>
              </div>
  </template>
</UPopover>
        <UButton variant="ghost" size="xs" icon="i-mdi-chevron-right" class="shrink-0" :disabled="!canPageForward || loading" @click="pageWindow(1)" />
        <DownloadButton :datasets="csvDatasets" size="xs" class="shrink-0" />
      </div>

      <UAlert
        v-if="loadError"
        color="error"
        variant="subtle"
        class="mb-2 shrink-0"
        :description="loadError"
      />

      <!-- Not an error: the user clicked somewhere the model does not reach.
           Informational tone, and it says what *is* on screen rather than only
           what is missing. -->
      <UAlert
        v-else-if="showOutOfDomainAlert"
        color="info"
        variant="subtle"
        icon="i-mdi-map-marker-off-outline"
        class="mb-2 shrink-0"
        close
        @update:open="outOfDomainDismissed = true"
        :title="`Outside the SalishSeaCast model domain (nearest model cell is ${outOfDomain.distanceKm.toFixed(0)} km away)`"
        :description="sensorMeta
          ? `No model values here — the chart shows ${sensorMeta.name} only, at its own depth of ${sensorMeta.depthLabel}.`
          : 'No model values here. Pick a point inside the Salish Sea, or select a sensor at this location.'"
      />

      <ChartContextBar :items="contextItems" />

      <div class="chart-region">
        <!-- One source at a time. Showing model and sensor side by side here is
             what the Comparison tab is for; this tab answers "what does this one
             record look like through the water column". -->
        <div v-if="showSection" class="hm-slot" :style="{ height: sectionH + 'px' }">
          <TimeDepthHeatmap ref="modelPanel" :label="panelLabel" :depths="depths" :values="activeGrid"
            :bin-count="binCount" :color-fn="seqColorFn" :gridline-bins="gridlineBins" :mark-cell="selectedCell"
            :tooltip-formatter="cellTooltip" show-x-axis :x-label="xLabel" @cell-click="onCellClick" />
        </div>

        <!-- Legend only — per-cell values now show in the heatmap's own ECharts
             tooltip (see `cellTooltip`) instead of a shared readout row here.
             This pane lives in a short resizable footer, so every fixed row
             costs the charts real height: series view has nothing left to put
             here (its field/depth are already in ChartContextBar above), so it
             collapses to zero height rather than an empty bar — kept in the DOM
             either way (not v-if'd out) so computeLayout below still has a real
             rect to measure. -->
        <div ref="depthHeaderRef" class="info-row" :class="{ 'info-row--compact': !showSection }">
          <template v-if="showSection">
            <div class="grow" />
            <!-- Scale is the map's; its colorbar is the legend. Only the
                 "no data" convention is specific to this view. -->
            <div class="legend">
              <span class="swatch-hatch" :title="emptyCellLabel" />
              <span class="legend-label">{{ emptyCellLabel }}</span>
            </div>
          </template>
        </div>

        <div v-if="!showSection" class="line-slot" :style="{ height: chartH + 'px' }">
          <TimeseriesChart ref="lineChart" :window-start="windowStart" :window-end="chartWindowEnd"
            :grid-left="AXIS_LEFT_PX" :grid-right="DATAZOOM_RIGHT_PX" legend-layout="top" />
          <!-- Out-of-domain already has the alert above; repeating it across the
               plot labels a chart that is drawing real sensor data as empty. -->
          <div v-if="!depthHasData && !loading && !outOfDomain" class="line-empty">{{ emptyLineMessage }}</div>
        </div>

        <div v-if="loading" class="loading-overlay">
          <UIcon name="i-mdi-loading" class="animate-spin size-[42px] text-teal-400" />
        </div>
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from 'vue'
import moment from 'moment'
import { useMainStore, formatDepthLabel } from '../stores/main'
import { fetchDepthProfile, parseCoverageBound, asOutsideDomainError, type DepthProfileResponse, type OutsideDomainError } from '~~/composables/useDepthProfileFetch'
import { resolveColormap } from '~~/composables/useColormapResolver'
import { BIN_CONFIG, useTimeDepthWindow, toApiIso, binSeries, floorToBin, type BinMode } from '~~/composables/useTimeDepthWindow'
import { fetchClimateTimeseries } from '~~/composables/useClimateTimeseries'
import { getSensorTimeseries } from '~~/composables/useSensorTimeseries'
import { csvMeta, csvTimestamp, provideCsvExport, type CsvContext, type CsvDataset } from '~~/composables/useCsvExport'
import DownloadButton from './ui/DownloadButton.vue'
import { useVariableRegistry } from '~~/composables/useVariableRegistry'
import TimeDepthHeatmap from './depth/TimeDepthHeatmap.vue'
import TimeseriesChart from './TimeseriesChart.vue'
import TimeControls from './TimeControls.vue'
import ChartContextBar, { type ContextItem } from './ChartContextBar.vue'
import SegmentedControl from './ui/SegmentedControl.vue'
import { toCalendarDate, fromCalendarDate } from '~~/composables/useCalendarDate'

/**
 * Explore: the one footer pane that is tied to the map.
 *
 * It reads the map's coordinate, depth and clock, and offers three ways to look
 * at that point over a shared, paged time window — picked from a sub-list under
 * "Explore" in the footer's nav rail (`mainStore.exploreView`), not a toggle in
 * this component:
 *   series       — the single-depth timeseries: model and sensor overlaid at
 *                  the selected depth, plus the climatology envelope and
 *                  day/night shading
 *   model-depth  — the model's full water column as a time-depth section
 *   sensor-depth — a profiler's binned casts as the same kind of section
 *
 * Only one of the three is ever on screen — the depth sections are pure
 * heatmaps now, not a lens sitting above the timeseries — so each gets the
 * pane's full height. Clicking a section cell always re-centres the map's
 * selected depth and selected time, in every bin mode; hourly additionally
 * snaps the time to the nearest real model output instant (see onCellClick).
 */

const props = defineProps<{ active?: boolean }>()

const mainStore = useMainStore()
const { toDisplayValue, displayUnit } = useVariableRegistry()

const point = computed(() => mainStore.lastClickedMapPoint)
const source = computed(() => mainStore.selected_variable.source)
const varId = computed(() => mainStore.selected_variable.var)
const varMeta = computed(() => mainStore.variables.find(v => v.source === source.value && v.var === varId.value) ?? null)
const varName = computed(() => varMeta.value?.name || varId.value || 'Variable')

const panelLabel = computed(() => `${showingSensor.value ? 'SENSOR · BINNED' : 'MODEL'} · ${BIN_CONFIG[binMode.value].short}`)

// ── DEPTH LEVELS — as in the Comparison view: real model levels, minus the -1
// "bottom" pseudo-level which would NaN out the sqrt depth scale. ─────────────
const FALLBACK_DEPTHS = [0.5, 1.5, 3, 5, 7, 10, 14, 19, 25, 32, 40, 50, 62, 76, 92, 110]
const depths = computed<number[]>(() => {
  const numeric = varMeta.value?.depths?.filter(d => d >= 0)
  return numeric && numeric.length > 1 ? [...numeric].sort((a, b) => a - b) : FALLBACK_DEPTHS
})

// ── WINDOW ────────────────────────────────────────────────────────────────────
// Monthly is offered here (unlike the Comparison view) because the daily table
// reaches back ~two decades at any grid cell — see the coverage note below.
const AVAILABLE_MODES: BinMode[] = ['hourly', 'daily', 'monthly']
const pickedCalendarDate = computed({
  get: () => toCalendarDate(pickedDate.value),
  set: (v) => { pickedDate.value = fromCalendarDate(v) },
})
const minCalendarDate = computed(() => toCalendarDate(minDateStr.value) ?? undefined)
const maxCalendarDate = computed(() => toCalendarDate(maxDateStr.value) ?? undefined)
const binModeItems = computed(() => AVAILABLE_MODES.map(m => ({ value: m, label: BIN_CONFIG[m].short, title: BIN_CONFIG[m].label })))
// Lives in the store, not a local ref: the vertical profile drawer (a sibling
// under index.vue, not a child of this panel) reads it to aggregate its own
// profile the same way this panel's depth section is currently binned.
const binMode = computed<BinMode>({
  get: () => mainStore.exploreBinMode,
  set: (m) => mainStore.setExploreBinMode(m),
})

// Coverage differs per bin mode (hourly and daily are different tables, nearly
// two decades apart), and is per grid cell, so it can only come from the
// backend. Until the first response lands, seed the ceiling from the variable's
// own timestamp list — always a valid instant to anchor the first window at —
// and leave the floor open.
const coverage = ref<{ from: Date | null, to: Date | null }>({ from: null, to: null })
const latestKnownDt = computed(() => {
  const dts = varMeta.value?.dts
  const last = dts?.length ? dts[dts.length - 1] : null
  return last ? new Date(typeof last === 'string' && !String(last).endsWith('Z') ? `${last}Z` : last) : new Date()
})
const dataFloor = computed(() => coverage.value.from)
const dataCeil = computed(() => coverage.value.to ?? latestKnownDt.value)

const {
  binCount, gridlineBins, windowEnd, windowStart, binStarts,
  canPageBack, canPageForward, page, clampWindowEnd, rangeLabel,
  dateMenuOpen, pickedDate, minDateStr, maxDateStr, confirmDatePick,
} = useTimeDepthWindow({
  binMode, dataFloor, dataCeil,
  resetOn: [computed(() => `${point.value?.lat},${point.value?.lng}`), source, varId],
})

// Only meaningful at the live edge — paging back into history is "what
// happened then", not "what's next" — so this tracks whether the window is
// still following "latest" rather than sitting wherever a manual page/jump
// left it. Reset on every trigger that snaps the window back to latest
// (mirrors useTimeDepthWindow's own `resetOn`+binMode watch); cleared by the
// wrapped page()/confirmDatePick() below, and re-set if paging happens to
// land back on the live edge.
const pinnedToLatest = ref(true)
watch([binMode, point, source, varId], () => { pinnedToLatest.value = true })

// The section's own highlighted cell — replaces the old horizontal depth
// line now that there's no companion line chart below the section for a
// line to mean "this is the depth it's plotted at". Derived straight from
// the universal selected instant/depth (`selected_variable.dt`/
// `selectedDepthIdx`) rather than tracked separately, so the mark stays in
// sync no matter what moved that state — a cell click here, the time
// controls, a click on the timeseries chart, the map's own depth picker.
// Falls out of the visible window automatically (no match found) rather than
// needing an explicit reset watcher — paging back into view brings it back.
const selectedCell = computed<{ binIdx: number, depthIdx: number } | null>(() => {
  const dt = mainStore.selected_variable.dt
  if (!dt) return null
  const target = floorToBin(moment.utc(dt).toDate(), binMode.value).getTime()
  const binIdx = binStarts.value.findIndex(b => b.getTime() === target)
  if (binIdx < 0) return null
  return { binIdx, depthIdx: selectedDepthIdx.value }
})

function pageWindow(dir: number) {
  page(dir)
  pinnedToLatest.value = !canPageForward.value
}
function confirmDatePickAndUnpin() {
  confirmDatePick()
  pinnedToLatest.value = !canPageForward.value
}

// The paging window (above) is deliberately clamped to real data — the section
// can't render bins that don't exist. But the single-depth line chart used to
// double as a "coming days" preview: climatology has no dependency on model
// runs, so it can draw out past the last ingested point even though the model
// and sensor lines simply stop there.
//
// Hourly-only: the old feature this restores. Daily/monthly stay strictly
// data-bound — no NOW marker there, since a 1-year/20-year window makes even
// a same-bin nudge toward "today" visually meaningless.
const FUTURE_PREVIEW_DAYS = 5
const chartWindowEnd = computed(() => {
  if (viewMode.value !== 'series' || binMode.value !== 'hourly' || canPageForward.value) return windowEnd.value
  return new Date(windowEnd.value.getTime() + FUTURE_PREVIEW_DAYS * 86400e3)
})

// A mode switch re-anchors the window off the *previous* mode's coverage,
// which can be stale or from a different table entirely (daily/monthly reach
// further back, the hourly table's own ceiling lags "now" differently) — the
// window can land short of the new mode's real ceiling as easily as past it.
// While pinned to latest, always re-snap to the fresh ceiling in *either*
// direction once real coverage lands; only clamp (bounds-check, no snap-up)
// once the user has manually paged or jumped away from the live edge.
//
// Compared by timestamp, not identity: both paths hand back a fresh Date when
// they move the window, and assigning one unconditionally would retrigger the
// fetch that produced this very coverage — a refetch loop that also pins
// `loading` on, disabling the mode toggle.
watch(coverage, () => {
  const next = pinnedToLatest.value ? clampWindowEnd(dataCeil.value) : clampWindowEnd(windowEnd.value)
  if (next.getTime() !== windowEnd.value.getTime()) windowEnd.value = next
})

// ── DATA ──────────────────────────────────────────────────────────────────────
const grid = ref<(number | null)[][]>([])
const sensorGrid = ref<(number | null)[][] | null>(null)
const loading = ref(false)
const loadError = ref<string | null>(null)

/**
 * Set when the API reports this point has no model cell within range — a
 * separate ref from `loadError` on purpose. Clicking a deep-ocean mooring
 * outside the SalishSeaCast domain is a normal, deliberate thing to do, and a
 * red "failed to load" alert framed the user's own valid sensor data as a
 * malfunction. This drives an informational note instead, and lets the context
 * bar say *why* the model line is missing rather than leaving a model depth
 * sitting unexplained next to a sensor-only chart.
 */
const outOfDomain = ref<OutsideDomainError | null>(null)

/** Which model cell answered, and how far it snapped. From the last success. */
const gridCell = ref<{ lat: number, lon: number, distanceKm: number } | null>(null)

/**
 * The out-of-domain banner explains a situation once; the context bar's
 * `MODEL outside domain (N km)` item is the durable readout, so dismissing the
 * banner loses nothing. Scoped to the coordinate rather than to the fetch:
 * paging the window or switching bin mode refetches the same out-of-domain
 * point and must not resurrect a banner the user just closed, while a click on
 * a *different* point is a new thing to explain and earns the banner back.
 */
const outOfDomainDismissed = ref(false)
watch(point, () => { outOfDomainDismissed.value = false })
const showOutOfDomainAlert = computed(() => !!outOfDomain.value && !outOfDomainDismissed.value)

// Far enough that the point the chart describes is not the point that was
// clicked. Below this a snap is just grid resolution and not worth a word.
const FAR_SNAP_KM = 3

/**
 * Only a *profiler* sensor (`sensors.depth === -1`) casts through the whole
 * water column, so only it has something to put beside a model section. A
 * fixed-depth sensor is a single row and belongs in the Comparison pane's
 * timeseries instead. Centralised on the store (`selectedProfilerSensorId`)
 * since index.vue's nav rail needs the same "is a profiler selected" check to
 * decide whether to offer the Sensor depth sub-view at all.
 */
const sensorId = computed(() => mainStore.selectedProfilerSensorId)
const hasSensorGrid = computed(() => !!sensorGrid.value)

/**
 * The selected sensor's own identity and deployment depth — the number the
 * user just read off the sensor card, which the chart and the map box both
 * need to name explicitly so it is never confused with a model level. Any
 * selected sensor, not just a profiler: a fixed-depth mooring is exactly the
 * case that made the two depths collide.
 */
const sensorMeta = computed(() => {
  const sel = mainStore.selectedSensor
  if (!sel?.id) return null
  const rec = mainStore.sensors.find(s => s.id === sel.id)
  // Rounded exactly as sensorInfo.vue's `depth2txt` rounds it. The user is
  // being asked to compare this number against the one on the sensor card;
  // the raw stored float (1256.830810546875) reads as a different figure.
  const depthLabel = sel.depth === -1
    ? 'full column'
    : sel.depth === 0 ? 'surface' : `${sel.depth.toFixed(0)} m`
  return { name: rec?.name ?? 'Sensor', depth: sel.depth, depthLabel }
})

const viewMode = computed(() => mainStore.exploreView)
// Losing the profiler strands sensor-depth with nothing to draw.
watch(hasSensorGrid, (has) => { if (!has && viewMode.value === 'sensor-depth') mainStore.setExploreView('model-depth') })

const showSection = computed(() => viewMode.value !== 'series')
const showingSensor = computed(() => viewMode.value === 'sensor-depth' && hasSensorGrid.value)
const activeGrid = computed(() => showingSensor.value ? sensorGrid.value! : grid.value)
const emptyCellLabel = computed(() => showingSensor.value ? 'no cast in this bin' : 'no model data (below seabed)')



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
  const next: (number | null)[][] = ds.map(() => Array(binCount.value).fill(null))
  const nextSensor: (number | null)[][] | null = resp.sensor
    ? ds.map(() => Array(binCount.value).fill(null))
    : null

  ds.forEach((d, li) => {
    const ri = resp.depths.length ? nearestLevelIdx(d, resp.depths) : null
    if (ri === null) return
    for (let bi = 0; bi < binCount.value; bi++) {
      const ti = timeIndex.get(starts[bi]!.getTime())
      if (ti == null) continue
      next[li]![bi] = resp.model[ri]?.[ti] ?? null
      if (nextSensor) nextSensor[li]![bi] = resp.sensor?.[ri]?.[ti] ?? null
    }
  })
  grid.value = next
  sensorGrid.value = nextSensor
  snapDepthToData(next)

  gridCell.value = resp.grid ?? null
  mainStore.setModelDomainStatus({ inDomain: true, distanceKm: resp.grid?.distanceKm ?? null })

  if (resp.coverage) {
    coverage.value = {
      from: parseCoverageBound(resp.coverage.from),
      to: parseCoverageBound(resp.coverage.to),
    }
  }
}

/**
 * The sensor's own series for the timeseries overlay. Separate from the section
 * fetch on purpose: the section only exists for profilers, but *any* selected
 * sensor should show up on the chart — a fixed-depth mooring at its own depth,
 * a profiler at whichever depth is currently selected.
 *
 * The raw response comes back at the sensor's native reporting cadence, which
 * can be far denser than the model's own bins (e.g. minute-scale readings
 * against monthly model points) — plotted raw, it drowns the model line out.
 * Binned here to the same `binStarts` buckets via `binSeries` (mean, matching
 * how the model's own hourly/monthly bins are aggregated server-side) so the
 * overlay reads as a comparable trace at every bin mode, not a scribble.
 */
const sensorSeries = ref<{ time: string[]; value: (number | null)[] } | null>(null)

/** getSensorTimeseries wants `YYYY-MM-DDTHHmmss`, not a zoned ISO string. */
function toSensorApiDate(d: Date) {
  const iso = d.toISOString()
  return `${iso.slice(0, 10)}T${iso.slice(11, 19).replace(/:/g, '')}`
}

// Guards against an in-flight request's response landing after a newer one —
// a depth-profile fetch resolving mid-request can re-clamp `windowEnd` for
// coverage reasons, so two fetches can easily be outstanding at once.
let sensorFetchSeq = 0

async function fetchSensorOverlay() {
  const sel = mainStore.selectedSensor
  const meta = sel?.id ? mainStore.sensors.find(s => s.id === sel.id) : null
  if (!sel?.id || !meta) { sensorSeries.value = null; return }

  const isProfiler = meta.depth === -1
  const d = isProfiler ? (depths.value[selectedDepthIdx.value] ?? null) : sel.depth
  if (d == null) { sensorSeries.value = null; return }

  // Snapshot the window/bin mode this request is answering for. Reading
  // `binStarts`/`binMode` fresh *after* the await — rather than these
  // captured values — would bin the response against whatever window is
  // current when the response lands, not the one it was actually fetched
  // for, silently mislabelling every point if the window moved meanwhile.
  const reqId = ++sensorFetchSeq
  const mode = binMode.value
  const starts = binStarts.value

  try {
    const resp = await getSensorTimeseries(
      sel.id, varId.value,
      toSensorApiDate(windowStart.value), toSensorApiDate(windowEnd.value),
      d, isProfiler ? source.value : null,
    )
    if (reqId !== sensorFetchSeq) return
    const rawTime: string[] = resp?.data?.time ?? []
    const rawValue: (number | null)[] = resp?.data?.value ?? []
    sensorSeries.value = rawTime.length
      ? { time: starts.map(b => b.toISOString()), value: binSeries(rawTime, rawValue, mode, starts) }
      : { time: [], value: [] }
  } catch {
    if (reqId === sensorFetchSeq) sensorSeries.value = null  // overlay is supplementary; never blank the model line for it
  }
}

let fetchSeq = 0
async function fetchWindow() {
  const p = point.value, src = source.value, v = varId.value
  if (!p || !src || !v) return
  const reqId = ++fetchSeq
  loading.value = true
  loadError.value = null
  outOfDomain.value = null
  try {
    const resp = await fetchDepthProfile({
      source: src,
      var: v,
      lat: p.lat,
      lon: p.lng,
      fromDate: toApiIso(windowStart.value),
      toDate: toApiIso(windowEnd.value),
      binMode: binMode.value,
      ...(sensorId.value ? { sensorId: sensorId.value } : {}),
    })
    if (reqId !== fetchSeq) return
    applyResponse(resp)
  } catch (err: any) {
    if (reqId !== fetchSeq) return
    const domain = asOutsideDomainError(err)
    if (domain) {
      outOfDomain.value = domain
      mainStore.setModelDomainStatus({ inDomain: false, distanceKm: domain.distanceKm })
    } else {
      loadError.value = err?.response?.data?.detail || err?.message || 'Failed to load depth section.'
      mainStore.setModelDomainStatus(null)
    }
    gridCell.value = null
    grid.value = depths.value.map(() => Array(binCount.value).fill(null))
    sensorGrid.value = null
  } finally {
    if (reqId === fetchSeq) loading.value = false
    updateLineChart()
  }
}

// Only fetch while the tab is actually on screen — this sits in a v-show'd
// footer pane alongside three other tabs, and a map click would otherwise
// refetch here for a view nobody is looking at.
// mainStore.unitPreference is included so toggling the display unit
// re-fetches (a cheap cache hit against the same request params — see
// useDepthProfileFetch.ts) and re-converts, rather than leaving the section
// showing stale numbers under the old unit.
watch([windowEnd, depths, binMode, point, sensorId, () => props.active, () => mainStore.unitPreference[varId.value]], () => {
  if (props.active) fetchWindow()
}, { immediate: true })


// ── COLOUR RAMP ───────────────────────────────────────────────────────────────
// The section is a rendering of the same field the map paints, so it reads off
// the map's own colorbar — palette and min/max both. No picker and no second
// colorbar here: two independent scales for one variable made identical values
// look different between the map and the section directly below it. Changing
// the map's Color Settings restyles this automatically.
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

const SEQ = [hexToRgb('#0d366b'), hexToRgb('#3987e5'), hexToRgb('#cde2fb')]

const resolvedSeqStops = computed(() =>
  resolveColormap(mainStore.colormaps, mainStore.selected_variable.colormap)?.stops ?? null)

// Map colorbar bounds, with the variable's configured defaults as a fallback for
// the moment before the map has published its own. Converted to the current
// display unit — `grid`'s values are (useDepthProfileFetch.ts converts at the
// source), so these normalization bounds have to match or every cell would
// clamp to one end of the colour ramp under a non-canonical unit.
const seqMin = computed(() => toDisplayValue(varId.value, mainStore.selected_variable.colormapMin ?? varMeta.value?.colormapMin ?? 0) ?? 0)
const seqMax = computed(() => {
  const hi = toDisplayValue(varId.value, mainStore.selected_variable.colormapMax ?? varMeta.value?.colormapMax ?? 1) ?? 1
  const lo = seqMin.value
  return hi > lo ? hi : lo + 1
})

// Wrapped in a `computed` so its identity changes with the palette or the data
// range — that identity is what makes TimeDepthHeatmap repaint cells whose
// underlying values did not change.
const seqColorFn = computed(() => {
  const stops = resolvedSeqStops.value
  const lo = seqMin.value, hi = seqMax.value
  return (v: number) => {
    const t = hi === lo ? 0.5 : Math.max(0, Math.min(1, (v - lo) / (hi - lo)))
    if (stops && stops.length) return rgbCss(colorFromStops(stops, t))
    return rgbCss(t < 0.5 ? lerpRgb(SEQ[0]!, SEQ[1]!, t * 2) : lerpRgb(SEQ[1]!, SEQ[2]!, (t - 0.5) * 2))
  }
})



// ── PANEL / HOVER ─────────────────────────────────────────────────────────────
const modelPanel = ref<InstanceType<typeof TimeDepthHeatmap> | null>(null)
const sectionH = ref(140)
const chartH = ref(200)

function xLabel(binIdx: number) {
  const d = binStarts.value[Math.round(binIdx)]
  if (!d) return ''
  return binMode.value === 'monthly'
    ? d.toLocaleDateString('en-US', { month: 'short', year: 'numeric', timeZone: 'UTC' })
    : d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' })
}

// Per-cell tooltip, rendered by TimeDepthHeatmap's own ECharts tooltip rather
// than a shared info-row bar (see that component's `tooltipFormatter` prop
// comment for why single-panel consumers can use the native tooltip while
// Comparison's stacked model/sensor view still shares one row).
function cellTooltip(binIdx: number, depthIdx: number, value: number | null): string {
  const depth = depths.value[depthIdx]
  const dt = binStarts.value[binIdx]
  const dtOpts: Intl.DateTimeFormatOptions = binMode.value === 'hourly'
    ? { month: 'short', day: 'numeric', hour: 'numeric', timeZone: 'UTC' }
    : binMode.value === 'monthly'
      ? { month: 'short', year: 'numeric', timeZone: 'UTC' }
      : { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' }
  const dtStr = dt ? dt.toLocaleString('en-US', dtOpts) : '—'
  const depthStr = depth != null ? `${depth.toFixed(depth < 10 ? 1 : 0)}m` : '—'
  const valueStr = value == null ? 'no data' : `${value.toFixed(3)} ${displayUnit(varId.value)}`
  return `<div style="opacity:.7;margin-bottom:2px;">${dtStr} &middot; ${depthStr}</div><div>${varName.value}: <b>${valueStr}</b></div>`
}

// ── DEPTH SELECTION — two-way with the map's own depth control, so picking a
// level here re-renders the raster layer at that depth and vice versa. ────────
const selectedDepthIdx = ref(0)
const depthLabel = computed(() => {
  const d = depths.value[selectedDepthIdx.value]
  return d != null ? `${d.toFixed(d < 10 ? 1 : 0)} m` : '—'
})
const depthHasData = computed(() => (grid.value[selectedDepthIdx.value] ?? []).some(v => v != null))

/**
 * True when `snapDepthToData` has pulled the chart off the map's own depth
 * because that level is below the seabed at this cell. The map layer keeps its
 * depth (deliberately — see snapDepthToData), so the two readouts legitimately
 * differ and the bar has to account for the gap.
 */
const depthSnappedFromMap = computed(() => {
  const mapDepth = mainStore.selected_variable.depth_nc
  if (mapDepth == null || mapDepth < 0) return false
  const shown = depths.value[selectedDepthIdx.value]
  return shown != null && Math.abs(shown - mapDepth) > 0.01
})

// The old wording ("No model data at 442 m here") blamed the depth for what is
// usually a location problem. In-domain that is the right diagnosis — the level
// sits below the seabed at this cell — so name the seabed rather than leaving
// the user to guess. The out-of-domain case never reaches here: it has its own
// alert above the chart.
const emptyLineMessage = computed(() =>
  `No model data at ${depthLabel.value} here — the seabed is shallower than this level`)

// ── CHART CONTEXT — what the chart below is actually plotting, as opposed to
// selectedInfo.vue's map-corner box (map layer only). Depth is omitted for
// the heatmap sections: they plot every depth against time, so a single
// selected depth would misleadingly suggest the chart is scoped to it.
//
// Depths are labelled per source rather than as one bare "Depth". The two
// genuinely differ — a mooring at 1257 m overlaid on the nearest model level
// at 442 m — and the unattributed number read as a claim about whichever line
// the user happened to be looking at. Each source appears only when it is
// actually on screen, and carries its own reason when it has nothing to draw.
const contextItems = computed<ContextItem[]>(() => {
  const items: ContextItem[] = [{ label: 'Field', value: varName.value }]

  // Model — the sections plot every depth, so only the series view names one.
  if (outOfDomain.value) {
    items.push({
      label: 'Model',
      value: `outside domain (${outOfDomain.value.distanceKm.toFixed(0)} km)`,
      tone: 'warn',
      title: outOfDomain.value.message,
    })
  } else if (!showSection.value) {
    if (!depthHasData.value) {
      items.push({ label: 'Model', value: `${depthLabel.value} — no data`, tone: 'muted', title: `The model has no values at ${depthLabel.value} here.` })
    } else if (depthSnappedFromMap.value) {
      // The one remaining way the chart's depth and the map box's depth can
      // disagree, now that both are labelled. Say why rather than leaving two
      // different numbers on screen with nothing joining them.
      items.push({
        label: 'Model',
        value: `${depthLabel.value} — seabed`,
        title: `The map layer is at ${mainStore.selected_variable.depth} m, which is below the seabed here. The chart shows the deepest level with data.`,
      })
    } else {
      items.push({ label: 'Model', value: depthLabel.value })
    }
  }

  // Sensor — named whenever its series is the one on screen, or overlaid on it.
  if (sensorMeta.value && viewMode.value !== 'model-depth') {
    items.push({
      label: 'Sensor',
      value: sensorMeta.value.depthLabel,
      title: sensorMeta.value.name,
    })
  }

  items.push({ label: 'Range', value: rangeLabel.value })
  if (point.value) items.push({ label: 'Point', value: `${point.value.lat.toFixed(4)}, ${point.value.lng.toFixed(4)}` })

  // Only worth saying when the answering cell is not effectively the point
  // clicked — otherwise every reading carries a distracting "0.1 km away".
  if (gridCell.value && gridCell.value.distanceKm > FAR_SNAP_KM) {
    items.push({
      label: 'Model cell',
      value: `${gridCell.value.distanceKm.toFixed(1)} km away`,
      tone: 'muted',
      title: `Nearest model grid cell: ${gridCell.value.lat.toFixed(4)}, ${gridCell.value.lon.toFixed(4)}`,
    })
  }
  return items
})

// ── CSV EXPORT ──────────────────────────────────────────────────────────────
// This panel is the host: it owns the point, depth, window and bin mode, so it
// provides the context and renders the one download control in its toolbar.
// The section grid registers here; the timeseries sub-view's own files come
// from TimeseriesChart, which unmounts with the sub-view and so takes its
// registrations with it.
const csvContext = computed<CsvContext | null>(() => {
  if (!point.value || !varId.value) return null
  const sensorName = mainStore.sensors.find(s => s.id === mainStore.selectedSensor?.id)?.name
  return {
    source: showingSensor.value ? 'sensor' : 'model',
    sourceLabel: showingSensor.value ? `sensor — ${sensorName ?? ''}` : 'SalishSeaCast model',
    variable: varId.value,
    variableName: varName.value,
    unit: displayUnit(varId.value),
    // A section spans the whole water column, so no single depth describes it.
    depth: showSection.value ? null : (depths.value[selectedDepthIdx.value] ?? null),
    locationLabel: `${Math.abs(point.value.lat).toFixed(3)}${point.value.lat >= 0 ? 'N' : 'S'} ${Math.abs(point.value.lng).toFixed(3)}${point.value.lng >= 0 ? 'E' : 'W'}`,
    timeRange: [csvTimestamp(windowStart.value), csvTimestamp(chartWindowEnd.value)],
  }
})

const csvExport = provideCsvExport(csvContext)
const csvDatasets = csvExport.datasets

/**
 * The section, long rather than wide: one row per (time, depth) cell. A grid of
 * depths across the top would need the depth levels in the header, and those
 * change with the model level set — long format survives that and joins against
 * anything. Empty cells are dropped rather than written as blanks: below the
 * seabed that's most of the grid.
 */
const csvSectionRows = computed(() => {
  const g = activeGrid.value
  const ds = depths.value
  const starts = binStarts.value
  const rows: Record<string, unknown>[] = []
  for (let bi = 0; bi < binCount.value; bi++) {
    const t = csvTimestamp(starts[bi])
    for (let li = 0; li < ds.length; li++) {
      const v = g[li]?.[bi]
      if (v == null) continue
      rows.push({ time: t, depth: ds[li], value: v })
    }
  }
  return rows
})

csvExport.register((): CsvDataset[] => {
  if (!showSection.value || !csvSectionRows.value.length) return []
  const u = displayUnit(varId.value) ? ` (${displayUnit(varId.value)})` : ''
  return [{
    label: showingSensor.value ? 'Sensor depth section' : 'Model depth section',
    slug: showingSensor.value ? 'sensor-depth-section' : 'model-depth-section',
    omitDatasetLine: true,
    columns: [
      { header: 'time', accessorKey: 'time' },
      { header: 'depth_m', accessorKey: 'depth' },
      { header: `value${u}`, accessorKey: 'value' },
    ],
    rows: csvSectionRows.value,
    meta: csvMeta(csvContext.value, [
      ['bin_mode', BIN_CONFIG[binMode.value].label],
      ['note', showingSensor.value
        ? 'sensor casts binned onto the model time/depth grid; bins with no cast are omitted'
        : 'cells with no model data (below the seabed) are omitted'],
    ]),
  }]
})

/**
 * The selected depth is inherited from the map control, which is set for the
 * whole domain — at a shallow point it routinely sits below the seabed, where
 * the single-depth chart would render blank with no explanation. Fall back to
 * the deepest level that actually has data, i.e. the seabed here.
 *
 * Deliberately local: it does not write back to the store, so merely opening
 * this tab never moves the map's raster layer to a different depth. Only an
 * explicit cell click does that.
 */
function snapDepthToData(g: (number | null)[][]) {
  if (g[selectedDepthIdx.value]?.some(v => v != null)) return
  for (let i = g.length - 1; i >= 0; i--) {
    if (g[i]?.some(v => v != null)) { selectedDepthIdx.value = i; return }
  }
}

watch(depths, (ds) => {
  selectedDepthIdx.value = nearestLevelIdx(mainStore.selected_variable.depth_nc ?? 14, ds)
}, { immediate: true })

watch(() => mainStore.selected_variable.depth_nc, (d) => {
  if (d == null || d < 0 || !depths.value.length) return
  selectedDepthIdx.value = nearestLevelIdx(d, depths.value)
})

watch([selectedDepthIdx, viewMode], updateLineChart)

// Kept here rather than beside the section fetch: this names `selectedDepthIdx`,
// and a watcher's source array is evaluated during setup, so it has to come
// after that ref is declared.
watch(
  [windowEnd, binMode, varId, () => mainStore.selectedSensor?.id, selectedDepthIdx, () => props.active, () => mainStore.unitPreference[varId.value]],
  async () => {
    if (!props.active) return
    await fetchSensorOverlay()
    updateLineChart()
  },
  { immediate: true },
)

function onCellClick({ binIdx, depthIdx }: { binIdx: number, depthIdx: number }) {
  selectedDepthIdx.value = depthIdx
  const d = depths.value[depthIdx]
  const partial: Partial<typeof mainStore.selected_variable> = {}
  if (d != null) { partial.depth = formatDepthLabel(d); partial.depth_nc = d }
  const start = binStarts.value[binIdx]
  // Drives the universal selected instant (`selected_variable.dt`) — read by
  // the map's own clock, the vertical profile drawer, and (via `selectedCell`
  // above) this same highlight, in every bin mode. Without this, daily/
  // monthly clicks left dt frozen at its initial value and only refetched a
  // tile when the depth row also happened to change.
  if (start) {
    if (binMode.value === 'hourly') {
      // `start` is floored to the hour (the hourly table's own bin convention),
      // but SalishSeaCast's raster tiles are keyed by the model's actual output
      // instant — offset to :30 past the hour, not on it (see api/modules/
      // variables.py). Snap to the nearest real timestamp instead of handing
      // the raster layer an hour it has no tile for.
      const dts = varMeta.value?.dts
      if (dts?.length) {
        const target = start.getTime()
        let best = 0
        for (let i = 1; i < dts.length; i++) {
          if (Math.abs(dts[i] - target) < Math.abs(dts[best] - target)) best = i
        }
        partial.dt = moment.utc(dts[best])
      } else {
        partial.dt = moment.utc(start)
      }
    } else {
      partial.dt = moment.utc(start)
    }
  }
  if (Object.keys(partial).length) mainStore.updateSelectedVariable(partial)
}

// ── SINGLE-DEPTH LINE CHART ───────────────────────────────────────────────────
// Shared with the heatmap above so the two plot areas line up exactly.
const AXIS_LEFT_PX = 44
const DATAZOOM_RIGHT_PX = 26
const lineChart = ref<InstanceType<typeof TimeseriesChart> | null>(null)

/**
 * Feed the shared timeseries chart from data we already hold.
 *
 * The model series is sliced straight out of `grid` — the section above was
 * fetched at every depth, so the selected level costs no extra request. The
 * climatology envelope does need one: `/extract_climateTimeseries` returns
 * one point per *calendar day* in the window at daily/hourly bin mode
 * (deduped by month/day before querying, so a 20-year window is no more
 * expensive than a 14-day one) — a single annual cycle, repeated across the
 * window's span. At monthly bin mode it instead requests month-of-year
 * aggregation (`binMode`), one point per calendar month, so the overlay's
 * resolution matches the monthly view's own coarse bins instead of showing
 * day-level texture stretched across the window's two decades.
 */
async function updateLineChart() {
  // The chart only exists in the series sub-view now — depth sections are
  // heatmap-only. `lineChart.value` is already null once viewMode moves away
  // (its `v-if` unmounts the component), but guard on viewMode directly too so
  // an in-flight call from just before the switch doesn't race the teardown.
  if (!lineChart.value || viewMode.value !== 'series') return
  const d = selectedDepthIdx.value
  const model = {
    time: binStarts.value.map(b => b.toISOString()),
    value: binStarts.value.map((_, bi) => grid.value[d]?.[bi] ?? null),
  }

  let clim: any = null
  if (point.value && depths.value[d] != null) {
    try {
      // Pad a day on each side: the API places each calendar day's point at
      // noon UTC (extract_climate_timeseries.py), but the chart's axis bounds
      // are these same window edges rendered in local (Pacific) time — up to
      // ~8h off from UTC midnight. Without padding, the first/last real point
      // can land inside the local window instead of past its edge, so the
      // band/mean line stops short of the axis instead of running to it.
      const resp = await fetchClimateTimeseries({
        variable: varId.value,
        lat: point.value.lat,
        lon: point.value.lng,
        depth: depths.value[d]!,
        fromDate: toApiIso(new Date(windowStart.value.getTime() - 86400e3)),
        toDate: toApiIso(new Date(chartWindowEnd.value.getTime() + 86400e3)),
        binMode: binMode.value,
      })
      clim = resp?.data ?? null
    } catch {
      clim = null  // envelope is supplementary; a failure must not blank the model line
    }
  }

  const sensor = sensorSeries.value?.time.length
    ? { ...sensorSeries.value, depth: depths.value[d] }
    : null
  // Re-check: the awaited climate fetch above can outlive a sub-view switch,
  // which unmounts this chart (`v-if`) out from under the in-flight call.
  if (!lineChart.value || viewMode.value !== 'series') return
  lineChart.value.plot(model, clim, sensor)
}

// ── LAYOUT — heatmap and line chart split the space left after the fixed
// chrome, measured as a span between real rendered rects (Vuetify utility
// margins can CSS-collapse and vanish from getBoundingClientRect().height,
// which under-counts chrome and overflows the box). ───────────────────────────
const rootRef = ref<HTMLDivElement | null>(null)
const depthHeaderRef = ref<HTMLDivElement | null>(null)

function computeLayout() {
  if (!rootRef.value || !depthHeaderRef.value) return
  const rootRect = rootRef.value.getBoundingClientRect()
  const headerRect = depthHeaderRef.value.getBoundingClientRect()
  // Everything from the root's top down through the info row, minus the part
  // this function itself sets (the heatmap panels), is fixed chrome. Measuring
  // the span between two real rendered rects avoids counting Vuetify utility
  // margins that CSS-collapse and vanish from getBoundingClientRect().height.
  const consumed = headerRect.bottom - rootRect.top
  // The section is only part of `consumed` when it is actually rendered.
  const fixedChrome = consumed - (showSection.value ? sectionH.value : 0)
  const available = rootRect.height - fixedChrome
  // Only one of section/chart is ever rendered now — whichever it is gets the
  // pane's full height instead of splitting it with the other.
  if (showSection.value) {
    sectionH.value = Math.max(60, available)
  } else {
    chartH.value = Math.max(80, available)
  }
}

function resizeAll() {
  modelPanel.value?.resize()
  lineChart.value?.resize()
}


let resizeObs: ResizeObserver | null = null

onMounted(async () => {
  await nextTick()
  computeLayout()
  await nextTick()
  updateLineChart()
  resizeAll()
  if (typeof ResizeObserver !== 'undefined') {
    resizeObs = new ResizeObserver(() => { computeLayout(); resizeAll() })
    if (rootRef.value) resizeObs.observe(rootRef.value)
  }
})

// TimeseriesChart only exists inside the `v-else` branch that renders once a map
// point is picked, so its first appearance — not onMounted — is when there is
// finally something to draw into.
watch(lineChart, (c) => {
  if (!c) return
  updateLineChart()
  nextTick().then(() => { computeLayout(); resizeAll() })
})

// The footer keeps this pane mounted but hidden (v-show), so ECharts measures a
// zero-size canvas while it is off screen — re-measure when it comes back.
watch(() => props.active, async (isActive) => {
  if (!isActive) return
  await nextTick()
  computeLayout()
  resizeAll()
})

// Two ticks, not one: `computeLayout` only *assigns* `chartH`/`sectionH`, and
// the `:style` binding that turns those into real pixels is applied on the
// following tick — so resizing ECharts in the same turn measures the old box
// and paints a chart shorter than its container. onMounted already sequences
// it this way; this watcher did not, which showed up the moment the
// out-of-domain banner became dismissible (the ResizeObserver watches the
// panel root, whose own size never changes, so nothing corrected it after).
watch([loadError, showOutOfDomainAlert, point, showSection], async () => {
  await nextTick()
  computeLayout()
  await nextTick()
  resizeAll()
})

onBeforeUnmount(() => {
  resizeObs?.disconnect()
})
</script>

<style scoped>
.model-depth-profile {
  min-height: 0;
}

/* TimeControls is a v-row, whose negative Vuetify gutters collapse this
   wrapper to zero height unless neutralised directly. */
.time-controls-row :deep(.time-controls) {
  margin: 0 !important;
  flex-wrap: nowrap;
  align-items: center;
}

.chart-region {
  position: relative;
}

.loading-overlay {
  position: absolute;
  inset: 0;
  z-index: 20;
  display: flex;
  align-items: center;
  justify-content: center;
  background: rgba(10, 14, 18, 0.4);
  border-radius: 6px;
  pointer-events: none;
}

.range-label {
  min-width: 150px;
  text-align: center;
  color: rgba(255, 255, 255, 0.6);
  font-variant-numeric: tabular-nums;
}

.range-label--clickable {
  cursor: pointer;
  border-radius: 3px;
}

.range-label--clickable:hover {
  color: #fff;
  text-decoration: underline dotted;
}

.hm-slot {
  margin-bottom: 3px;
}

.info-row {
  display: flex;
  align-items: center;
  gap: 16px;
  margin-top: 6px;
  padding: 4px 10px;
  min-height: 30px;
  background: rgba(255, 255, 255, 0.03);
  border-radius: 4px;
  font-size: 11.5px;
}

.info-row--compact {
  margin-top: 0;
  padding: 0;
  min-height: 0;
  background: transparent;
}

.legend {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-shrink: 0;
}

.legend-item {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.legend-label {
  font-size: 9.5px;
  color: rgba(255, 255, 255, 0.4);
}

.ramp {
  width: 130px;
  height: 7px;
  border-radius: 4px;
}

.ramp-div {
  background: linear-gradient(90deg, #2b6cb0, #ffffff, #c53030);
}

.ramp-ticks {
  display: flex;
  justify-content: space-between;
  font-size: 9px;
  color: rgba(255, 255, 255, 0.4);
  width: 130px;
  font-variant-numeric: tabular-nums;
}

.swatch-hatch {
  width: 18px;
  height: 11px;
  border-radius: 2px;
  background-color: #1a232c;
  background-image: repeating-linear-gradient(45deg, rgba(255, 255, 255, 0.22) 0, rgba(255, 255, 255, 0.22) 1px, transparent 1px, transparent 6px);
  border: 1px solid rgba(255, 255, 255, 0.09);
}

.line-slot {
  position: relative;
  margin-top: 3px;
}

.line-empty {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 11px;
  color: rgba(255, 255, 255, 0.3);
  pointer-events: none;
}
</style>
