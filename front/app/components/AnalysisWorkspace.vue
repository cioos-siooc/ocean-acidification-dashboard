<template>
  <UModal v-model:open="isOpen" fullscreen :overlay="false">
    <template #content>
    <div class="flex flex-col bg-default" style="height:100vh;">
      <div class="flex items-center gap-2 px-3 h-12 bg-elevated shrink-0">
        <div class="font-medium truncate">
          Analysis — {{ varName }}
          <UBadge size="xs" color="warning" variant="subtle" class="ml-2 rounded-full" v-if="contextLabel">{{ contextLabel }}</UBadge>
        </div>
        <div class="grow" />
        <ShareButton />
        <DownloadButton :datasets="csvDatasets" class="shrink-0" />
        <UButton variant="ghost" icon="i-mdi-close" class="shrink-0" title="Close (Esc)" @click="isOpen = false" />
      </div>

      <UTabs v-model="activeTab" :items="tabItems" :content="false" class="shrink-0" />

      <div class="grow" style="min-height:0; overflow:hidden;">
        <!-- Overview keeps its own fetch and chart; the deep-dive tabs share one
             series fetched here. v-show so switching tabs never refetches. -->
        <div v-show="activeTab === 'builder'" style="height:100%;">
          <AnalysisBuilder :active="isOpen && activeTab === 'builder'" :source="source" />
        </div>

        <div v-if="activeTab !== 'builder'" class="h-full" style="overflow:auto;">
          <div class="flex items-center px-4 pt-3" style="gap:10px;">
            <span class="ctrl-label">Season</span>
            <SegmentedControl v-model="selectedSeason" :items="seasonItems" size="xs" aria-label="Season" />
          </div>

          <UAlert color="error" variant="subtle" class="m-4" v-if="primaryError" :description="primaryError" />

          <div v-else-if="!location || !variable || depth == null"
            class="flex flex-col items-center justify-center text-center px-6" style="height:60vh;">
            <UIcon name="i-mdi-poll" class="size-[56px] text-gray-500" />
            <div class="text-gray-500 mt-2">
              {{ source === 'sensor' ? 'Select a sensor and a depth first.' : 'Select a point, variable and depth first.' }}
            </div>
          </div>

          <div v-else-if="primaryLoading && !primarySeries.length"
            class="flex items-center justify-center" style="height:60vh;">
            <UIcon name="i-mdi-loading" class="animate-spin size-[48px] text-warning" />
          </div>

          <template v-else>
            <ExtremeEvents v-if="activeTab === 'extremes'" :series="primarySeries" :season="selectedSeason" :variable="variable" />
            <CompoundStress v-else-if="activeTab === 'compound'"
              :primary-series="primarySeries" :primary-variable="variable" :season="selectedSeason"
              :depth="depth" :location="location" :year-range="yearRange" :fetch-series="fetchSeriesFor" />
            <Trend v-else-if="activeTab === 'trend'" :series="primarySeries" :season="selectedSeason" :variable="variable" />
            <Climatology v-else-if="activeTab === 'climatology'" :series="primarySeries" :season="selectedSeason" :variable="variable" />
            <Correlation v-else-if="activeTab === 'correlation'"
              :primary-series="primarySeries" :primary-variable="variable" :season="selectedSeason"
              :depth="depth" :location="location" :year-range="yearRange" :fetch-series="fetchSeriesFor" />
          </template>
        </div>
      </div>
    </div>
    </template>
  </UModal>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useMainStore } from '../stores/main'
import { fetchAnalysisSeries, type SeriesPoint, type AnalysisLocation } from '~~/composables/useAnalysisFetch'
import { fetchSensorAnalysisSeries } from '~~/composables/useSensorAnalysisFetch'
import { availableVariables } from '~~/composables/useAnalysisStatistics'
import { useVariableRegistry } from '~~/composables/useVariableRegistry'
import { csvMeta, provideCsvExport, type CsvContext, type CsvDataset } from '~~/composables/useCsvExport'
import DownloadButton from './ui/DownloadButton.vue'
import ShareButton from './ShareButton.vue'
import AnalysisBuilder from './analysis/AnalysisBuilder.vue'
import ExtremeEvents from './analysis/ExtremeEvents.vue'
import CompoundStress from './analysis/CompoundStress.vue'
import Trend from './analysis/Trend.vue'
import Climatology from './analysis/Climatology.vue'
import Correlation from './analysis/Correlation.vue'
import SegmentedControl from './ui/SegmentedControl.vue'
const seasonItems = [{ value: 'full_year', label: 'All' }, { value: 'mam', label: 'MAM' }, { value: 'jja', label: 'JJA' }, { value: 'son', label: 'SON' }, { value: 'djf', label: 'DJF' }]
const tabItems = [{ value: 'builder', label: 'Overview' }, { value: 'extremes', label: 'Extreme Events' }, { value: 'compound', label: 'Compound Stress' }, { value: 'trend', label: 'Trend' }, { value: 'climatology', label: 'Climatology Anomaly' }, { value: 'correlation', label: 'Correlation' }]

/**
 * Analysis workspace — fullscreen, because nothing in it is tied to the map.
 * It takes a coordinate (or a sensor) as input and then works on that record
 * alone, so a map behind it is dead space and a footer strip is the worst place
 * to read statistics.
 *
 * This replaces the old compact pane + "Advanced Analysis" dialog pair: with the
 * whole surface already fullscreen, a second fullscreen dialog on top of it had
 * nothing left to offer. The former advanced views are now simply tabs.
 */

const isOpen = defineModel<boolean>()

const mainStore = useMainStore()

const source = computed(() => mainStore.analysisSource)
const hasSensor = computed(() => !!mainStore.selectedSensor?.id)
const sensorInfo = computed(() => mainStore.sensors.find(s => s.id === mainStore.selectedSensor?.id) ?? null)

// Deselecting the sensor would strand the workspace on an empty source.
watch(hasSensor, (has) => { if (!has && source.value === 'sensor') mainStore.setAnalysisSource('model') })

const variable = computed(() => mainStore.selected_variable.var)
const varName = computed(() => availableVariables.find(v => v.id === variable.value)?.name || variable.value || 'Variable')

const isVariableDepth = computed(() => sensorInfo.value?.depth === -1)
const depth = computed(() => {
  if (source.value === 'sensor' && !isVariableDepth.value) return mainStore.selectedSensor?.depth ?? null
  return mainStore.selected_variable.depth_nc
})

const contextLabel = computed(() => {
  if (source.value === 'sensor') return sensorInfo.value?.name ?? ''
  const pt = mainStore.lastClickedMapPoint
  return pt ? `${pt.lat.toFixed(3)}, ${pt.lng.toFixed(3)}` : ''
})

const location = computed<AnalysisLocation | null>(() => {
  if (source.value === 'sensor') return sensorInfo.value ? { sensorId: sensorInfo.value.id } : null
  const pt = mainStore.lastClickedMapPoint
  return pt ? { lat: pt.lat, lon: pt.lng } : null
})

const currentYear = new Date().getFullYear()
const yearRange = computed<[number, number]>(() => {
  if (source.value !== 'sensor') return [2007, 2026]
  const from = sensorInfo.value?.first_data_at ? parseInt(sensorInfo.value.first_data_at.slice(0, 4), 10) : currentYear
  const to = sensorInfo.value?.latest_data_at ? parseInt(sensorInfo.value.latest_data_at.slice(0, 4), 10) : currentYear
  return [from, to]
})

// Tab and season live on the store rather than in local refs so a share link
// can restore which analysis the sender was looking at — same reasoning that
// moved `exploreView`/`exploreBinMode` out of ExplorePanel.
type AnalysisTab = 'builder' | 'extremes' | 'compound' | 'trend' | 'climatology' | 'correlation'
const activeTab = computed<AnalysisTab>({
  get: () => mainStore.analysisTab as AnalysisTab,
  set: (t) => mainStore.setAnalysisTab(t),
})
const selectedSeason = computed<string>({
  get: () => mainStore.analysisSeason,
  set: (v) => mainStore.setAnalysisSeason(v),
})

// ── CSV EXPORT ──────────────────────────────────────────────────────────────
// The workspace owns the query (point/sensor, variable, depth, window) so it
// provides the context; each deep-dive tab registers the files it can produce
// and the single header button offers whatever the active tab registered.
const { displayUnit } = useVariableRegistry()

/** '49.283N 123.121W' — hemisphere letters keep the sign out of filenames. */
function formatLatLon(lat: number, lon: number) {
  return `${Math.abs(lat).toFixed(3)}${lat >= 0 ? 'N' : 'S'} ${Math.abs(lon).toFixed(3)}${lon >= 0 ? 'E' : 'W'}`
}

const csvContext = computed<CsvContext | null>(() => {
  if (!variable.value || !location.value) return null
  const pt = mainStore.lastClickedMapPoint
  return {
    source: source.value,
    sourceLabel: source.value === 'sensor'
      ? `sensor — ${sensorInfo.value?.name ?? ''}`
      : 'SalishSeaCast model',
    variable: variable.value,
    variableName: varName.value,
    unit: displayUnit(variable.value),
    depth: depth.value,
    locationLabel: source.value === 'sensor'
      ? (sensorInfo.value?.name ?? sensorInfo.value?.id ?? '')
      : (pt ? formatLatLon(pt.lat, pt.lng) : ''),
    timeRange: [`${yearRange.value[0]}-01-01`, `${yearRange.value[1]}-12-31`],
    season: selectedSeason.value,
  }
})

const csvExport = provideCsvExport(csvContext)
const csvDatasets = csvExport.datasets
// ── PRIMARY SERIES — fetched once per point/variable/depth, shared by the
// deep-dive tabs (the Overview tab runs its own, since it also varies by stat).
const primarySeries = ref<SeriesPoint[]>([])
const primaryLoading = ref(false)
const primaryError = ref<string | null>(null)
let primaryRequestId = 0

function fetchSeriesFor(variableId: string, depthVal: number, loc: AnalysisLocation): Promise<SeriesPoint[]> {
  if ('sensorId' in loc) {
    const [from, to] = [`${yearRange.value[0]}-01-01T000000`, `${yearRange.value[1]}-12-31T235959`]
    return fetchSensorAnalysisSeries(loc.sensorId, variableId, 'mean', depthVal, from, to,
      isVariableDepth.value ? mainStore.selected_variable.source : null)
  }
  return fetchAnalysisSeries({ variable: variableId, stat: 'mean', depth: depthVal, location: loc, yearRange: yearRange.value })
}

async function fetchPrimary() {
  if (!variable.value || depth.value == null || !location.value) return
  const requestId = ++primaryRequestId
  primaryLoading.value = true
  primaryError.value = null
  try {
    const data = await fetchSeriesFor(variable.value, depth.value, location.value)
    if (requestId !== primaryRequestId) return
    primarySeries.value = data
  } catch (err: any) {
    if (requestId !== primaryRequestId) return
    primaryError.value = err?.response?.data?.detail || err?.message || 'Failed to load data.'
  } finally {
    if (requestId === primaryRequestId) primaryLoading.value = false
  }
}

// Only fetch once a deep-dive tab is actually asked for — the Overview tab
// doesn't use this series, and most visits never leave it. mainStore.unitPreference
// is included so toggling the display unit re-fetches (a cheap cache hit — see
// useAnalysisFetch.ts/useSensorAnalysisFetch.ts) rather than every deep-dive tab
// showing stale numbers under the old unit.
watch([isOpen, activeTab, location, variable, depth, () => mainStore.unitPreference[variable.value]], () => {
  if (isOpen.value && activeTab.value !== 'builder') fetchPrimary()
})

// The series every deep-dive tab charts, offered from all of them — each tab
// then adds whatever it derives on top. Not on Overview: that tab runs its own
// fetch (it varies by stat too) and registers its own file.
csvExport.register((): CsvDataset[] => {
  if (activeTab.value === 'builder' || !primarySeries.value.length) return []
  const unit = displayUnit(variable.value)
  return [{
    label: 'Daily series',
    slug: 'daily-series',
    columns: [
      { header: 'time', accessorKey: 'time' },
      { header: unit ? `value (${unit})` : 'value', accessorKey: 'value' },
    ],
    rows: primarySeries.value as unknown as Record<string, unknown>[],
    meta: csvMeta(csvContext.value, [
      ['note', 'the full daily record — the season filter applies to the tabs\' own derived files, not this one'],
    ]),
  }]
})

// Secondary variables (Compound Stress / Correlation) call fetchSeriesFor
// directly — fetchAnalysisSeries/fetchSensorAnalysisSeries already dedupe and
// cache underneath (see composables/useRequestCache.ts), so no component-local
// cache is needed here.
</script>

<style scoped>
.ctrl-label {
  font-size: 0.63rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: rgba(255, 255, 255, 0.6);
}
</style>
