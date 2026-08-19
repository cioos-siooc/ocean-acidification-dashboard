<template>
  <v-dialog v-model="isOpen" fullscreen transition="dialog-bottom-transition" :scrim="false">
    <v-card class="d-flex flex-column" style="height:100vh;">
      <v-toolbar color="grey-darken-4" density="compact">
        <v-toolbar-title class="text-body-2">
          Analysis — {{ varName }}
          <v-chip v-if="contextLabel" size="x-small" color="warning" variant="tonal" class="ml-2">{{ contextLabel }}</v-chip>
        </v-toolbar-title>
        <v-spacer />
        <v-btn icon="mdi-close" variant="text" title="Close (Esc)" @click="isOpen = false" />
      </v-toolbar>

      <v-tabs v-model="activeTab" color="warning" density="compact" class="flex-shrink-0">
        <v-tab value="builder">Overview</v-tab>
        <v-tab value="extremes">Extreme Events</v-tab>
        <v-tab value="compound">Compound Stress</v-tab>
        <v-tab value="trend">Trend</v-tab>
        <v-tab value="climatology">Climatology Anomaly</v-tab>
        <v-tab value="correlation">Correlation</v-tab>
      </v-tabs>

      <div class="flex-grow-1" style="min-height:0; overflow:hidden;">
        <!-- Overview keeps its own fetch and chart; the deep-dive tabs share one
             series fetched here. v-show so switching tabs never refetches. -->
        <div v-show="activeTab === 'builder'" style="height:100%;">
          <AnalysisBuilder :active="isOpen && activeTab === 'builder'" :source="source" />
        </div>

        <div v-if="activeTab !== 'builder'" class="h-100" style="overflow:auto;">
          <div class="d-flex align-center px-4 pt-3" style="gap:10px;">
            <span class="ctrl-label">Season</span>
            <v-btn-toggle v-model="selectedSeason" mandatory variant="tonal" density="compact">
              <v-btn value="full_year" size="x-small">All</v-btn>
              <v-btn value="mam" size="x-small">MAM</v-btn>
              <v-btn value="jja" size="x-small">JJA</v-btn>
              <v-btn value="son" size="x-small">SON</v-btn>
              <v-btn value="djf" size="x-small">DJF</v-btn>
            </v-btn-toggle>
          </div>

          <v-alert v-if="primaryError" type="error" variant="tonal" class="ma-4">{{ primaryError }}</v-alert>

          <div v-else-if="!location || !variable || depth == null"
            class="d-flex flex-column align-center justify-center text-center px-6" style="height:60vh;">
            <v-icon size="56" color="grey-darken-1">mdi-poll</v-icon>
            <div class="text-caption text-grey-darken-1 mt-2">
              {{ source === 'sensor' ? 'Select a sensor and a depth first.' : 'Select a point, variable and depth first.' }}
            </div>
          </div>

          <div v-else-if="primaryLoading && !primarySeries.length"
            class="d-flex align-center justify-center" style="height:60vh;">
            <v-progress-circular indeterminate color="warning" size="48" />
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
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useMainStore } from '../stores/main'
import { fetchAnalysisSeries, type SeriesPoint, type AnalysisLocation } from '~~/composables/useAnalysisFetch'
import { fetchSensorAnalysisSeries } from '~~/composables/useSensorAnalysisFetch'
import { availableVariables } from '~~/composables/useAnalysisStatistics'
import AnalysisBuilder from './analysis/AnalysisBuilder.vue'
import ExtremeEvents from './analysis/ExtremeEvents.vue'
import CompoundStress from './analysis/CompoundStress.vue'
import Trend from './analysis/Trend.vue'
import Climatology from './analysis/Climatology.vue'
import Correlation from './analysis/Correlation.vue'

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

const activeTab = ref<'builder' | 'extremes' | 'compound' | 'trend' | 'climatology' | 'correlation'>('builder')
const selectedSeason = ref('full_year')

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
