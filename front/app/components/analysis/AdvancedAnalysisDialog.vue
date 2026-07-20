<template>
  <v-dialog v-model="isOpen" fullscreen transition="dialog-bottom-transition" :scrim="false">
    <v-card class="d-flex flex-column" style="height:100vh;">
      <v-toolbar density="compact" color="grey-darken-4">
        <v-toolbar-title class="text-body-2">
          Advanced Analysis — {{ varName }} @ {{ depth }}m
          <v-chip v-if="pointLabel" size="x-small" color="warning" variant="tonal" class="ml-2">{{ pointLabel }}</v-chip>
        </v-toolbar-title>
        <v-spacer />
        <span class="ctrl-label mr-2">Season</span>
        <v-btn-toggle v-model="selectedSeason" mandatory density="compact" variant="tonal" class="mr-4">
          <v-btn value="full_year" size="x-small">All</v-btn>
          <v-btn value="mam" size="x-small">MAM</v-btn>
          <v-btn value="jja" size="x-small">JJA</v-btn>
          <v-btn value="son" size="x-small">SON</v-btn>
          <v-btn value="djf" size="x-small">DJF</v-btn>
        </v-btn-toggle>
        <v-btn icon="mdi-close" variant="text" @click="isOpen = false" title="Close" />
      </v-toolbar>

      <v-tabs v-model="activeTab" density="compact" color="warning" class="flex-shrink-0">
        <v-tab value="extremes">Extreme Events</v-tab>
        <v-tab value="compound">Compound Stress</v-tab>
        <v-tab value="trend">Trend</v-tab>
        <v-tab value="climatology">Climatology Anomaly</v-tab>
        <v-tab value="correlation">Correlation</v-tab>
      </v-tabs>

      <v-card-text class="flex-grow-1 pa-0" style="overflow:auto; min-height:0;">
        <v-alert v-if="primaryError" type="error" variant="tonal" class="ma-4">{{ primaryError }}</v-alert>

        <div v-else-if="!location || !variable || depth == null" class="d-flex flex-column align-center justify-center h-100 text-center px-6">
          <v-icon size="56" color="grey-darken-1">mdi-poll</v-icon>
          <div class="text-caption text-grey-darken-1 mt-2">Select a point, variable, and depth first.</div>
        </div>

        <div v-else-if="primaryLoading && !primarySeries.length" class="d-flex align-center justify-center h-100">
          <v-progress-circular indeterminate color="warning" size="48" />
        </div>

        <template v-else>
          <ExtremeEvents v-if="activeTab === 'extremes'" :series="primarySeries" :season="selectedSeason" :variable="variable" />
          <CompoundStress v-else-if="activeTab === 'compound'"
            :primary-series="primarySeries" :primary-variable="variable" :season="selectedSeason"
            :depth="depth" :location="location" :year-range="yearRange" :fetch-series="cachedFetch" />
          <Trend v-else-if="activeTab === 'trend'" :series="primarySeries" :season="selectedSeason" />
          <Climatology v-else-if="activeTab === 'climatology'" :series="primarySeries" :season="selectedSeason" />
          <Correlation v-else-if="activeTab === 'correlation'"
            :primary-series="primarySeries" :primary-variable="variable" :season="selectedSeason"
            :depth="depth" :location="location" :year-range="yearRange" :fetch-series="cachedFetch" />
        </template>
      </v-card-text>
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { fetchAnalysisSeries, type SeriesPoint, type AnalysisLocation } from '../../../composables/useAnalysisFetch'
import { fetchSensorAnalysisSeries } from '../../../composables/useSensorAnalysisFetch'
import { availableVariables } from '../../../composables/useAnalysisStatistics'
import ExtremeEvents from './ExtremeEvents.vue'
import CompoundStress from './CompoundStress.vue'
import Trend from './Trend.vue'
import Climatology from './Climatology.vue'
import Correlation from './Correlation.vue'

const props = defineProps<{
  modelValue: boolean
  variable: string
  depth: number | null
  location: AnalysisLocation | null
  yearRange: [number, number]
  pointLabel?: string
  // Model source (e.g. "SalishSeaCast") — only needed to resolve `depth` for a
  // variable-depth ("profiler") sensor location; ignored for model/fixed-depth locations.
  source?: string | null
}>()
const emit = defineEmits<{ 'update:modelValue': [boolean] }>()

const isOpen = computed({
  get: () => props.modelValue,
  set: (v: boolean) => emit('update:modelValue', v),
})

const variable = computed(() => props.variable)
const depth = computed(() => props.depth)
const location = computed(() => props.location)
const yearRange = computed(() => props.yearRange)

const varName = computed(() => availableVariables.find(v => v.id === variable.value)?.name || variable.value || 'Variable')
const pointLabel = computed(() => props.pointLabel || '')

/** Dispatches to the sensor or model series fetcher based on the shape of `loc`. */
function fetchSeriesFor(variableId: string, depthVal: number, loc: AnalysisLocation): Promise<SeriesPoint[]> {
  if ('sensorId' in loc) {
    const [fromDate, toDate] = [`${yearRange.value[0]}-01-01T000000`, `${yearRange.value[1]}-12-31T235959`]
    return fetchSensorAnalysisSeries(loc.sensorId, variableId, 'mean', depthVal, fromDate, toDate, props.source ?? null)
  }
  return fetchAnalysisSeries({ variable: variableId, stat: 'mean', depth: depthVal, location: loc, yearRange: yearRange.value })
}

const activeTab = ref<'extremes' | 'compound' | 'trend' | 'climatology' | 'correlation'>('extremes')
const selectedSeason = ref('full_year')

// --- PRIMARY SERIES (fetched once per point/variable/depth, shared across tabs) ---
const primarySeries = ref<SeriesPoint[]>([])
const primaryLoading = ref(false)
const primaryError = ref<string | null>(null)
let primaryRequestId = 0

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

// Fetch when the dialog opens, and re-fetch if point/variable/depth changed since.
watch(isOpen, (open) => { if (open) fetchPrimary() })
watch([location, variable, depth], () => { if (isOpen.value) fetchPrimary() })

// --- SHARED FETCH CACHE (secondary variables picked in Compound Stress / Correlation tabs) ---
const seriesCache = new Map<string, Promise<SeriesPoint[]>>()
function cachedFetch(variableId: string, depthVal: number, loc: AnalysisLocation): Promise<SeriesPoint[]> {
  const key = JSON.stringify({ variableId, depthVal, loc })
  let entry = seriesCache.get(key)
  if (!entry) {
    entry = fetchSeriesFor(variableId, depthVal, loc)
    seriesCache.set(key, entry)
  }
  return entry
}
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
