<template>
  <div class="analytics-panel d-flex h-100" style="overflow: hidden;">

    <!-- LEFT: Controls sidebar -->
    <div class="analytics-sidebar pa-3 d-flex flex-column" style="width:300px; min-width:300px; overflow-y:auto; border-right:1px solid rgba(255,255,255,0.08);">

      <div class="d-flex align-center justify-space-between mb-2">
        <span class="text-subtitle-2 font-weight-bold">Analysis Builder</span>
        <v-btn icon="mdi-refresh" size="x-small" variant="text" @click="resetParameters" title="Reset" />
      </div>

      <v-row dense>
        <v-col cols="12">
          <v-select v-model="selectedAnalysisMode" :items="analysisModes" item-title="name" item-value="id"
            label="Analysis Mode" density="compact" variant="outlined" hide-details color="warning" />
        </v-col>

        <v-col cols="12">
          <v-text-field v-model.number="thresholdValue" type="number" label="Threshold Value" variant="outlined"
            density="compact" hide-details color="warning" />
        </v-col>

        <v-col cols="12">
          <v-btn-toggle v-model="thresholdDirection" mandatory variant="tonal" density="compact" class="w-100">
            <v-btn value=">">Above</v-btn>
            <v-btn value="<">Below</v-btn>
          </v-btn-toggle>
        </v-col>

        <v-col cols="12">
          <v-select v-model="selectedSeason" :items="seasons" item-title="name" item-value="id" label="Season"
            variant="outlined" density="compact" hide-details color="warning" />
        </v-col>

        <v-col v-if="selectedSeason === 'custom'" cols="12">
          <div class="text-caption text-grey-darken-1 mb-1">Select months</div>
          <v-btn-toggle v-model="customMonths" multiple variant="tonal" density="compact" class="flex-wrap gap-1">
            <v-btn v-for="(name, idx) in monthNames" :key="idx + 1" :value="idx + 1" size="small" class="month-btn">
              {{ name }}
            </v-btn>
          </v-btn-toggle>
        </v-col>

        <v-col cols="12">
          <v-select v-model="variableListModel" :items="availableVariables" item-title="name" item-value="id"
            label="Primary Variable" variant="outlined" density="compact" hide-details color="warning" />
        </v-col>

        <v-col cols="12">
          <v-btn-toggle v-model="primaryStat" mandatory variant="outlined" density="compact" class="w-100">
            <v-btn v-for="stat in statsTypes" :key="stat.id" :value="stat.id">{{ stat.name }}</v-btn>
          </v-btn-toggle>
        </v-col>

        <v-col v-if="selectedAnalysisMode === 'threshold' || selectedAnalysisMode === 'streak'" cols="12">
          <v-switch v-model="useSecondCondition" label="Add second condition" color="warning" density="compact"
            hide-details />
        </v-col>

        <template v-if="isCompoundMode">
          <v-col cols="12"><v-divider /></v-col>
          <v-col cols="12">
            <div class="text-caption text-grey-darken-1 mb-1">Second Condition</div>
            <v-btn-toggle v-model="compoundLogic" mandatory variant="tonal" density="compact" class="w-100">
              <v-btn value="AND">AND</v-btn>
              <v-btn value="OR">OR</v-btn>
            </v-btn-toggle>
          </v-col>
          <v-col cols="12">
            <v-select v-model="secondVariable" :items="availableVariables" item-title="name" item-value="id"
              label="Second Variable" variant="outlined" density="compact" hide-details color="warning" />
          </v-col>
          <v-col cols="12">
            <v-btn-toggle v-model="secondStat" mandatory variant="outlined" density="compact" class="w-100">
              <v-btn v-for="stat in statsTypes" :key="stat.id" :value="stat.id">{{ stat.name }}</v-btn>
            </v-btn-toggle>
          </v-col>
          <v-col cols="12">
            <v-text-field v-model.number="secondThresholdValue" type="number" label="Second Threshold"
              variant="outlined" density="compact" hide-details color="warning" />
          </v-col>
          <v-col cols="12">
            <v-btn-toggle v-model="secondThresholdDirection" mandatory variant="tonal" density="compact" class="w-100">
              <v-btn value=">">Above</v-btn>
              <v-btn value="<">Below</v-btn>
            </v-btn-toggle>
          </v-col>
        </template>
      </v-row>

      <v-btn block color="warning" size="large" prepend-icon="mdi-chart-line" class="mt-3" :loading="isGenerating"
        @click="runAnalysis">
        Run Analysis
      </v-btn>
    </div>

    <!-- RIGHT: Chart + summary -->
    <div class="analytics-main flex-grow-1 d-flex flex-column" style="overflow:hidden; min-width:0;">

      <!-- Chart header -->
      <div class="d-flex align-center justify-space-between px-3 py-1" style="flex-shrink:0; border-bottom:1px solid rgba(255,255,255,0.06);">
        <div>
          <div class="text-subtitle-2 font-weight-bold">{{ generatedPlotTitle }}</div>
          <div class="text-caption text-grey-darken-1">{{ props.volumeLabel }}</div>
        </div>
        <v-chip color="warning" variant="tonal" size="small">{{ selectedSeasonLabel }}</v-chip>
      </div>

      <!-- Chart body -->
      <div class="chart-body flex-grow-1" style="position:relative; min-height:0;">
        <div v-if="isGenerating" class="d-flex flex-column align-center justify-center fill-height">
          <v-progress-circular indeterminate color="warning" size="48" class="mb-3" />
          <div class="text-subtitle-2 font-weight-medium text-warning">Querying ClickHouse...</div>
          <div class="text-body-2 text-grey mt-1">Aggregating data for selected region.</div>
        </div>

        <div v-else-if="hasActivePlot" ref="chartContainerRef" class="real-plot-area w-100 h-100" />

        <v-alert v-else-if="plotErrorMessage" type="error" icon="mdi-alert-octagon" class="ma-4"
          variant="tonal" border="start">
          <template #title>Analysis Failed</template>
          {{ plotErrorMessage }}
        </v-alert>

        <div v-else class="empty-plot-state d-flex flex-column align-center justify-center text-center px-6 h-100">
          <v-icon size="72" icon="mdi-poll" class="text-grey-darken-1" />
          <div class="text-subtitle-1 font-weight-regular mt-3 text-grey-darken-1">Statistical Visualization Canvas</div>
          <div class="text-body-2 text-grey-darken-1 mt-1">Configure analysis on the left, then click Run.</div>
        </div>
      </div>

      <!-- Summary table (only when populated) -->
      <div v-if="summaryRows.length > 0" style="flex-shrink:0; max-height:120px; overflow-y:auto; border-top:1px solid rgba(255,255,255,0.08);">
        <div class="d-flex align-center justify-space-between px-3 py-1">
          <span class="text-caption font-weight-medium text-grey-darken-1">Summary</span>
          <v-chip color="warning" variant="tonal" size="x-small">{{ conditionLabel }} · {{ selectedSeasonLabel }}</v-chip>
        </div>
        <v-data-table :headers="summaryHeaders" :items="summaryRows" density="compact" hide-default-footer
          class="summary-table" />
      </div>
    </div>

  </div>
</template>


<script setup lang="ts">
import { ref, computed, watch, nextTick, onMounted, onBeforeUnmount } from 'vue'
import axios from 'axios'
import * as echarts from 'echarts'
import { registerEchartsDarkTheme } from '../../composables/useEchartsTheme'

const config = useRuntimeConfig();
const apiBaseUrl = config.public.apiBaseUrl


// --- DATA CONFIGURATION (Static Metadata) ---

const minYear = 2007
const maxYear = 2026

const availableVariables = [
  { id: 'temperature', name: 'Temperature', icon: 'mdi-thermometer' },
  { id: 'salinity', name: 'Salinity', icon: 'mdi-water-percent' },
  { id: 'dissolved_oxygen', name: 'Dissolved Oxygen', icon: 'mdi-molecule' },
  { id: 'total_alkalinity', name: 'Total Alkalinity', icon: 'mdi-flask-outline' },
  { id: 'dissolved_inorganic_carbon', name: 'Dissolved Inorganic Carbon', icon: 'mdi-molecule-co2' },
  { id: 'ph_total', name: 'pH (Total Scale)', icon: 'mdi-flask' },
  { id: 'omega_arag', name: 'Omega Aragonite', icon: 'mdi-omega' },
  { id: 'omega_cal', name: 'Omega Calcite', icon: 'mdi-omega' }
]

const statsTypes = [
  { id: 'min', name: 'Min' },
  { id: 'mean', name: 'Mean' },
  { id: 'max', name: 'Max' }
]

const analysisModes = [
  { id: 'overlay', name: 'Overlay Yearly Timeseries', icon: 'mdi-layers-outline', desc: 'Compare selected season/months across multiple years.' },
  { id: 'climatology', name: 'Aggregated Climatology Cycle', icon: 'mdi-calendar-sync', desc: 'Averaged seasonal cycle over the full selected period.' },
  { id: 'threshold', name: 'Extreme Event (Threshold) Count', icon: 'mdi-thermometer-alert', desc: 'Count days matching extreme conditions per year.' },
  { id: 'streak', name: 'Consecutive Day Streak', icon: 'mdi-fire', desc: 'Longest consecutive-day run matching the threshold condition, per year.' },
  { id: 'trend', name: 'Inter-Annual Trend Analysis', icon: 'mdi-trending-up', desc: 'Long-term change tracking based on annual means.' },
  { id: 'extremes', name: 'Record Finder (Extremes)', icon: 'mdi-trophy', desc: 'All-time record high and low values with their dates across the selected period.' },
  { id: 'correlation', name: 'Multi-Variable Correlation', icon: 'mdi-chart-scatter-plot', desc: 'Relationship between two variables within the selected period.' }
]

const seasons = [
  { id: 'full_year', name: 'Full Year (Jan - Dec)' },
  { id: 'jja', name: 'Summer (JJA)' },
  { id: 'mam', name: 'Spring (MAM)' },
  { id: 'son', name: 'Autumn (SON)' },
  { id: 'djf', name: 'Winter (DJF)' },
  { id: 'custom', name: 'Custom Month Range' }
]

const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

// --- REACTIVE STATE (User Selections) ---
// Current Fixed Volume Context (Passed down as props in real app)
const props = defineProps({
  volumeLabel: { type: String, default: 'Northern Strait of Georgia (Depth: 10-20m)' }
})

// UI State
const variableListModel = ref('temperature') // Default list selection
const primaryStat = ref('mean') // Default stat toggle

const selectedAnalysisMode = ref('overlay')
const yearRange = ref([2015, 2020])
const selectedSeason = ref('full_year')
const customMonths = ref<number[]>([6, 7, 8])

// --- SPATIAL EXTENT STATE ---
const latitudeMin = ref(46)
const latitudeMax = ref(52)
const longitudeMin = ref(-127)
const longitudeMax = ref(-121)
const depthMin = ref(0)
const depthMax = ref(100)

// Dynamic Parameter States (Conditionally shown)
const thresholdValue = ref(19) // e.g., 19°C
const thresholdDirection = ref('>') // Above/Below
const secondVariable = ref('dissolved_oxygen')
const secondStat = ref('min')
const useSecondCondition = ref(false)
const secondThresholdValue = ref(0)
const secondThresholdDirection = ref('>')
const compoundLogic = ref<'AND' | 'OR'>('AND')

// --- HORIZONTAL REFERENCE LINES STATE ---
const showHorizontalLines = ref(false)
const hline1Label = ref('Ref A')
const hline1Value = ref(15)
const hline2Label = ref('Ref B')
const hline2Value = ref(20)

// --- COMPUTED STATISTICS FOR SNAP CHIPS ---
const computedMin = computed(() => {
  if (!chartData || !Array.isArray(chartData.series) || chartData.series.length === 0) return 0
  const allValues: number[] = []
  for (const serie of chartData.series) {
    if (Array.isArray(serie.data)) {
      for (const d of serie.data) {
        if (d.value != null) allValues.push(d.value)
      }
    }
  }
  return allValues.length > 0 ? Math.min(...allValues) : 0
})

const computedMax = computed(() => {
  if (!chartData || !Array.isArray(chartData.series) || chartData.series.length === 0) return 0
  const allValues: number[] = []
  for (const serie of chartData.series) {
    if (Array.isArray(serie.data)) {
      for (const d of serie.data) {
        if (d.value != null) allValues.push(d.value)
      }
    }
  }
  return allValues.length > 0 ? Math.max(...allValues) : 0
})

const computedMean = computed(() => {
  if (!chartData || !Array.isArray(chartData.series) || chartData.series.length === 0) return 0
  const allValues: number[] = []
  for (const serie of chartData.series) {
    if (Array.isArray(serie.data)) {
      for (const d of serie.data) {
        if (d.value != null) allValues.push(d.value)
      }
    }
  }
  return allValues.length > 0 ? allValues.reduce((a, b) => a + b, 0) / allValues.length : 0
})

const summaryHeaders = [
  { title: 'Series', key: 'series' },
  { title: 'Matches', key: 'matches' },
  { title: 'Min', key: 'min' },
  { title: 'Max', key: 'max' },
  { title: 'Mean', key: 'mean' },
  { title: 'Condition', key: 'condition' }
]

const selectedSeasonLabel = computed(() => {
  if (selectedSeason.value === 'custom') {
    if (customMonths.value.length === 0) return 'Custom (none)'
    const names = [...customMonths.value].sort((a, b) => a - b).map(m => monthNames[m - 1])
    return `Custom: ${names.join(', ')}`
  }
  return seasons.find(s => s.id === selectedSeason.value)?.name || 'Season'
})

const thresholdDirectionLabel = computed(() => (thresholdDirection.value === '>' ? 'Above' : 'Below'))

const isCompoundMode = computed(() =>
  useSecondCondition.value &&
  (selectedAnalysisMode.value === 'threshold' || selectedAnalysisMode.value === 'streak')
)

const conditionLabel = computed(() => {
  const dir1 = thresholdDirectionLabel.value
  const t1 = thresholdValue.value
  if (isCompoundMode.value) {
    const dir2 = secondThresholdDirection.value === '>' ? 'Above' : 'Below'
    const var2Name = availableVariables.find(v => v.id === secondVariable.value)?.name || secondVariable.value
    return `${dir1} ${t1} ${compoundLogic.value} ${var2Name} ${dir2} ${secondThresholdValue.value}`
  }
  return `${dir1} ${t1}`
})

const summaryRows = computed(() => {
  if (!chartData) return []

  const threshold = thresholdValue.value
  const direction = thresholdDirection.value
  const compare = (value: number) => direction === '>' ? value > threshold : value < threshold

  // Extremes mode: show record high and record low as two rows;
  // repurpose "Condition" column as the date the record occurred.
  if (chartData.globalMax != null || chartData.globalMin != null) {
    const rows: any[] = []
    if (chartData.globalMax) rows.push({
      series: 'Record High',
      matches: '—',
      min: '—',
      max: Number(chartData.globalMax.value).toFixed(4),
      mean: '—',
      condition: String(chartData.globalMax.time).slice(0, 10)
    })
    if (chartData.globalMin) rows.push({
      series: 'Record Low',
      matches: '—',
      min: Number(chartData.globalMin.value).toFixed(4),
      max: '—',
      mean: '—',
      condition: String(chartData.globalMin.time).slice(0, 10)
    })
    return rows
  }

  if (Array.isArray(chartData.series)) {
    return chartData.series.map((serie: any) => {
      const values: number[] = Array.isArray(serie.data)
        ? serie.data.map((d: any) => Number(d.value)).filter((v: number) => !Number.isNaN(v))
        : []
      const matches = values.filter(compare)
      const min = values.length ? Math.min(...values) : null
      const max = values.length ? Math.max(...values) : null
      const mean = values.length ? (values.reduce((sum: number, v: number) => sum + v, 0) / values.length) : null

      return {
        series: `Year ${serie.year}`,
        matches: matches.length,
        min: min != null ? min.toFixed(2) : '—',
        max: max != null ? max.toFixed(2) : '—',
        mean: mean != null ? mean.toFixed(2) : '—',
        condition: conditionLabel.value
      }
    })
  }

  if (Array.isArray(chartData.data)) {
    const values = chartData.data.map((d: any) => Number(d.value)).filter((v: number) => !Number.isNaN(v))
    const matches = values.filter(compare)
    const min = values.length ? Math.min(...values) : null
    const max = values.length ? Math.max(...values) : null
    const mean = values.length ? (values.reduce((sum: number, v: number) => sum + v, 0) / values.length) : null

    const seriesLabel = selectedAnalysisMode.value === 'trend'
      ? 'Annual Mean'
      : selectedAnalysisMode.value === 'threshold'
        ? 'Days Matching Condition'
        : selectedAnalysisMode.value === 'streak'
          ? 'Max Streak (days)'
          : 'Climatology Cycle'

    return [{
      series: seriesLabel,
      matches: matches.length,
      min: min != null ? min.toFixed(2) : '—',
      max: max != null ? max.toFixed(2) : '—',
      mean: mean != null ? mean.toFixed(2) : '—',
      condition: conditionLabel.value
    }]
  }

  return []
})

// --- CLIENT-SIDE ANALYTICS (derived from a flat daily {time, value} series) ---

type SeriesPoint = { time: string; value: number | null }

const SEASON_MONTHS: Record<string, number[]> = {
  full_year: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
  jja: [6, 7, 8],
  mam: [3, 4, 5],
  son: [9, 10, 11],
  djf: [12, 1, 2]
}

/** Bounding-box polygon (closed ring) built from the lat/lon extent controls */
const polygonFromBounds = computed<[number, number][]>(() => [
  [longitudeMin.value, latitudeMin.value],
  [longitudeMax.value, latitudeMin.value],
  [longitudeMax.value, latitudeMax.value],
  [longitudeMin.value, latitudeMax.value],
  [longitudeMin.value, latitudeMin.value]
])

async function fetchRegionTimeseries(variable: string, stat: string): Promise<SeriesPoint[]> {
  const payload = {
    polygon: polygonFromBounds.value,
    depth: { min: depthMin.value, max: depthMax.value },
    primaryMetric: { variable, stat },
    temporal: { yearRange: yearRange.value }
  }
  const response = await axios.post(`${apiBaseUrl}/analysis/timeseries`, payload)
  return response.data?.data || []
}

function filterBySeason(data: SeriesPoint[], season: string): SeriesPoint[] {
  const months = season === 'custom'
    ? (customMonths.value.length > 0 ? customMonths.value : SEASON_MONTHS.full_year)
    : (SEASON_MONTHS[season] || SEASON_MONTHS.full_year)
  return data.filter(d => months.includes(new Date(d.time).getUTCMonth() + 1))
}

function groupByYear(data: SeriesPoint[]): { year: number; data: SeriesPoint[] }[] {
  const byYear = new Map<number, SeriesPoint[]>()
  for (const d of data) {
    const year = new Date(d.time).getUTCFullYear()
    if (!byYear.has(year)) byYear.set(year, [])
    byYear.get(year)!.push(d)
  }
  return Array.from(byYear.entries())
    .sort((a, b) => a[0] - b[0])
    .map(([year, points]) => ({ year, data: points }))
}

/** Average value per calendar month, across all years in the series */
function computeClimatology(data: SeriesPoint[]): SeriesPoint[] {
  const monthly = new Map<number, { sum: number; count: number }>()
  for (const d of data) {
    if (d.value == null) continue
    const month = new Date(d.time).getUTCMonth() + 1
    const entry = monthly.get(month) || { sum: 0, count: 0 }
    entry.sum += d.value
    entry.count += 1
    monthly.set(month, entry)
  }
  return Array.from(monthly.entries())
    .sort((a, b) => a[0] - b[0])
    .map(([month, { sum, count }]) => ({
      time: String(month).padStart(2, '0'),
      value: count > 0 ? sum / count : null
    }))
}

/** Average value per year, for inter-annual trend tracking */
function computeTrend(data: SeriesPoint[]): SeriesPoint[] {
  const yearly = new Map<number, { sum: number; count: number }>()
  for (const d of data) {
    if (d.value == null) continue
    const year = new Date(d.time).getUTCFullYear()
    const entry = yearly.get(year) || { sum: 0, count: 0 }
    entry.sum += d.value
    entry.count += 1
    yearly.set(year, entry)
  }
  return Array.from(yearly.entries())
    .sort((a, b) => a[0] - b[0])
    .map(([year, { sum, count }]) => ({
      time: String(year),
      value: count > 0 ? sum / count : null
    }))
}

/** Per-year count of days matching the threshold condition */
function computeThresholdCounts(data: SeriesPoint[], threshold: number, direction: string): SeriesPoint[] {
  const matches = (value: number) => (direction === '>' ? value > threshold : value < threshold)
  const counts = new Map<number, number>()
  for (const d of data) {
    if (d.value == null) continue
    const year = new Date(d.time).getUTCFullYear()
    if (!counts.has(year)) counts.set(year, 0)
    if (matches(d.value)) counts.set(year, counts.get(year)! + 1)
  }
  return Array.from(counts.entries())
    .sort((a, b) => a[0] - b[0])
    .map(([year, count]) => ({ time: String(year), value: count }))
}

/** Per-year longest consecutive-day run matching the threshold condition.
 *  Adjacency is checked by calendar date (1-day gap) so season gaps (e.g. DJF
 *  Feb→Dec jump) correctly break the streak. */
function computeStreaks(data: SeriesPoint[], threshold: number, direction: string): SeriesPoint[] {
  const isMatch = (v: number) => direction === '>' ? v > threshold : v < threshold

  return groupByYear(data).map(({ year, data: pts }) => {
    let maxStreak = 0
    let curStreak = 0
    for (let i = 0; i < pts.length; i++) {
      const { time, value } = pts[i]
      const prev = pts[i - 1]
      const adjacent = i > 0 && prev != null &&
        new Date(time).getTime() - new Date(prev.time).getTime() === 86_400_000
      if (value != null && isMatch(value)) {
        curStreak = adjacent ? curStreak + 1 : 1
      } else {
        curStreak = 0
      }
      if (curStreak > maxStreak) maxStreak = curStreak
    }
    return { time: String(year), value: maxStreak }
  })
}

/** Per-year count of days where both (or either) variable conditions are met */
function computeCompoundThresholdCounts(
  data1: SeriesPoint[], threshold1: number, direction1: string,
  data2: SeriesPoint[], threshold2: number, direction2: string,
  logic: 'AND' | 'OR'
): SeriesPoint[] {
  const m1 = (v: number) => direction1 === '>' ? v > threshold1 : v < threshold1
  const m2 = (v: number) => direction2 === '>' ? v > threshold2 : v < threshold2
  const sec = new Map(data2.map(d => [d.time, d.value]))
  const counts = new Map<number, number>()
  for (const d of data1) {
    if (d.value == null) continue
    const year = new Date(d.time).getUTCFullYear()
    if (!counts.has(year)) counts.set(year, 0)
    const v2 = sec.get(d.time)
    const cond1 = m1(d.value)
    const cond2 = v2 != null && m2(v2 as number)
    if (logic === 'AND' ? (cond1 && cond2) : (cond1 || cond2))
      counts.set(year, counts.get(year)! + 1)
  }
  return Array.from(counts.entries())
    .sort((a, b) => a[0] - b[0])
    .map(([year, count]) => ({ time: String(year), value: count }))
}

/** Per-year longest consecutive-day streak where both (or either) conditions are met */
function computeCompoundStreaks(
  data1: SeriesPoint[], threshold1: number, direction1: string,
  data2: SeriesPoint[], threshold2: number, direction2: string,
  logic: 'AND' | 'OR'
): SeriesPoint[] {
  const m1 = (v: number) => direction1 === '>' ? v > threshold1 : v < threshold1
  const m2 = (v: number) => direction2 === '>' ? v > threshold2 : v < threshold2
  const sec = new Map(data2.map(d => [d.time, d.value]))
  return groupByYear(data1).map(({ year, data: pts }) => {
    let maxStreak = 0
    let curStreak = 0
    for (let i = 0; i < pts.length; i++) {
      const { time, value } = pts[i]
      const prev = pts[i - 1]
      const adjacent = i > 0 && prev != null &&
        new Date(time).getTime() - new Date(prev.time).getTime() === 86_400_000
      if (value != null) {
        const v2 = sec.get(time)
        const cond1 = m1(value)
        const cond2 = v2 != null && m2(v2 as number)
        const passes = logic === 'AND' ? (cond1 && cond2) : (cond1 || cond2)
        curStreak = passes ? (adjacent ? curStreak + 1 : 1) : 0
      } else {
        curStreak = 0
      }
      if (curStreak > maxStreak) maxStreak = curStreak
    }
    return { time: String(year), value: maxStreak }
  })
}

type ExtremePoint = { time: string; value: number }

/** Scans the full season-filtered series for the all-time record high and low. */
function computeExtremes(data: SeriesPoint[]): {
  rawData: SeriesPoint[]
  globalMax: ExtremePoint | null
  globalMin: ExtremePoint | null
} {
  let globalMax: ExtremePoint | null = null
  let globalMin: ExtremePoint | null = null

  for (const d of data) {
    if (d.value == null) continue
    if (globalMax == null || d.value > globalMax.value) globalMax = { time: d.time, value: d.value }
    if (globalMin == null || d.value < globalMin.value) globalMin = { time: d.time, value: d.value }
  }

  return { rawData: data, globalMax, globalMin }
}

// Plotting States
const isGenerating = ref(false)
const hasActivePlot = ref(false)
const plotErrorMessage = ref<string | null>(null)

// Chart refs
const chartContainerRef = ref<HTMLDivElement | null>(null)
let chartInstance: echarts.ECharts | null = null

// Store the raw response data for redrawing on resize
let chartData: any = null

// --- COMPUTED LOGIC ---

// Dynamically generate the plot title based on selections
const generatedPlotTitle = computed(() => {
  const modeName = analysisModes.find(m => m.id === selectedAnalysisMode.value)?.name || ''
  const varName = availableVariables.find(v => v.id === variableListModel.value)?.name || ''
  const statName = primaryStat.value.toUpperCase()
  const years = `${yearRange.value[0]}-${yearRange.value[1]}`

  if (selectedAnalysisMode.value === 'correlation') {
    const var2Name = availableVariables.find(v => v.id === secondVariable.value)?.name || ''
    const stat2Name = secondStat.value.toUpperCase()
    return `Correlation: ${varName} (${statName}) vs ${var2Name} (${stat2Name}) | ${years}`
  }

  if (isCompoundMode.value) {
    const var2Name = availableVariables.find(v => v.id === secondVariable.value)?.name || ''
    const stat2Name = secondStat.value.toUpperCase()
    return `${modeName}: ${varName} (${statName}) ${compoundLogic.value} ${var2Name} (${stat2Name}) | ${years}`
  }

  return `${modeName}: ${varName} (${statName}) | ${years}`
})

// Watch analysis mode to reset dynamic parameters to sane defaults
watch(selectedAnalysisMode, (newMode: string) => {
  selectedSeason.value = newMode === 'overlay' ? 'jja' : 'full_year'
})


// --- ECHARTS RENDERING ---

function renderOverlayChart(responseData: any) {
  if (!chartContainerRef.value || !responseData || !Array.isArray(responseData.series)) return

  registerEchartsDarkTheme()

  if (chartInstance) {
    chartInstance.dispose()
    chartInstance = null
  }

  try {
    chartInstance = echarts.init(chartContainerRef.value, 'dark', { renderer: 'canvas' })
  } catch (e) {
    console.error('ECharts initialization failed', e)
    return
  }

  const series = responseData.series || []
  const timePoints = Array.isArray(series[0]?.data)
    ? series[0].data.map((d: any) => d.time)
    : []

  const echartsSeries: echarts.LineSeriesOption[] = series.map((serie: any) => ({
    name: String(serie.year),
    type: 'line',
    smooth: true,
    symbol: 'none',
    lineStyle: { width: 2 },
    data: Array.isArray(serie.data) ? serie.data.map((d: any) => d.value) : []
  }))

  // Build markLine data for reference lines
  const markLineData: any[] = []
  if (showHorizontalLines.value) {
    if (hline1Value.value != null && !isNaN(hline1Value.value)) {
      markLineData.push({
        yAxis: hline1Value.value,
        label: {
          formatter: hline1Label.value || 'Line 1',
          color: '#00e5ff',
          fontSize: 11,
          fontWeight: 'bold'
        },
        lineStyle: {
          color: '#00e5ff',
          type: 'dashed',
          width: 2
        }
      })
    }
    if (hline2Value.value != null && !isNaN(hline2Value.value)) {
      markLineData.push({
        yAxis: hline2Value.value,
        label: {
          formatter: hline2Label.value || 'Line 2',
          color: '#ff9100',
          fontSize: 11,
          fontWeight: 'bold'
        },
        lineStyle: {
          color: '#ff9100',
          type: 'dashed',
          width: 2
        }
      })
    }
  }

  // Apply markLine to last series so it draws on top, or to all series
  if (markLineData.length > 0 && echartsSeries.length > 0) {
    const lastSeries = echartsSeries[echartsSeries.length - 1]
    lastSeries.markLine = {
      silent: false,
      symbol: 'none',
      animation: false,
      data: markLineData
    }
  }

  const option: echarts.EChartsOption = {
    tooltip: {
      trigger: 'axis',
      formatter: (params: any) => {
        const items = Array.isArray(params) ? params : [params]
        let tooltip = `<strong>${items[0]?.axisValue || ''}</strong><br/>`
        items.forEach((p: any) => {
          tooltip += `${p.marker} ${p.seriesName}: <strong>${p.value != null ? Number(p.value).toFixed(4) : 'N/A'}</strong><br/>`
        })
        return tooltip
      }
    },
    legend: {
      data: series.map((s: any) => String(s.year)),
      top: 40,
      type: 'scroll'
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '8%',
      top: '18%',
      containLabel: true
    },
    xAxis: {
      type: 'category',
      data: timePoints,
      boundaryGap: false,
      axisLabel: {
        rotate: 45,
        fontSize: 10
      }
    },
    yAxis: {
      type: 'value',
      name: `${availableVariables.find((v) => v.id === variableListModel.value)?.name || ''} (${primaryStat.value})`,
      nameLocation: 'middle',
      nameGap: 45
    },
    dataZoom: [
      { type: 'inside', start: 0, end: 100 },
      { type: 'slider', start: 0, end: 100, bottom: 5 }
    ],
    series: echartsSeries
  }
  chartInstance.setOption(option, true)
  chartInstance.resize()
}

/** Renders a single-line chart for a flat {data: [{time, value}]} series (climatology/trend/threshold) */
function renderSingleSeriesChart(responseData: any, seriesName: string, yAxisLabel: string) {
  if (!chartContainerRef.value || !responseData || !Array.isArray(responseData.data)) return

  registerEchartsDarkTheme()

  if (chartInstance) {
    chartInstance.dispose()
    chartInstance = null
  }

  try {
    chartInstance = echarts.init(chartContainerRef.value, 'dark', { renderer: 'canvas' })
  } catch (e) {
    console.error('ECharts initialization failed', e)
    return
  }

  const series = responseData.data || []
  const timePoints = series.map((d: any) => d.time)

  const echartsSeries: echarts.LineSeriesOption = {
    name: seriesName,
    type: 'line',
    smooth: true,
    symbol: 'none',
    lineStyle: { width: 2 },
    data: series.map((d: any) => d.value)
  }

  const option: echarts.EChartsOption = {
    tooltip: {
      trigger: 'axis',
      formatter: (params: any) => {
        const items = Array.isArray(params) ? params : [params]
        let tooltip = `<strong>${items[0]?.axisValue || ''}</strong><br/>`
        items.forEach((p: any) => {
          tooltip += `${p.marker} ${p.seriesName}: <strong>${p.value != null ? Number(p.value).toFixed(4) : 'N/A'}</strong><br/>`
        })
        return tooltip
      }
    },
    legend: {
      data: [seriesName],
      top: 40,
      type: 'scroll'
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '8%',
      top: '18%',
      containLabel: true
    },
    xAxis: {
      type: 'category',
      data: timePoints,
      boundaryGap: false,
      axisLabel: {
        rotate: 45,
        fontSize: 10
      }
    },
    yAxis: {
      type: 'value',
      name: yAxisLabel,
      nameLocation: 'middle',
      nameGap: 45
    },
    dataZoom: [
      { type: 'inside', start: 0, end: 100 },
      { type: 'slider', start: 0, end: 100, bottom: 5 }
    ],
    series: echartsSeries
  }
  chartInstance.setOption(option, true)
  chartInstance.resize()
}

/** Renders the full daily timeseries as a continuous line with record-high/low markPoints */
function renderExtremesChart(responseData: any) {
  if (!chartContainerRef.value || !responseData || !Array.isArray(responseData.rawData)) return

  registerEchartsDarkTheme()
  if (chartInstance) { chartInstance.dispose(); chartInstance = null }

  try {
    chartInstance = echarts.init(chartContainerRef.value, 'dark', { renderer: 'canvas' })
  } catch (e) {
    console.error('ECharts initialization failed', e)
    return
  }

  const pts = responseData.rawData as SeriesPoint[]
  const timePoints = pts.map(d => d.time)
  const values = pts.map(d => d.value)
  const varName = availableVariables.find(v => v.id === variableListModel.value)?.name || ''
  const yAxisLabel = `${varName} (${primaryStat.value})`

  const markPointData: any[] = []
  const { globalMax, globalMin } = responseData
  if (globalMax) markPointData.push({
    coord: [globalMax.time, globalMax.value],
    name: 'Record High',
    itemStyle: { color: '#ff5252' },
    label: { show: true, formatter: `High\n${String(globalMax.time).slice(0, 10)}`, color: '#ff5252', fontSize: 10 }
  })
  if (globalMin) markPointData.push({
    coord: [globalMin.time, globalMin.value],
    name: 'Record Low',
    itemStyle: { color: '#40c4ff' },
    label: { show: true, formatter: `Low\n${String(globalMin.time).slice(0, 10)}`, color: '#40c4ff', fontSize: 10 }
  })

  const option: echarts.EChartsOption = {
    tooltip: {
      trigger: 'axis',
      formatter: (params: any) => {
        const items = Array.isArray(params) ? params : [params]
        let tooltip = `<strong>${items[0]?.axisValue || ''}</strong><br/>`
        items.forEach((p: any) => {
          tooltip += `${p.marker} ${p.seriesName}: <strong>${p.value != null ? Number(p.value).toFixed(4) : 'N/A'}</strong><br/>`
        })
        return tooltip
      }
    },
    grid: { left: '3%', right: '4%', bottom: '8%', top: '12%', containLabel: true },
    xAxis: {
      type: 'category',
      data: timePoints,
      boundaryGap: false,
      axisLabel: { rotate: 45, fontSize: 10 }
    },
    yAxis: { type: 'value', name: yAxisLabel, nameLocation: 'middle', nameGap: 45 },
    dataZoom: [
      { type: 'inside', start: 0, end: 100 },
      { type: 'slider', start: 0, end: 100, bottom: 5 }
    ],
    series: [{
      name: varName,
      type: 'line',
      smooth: false,
      symbol: 'none',
      lineStyle: { width: 1 },
      data: values,
      markPoint: {
        symbol: 'pin',
        symbolSize: 44,
        data: markPointData
      }
    }]
  }
  chartInstance.setOption(option, true)
  chartInstance.resize()
}

function renderChart(responseData: any) {
  if (!responseData) return

  const varName = availableVariables.find((v) => v.id === variableListModel.value)?.name || ''
  const yAxisLabel = `${varName} (${primaryStat.value})`

  switch (selectedAnalysisMode.value) {
    case 'overlay':
      renderOverlayChart(responseData)
      break

    case 'climatology':
      renderSingleSeriesChart(responseData, 'Climatology Cycle', yAxisLabel)
      break

    case 'trend':
      renderSingleSeriesChart(responseData, 'Annual Mean', yAxisLabel)
      break

    case 'threshold':
      renderSingleSeriesChart(responseData, 'Days Matching Condition', 'Day count')
      break

    case 'streak':
      renderSingleSeriesChart(responseData, 'Max Consecutive Days', 'Day count')
      break

    case 'extremes':
      renderExtremesChart(responseData)
      break

    default:
      break
  }
}

/** Re-render the overlay chart with current reference line values without re-fetching */
function updateReferenceLines() {
  if (!chartInstance || !chartData || selectedAnalysisMode.value !== 'overlay') return
  renderOverlayChart(chartData)
}

// Watch horizontal line values to update chart in real-time
watch([showHorizontalLines, hline1Value, hline2Value, hline1Label, hline2Label], () => {
  if (hasActivePlot.value && chartData && selectedAnalysisMode.value === 'overlay') {
    updateReferenceLines()
  }
})

function handleResize() {
  if (chartInstance) chartInstance.resize()
}

// ResizeObserver so ECharts resizes when the footer panel grows/shrinks (e.g. tab switch)
let resizeObserver: ResizeObserver | null = null
watch(chartContainerRef, (el) => {
  if (resizeObserver) { resizeObserver.disconnect(); resizeObserver = null }
  if (el && typeof ResizeObserver !== 'undefined') {
    resizeObserver = new ResizeObserver(() => { if (chartInstance) chartInstance.resize() })
    resizeObserver.observe(el)
  }
})

onMounted(() => {
  registerEchartsDarkTheme()
  window.addEventListener('resize', handleResize)
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleResize)
  if (resizeObserver) { resizeObserver.disconnect(); resizeObserver = null }
  if (chartInstance) {
    chartInstance.dispose()
    chartInstance = null
  }
})

// --- ACTIONS ---

const runAnalysis = async () => {
  plotErrorMessage.value = null
  isGenerating.value = true
  hasActivePlot.value = false

  try {
    // For compound threshold/streak, fetch both series in parallel.
    const needsSecondary = isCompoundMode.value
    const [rawData, rawData2] = await Promise.all([
      fetchRegionTimeseries(variableListModel.value, primaryStat.value),
      needsSecondary
        ? fetchRegionTimeseries(secondVariable.value, secondStat.value)
        : Promise.resolve([] as SeriesPoint[])
    ])

    const seasonal = filterBySeason(rawData, selectedSeason.value)
    const seasonal2 = needsSecondary ? filterBySeason(rawData2, selectedSeason.value) : []

    let result: any
    switch (selectedAnalysisMode.value) {
      case 'overlay':
        result = { series: groupByYear(seasonal) }
        break
      case 'climatology':
        result = { data: computeClimatology(seasonal) }
        break
      case 'trend':
        result = { data: computeTrend(seasonal) }
        break
      case 'threshold':
        result = needsSecondary
          ? { data: computeCompoundThresholdCounts(seasonal, thresholdValue.value, thresholdDirection.value, seasonal2, secondThresholdValue.value, secondThresholdDirection.value, compoundLogic.value) }
          : { data: computeThresholdCounts(seasonal, thresholdValue.value, thresholdDirection.value) }
        break
      case 'streak':
        result = needsSecondary
          ? { data: computeCompoundStreaks(seasonal, thresholdValue.value, thresholdDirection.value, seasonal2, secondThresholdValue.value, secondThresholdDirection.value, compoundLogic.value) }
          : { data: computeStreaks(seasonal, thresholdValue.value, thresholdDirection.value) }
        break
      case 'extremes':
        result = computeExtremes(seasonal)
        break
      default:
        throw new Error(`Analysis mode "${selectedAnalysisMode.value}" is not yet supported.`)
    }

    chartData = result
    // Set hasActivePlot before nextTick so the template renders the chart container
    hasActivePlot.value = true
    plotErrorMessage.value = null

    // Wait for the DOM to update then render the chart
    await nextTick()
    setTimeout(() => {
      if (chartContainerRef.value) {
        renderChart(chartData)
      } else {
        plotErrorMessage.value = 'Chart container not found. Unable to render plot.'
        hasActivePlot.value = false
      }
    }, 100) // Slight delay to ensure container is ready
  } catch (error: any) {
    plotErrorMessage.value = error?.response?.data?.detail || error?.message || 'Failed to generate analysis. Please try again.'
    hasActivePlot.value = false
  } finally {
    isGenerating.value = false
  }
}

const resetParameters = () => {
  variableListModel.value = 'temperature'
  primaryStat.value = 'mean'
  selectedAnalysisMode.value = 'overlay'
  yearRange.value = [2015, 2020]
  selectedSeason.value = 'full_year'
  customMonths.value = [6, 7, 8]
  latitudeMin.value = 46
  latitudeMax.value = 52
  longitudeMin.value = -127
  longitudeMax.value = -121
  depthMin.value = 0
  depthMax.value = 100
  useSecondCondition.value = false
  secondThresholdValue.value = 0
  secondThresholdDirection.value = '>'
  compoundLogic.value = 'AND'
  showHorizontalLines.value = false
  hline1Label.value = 'Ref A'
  hline1Value.value = 15
  hline2Label.value = 'Ref B'
  hline2Value.value = 20
  hasActivePlot.value = false
  plotErrorMessage.value = null
  chartData = null
  if (chartInstance) {
    chartInstance.dispose()
    chartInstance = null
  }
}

</script>


<style scoped>
.analytics-panel {
  height: 100%;
  font-family: 'Roboto', sans-serif;
}

.analytics-sidebar {
  background: rgba(255, 255, 255, 0.02);
}

.chart-body {
  flex: 1;
  min-height: 0;
}

.real-plot-area {
  width: 100%;
  height: 100%;
}

.summary-table {
  width: 100%;
}

.summary-table :deep(.v-data-table__td),
.summary-table :deep(.v-data-table__th) {
  font-size: 0.75rem !important;
  padding: 2px 8px !important;
}

/* Month picker: wrap 12 buttons into rows */
.month-btn {
  min-width: 44px !important;
  flex: 0 0 auto;
}

.gap-1 {
  gap: 4px;
}
</style>