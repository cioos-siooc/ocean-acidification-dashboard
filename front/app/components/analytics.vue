<template>
  <v-dialog v-model="showDialog" width="100%" max-width="1200"  scrollable>
    <v-card class="ocean-analysis-builder" elevation="0">
      <!-- Component Header with Reset Action -->
      <v-toolbar color="background" density="comfortable">
        <v-toolbar-title class="text-subtitle-1 font-weight-bold ml-1">
          Ocean Analysis Builder
        </v-toolbar-title>
        <v-spacer></v-spacer>
        <v-btn icon="mdi-refresh" variant="text" @click="resetParameters" title="Reset all criteria"></v-btn>
      </v-toolbar>

      <!-- Main Content: Two Columns Split -->
      <v-row no-gutters>
        <!-- LEFT COLUMN: Parameter Sidebar (Fixed Width) -->
        <v-col cols="12" md="4" class="parameters-sidebar border-e">
          <v-container class="pa-4 pt-0">

            <!-- Fixed Volume Context Alert -->
            <v-alert density="compact" color="info" variant="tonal" icon="mdi-cube-outline" class="mb-4 text-caption"
              border="start">
              Analyzing Volume: <span class="font-weight-bold">{{ props.volumeLabel }}</span>
            </v-alert>

            <!-- 0. SPATIAL EXTENT: Latitude, Longitude, Depth -->
            <v-list-subheader class="font-weight-bold px-1 text-uppercase text-grey">0. Spatial Extent</v-list-subheader>

            <!-- Latitude Range -->
            <div class="px-1 mb-2">
              <div class="text-caption font-weight-medium mb-1">Latitude Range:</div>
              <v-row density="compact" no-gutters>
                <v-col cols="6" class="pr-1">
                  <v-text-field v-model.number="latitudeMin" type="number" variant="outlined" density="compact"
                    hide-details single-line label="Min" color="warning" step="0.01"></v-text-field>
                </v-col>
                <v-col cols="6" class="pl-1">
                  <v-text-field v-model.number="latitudeMax" type="number" variant="outlined" density="compact"
                    hide-details single-line label="Max" color="warning" step="0.01"></v-text-field>
                </v-col>
              </v-row>
            </div>

            <!-- Longitude Range -->
            <div class="px-1 mb-2">
              <div class="text-caption font-weight-medium mb-1">Longitude Range:</div>
              <v-row density="compact" no-gutters>
                <v-col cols="6" class="pr-1">
                  <v-text-field v-model.number="longitudeMin" type="number" variant="outlined" density="compact"
                    hide-details single-line label="Min" color="warning" step="0.01"></v-text-field>
                </v-col>
                <v-col cols="6" class="pl-1">
                  <v-text-field v-model.number="longitudeMax" type="number" variant="outlined" density="compact"
                    hide-details single-line label="Max" color="warning" step="0.01"></v-text-field>
                </v-col>
              </v-row>
            </div>

            <!-- Depth Range -->
            <div class="px-1 mb-2">
              <div class="text-caption font-weight-medium mb-1">Depth Range (m):</div>
              <v-row density="compact" no-gutters>
                <v-col cols="6" class="pr-1">
                  <v-text-field v-model.number="depthMin" type="number" variant="outlined" density="compact"
                    hide-details single-line label="Min" color="warning" step="1"></v-text-field>
                </v-col>
                <v-col cols="6" class="pl-1">
                  <v-text-field v-model.number="depthMax" type="number" variant="outlined" density="compact"
                    hide-details single-line label="Max" color="warning" step="1"></v-text-field>
                </v-col>
              </v-row>
            </div>

            <v-divider class="my-4"></v-divider>

            <!-- 1. DATA SOURCE: Variable List & Stat Picker Combined -->
            <v-list-subheader class="font-weight-bold px-1 text-uppercase text-grey">1. Define Data
              Metric</v-list-subheader>
            <v-row density="compact">
              <v-col cols="8" class="pr-1">
                <!-- Using v-select for variables as requested, it's efficient space-wise -->
                <v-select v-model="variableListModel" :items="availableVariables" item-title="name" item-value="id"
                  variant="outlined" density="compact" hide-details base-color="warning" label="Primary Variable">
                  <template v-slot:item="{ props, item }">
                    <v-list-item v-bind="props"></v-list-item>
                  </template>
                </v-select>
              </v-col>
              <v-col cols="4" class="pl-1">
                <!-- Button Toggle is perfect for Min/Mean/Max, very visual -->
                <v-btn-toggle v-model="primaryStat" color="warning" mandatory variant="outlined" density="compact"
                  full-width>
                  <v-btn v-for="stat in statsTypes" :key="stat.id" :value="stat.id">
                    {{ stat.name }}
                  </v-btn>
                </v-btn-toggle>
              </v-col>
            </v-row>

            <v-divider class="my-4"></v-divider>

            <!-- 2. ANALYSIS MODE: Prominent Switcher -->
            <v-list-subheader class="font-weight-bold px-1 text-uppercase text-grey">2. Select Analysis
              Mode</v-list-subheader>
            <v-select v-model="selectedAnalysisMode" :items="analysisModes" item-title="name" item-value="id"
              variant="outlined" density="compact" color="warning" hide-details>
              <template v-slot:item="{ props, item }">
                <!-- <v-list-item v-bind="props" :prepend-icon="item.icon" :subtitle="item.desc"></v-list-item> -->
                <v-list-item v-bind="props" :prepend-icon="item.icon">
                  <template v-slot:subtitle>
                    <div class="text-caption text-grey-darken-1">{{ item.desc }}</div>
                  </template>
                </v-list-item>
              </template>
            </v-select>

            <!-- Dynamic Parameter Panel: Threshold (Progressive Disclosure) -->
            <v-expand-transition>
              <v-sheet v-if="showThresholdParams" class="pa-3 border border-alert bg-alert-lighten rounded mb-2"
                variant="outlined">
                <div class="text-caption font-weight-bold mb-2">Threshold Condition:</div>
                <v-row density="compact" align="center" no-gutters>
                  <v-col cols="8">
                    <v-text-field v-model.number="thresholdValue" type="number" variant="outlined" density="compact"
                      hide-details single-line label="Value" color="warning"></v-text-field>
                  </v-col>
                  <v-col cols="4" class="pl-1">
                    <!-- Segmented button for direct selector like this -->
                    <v-btn-toggle v-model="thresholdDirection" mandatory variant="tonal" density="compact" full-width>
                      <v-btn value=">">Above</v-btn>
                      <v-btn value="<">Below</v-btn>
                    </v-btn-toggle>
                  </v-col>
                </v-row>
              </v-sheet>
            </v-expand-transition>

            <!-- Dynamic Parameter Panel: Correlation Variables (Progressive Disclosure) -->
            <v-expand-transition>
              <v-sheet v-if="showCorrelationParams"
                class="pa-3 border border-secondary bg-secondary-lighten rounded mb-2" variant="outlined">
                <div class="text-caption font-weight-bold mb-2">Compare against:</div>
                <v-row density="compact" no-gutters>
                  <v-col cols="8">
                    <v-select v-model="secondVariable" :items="availableVariables" item-title="name" item-value="id"
                      variant="outlined" density="compact" hide-details label="Secondary Variable"
                      color="secondary"></v-select>
                  </v-col>
                  <v-col cols="4" class="pl-1">
                    <v-btn-toggle v-model="secondStat" color="secondary" mandatory variant="outlined" density="compact"
                      full-width>
                      <v-btn value="min" class="px-1 text-caption">Min</v-btn>
                      <v-btn value="mean" class="px-1 text-caption">Mean</v-btn>
                    </v-btn-toggle>
                  </v-col>
                </v-row>
              </v-sheet>
            </v-expand-transition>

            <v-divider class="my-4"></v-divider>

            <!-- 3. TEMPORAL FOCUS: Slider for Years, Select for Season -->
            <v-list-subheader class="font-weight-bold px-1 text-uppercase text-grey">3. Set Temporal
              Focus</v-list-subheader>

            <div class="px-1 mb-2">
              <div class="d-flex justify-space-between text-caption font-weight-medium mb-n1">
                <span>Years Range:</span>
                <span class="text-warning font-weight-bold">{{ yearRange[0] }} - {{ yearRange[1] }}</span>
              </div>
              <!-- Range Slider is undisputedly best for this visual range selection -->
              <v-range-slider v-model="yearRange" :min="minYear" :max="maxYear" :step="1" hide-details color="warning"
                density="compact" thumb-label="always" class="year-range-slider"></v-range-slider>
            </div>

            <v-select v-model="selectedSeason" :items="seasons" item-title="name" item-value="id"
              label="Seasonality Filter" variant="outlined" density="compact" hide-details color="warning"
              class="mb-1"></v-select>

            <v-divider class="my-4"></v-divider>

            <!-- 4. REFERENCE LINES (only for overlay mode) -->
            <v-expand-transition>
              <v-sheet v-if="selectedAnalysisMode === 'overlay'"
                class="pa-3 border border-info bg-info-lighten rounded mb-2" variant="outlined">
                <div class="d-flex align-center mb-2">
                  <span class="text-caption font-weight-bold">4. Reference Lines</span>
                  <v-spacer></v-spacer>
                  <v-switch v-model="showHorizontalLines" hide-details density="compact" color="info" inset
                    class="mt-n2"></v-switch>
                </div>
                <v-expand-transition>
                  <div v-if="showHorizontalLines">
                    <!-- Reference Line 1 -->
                    <v-row density="compact" align="center" no-gutters class="mb-1">
                      <v-col cols="5" class="pr-1">
                        <v-text-field v-model="hline1Label" variant="outlined" density="compact" hide-details
                          single-line label="Label 1" color="info" placeholder="Ref A"></v-text-field>
                      </v-col>
                      <v-col cols="7" class="pl-1">
                        <v-text-field v-model.number="hline1Value" type="number" variant="outlined" density="compact"
                          hide-details single-line label="Value 1" color="info"
                          :step="variableListModel === 'temperature' ? 0.5 : 0.1"></v-text-field>
                      </v-col>
                    </v-row>
                    <!-- Reference Line 2 -->
                    <v-row density="compact" align="center" no-gutters class="mb-1">
                      <v-col cols="5" class="pr-1">
                        <v-text-field v-model="hline2Label" variant="outlined" density="compact" hide-details
                          single-line label="Label 2" color="info" placeholder="Ref B"></v-text-field>
                      </v-col>
                      <v-col cols="7" class="pl-1">
                        <v-text-field v-model.number="hline2Value" type="number" variant="outlined" density="compact"
                          hide-details single-line label="Value 2" color="info"
                          :step="variableListModel === 'temperature' ? 0.5 : 0.1"></v-text-field>
                      </v-col>
                    </v-row>
                    <!-- Quick min/mean/max suggestion chips -->
                    <div class="d-flex gap-1 mt-1 flex-wrap">
                      <span class="text-caption text-grey mr-1">Snap:</span>
                      <v-chip size="x-small" color="info" variant="tonal" @click="hline1Value = computedMin"
                        class="text-caption">Min</v-chip>
                      <v-chip size="x-small" color="info" variant="tonal" @click="hline1Value = computedMean"
                        class="text-caption">Mean</v-chip>
                      <v-chip size="x-small" color="info" variant="tonal" @click="hline1Value = computedMax"
                        class="text-caption">Max</v-chip>
                    </div>
                  </div>
                </v-expand-transition>
              </v-sheet>
            </v-expand-transition>

            <v-divider class="my-4"></v-divider>

            <!-- Action Button: Prominent Execution -->
            <v-btn block color="warning" size="large" prepend-icon="mdi-chart-line"
              class="mt-6 run-analysis-btn font-weight-bold" :loading="isGenerating" @click="runAnalysis">
              Run Statistical Analysis
            </v-btn>

          </v-container>
        </v-col>

        <!-- RIGHT COLUMN: Visualization Canvas -->
        <v-col cols="12" md="8" class="visualization-canvas bg-background pl-md-2">

          <!-- Plot Placeholder: Designed to look like a common Chart Empty State -->
          <v-sheet
            class="plot-placeholder rounded-lg ma-4 pa-8 border-dashed d-flex align-center justify-center flex-column text-center"
            elevation="0" :height="500" :color="hasActivePlot ? 'white' : 'grey-lighten-4'" position="relative">
            <!-- Dynamic Header provided by computed logic -->
            <div v-if="hasActivePlot" class="plot-header w-100 pa-3 px-4 position-absolute top-0 left-0 text-start">
              <div class="text-subtitle-2 font-weight-bold text-grey-darken-3">{{ generatedPlotTitle }}</div>
              <div class="text-caption text-grey-darken-1">{{ props.volumeLabel }}</div>
            </div>

            <!-- GENERATING State -->
            <div v-if="isGenerating" class="d-flex flex-column align-center animate-pulse">
              <v-progress-circular indeterminate color="warning" size="64" class="mb-4"></v-progress-circular>
              <div class="text-h6 font-weight-medium text-warning">Querying ClickHouse...</div>
              <div class="text-caption text-grey mt-1">Aggregating 20 years of daily stats within chosen volume...</div>
            </div>

            <!-- ACTIVE PLOT rendered with echarts -->
            <div v-else-if="hasActivePlot" ref="chartContainerRef" class="real-plot-area w-100 h-100"></div>

            <!-- ERROR state -->
            <v-alert v-else-if="plotErrorMessage" type="error" icon="mdi-alert-octagon" class="w-75 pa-6"
              variant="tonal" border="start">
              <template v-slot:title> Analysis Failed </template>
              {{ plotErrorMessage }} Please adjust your constraints or selected variables.
            </v-alert>

            <!-- INITIAL/EMPTY state -->
            <div v-else
              class="empty-plot-state d-flex flex-column align-center pa-10 text-grey border rounded-circle border-lg border-dashed">
              <v-icon size="128" icon="mdi-poll"></v-icon>
              <div class="text-h6 font-weight-regular mt-5 text-grey-darken-1">Statistical Visualization Canvas</div>
              <div class="text-body-2 text-grey-darken-1 mt-1">Define your analytical constraints on the left
                sidebar<br>and
                click [Run Statistical Analysis] to visualize results here.</div>
            </div>
          </v-sheet>

        </v-col>
      </v-row>
    </v-card>
  </v-dialog>
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
  { id: 'oxygen', name: 'Dissolved Oxygen', icon: 'mdi-oxygen' },
  { id: 'u_velocity', name: 'U-Velocity (East)', icon: 'mdi-arrow-right-bold' },
  { id: 'v_velocity', name: 'V-Velocity (North)', icon: 'mdi-arrow-up-bold' },
  { id: 'nitrate', name: 'Nitrate', icon: 'mdi-molecule' },
  { id: 'diatoms', name: 'Diatoms', icon: 'mdi-microscope' }
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
  { id: 'trend', name: 'Inter-Annual Trend Analysis', icon: 'mdi-trending-up', desc: 'Long-term change tracking based on annual means.' },
  { id: 'correlation', name: 'Multi-Variable Correlation', icon: 'mdi-chart-scatter-plot', desc: 'Relationship between two variables within the selected period.' }
]

const seasons = [
  { id: 'full_year', name: 'Full Year (Jan - Dec)' },
  { id: 'jja', name: 'Summer (JJA)' },
  { id: 'mam', name: 'Spring (MAM)' },
  { id: 'son', name: 'Autumn (SON)' },
  { id: 'djf', name: 'Winter (DJF)' }
]

// --- REACTIVE STATE (User Selections) ---
const showDialog = ref(true)

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
const secondVariable = ref('oxygen')
const secondStat = ref('min')

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
    return `Correlation: ${varName} (${statName}) vs ${var2Name} (${statName}) | ${years}`
  }

  return `${modeName}: ${varName} (${statName}) | ${years}`
})

// Simple helper to check relevance of dynamic panels
const showThresholdParams = computed(() => selectedAnalysisMode.value === 'threshold')
const showCorrelationParams = computed(() => selectedAnalysisMode.value === 'correlation')

// Watch analysis mode to reset dynamic parameters to sane defaults
watch(selectedAnalysisMode, (newMode: string) => {
  selectedSeason.value = newMode === 'overlay' ? 'jja' : 'full_year'
})


// --- ECHARTS RENDERING ---

function renderOverlayChart(responseData: any) {
  console.log(chartContainerRef.value, responseData, Array.isArray(responseData.series));
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

  const echartsSeries = series.map((serie: any) => ({
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
    // Apply to the last series so it renders on top
    echartsSeries[echartsSeries.length - 1].markLine = {
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
  console.log(option);
  chartInstance.setOption(option, true)
  chartInstance.resize()
}

function renderClimatologyChart(responseData: any) {
  console.log(chartContainerRef.value, responseData, Array.isArray(responseData.data));
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

  const echartsSeries = {
    name: "Climatology Cycle",
    type: 'line',
    smooth: true,
    symbol: 'none',
    lineStyle: { width: 2 },
    data: Array.isArray(series) ? series.map((d: any) => d.value) : []
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
      data: ["Climatology Cycle"],
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
  console.log(option);
  chartInstance.setOption(option, true)
  chartInstance.resize()
}

function renderChart(responseData: any) {
  if (!responseData) return

  switch (selectedAnalysisMode.value) {
    case 'overlay':
      renderOverlayChart(responseData)
      break

    case 'climatology':
      renderClimatologyChart(responseData)
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

// Handle window resize to keep the chart responsive
function handleResize() {
  if (chartInstance) {
    chartInstance.resize()
  }
}

onMounted(() => {
  registerEchartsDarkTheme()
  window.addEventListener('resize', handleResize)
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleResize)
  if (chartInstance) {
    chartInstance.dispose()
    chartInstance = null
  }
})

// --- ACTIONS ---

const runAnalysis = () => {
  plotErrorMessage.value = null
  isGenerating.value = true
  hasActivePlot.value = false

  // Prepare the JSON structure to send to backend...
  const queryJSON = {
    gridX: { min: 100, max: 200 },
    gridY: { min: 200, max: 400 },
    depth: { min: 10, max: 20 },
    primaryMetric: { variable: variableListModel.value, stat: primaryStat.value },
    temporal: { yearRange: yearRange.value, season: selectedSeason.value },
    // Conditional addition...
    ...(showThresholdParams.value ? { threshold: { value: thresholdValue.value, direction: thresholdDirection.value } } : {}),
    ...(showCorrelationParams.value ? { secondMetric: { variable: secondVariable.value, stat: secondStat.value } } : {})
  }

  switch (selectedAnalysisMode.value) {
    case 'overlay':
      axios.post(`${apiBaseUrl}/analysis/overlay`, queryJSON)
        .then((response: any) => {
          chartData = response.data
          // Set hasActivePlot before nextTick so the template renders the chart container
          hasActivePlot.value = true
          plotErrorMessage.value = null
          // Wait for the DOM to update then render the chart
          nextTick(() => {
            setTimeout(() => {
              if (chartContainerRef.value) {
                renderChart(chartData)
              } else {
                plotErrorMessage.value = 'Chart container not found. Unable to render plot.'
                hasActivePlot.value = false
              }
            }, 100) // Slight delay to ensure container is ready
          })
        })
        .catch((error: any) => {
          plotErrorMessage.value = 'Failed to generate overlay plot. Please try again.'
          hasActivePlot.value = false
        })
        .finally(() => {
          isGenerating.value = false
        })
      break;

    case 'climatology':
      axios.post(`${apiBaseUrl}/analysis/climatology`, queryJSON)
        .then((response: any) => {
          chartData = response.data
          // Set hasActivePlot before nextTick so the template renders the chart container
          hasActivePlot.value = true
          plotErrorMessage.value = null
          // Wait for the DOM to update then render the chart
          nextTick(() => {
            setTimeout(() => {
              if (chartContainerRef.value) {
                renderChart(chartData)
              } else {
                plotErrorMessage.value = 'Chart container not found. Unable to render plot.'
                hasActivePlot.value = false
              }
            }, 100) // Slight delay to ensure container is ready
          })
        })
        .catch((error: any) => {
          plotErrorMessage.value = 'Failed to generate overlay plot. Please try again.'
          hasActivePlot.value = false
        })
        .finally(() => {
          isGenerating.value = false
        })
      break;
    default:
      isGenerating.value = false
      break;
  }
}

const resetParameters = () => {
  variableListModel.value = 'temperature'
  primaryStat.value = 'mean'
  selectedAnalysisMode.value = 'overlay'
  yearRange.value = [2015, 2020]
  selectedSeason.value = 'full_year'
  latitudeMin.value = 46
  latitudeMax.value = 52
  longitudeMin.value = -127
  longitudeMax.value = -121
  depthMin.value = 0
  depthMax.value = 100
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
.ocean-analysis-builder {
  font-family: 'Roboto', sans-serif;
}

.parameters-sidebar {
  max-width: 420px;
  /* Kept tight to maximize chart space */
}

.visualization-canvas {
  /* height: calc(100vh - 64px); */
  /* Fills screen height minus toolbar, adjustable */
  min-height: 580px;
}

.mode-selector :deep(.v-list-item-subtitle) {
  opacity: 0.8 !important;
  font-size: 0.8rem;
  line-height: 1.1rem;
  white-space: pre-line;
  /* Allows line breaks in desc */
}

/* Adjust Range Slider labels for tight layout */
.year-range-slider :deep(.v-slider-thumb__label) {
  background-color: var(--v-theme-warning);
}

/* Designed Empty State appearance */
.plot-placeholder {
  transition: background-color 0.4s ease-in-out;
}

/* Dash border styling for placeholder */
.plot-placeholder.border-dashed {
  border: 3px dashed rgba(var(--v-theme-grey-darken-1), 0.2) !important;
}

.real-plot-area {
  min-height: 400px;
  width: 100%;
  height: 460px;
}

@keyframes pulse {

  0%,
  100% {
    opacity: 1;
  }

  50% {
    opacity: 0.7;
  }
}

.animate-pulse {
  animation: pulse 2.5s cubic-bezier(0.4, 0, 0.6, 1) infinite;
}

/* Colors for dynamic parameter background highlights */
.border-alert {
  border-color: rgba(var(--v-theme-error), 0.5) !important;
}

.bg-alert-lighten {
  background-color: rgba(var(--v-theme-error), 0.05) !important;
}

.border-secondary {
  border-color: rgba(var(--v-theme-secondary), 0.5) !important;
}

.bg-secondary-lighten {
  background-color: rgba(var(--v-theme-secondary), 0.05) !important;
}

/* Reference lines panel */
.border-info {
  border-color: rgba(var(--v-theme-info), 0.5) !important;
}

.bg-info-lighten {
  background-color: rgba(var(--v-theme-info), 0.05) !important;
}

/* Ensure chips in the snap row are inline */
.gap-1 {
  gap: 4px;
}
</style>