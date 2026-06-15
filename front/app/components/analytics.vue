<template>
  <v-navigation-drawer v-model="isOpen" location="right" width="50%" class="pa-2" absolute persistent mobile
    :scrim="false" style="height:100%; z-index:9999; top:0;">
    <v-card class="ocean-analysis-builder pa-4" elevation="0">
      <v-toolbar flat dense class="px-0 mb-4">
        <v-toolbar-title class="text-h6 font-weight-bold">Ocean Analysis Builder</v-toolbar-title>
        <v-spacer />
        <v-btn icon="mdi-refresh" variant="text" @click="resetParameters" title="Reset all criteria" />
      </v-toolbar>

      <v-sheet class="chart-card rounded-lg mb-4" elevation="2">
        <div class="chart-header d-flex align-center justify-space-between px-4 py-3">
          <div>
            <div class="text-subtitle-1 font-weight-bold">{{ generatedPlotTitle }}</div>
            <div class="text-caption text-grey-darken-1">{{ props.volumeLabel }}</div>
          </div>
          <v-chip color="warning" variant="tonal">{{ selectedSeasonLabel }}</v-chip>
        </div>

        <div class="chart-body">
          <div v-if="isGenerating" class="d-flex flex-column align-center justify-center fill-height">
            <v-progress-circular indeterminate color="warning" size="64" class="mb-4" />
            <div class="text-subtitle-2 font-weight-medium text-warning">Querying ClickHouse...</div>
            <div class="text-body-2 text-grey mt-1">Aggregating data for selected bounds and period.</div>
          </div>

          <div v-else-if="hasActivePlot" ref="chartContainerRef" class="real-plot-area w-100 h-100" />

          <v-alert v-else-if="plotErrorMessage" type="error" icon="mdi-alert-octagon" class="mx-4 my-8 pa-6"
            variant="tonal" border="start">
            <template #title>Analysis Failed</template>
            {{ plotErrorMessage }} Please adjust your threshold or selected variables.
          </v-alert>

          <div v-else class="empty-plot-state d-flex flex-column align-center text-center px-6 py-10">
            <v-icon size="96" icon="mdi-poll" />
            <div class="text-h6 font-weight-regular mt-4 text-grey-darken-1">Statistical Visualization Canvas</div>
            <div class="text-body-2 text-grey-darken-1 mt-2">Choose threshold and season settings below, then run the
              analysis.
            </div>
          </div>
        </div>
      </v-sheet>

      <v-row dense>
        <v-col cols="12" md="5">
          <v-card class="analysis-control-panel pa-4" elevation="1">
            <div class="text-subtitle-2 font-weight-bold mb-3">Threshold & Condition Controls</div>

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

              <v-col cols="12">
                <v-select v-model="variableListModel" :items="availableVariables" item-title="name" item-value="id"
                  label="Primary Variable" variant="outlined" density="compact" hide-details color="warning" />
              </v-col>

              <v-col cols="12">
                <v-btn-toggle v-model="primaryStat" mandatory variant="outlined" density="compact" class="w-100">
                  <v-btn v-for="stat in statsTypes" :key="stat.id" :value="stat.id">{{ stat.name }}</v-btn>
                </v-btn-toggle>
              </v-col>
            </v-row>

            <v-btn block color="warning" size="large" prepend-icon="mdi-chart-line" class="mt-4" :loading="isGenerating"
              @click="runAnalysis">
              Run Statistical Analysis
            </v-btn>
          </v-card>
        </v-col>

        <v-col cols="12" md="7">
          <v-card class="analysis-results-panel pa-4" elevation="1">
            <div class="d-flex align-center justify-space-between mb-3">
              <div>
                <div class="text-subtitle-2 font-weight-bold">Analysis Results</div>
                <div class="text-caption text-grey-darken-1">Summary computed from chart data and current threshold.
                </div>
              </div>
              <v-chip color="warning" variant="tonal">
                {{ thresholdDirectionLabel }} {{ thresholdValue }} · {{ selectedSeasonLabel }}
              </v-chip>
            </div>

            <v-data-table :headers="summaryHeaders" :items="summaryRows" dense hide-default-footer
              class="summary-table" />

            <div v-if="summaryRows.length === 0" class="text-caption text-center text-grey-darken-1 py-8">
              Run the analysis to populate threshold results.
            </div>
          </v-card>
        </v-col>
      </v-row>
    </v-card>
  </v-navigation-drawer>
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
// Current Fixed Volume Context (Passed down as props in real app)
const props = defineProps({
  volumeLabel: { type: String, default: 'Northern Strait of Georgia (Depth: 10-20m)' }
})

// UI State
const isOpen = ref(true)

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

const summaryHeaders = [
  { title: 'Series', key: 'series' },
  { title: 'Matches', key: 'matches' },
  { title: 'Min', key: 'min' },
  { title: 'Max', key: 'max' },
  { title: 'Mean', key: 'mean' },
  { title: 'Condition', key: 'condition' }
]

const selectedSeasonLabel = computed(() => {
  return seasons.find(season => season.id === selectedSeason.value)?.name || 'Season'
})

const thresholdDirectionLabel = computed(() => (thresholdDirection.value === '>' ? 'Above' : 'Below'))

const summaryRows = computed(() => {
  if (!chartData) return []

  const threshold = thresholdValue.value
  const direction = thresholdDirection.value
  const compare = (value: number) => direction === '>' ? value > threshold : value < threshold

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
        condition: `${thresholdDirectionLabel.value} ${threshold}`
      }
    })
  }

  if (Array.isArray(chartData.data)) {
    const values = chartData.data.map((d: any) => Number(d.value)).filter((v: number) => !Number.isNaN(v))
    const matches = values.filter(compare)
    const min = values.length ? Math.min(...values) : null
    const max = values.length ? Math.max(...values) : null
    const mean = values.length ? (values.reduce((sum: number, v: number) => sum + v, 0) / values.length) : null

    return [{
      series: 'Climatology Cycle',
      matches: matches.length,
      min: min != null ? min.toFixed(2) : '—',
      max: max != null ? max.toFixed(2) : '—',
      mean: mean != null ? mean.toFixed(2) : '—',
      condition: `${thresholdDirectionLabel.value} ${threshold}`
    }]
  }

  return []
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

  const echartsSeries: echarts.LineSeriesOption = {
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

.chart-card {
  min-height: 460px;
  display: flex;
  flex-direction: column;
}

.chart-body {
  flex: 1;
  min-height: 360px;
  position: relative;
}

.analysis-control-panel,
.analysis-results-panel {
  min-height: 420px;
}

.summary-table {
  width: 100%;
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