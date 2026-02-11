<template>
    <v-main>
        <!-- Header with Controls -->
        <v-container class="pa-4 bg-light">
            <!-- Row 1: Sensor, Variable, Model, Load Button -->
            <v-row align="center" class="mb-4">
                <v-col cols="12" md="3">
                    <v-select v-model="selectedSensorId" :items="sensorIds" label="Select Sensor ID" outlined dense
                        @update:modelValue="onSelectionChange" />
                </v-col>
                <v-col cols="12" md="3">
                    <v-select v-model="selectedVariable" :items="variables" label="Select Variable" outlined dense
                        @update:modelValue="onSelectionChange" />
                </v-col>
                <v-col cols="12" md="2">
                    <v-select v-model="selectedModel" :items="models" label="Select Model" outlined dense
                        @update:modelValue="onSelectionChange" />
                </v-col>
                <v-col cols="12" md="4">
                    <v-btn color="primary" @click="fetchEvalData" :loading="loading" block>
                        Load Data
                    </v-btn>
                </v-col>
            </v-row>

            <!-- Row 2: Date Range Selector -->
            <v-row align="center" class="mb-4">
                <v-col cols="12" sm="6" md="4">
                    <v-menu v-model="fromDateMenu" :close-on-content-click="false" :disabled="isDatePickersDisabled">
                        <template #activator="{ props }">
                            <v-text-field :model-value="fromDateDisplay" label="From Date" variant="outlined" density="compact"
                                readonly :disabled="isDatePickersDisabled" v-bind="props" />
                        </template>
                        <v-date-picker v-model="fromDate" :min="minDate" :max="toDate || maxDate" @update:modelValue="onDateChange" />
                    </v-menu>
                </v-col>
                <v-col cols="12" sm="6" md="4">
                    <v-menu v-model="toDateMenu" :close-on-content-click="false" :disabled="isDatePickersDisabled">
                        <template #activator="{ props }">
                            <v-text-field :model-value="toDateDisplay" label="To Date" variant="outlined" density="compact"
                                readonly :disabled="isDatePickersDisabled" v-bind="props" />
                        </template>
                        <v-date-picker v-model="toDate" :min="fromDate || minDate" :max="maxDate" @update:modelValue="onDateChange" />
                    </v-menu>
                </v-col>
            </v-row>
        </v-container>

        <!-- Error Message -->
        <v-alert v-if="error" type="error" class="mx-4 mt-4">
            {{ error }}
        </v-alert>

        <!-- Time Series Chart Component -->
        <EvalTimeseries v-if="data" :data="data" :selected-variable="selectedVariable" :start-time="startTimeFromDate" :end-time="endTimeFromDate" @update-date-range="onTimeseriesZoom" />

        <!-- Scatter Chart Component -->
        <EvalScatter v-if="data" :data="data" :start-time="startTimeFromDate" :end-time="endTimeFromDate" />
    </v-main>
</template>


<script setup lang="ts">
import { ref, onMounted, computed } from 'vue'
import axios from 'axios'
import { useRuntimeConfig } from '#app'
import EvalTimeseries from '~/components/eval/timeseries.vue'
import EvalScatter from '~/components/eval/scatter.vue'
import { da } from 'vuetify/locale'

// Configuration
const apiBaseUrl = useRuntimeConfig().public.apiBaseUrl || 'http://localhost:3000'

// Data & State
const selectedSensorId = ref<number | null>(1)
const selectedVariable = ref<string>('temperature')
const selectedModel = ref<string>('SSC')
const sensorIds = ref<number[]>([])
const variables = ref<string[]>(['temperature', 'salinity', 'dissolved_oxygen', 'pCO2'])
const models = ref<string[]>(['SSC', 'LiveOcean'])

const loading = ref(false)
const error = ref('')
const data = ref<any>(null)

// Zoom state
const startTime = ref<number | null>(null)
const endTime = ref<number | null>(null)

// Date range state
const fromDate = ref<string>('')
const toDate = ref<string>('')

// Date menu state
const fromDateMenu = ref(false)
const toDateMenu = ref(false)

/**
 * Format date string to display format (e.g., "Feb 03, 2026")
 */
function formatDateDisplay(dateString: string): string {
    if (!dateString) return ''
    const date = new Date(dateString)
    return date.toLocaleDateString('en-US', { month: 'short', day: '2-digit', year: 'numeric' })
}

/**
 * Computed properties for formatted date display
 */
const fromDateDisplay = computed({
    get: () => formatDateDisplay(fromDate.value),
    set: (val) => {
        // Keep the actual value in ISO format for v-date-picker
    }
})

const toDateDisplay = computed({
    get: () => formatDateDisplay(toDate.value),
    set: (val) => {
        // Keep the actual value in ISO format for v-date-picker
    }
})

/**
 * Convert timestamp to date string (YYYY-MM-DD format)
 */
function formatDateString(timestamp: number | null): string {
    if (!timestamp) return ''
    const date = new Date(timestamp)
    return date.toISOString().split('T')[0]
}

/**
 * Computed properties for date picker limits and disabled state
 */
const minDate = computed(() => formatDateString(startTime.value))
const maxDate = computed(() => formatDateString(endTime.value))
const isDataLoaded = computed(() => !!data.value)
const isDatePickersDisabled = computed(() => !isDataLoaded.value)

/**
 * Convert date string to timestamp (for scatter chart filtering)
 */
function dateStringToTimestamp(dateString: string): number | null {
    if (!dateString) return null
    return new Date(dateString).getTime()
}

/**
 * Computed properties for start and end times from date inputs
 */
const startTimeFromDate = computed(() => dateStringToTimestamp(fromDate.value))
const endTimeFromDate = computed(() => dateStringToTimestamp(toDate.value))
async function fetchEvalData() {
    if (!selectedSensorId.value || !selectedVariable.value) {
        error.value = 'Please select both sensor ID and variable'
        return
    }

    loading.value = true
    error.value = ''

    try {
        const response = await axios.post(`${apiBaseUrl}/getEval`, {
            sensor_id: selectedSensorId.value,
            variable: selectedVariable.value,
            model: selectedModel.value
        })

        data.value = response.data
        const len_time = data.value.time.length
        if (len_time > 0) {
            startTime.value = new Date(data.value.time[0]).getTime()
            endTime.value = new Date(data.value.time[len_time - 1]).getTime()
        }
        
        // Reset date pickers to full data range when new data is loaded
        if (startTime.value) {
            fromDate.value = formatDateString(startTime.value)
        }
        if (endTime.value) {
            toDate.value = formatDateString(endTime.value)
        }
    } catch (err: any) {
        error.value = `Failed to fetch data: ${err.message}`
        console.error(err)
    } finally {
        loading.value = false
    }
}

/**
 * Handle selection change
 */
function onSelectionChange() {
    error.value = ''
}

/**
 * Handle date range change
 */
function onDateChange() {
    // Validate dates don't cross each other
    if (fromDate.value && toDate.value) {
        if (fromDate.value > toDate.value) {
            // If from date is greater than to date, adjust to date to match from date
            toDate.value = fromDate.value
        }
        if (toDate.value < fromDate.value) {
            // If to date is less than from date, adjust from date to match to date
            fromDate.value = toDate.value
        }
    }
    
    // Clear error on date change
    error.value = ''
    // Close menus after selection
    fromDateMenu.value = false
    toDateMenu.value = false
}

/**
 * Handle timeseries zoom event - update date pickers from chart zoom
 */
function onTimeseriesZoom(event: { fromDate: string; toDate: string }) {
    fromDate.value = event.fromDate
    toDate.value = event.toDate
    error.value = ''
    console.log(`Timeseries zoom updated date range: from ${event.fromDate} to ${event.toDate}`)
}

/**
 * Initialize on mount
 */
onMounted(async () => {
    try {
        const response = await axios.get(`${apiBaseUrl}/sensors`)
        sensorIds.value = response.data.map((s: any) => s.id)
    } catch (err) {
        console.warn('Failed to fetch sensor IDs')
    }
})
</script>

<style scoped>
.bg-light {
    background-color: #fff;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
}

h3 {
    margin-bottom: 16px;
    color: #333;
}
</style>