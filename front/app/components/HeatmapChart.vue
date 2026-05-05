<template>
    <v-card class="pa-4" style="width: 100%; height: 100%;">
        <v-row>
            <v-col v-if="showDateWarning" cols="12">
                <v-alert type="warning" density="compact">
                    {{ dateWarning }}
                </v-alert>
            </v-col>

            <v-col cols="auto" style="align-content: center;">
                <v-select v-model="plotVariable" :items="sensorVariables" label="Variable" item-title="label"
                    item-value="var" density="compact" hide-details variant="outlined" class="my-4">
                </v-select>
            </v-col>

            <v-col cols="auto" style="align-content: center;">
                <v-btn-group density="compact" variant="outlined" class="ml-2">
                    <v-btn size="small" @click="setPresetDateRange(7)">1W</v-btn>
                    <v-btn size="small" @click="setPresetDateRange(30)">1M</v-btn>
                    <v-btn size="small" @click="setPresetDateRange(90)">3M</v-btn>
                    <v-btn size="small" @click="setPresetDateRange(365)">1Y</v-btn>
                </v-btn-group>
            </v-col>

            <v-col cols="auto" style="align-content: center;">
                <v-menu v-model="datePickerMenuOpen" :close-on-content-click="false" offset-y>
                    <template #activator="{ props }">
                        <v-btn variant="outlined" v-bind="props">{{ 
                    fromDate && toDate 
                        ? `${moment(fromDate).format('DD MMM, YYYY')} - ${moment(toDate).format('DD MMM, YYYY')}` 
                        : (pendingDateRange.length === 2 
                            ? `${moment(pendingDateRange[0]).format('DD MMM, YYYY')} - ${moment(pendingDateRange[1]).format('DD MMM, YYYY')}` 
                            : 'Select Time Range')
                }}</v-btn>
                    </template>

                    <v-card>
                        <v-date-picker v-model="pendingDateRange" multiple="range" :min="minDate" :max="maxDate"
                            class="ma-0 pa-0" />
                        <v-card-actions>
                            <v-spacer></v-spacer>
                            <v-btn text @click="cancelDatePicker">Cancel</v-btn>
                            <v-btn color="primary" @click="confirmDatePicker">OK</v-btn>
                        </v-card-actions>
                    </v-card>
                </v-menu>
            </v-col>

        </v-row>

        <div ref="chartContainer" style="width: 100%; height: 100%;"></div>
        <div v-if="loading" class="global-chart-overlay">
            <v-progress-circular indeterminate color="warning" :size="64" :width="12" class="progress" />
        </div>
    </v-card>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onBeforeUnmount } from 'vue';
import * as echarts from 'echarts';
import { registerEchartsDarkTheme } from '../../composables/useEchartsTheme';
import axios from 'axios';
import moment from 'moment';

import { useMainStore } from '../stores/main';
const mainStore = useMainStore();

import colors from 'vuetify/util/colors';

const config = useRuntimeConfig();
const apiBaseUrl = config.public.apiBaseUrl

// ── Props ─────────────────────────────────────────────────────────────────────

const props = defineProps<{
    sensorId: number;
    modelVariable: string;
}>();

///////////////////////////////////////////  STATE  ///////////////////////////////////////////

const chartContainer = ref<HTMLDivElement | null>(null);
let chart: echarts.ECharts | null = null;
const loading = ref(false);

const fromDate = ref<string>('');
const toDate = ref<string>('');
const datePickerMenuOpen = ref(false);
const pendingDateRange = ref<string[]>([]);
const showDateWarning = ref(false);
const dateWarning = ref('');

const times = ref<string[]>([]);
const depths = ref<number[]>([]);
const data = ref<number[][]>([]);

const plotVariable = ref<string>(props.modelVariable);
const sensorVariables = computed(() => {
    const sensor = mainStore.sensors.find(s => s.id === props.sensorId);
    return sensor ? Object.keys(sensor.variables).filter(v=>v!=='time') : [];
});

const minDate = computed(() => {
    const sensor = mainStore.sensors.find(s => s.id === props.sensorId);
    if (!sensor) return '';
    const varData = sensor.variables[plotVariable.value];
    return varData && typeof varData === 'object' ? varData.from_datetime || '' : '';
});
const maxDate = computed(() => {
    const sensor = mainStore.sensors.find(s => s.id === props.sensorId);
    if (!sensor) return '';
    const varData = sensor.variables[plotVariable.value];
    return varData && typeof varData === 'object' ? varData.to_datetime || '' : '';
});

/**
 * Get the colormap colors for the current variable from mainStore.
 * Returns an array of hex colors or the default blue-red diverging colormap if not found.
 */
const colormapColors = computed(() => {
    const sensor = mainStore.sensors.find(s => s.id === props.sensorId);
    if (!sensor) return getDefaultColormap();
    
    const varData = sensor.variables[plotVariable.value];
    if (!varData || typeof varData !== 'object') return getDefaultColormap();
    
    // The variable data should have a colormap field with the colormap name
    // For now, we'll look for it in the stored variables from the API
    // This could be enhanced if we store the colormap name in sensor.variables
    
    // Try to find the variable in mainStore.variables
    const mainVar = mainStore.variables?.find((v: any) => v.var === plotVariable.value);
    if (!mainVar || !mainVar.colormap) return getDefaultColormap();
    
    const colormap = mainStore.colormaps[mainVar.colormap];
    if (!colormap || !colormap.stops) return getDefaultColormap();
    
    // If stops is an array of [value, color] pairs, extract just the colors
    if (Array.isArray(colormap.stops[0])) {
        return colormap.stops.map((stop: any) => stop[1]);
    }
    
    // If stops is already just an array of colors
    return colormap.stops;
});

function getDefaultColormap(): string[] {
    // Default blue-red diverging colormap
    return [
        '#313695', '#4575b4', '#74add1', '#abd9e9',
        '#e0f3f8', '#ffffbf',
        '#fee090', '#fdae61', '#f46d43', '#d73027', '#a50026',
    ];
}

///////////////////////////////////////////  METHODS  ///////////////////////////////////////////

function resolvedTimes() {
    // const times = times.value.length ? times.value : PLACEHOLDER_TIMES;

    // Return uniq values
    return Array.from(new Set(times.value));
}
function resolvedDepths() {
    // const depths = depths.value.length ? depths.value : PLACEHOLDER_DEPTHS;
    return Array.from(new Set(depths.value));
}
function resolvedData() { return data.value.length ? data.value : []; }

/** Convert flat 1D data into ECharts [ti, di, value] triplets. */
function toTriplets(): [number, number, number | null][] {
    const flat = resolvedData();
    const depths = resolvedDepths();
    const times = resolvedTimes();


    const nd = depths.length
    const nt = times.length;
    console.log(`Resolved times: ${nt}, depths: ${nd}, data points: ${flat.length}`);

    return flat.map((v, i) => {
        const ti = i / nd;
        const di = i % nd;
        return [ti, di, v];
    });
}

function dataMinMax(): [number, number] {
    const values = resolvedData().filter((v): v is number => v !== null && isFinite(v));
    if (!values.length) return [0, 1];
    
    let min = Infinity;
    let max = -Infinity;
    for (let i = 0; i < values.length; i++) {
        if (values[i] < min) min = values[i];
        if (values[i] > max) max = values[i];
    }
    
    return [min, max];
}

// ── Sensor Gridded Data Fetcher ───────────────────────────────────────────────

/**
 * Fetch gridded sensor data (time x depth) from the API.
 * 
 * @param sensorId - Sensor ID
 * @param modelVariable - Canonical variable name (e.g., 'dissolved_oxygen', 'temperature')
 * @param fromDate - Start date (ISO-8601 string)
 * @param toDate - End date (ISO-8601 string)
 * @returns Promise resolving to {times, depths, data} or null on error
 */
async function fetchSensorGriddedData(
    sensorId: number,
    modelVariable: string,
    fromDate: string,
    toDate: string
): Promise<{ times: string[]; depths: number[]; data: (number | null)[] } | null> {
    try {
        console.log(`Fetching gridded data for sensor ${sensorId}, variable ${modelVariable}, from ${fromDate} to ${toDate}`);
        loading.value = true;

        const response = await axios.post(`${apiBaseUrl}/sensorTimeseries`, {
            sensorId,
            modelVariable,
            fromDate,
            toDate,
            // Omit depth to get all depths (2D gridded response)
        });

        console.log(response);

        if (response.status !== 200) {
            const errorText = await response.text();
            console.error(`API error: ${response.status} - ${errorText}`);
            return null;
        }

        const result = await response.data;

        // Handle 2D response: { time: [...], depth: [...], value: [...] }
        if (result.depth && Array.isArray(result.depth)) {
            const times = result.time || [];
            const depths = result.depth || [];
            const values = result.value || [];
            return { times, depths, data: values };
        }

        // Handle 1D response: { time: [...], value: [...] }
        // (when sensor only has single depth level or API returns 1D)
        if (result.time && !result.depth) {
            const times = result.time || [];
            const values = result.value || [];
            return { times, depths: [0], data: values };
        }

        console.warn('Unexpected API response format:', result);
        return null;
    } catch (error) {
        console.error('Failed to fetch sensor gridded data:', error);
        return null;
    } finally {
        loading.value = false;
    }
}

// ── Chart ─────────────────────────────────────────────────────────────────────

function buildOption(): echarts.EChartsOption {
    const times = resolvedTimes();
    const depths = resolvedDepths();
    const [autoMin, autoMax] = dataMinMax();

    const minVal = autoMin;
    const maxVal = autoMax;

    const options = {
        tooltip: {
            trigger: 'item',
            formatter: (params: any) => {
                const [ti, di, v] = params.data as [number, number, number | null];
                const t = times[ti] ?? ti;
                const d = depths[di] ?? di;
                const val = v !== null ? v.toFixed(3) : 'N/A';
                return `${t}<br/>Depth: ${d} m<br/>${props.modelVariable ?? 'Value'}: ${val}`;
            },
        },
        toolbox: {
            feature: {
                dataZoom: {},
                saveAsImage: {},
            },
        },
        grid: { left: 70, right: 120, top: 40, bottom: 60 },
        xAxis: {
            type: 'category',
            data: times,
            axisLabel: {
                color: '#e0e0e0',
                rotate: 30,
                formatter: (val: string) => moment(val).format('DD MMM, YYYY'),
            },
            splitArea: { show: false },
        },
        yAxis: {
            type: 'category',
            data: depths.map(d => `${d} m`),
            axisLabel: { color: '#e0e0e0' },
            inverse: true,   // depth increases downward
            splitArea: { show: false },
        },
        visualMap: {
            min: minVal,
            max: maxVal,
            calculable: true,
            orient: 'vertical',
            right: 10,
            top: 'center',
            inRange: {
                color: colormapColors.value,
            },
            text: [String(maxVal.toFixed(1)), String(minVal.toFixed(1))],
            textStyle: { color: '#e0e0e0' },
        },
        series: [
            {
                type: 'heatmap',
                data: toTriplets(),
                progressive: 1000,
                progressiveThreshold: 3000,
                emphasis: {
                    itemStyle: { borderColor: '#fff', borderWidth: 1 },
                },
            },
        ],
    };

    console.log(options);
    return options;
}

function initChart() {
    if (!chartContainer.value) return;
    try { if (chart) { chart.dispose(); chart = null; } } catch (_) { }
    registerEchartsDarkTheme();
    chart = echarts.init(chartContainer.value, 'dark', { renderer: 'canvas' });
    chart.setOption(buildOption());
    chart.resize();
}

function updateChart() {
    if (!chart) return;
    chart.setOption(buildOption(), { notMerge: true });
}

/**
 * Common helper to load chart data for a given date range and variable.
 * Handles data fetching, clearing, and error handling in one place.
 */
async function loadChartData(
    sensorId: number,
    variable: string,
    start: string,
    end: string
): Promise<boolean> {
    try {
        console.log(`Loading data: sensor=${sensorId}, variable=${variable}, from=${start} to=${end}`);
        
        // Clear data BEFORE fetching to trigger single watch update when new data arrives
        times.value = [];
        depths.value = [];
        data.value = [];
        
        const result = await fetchSensorGriddedData(sensorId, variable, start, end);
        if (result) {
            times.value = result.times;
            depths.value = result.depths;
            data.value = result.data;
            return true;
        }
        return false;
    } catch (error) {
        console.error('Error loading chart data:', error);
        return false;
    }
}

function confirmDatePicker() {
    if (pendingDateRange.value && pendingDateRange.value.length === 2) {
        const start = moment(pendingDateRange.value[0]);
        const end = moment(pendingDateRange.value[1]);
        const daysDiff = end.diff(start, 'days');
        const maxDays = 370; // ~1 year

        // Check if date range exceeds 1 year
        if (daysDiff > maxDays) {
            dateWarning.value = `Date range exceeds 1 year (${daysDiff} days selected). Please select a shorter range to avoid performance issues.`;
            showDateWarning.value = true;
            datePickerMenuOpen.value = false;
            return;
        }
        else {
            showDateWarning.value = false;
            dateWarning.value = '';
        }

        fromDate.value = pendingDateRange.value[0];
        toDate.value = pendingDateRange.value[1];
        showDateWarning.value = false;
        dateWarning.value = '';
        
        loadChartData(props.sensorId, plotVariable.value, fromDate.value, toDate.value);
    }
    datePickerMenuOpen.value = false;
}

function cancelDatePicker() {
    pendingDateRange.value = [];
    datePickerMenuOpen.value = false;
}

/**
 * Set a preset date range and fetch data.
 * @param days Number of days back from today
 */
function setPresetDateRange(days: number) {
    const endDate = moment().isAfter(moment(maxDate)) ? moment(maxDate) : moment();
    const startDate = endDate.clone().subtract(days, 'days');

    // Ensure start date doesn't go before available data
    const minDateMoment = moment(minDate);
    if (startDate.isBefore(minDateMoment)) {
        startDate.set(minDateMoment.toDate());
    }

    fromDate.value = startDate.format('YYYY-MM-DD');
    toDate.value = endDate.format('YYYY-MM-DD');
    pendingDateRange.value = [fromDate.value, toDate.value];
    
    showDateWarning.value = false;
    dateWarning.value = '';
    
    loadChartData(props.sensorId, plotVariable.value, fromDate.value, toDate.value);
}

// ── Resize observer ───────────────────────────────────────────────────────────

let resizeObserver: ResizeObserver | null = null;

///////////////////////////////  LIFECYCLE  ////////////////////////////////////////////

onMounted(() => {
    initChart();
    if (chartContainer.value) {
        resizeObserver = new ResizeObserver(() => chart?.resize());
        resizeObserver.observe(chartContainer.value);
    }

    // Fetch data from API
    fromDate.value = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString(); // default to 30 days ago if no minDate
    toDate.value = new Date().toISOString(); // default to now if no maxDate
    loadChartData(props.sensorId, plotVariable.value, fromDate.value, toDate.value);
});

onBeforeUnmount(() => {
    resizeObserver?.disconnect();
    try { chart?.dispose(); } catch (_) { }
    chart = null;
});

/////////////////////////////////////  WATCHERS  ////////////////////////////////////

watch(() => [times.value, depths.value, data.value], updateChart, { deep: true });

watch(() => plotVariable.value, (newVariable) => {
    if (fromDate.value && toDate.value) {
        loadChartData(props.sensorId, newVariable, fromDate.value, toDate.value);
    }
});

</script>
