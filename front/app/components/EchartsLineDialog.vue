<template>
    <v-dialog v-model="modelValue" style="z-index: 9999;">
        <v-card>
            <v-card-title class="d-flex align-center">
                <v-spacer />
                <v-btn size="small" flat color="error" icon @click="close">
                    <v-icon>mdi-close</v-icon>
                </v-btn>
            </v-card-title>

            <v-card-text>
                <div ref="chartRef" style="width:100%; height:70vh;"></div>
            </v-card-text>
        </v-card>
    </v-dialog>
</template>

<script setup>
import { ref, watch, onMounted, onUnmounted, nextTick } from 'vue';
import * as echarts from 'echarts';
import axios from 'axios';

const config = useRuntimeConfig();
const apiBaseUrl = config.public.apiBaseUrl;

///////////////////////////////////  PROP  ///////////////////////////////////

const props = defineProps({
    modelValue: { type: Boolean, required: true },
    // Fetch by coord/variable/depth
    coord: { type: Object, required: true },
    variable: { type: String, required: true },
    depth: { type: Number, required: true },
});

///////////////////////////////////  SETUP  ///////////////////////////////////

const emit = defineEmits(['update:modelValue']);

///////////////////////////////////  REF  ///////////////////////////////////

const modelValue = ref(props.modelValue);
const chartRef = ref(null);
let chartInstance = null;

///////////////////////////////////  COMPUTE  ///////////////////////////////////

const title = computed(() => {
    return `${props.variable} climatology at (${props.coord.lat?.toFixed(2)}, ${props.coord.lon?.toFixed(2)}) at depth ${props.depth}m`;
});

///////////////////////////////////  LIFECYCLE  ///////////////////////////////////

onMounted(async () => {
    // Do not init chart here — it may be hidden (dialog closed) and the element not ready.
    // Initialization is deferred until the dialog opens and the element has measurable size.
});

async function ensureChartInit() {
    // Wait for DOM updates and ensure the chart container is visible with a non-zero size.
    await nextTick();
    let el = chartRef.value;
    // retry a few times if element not yet mounted or still hidden
    for (let i = 0; i < 10 && (!el || !el.offsetWidth || !el.offsetHeight); i++) {
        // small delay
        // eslint-disable-next-line no-await-in-loop
        await new Promise((r) => setTimeout(r, 100));
        el = chartRef.value;
    }
    if (!el || !el.offsetWidth || !el.offsetHeight) {
        throw new Error('Chart element not ready or has zero size')
    }

    // If an existing instance was created against a different DOM node (teleport/portal behaviour), re-create it
    if (chartInstance) {
        try {
            const existingDom = chartInstance.getDom && chartInstance.getDom();
            if (existingDom !== el) {
                try { chartInstance.dispose(); } catch (e) { /* ignore */ }
                chartInstance = null;
            }
        } catch (e) {
            // if anything unexpected, dispose and null
            try { chartInstance.dispose(); } catch (e2) { }
            chartInstance = null;
        }
    }

    if (!chartInstance) {
        chartInstance = echarts.init(el);
    }
    try {
        chartInstance.resize();
    } catch (e) {
        // ignore resize errors
    }
}

onUnmounted(() => {
    if (chartInstance) {
        chartInstance.dispose();
        chartInstance = null;
    }
});

///////////////////////////////////  WATCH  ///////////////////////////////////

/**
 * Synchronizes the internal `modelValue` state with the `modelValue` property.
 * This watcher ensures that any changes to the external v-model are reflected
 * within the component's local reactive state.
 *
 * @param {any} v - The updated value from the prop.
 */
watch(() => props.modelValue, (v) => {
    modelValue.value = v
    if (v) {
        ensureChartInit()
            .then(() => getDataForCoord())
            .then((d) => {
                if (d && d.data) renderFromSeries(d.data);
            })
            .catch((e) => {
                console.warn('Failed to initialize chart or fetch data for dialog:', e);
                try { if (chartInstance) chartInstance.clear(); } catch (er) { }
            });
    } else {
        // Dialog closed: dispose chart instance so it is recreated cleanly next open
        try {
            if (chartInstance) {
                chartInstance.dispose();
            }
        } catch (e) {
            console.warn('Error disposing chart instance on close:', e);
        } finally {
            chartInstance = null;
        }
    }
});
watch(modelValue, (v) => emit('update:modelValue', v));




///////////////////////////////////  METHODS  ///////////////////////////////////

const close = () => {
    modelValue.value = false;
};

const getDataForCoord = async () => {
    if (!props.coord || props.coord.lat === undefined || props.coord.lon === undefined || !props.variable) return null;
    console.log(props.coord, props.variable, props.depth);
    try {
        const response = await axios.post(`${apiBaseUrl}/getMonthlyClimatologyAtCoord`, {
            variable: props.variable,
            depth: props.depth,
            lat: props.coord.lat,
            lon: props.coord.lon,
        });
        return response;
    } catch (e) {
        console.warn('API fetch failed:', e);
        return null;
    }
};

const renderFromSeries = (data) => {
    console.log(data);
    if (!chartInstance || !data || !data.timeseries) {
        if (chartInstance) chartInstance.clear();
        return;
    }

    // Expect data.timeseries.by_year -> object mapping year -> {time:[], value:[]}
    const byYear = data.timeseries && data.timeseries.by_year ? data.timeseries.by_year : null;
    const virtual_time = data.climatology && data.climatology.virtual_time ? data.climatology.virtual_time : null;
    if (!byYear) {
        chartInstance.clear();
        return;
    }

    const years = Object.keys(byYear).sort();
    const series = [];

    for (let i = 0; i < years.length; i++) {
        const y = years[i];
        // color as range from light to dark red based on position in list
        const color = `rgba(231, 76, 60, ${0.3 + 0.7 * (i / years.length)})`;
        const entry = byYear[y];
        if (!entry || !entry.time) continue;
        // convert to [time, value] pairs
        const dataPairs = virtual_time.map((t, idx) => [t, entry.value[idx]]);
        series.push({
            name: y,
            type: 'scatter',
            data: dataPairs,
            // showSymbol: false,
            // smooth: true,
            emphasis: { focus: "series" },
            itemStyle: { color },
            // lineStyle: { color, width: 2, opacity: 0.8 }
        });
    }

    // Climatology
    if (data.climatology) {
        const clim = data.climatology;
        // Prepare stacked area series for min-max and IQR
        const __series_min = clim.min.map((v, idx) => [virtual_time[idx], v]);
        const __series_max = clim.max.map((v, idx) => [virtual_time[idx], v - clim.min[idx]]);
        const __series_q1 = clim.q1.map((v, idx) => [virtual_time[idx], v]);
        const __series_q3 = clim.q3.map((v, idx) => [virtual_time[idx], v - clim.q1[idx]]);
        const __series_mean = clim.mean.map((v, idx) => [virtual_time[idx], v]);

        series.push(...[
            {
                // Series for the base of the stack - no name so it's hidden in legend
                type: 'line',
                data: __series_min,
                // smooth: true,
                lineStyle: { opacity: 0 },
                stack: 'minmax',
                symbol: 'none'
            },
            {
                name: 'Min-Max Range',
                type: 'line',
                data: __series_max,
                lineStyle: { opacity: 0 },
                // smooth: true,
                areaStyle: {
                    "color": "#3498DB",
                    "opacity": 0.25
                },
                stack: 'minmax',
                symbol: 'none'
            },

            {
                // Series for the base of Q1nd stack - hidden in legend
                type: 'line',
                data: __series_q1,
                stack: 'range',
                lineStyle: { opacity: 0 },
                // smooth: true,
                symbol: 'none'
            },
            {
                name: 'Interquartile Range',
                type: 'line',
                data: __series_q3,
                stack: 'range',
                lineStyle: { opacity: 0 },
                // smooth: true,
                areaStyle: {
                    "color": "#3498DB",
                    "opacity": 0.25
                },
                symbol: 'none'
            },

            {
                name: 'Mean',
                type: 'line',
                data: __series_mean,
                // smooth: true,
                lineStyle: {
                    "color": "#3498DB",
                    "opacity": 0.8,
                    "width": 5
                },
                symbol: 'none',
            }
        ]);
    }

    console.log(series);

    const option = {
        animation: false,
        title: { text: title.value },
        tooltip: {
            trigger: 'axis', axisPointer: { type: 'cross' }, formatter: (params) => {
                if (!Array.isArray(params)) return '';
                const timeVal = params[0]?.value?.[0] ?? params[0]?.axisValue;
                let out = `<b>${timeVal}</b><br/>`;
                for (const p of params) {
                    const val = Array.isArray(p.value) ? p.value[1] : p.value;
                    out += `<span style="color:${p.color}">●</span> ${p.seriesName}: ${Number(val).toFixed(3)}<br/>`;
                }
                return out;
            }
        },
        legend: {
            data: years,
        },
        xAxis: { type: 'time' },
        yAxis: {
            type: 'value',
            min: (value) => {
                const padding = (value.max - value.min) * 0.1;
                return (value.min - padding).toFixed(1);
            },
            max: (value) => {
                const padding = (value.max - value.min) * 0.1;
                return (value.max + padding).toFixed(1);
            },
        },
        series,
        grid: { left: 60, right: 30, top: 40, bottom: 60 }
    };

    chartInstance.setOption(option, { notMerge: true });
};


</script>

<style scoped>
/* Add any dialog-specific styles if necessary */
</style>
