<template>
    <v-navigation-drawer v-model="isOpen" location="right" width="300" class="pa-2" absolute persistent mobile :scrim="false"
        style="height:100%; z-index:9999; top:0;">
        <v-row class="ma-0 pa-0" style="height: 20px;">
            <v-btn icon size="20px" color="error" flat @click="isOpen = false">
                <v-icon size="16px">mdi-close</v-icon>
            </v-btn>
        </v-row>

        <div class="profile-chart-wrapper">
            <div ref="chartContainer" class="profile-chart"></div>
            <div v-if="statusMessage" class="profile-chart-overlay" :class="{ error: !!errorMessage }">
                <span>{{ statusMessage }}</span>
            </div>
        </div>
    </v-navigation-drawer>
</template>

<script setup lang="ts">
import { computed, ref, watch, onMounted, onBeforeUnmount } from 'vue';
import { useRuntimeConfig } from '#app';
import axios from 'axios';
import * as echarts from 'echarts';
import type { PropType } from 'vue';
import moment, { type MomentInput } from 'moment-timezone';
import { var2name } from '../../composables/useVar2Name';
import { formatDepth } from '../../composables/useFormatDepth';
import { utc2pst } from '../../composables/useUTC2PST';
import { useMainStore } from '../stores/main';

type SelectedPoint = {
    lat: number;
    lon: number;
} | null;

interface ProfileRequest {
    var: string;
    lat: number;
    lon: number;
    dt: string;
}

interface ProfilePoint {
    depth: number;
    value: number;
}

const props = defineProps({
    modelValue: {
        type: Boolean,
        required: true
    },
    footerHeight: {
        type: String,
        required: true
    },
    selectedPoint: {
        type: Object as PropType<SelectedPoint>,
        default: null
    }
});

const emit = defineEmits(['update:modelValue']);

const isOpen = computed({
    get: () => props.modelValue,
    set: (value: boolean) => emit('update:modelValue', value)
});

const mainStore = useMainStore();

const title = computed(() => {
    const varId = mainStore.selected_variable?.var;
    if (!varId) return 'No variable selected';
    return `${var2name(varId)} Profile`;
});

const timestamp = computed(() => {
    const dt = mainStore.selected_variable?.dt;
    if (!dt) return 'Data timestamp: –';
    const parsed = moment(dt);
    if (!parsed.isValid()) return 'Data timestamp: –';
    return `Data timestamp: ${utc2pst(parsed)}`;
});

const config = useRuntimeConfig();
const apiBaseUrl = config.public.apiBaseUrl;

const chartContainer = ref<HTMLDivElement | null>(null);
let profileChart: echarts.ECharts | null = null;
const loading = ref(false);
const errorMessage = ref<string | null>(null);
const profilePoints = ref<ProfilePoint[]>([]);
let currentController: AbortController | null = null;
let requestSequence = 0;

const variableLabel = computed(() => var2name(mainStore.selected_variable.var ?? 'Value'));

const requestParams = computed<ProfileRequest | null>(() => {
    const lat = props.selectedPoint?.lat;
    const lon = props.selectedPoint?.lon;
    const dt = mainStore.selected_variable?.dt;
    const variable = mainStore.selected_variable?.var;
    if (typeof lat !== 'number' || typeof lon !== 'number' || !dt) return null;
    const parsed = moment(dt);
    if (!parsed.isValid()) return null;
    return {
        var: variable,
        lat,
        lon,
        dt: parsed.utc().format('YYYY-MM-DDTHHmmss')
    };
});

const statusMessage = computed(() => {
    if (!requestParams.value) return 'Click anywhere on the map to load a profile';
    if (loading.value) return 'Loading profile...';
    if (errorMessage.value) return errorMessage.value;
    if (!profilePoints.value.length) return 'No profile data returned for this location';
    return '';
});

const chartResizeHandler = () => {
    profileChart?.resize();
};

onMounted(() => {
    ensureChart();
    renderChart(profilePoints.value);
    window.addEventListener('resize', chartResizeHandler);
});

onBeforeUnmount(() => {
    window.removeEventListener('resize', chartResizeHandler);
    profileChart?.dispose();
    profileChart = null;
    cancelRequest();
});

watch([requestParams, isOpen], ([params, open]) => {
    if (open && params) {
        fetchProfile(params);
    } else {
        clearChart();
    }
}, { immediate: true, flush: 'post' });

function ensureChart() {
    if (profileChart || !chartContainer.value) return;
    profileChart = echarts.init(chartContainer.value, undefined, { renderer: 'canvas' });
}

function renderChart(points: ProfilePoint[]) {
    if (!chartContainer.value) return;
    ensureChart();
    if (!profileChart) return;

    const sorted = [...points].sort((a, b) => a.depth - b.depth);
    const data = sorted.map((point) => [point.value, point.depth]);

    const option = {
        tooltip: {
            trigger: 'axis',
            axisPointer: { type: 'cross' },
            formatter: (params: any) => {
                const entry = params?.[0];
                if (!entry) return '';
                const [value, depth] = entry.value ?? [];
                return `${variableLabel.value}<br/>Value: ${value ?? '–'}<br/>Depth: ${depth ?? '–'} m`;
            }
        },
        grid: { left: 32, right: 20, top: 12, bottom: 12 },
        toolbox: {
            feature: {
                saveAsImage: {}
            }
        },
        xAxis: {
            type: 'value',
            name: variableLabel.value,
            nameLocation: 'middle',
            nameGap: 24,
            axisLine: { show: true },
            scale: true
        },
        yAxis: {
            type: 'value',
            name: 'Depth (m)',
            nameLocation: 'middle',
            inverse: true,
            axisLine: { show: true },
            scale: true,
        },
        series: [
            {
                name: variableLabel.value,
                type: 'line',
                // type: "scatter",
                // showSymbol: false,
                smooth: true,
                data,
                lineStyle: { width: 3, color: '#1976d2' },
                // areaStyle: { opacity: data.length ? 0.25 : 0 }
                // itemStyle: {
                //     color: '#f976d2',
                //     borderColor: '#c2185b',
                //     borderWidth: 1,
                //     shadowColor: 'rgba(194, 24, 91, 0.5)',
                //     shadowBlur: 10,
                // },
            }
        ],
        animation: false
    }
    profileChart.setOption(option, true);

    profileChart.resize();
}

function normalizeProfileResponse(data: any): ProfilePoint[] {
    if (!data) return [];

    if (Array.isArray(data)) {
        return data.map(normalizeEntry).filter(Boolean) as ProfilePoint[];
    }

    if (Array.isArray(data.profile)) {
        return data.profile.map(normalizeEntry).filter(Boolean) as ProfilePoint[];
    }

    if (Array.isArray(data.data)) {
        return data.data.map(normalizeEntry).filter(Boolean) as ProfilePoint[];
    }

    if (Array.isArray(data.depth) && Array.isArray(data.value) && data.depth.length === data.value.length) {
        return data.depth.map((depthValue: any, idx: number) => {
            const depth = toNumber(depthValue);
            const value = toNumber(data.value?.[idx]);
            if (depth === null || value === null) return null;
            return { depth, value };
        }).filter(Boolean) as ProfilePoint[];
    }

    return [];
}

function normalizeEntry(entry: any): ProfilePoint | null {
    if (!entry) return null;
    if (Array.isArray(entry) && entry.length >= 2) {
        const first = toNumber(entry[0]);
        const second = toNumber(entry[1]);
        if (first !== null && second !== null) {
            return { depth: first, value: second };
        }
    }
    if (typeof entry === 'object') {
        const depth = toNumber(entry.depth ?? entry.z ?? entry.depth_m);
        const value = toNumber(entry.value ?? entry.var ?? entry.t ?? entry.temperature ?? entry.salinity);
        if (depth !== null && value !== null) {
            return { depth, value };
        }
    }
    return null;
}

function toNumber(value: any): number | null {
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
}

async function fetchProfile(params: ProfileRequest) {
    loading.value = true;
    errorMessage.value = null;
    cancelRequest();
    currentController = new AbortController();
    const currentRequest = ++requestSequence;

    try {
        const payload: Record<string, any> = { var: params.var, lat: params.lat, lon: params.lon, dt: params.dt };
        const response = await axios.post(`${apiBaseUrl}/getProfile`, payload, { signal: currentController.signal });

        if (currentRequest !== requestSequence) return;
        const normalized = normalizeProfileResponse(response.data);
        if (!normalized.length) {
            errorMessage.value = 'No profile data returned for this location';
        }
        updateChart(normalized);
    } catch (error: any) {
        const isCanceled = axios.isCancel(error) || error?.name === 'CanceledError';
        if (isCanceled) return;
        errorMessage.value = error?.message ? `Unable to load profile: ${error.message}` : 'Unable to load profile';
        updateChart([]);
    } finally {
        loading.value = false;
    }
}

function updateChart(points: ProfilePoint[]) {
    profilePoints.value = points;
    renderChart(points);
}

function clearChart() {
    cancelRequest();
    loading.value = false;
    errorMessage.value = null;
    profilePoints.value = [];
    renderChart([]);
}

function cancelRequest() {
    if (currentController) {
        currentController.abort();
        currentController = null;
    }
}
</script>

<style scoped>
.drawer-header {
    border-bottom: 1px solid rgba(0, 0, 0, 0.08);
    padding-bottom: 8px;
}

.title-text {
    font-weight: 600;
    font-size: 0.95rem;
}

.subtitle-text {
    font-size: 0.8rem;
    color: rgba(0, 0, 0, 0.6);
}

.profile-chart-wrapper {
    flex: 1;
    position: relative;
    /* min-height: 200px; */
    height: calc(100% - 20px);
    /* margin-top: 6px; */
    /* background:red; */
}

.profile-chart {
    width: 100%;
    height: 100%;
    /* background-color: blue; */
}

.profile-chart-overlay {
    position: absolute;
    inset: 0;
    display: flex;
    justify-content: center;
    align-items: center;
    text-align: center;
    padding: 12px;
    color: #333;
    background: rgba(255, 255, 255, 0.9);
    font-size: 0.85rem;
    line-height: 1.4;
}

.profile-chart-overlay.error {
    color: #b71c1c;
}
</style>
