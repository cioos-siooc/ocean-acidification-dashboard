<template>
    <aside v-show="isOpen" class="p-2 absolute right-0 top-0 h-full w-[300px] overflow-y-auto bg-default border-l border-default"
        style="z-index:999;">
        <div class="flex flex-wrap m-0 p-0 items-center" style="height: 38px;">
            <div class="drawer-header grow" style="min-width:0;">
                <div class="title-text truncate">{{ title }}</div>
                <div class="subtitle-text truncate">{{ timestamp }}</div>
            </div>
            <UButton variant="solid" color="error" class="size-[20px] p-0 justify-center shrink-0" @click="isOpen = false">
                <UIcon name="i-mdi-close" class="size-[16px]" />
            </UButton>
        </div>

        <div class="profile-chart-wrapper">
            <div ref="chartContainer" class="profile-chart"></div>
            <div v-if="statusMessage" class="profile-chart-overlay"
                :class="{ loading: loading, error: !!errorMessage }">
                <UIcon name="i-mdi-loading" class="animate-spin text-warning progress" :style="{ fontSize: 64 + 'px' }" v-if="loading" />
                <span v-else>{{ statusMessage }}</span>
            </div>
        </div>
    </aside>
</template>

<script setup lang="ts">
import { computed, ref, watch, onMounted, onBeforeUnmount } from 'vue';
import { useRuntimeConfig } from '#app';
import axios from 'axios';
import * as echarts from 'echarts';
import { registerEchartsDarkTheme } from '~~/composables/useEchartsTheme';
import type { PropType } from 'vue';
import moment, { type MomentInput } from 'moment-timezone';
import { useVariableRegistry } from '~~/composables/useVariableRegistry';
import { utc2pst } from '~~/composables/useUTC2PST';
import { useMainStore } from '../stores/main';

type SelectedPoint = {
    lat: number;
    lng: number;
} | null;

interface ProfileRequest {
    source: string;
    var: string;
    lat: number;
    lng: number;
    dt: string;
    binMode: 'hourly' | 'daily' | 'monthly';
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

const { variableLabel, displayUnit, toDisplayValue } = useVariableRegistry();

const title = computed(() => {
    const varId = mainStore.selected_variable?.var;
    if (!varId) return 'No variable selected';
    return `${variableLabel(varId)} Profile`;
});

// The instant this profile represents — the same universal selected instant
// every other view reads (`selected_variable.dt`), so this drawer stays in
// sync with the time controls, a depth-section cell click, or a timeseries
// chart click alike.
const profileDt = computed<MomentInput>(() => mainStore.selected_variable?.dt);

// The same chart looks identical whether it's an instantaneous reading or a
// daily/monthly mean — this is the only visible cue for which one it is, so
// it has to say the aggregation, not just the moment.
const timestamp = computed(() => {
    const dt = profileDt.value;
    if (!dt) return 'Data timestamp: –';
    const parsed = moment.utc(dt);
    if (!parsed.isValid()) return 'Data timestamp: –';
    const mode = mainStore.exploreBinMode;
    if (mode === 'monthly') return `Monthly mean · ${parsed.format('MMMM YYYY')}`;
    if (mode === 'daily') return `Daily mean · ${parsed.format('MMM D, YYYY')}`;
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

const selectedVariableLabel = computed(() => variableLabel(mainStore.selected_variable.var ?? 'Value'));
const selectedVariableUnit = computed(() => displayUnit(mainStore.selected_variable.var ?? ''));
const selectedVariableAxisName = computed(() =>
    selectedVariableUnit.value ? `${selectedVariableLabel.value} (${selectedVariableUnit.value})` : selectedVariableLabel.value
);

const requestParams = computed<ProfileRequest | null>(() => {
    const lat = props.selectedPoint?.lat;
    const lng = props.selectedPoint?.lng;
    const dt = profileDt.value;
    const variable = mainStore.selected_variable?.var;
    const source = mainStore.selected_variable?.source;
    if (typeof lat !== 'number' || typeof lng !== 'number' || !dt || !variable || !source) {
        return null;
    }
    const parsed = moment(dt);
    if (!parsed.isValid()) return null;
    return {
        source: source,
        var: variable,
        lat,
        lng,
        dt: parsed.utc().format('YYYY-MM-DDTHHmmss'),
        // The Depth tab's own bin-mode toggle, mirrored via the store — daily/
        // monthly resolve to that calendar day/month's mean at the backend
        // regardless of the exact hour in `dt`, so this only needs to be part
        // of the request, not folded into `dt` itself.
        binMode: mainStore.exploreBinMode,
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

onMounted(async () => {
    registerEchartsDarkTheme();
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

// Re-render off the already-fetched (canonical) points when the display unit
// changes — no refetch needed, only the conversion applied at render time.
watch(() => mainStore.unitPreference[mainStore.selected_variable.var], () => {
    renderChart(profilePoints.value);
});

function ensureChart() {
    if (profileChart || !chartContainer.value) return;
    profileChart = echarts.init(chartContainer.value, 'dark', { renderer: 'canvas' });
}

function renderChart(points: ProfilePoint[]) {
    if (!chartContainer.value) return;
    ensureChart();
    if (!profileChart) return;

    const varId = mainStore.selected_variable.var;
    const sorted = [...points].sort((a, b) => a.depth - b.depth);
    const data = sorted.map((point) => [toDisplayValue(varId, point.value), point.depth]);

    const option = {
        tooltip: {
            trigger: 'axis',
            axisPointer: { type: 'cross' },
            formatter: (params: any) => {
                const entry = params?.[0];
                if (!entry) return '';
                const [value, depth] = entry.value ?? [];
                return `${selectedVariableLabel.value}<br/>Value: ${value ?? '–'} ${selectedVariableUnit.value}<br/>Depth: ${depth ?? '–'} m`;
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
            name: selectedVariableAxisName.value,
            nameLocation: 'middle',
            nameGap: 24,
            axisLine: { show: true },
            scale: true,
            axisLabel: { color: '#e0e0e0' }
        },
        yAxis: {
            type: 'value',
            name: 'Depth (m)',
            nameLocation: 'middle',
            inverse: true,
            axisLine: { show: true },
            scale: true,
            axisLabel: { color: '#e0e0e0' }
        },
        series: [
            {
                name: selectedVariableLabel.value,
                type: 'line',
                // type: "scatter",
                // showSymbol: false,
                smooth: true,
                showSymbol: true,
                data,
                // lineStyle: {
                //     width: 1,
                //     color: mainStore.colors.model.line,
                //     shadowColor: mainStore.colors.model.shadow,
                //     shadowBlur: 3,
                // },
                // // areaStyle: { opacity: data.length ? 0.25 : 0 }
                // itemStyle: {
                //     color: mainStore.colors.model.line,
                //     borderColor: mainStore.colors.model.line,
                //     borderWidth: 1,
                //     shadowColor: mainStore.colors.model.shadow,
                //     shadowBlur: 3,
                // },
                lineStyle: { width: 1, color: mainStore.colors.model.line, shadowColor: mainStore.colors.model.shadow, shadowBlur: mainStore.colors.model.shadowBlur, opacity: 0.8 },
                itemStyle: { color: mainStore.colors.model.line },
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
        const payload: Record<string, any> = { source: params.source, var: params.var, lat: params.lat, lng: params.lng, dt: params.dt, bin_mode: params.binMode };
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
    line-height: 1.25;
}

.title-text {
    font-weight: 600;
    font-size: 0.8rem;
    color: rgba(255, 255, 255, 0.9);
}

.subtitle-text {
    font-size: 0.7rem;
    color: rgba(255, 255, 255, 0.55);
}

.profile-chart-wrapper {
    flex: 1;
    position: relative;
    /* min-height: 200px; */
    height: calc(100% - 38px);
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
    font-size: 0.85rem;
    line-height: 1.4;
    z-index: 9999;
}

.profile-chart-overlay:not(.loading) {
    color: #333;
    background: rgba(255, 255, 255, 0.9);
}

.profile-chart-overlay.loading {
    background: #33333366;
}

.profile-chart-overlay.error {
    color: #b71c1c;
}

.progress {
    position: absolute;
    inset: 0;
    display: flex;
    justify-content: center;
    align-items: center;
    place-self: center;
}
</style>
