<template>
    <div class="global-chart-wrapper" style="width: 100%; height: 100%;">
        <div ref="chartContainer" style="width: 100%; height: 100%;"></div>
        <div v-if="loading" class="global-chart-overlay">
            <v-progress-circular indeterminate color="warning" :size="64" :width="12" class="progress" />
        </div>

        <!-- <v-btn icon variant="text" flat size="18px" class="chart-info" @click="showChartInfo = true">
            <v-icon color="yellow" size="14px">mdi-information-variant</v-icon>
        </v-btn> -->

        <!-- CHART USAGE AND PLOTS HELP DIALOG -->
        <v-dialog v-model="showChartInfo" max-width="75%">
            <v-card class="pa-5">
                <v-img :src="timeseriesChartHelpImg" alt="Timeseries Chart Help" ></v-img>
            </v-card>
        </v-dialog>
    </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onBeforeUnmount } from 'vue';
import * as echarts from 'echarts';
import { registerEchartsDarkTheme } from '~~/composables/useEchartsTheme';
import { computeNightRanges } from '~~/composables/useSunCalc';
import moment from 'moment-timezone';
import colors from 'vuetify/util/colors';
import timeseriesChartHelpImg from '../../public/timeseriesChartHelp.png';

import { useMainStore } from '../stores/main';
const mainStore = useMainStore();

const chartContainer = ref<HTMLDivElement | null>(null);
let chart: echarts.ECharts | null = null;
let model_timestamps: number[] = [];
let _statsLegendHandler: ((params: any) => void) | null = null;
let zrClickHandler: ((evt: any) => void) | null = null;

const loading = ref(false);

/**
 * Time domain. By default the chart spans `midDate ± dfnDays` — the map clock's
 * neighbourhood, which is what this chart showed when it was the Timeseries
 * tab's whole reason to exist. Hosts that own their own window (the Depth tab
 * pages through 14-day / 1-year / 20-year spans) pass it explicitly instead, so
 * the line chart lines up with the section above it.
 *
 * `dayNight` is a tri-state string, not a boolean, on purpose: Vue coerces an
 * absent Boolean-typed prop to `false` rather than leaving it undefined, so a
 * `props.flag ?? autoRule` default silently never reaches the auto rule and the
 * bands vanish for every host that doesn't pass the prop. 'auto' follows window
 * length — dusk/dawn bands read well across a fortnight and turn into an
 * unreadable moiré across a year.
 */
const props = defineProps<{
    windowStart?: Date | null
    windowEnd?: Date | null
    dayNight?: 'auto' | 'on' | 'off'
    /**
     * Plot-area gutters. The default 160px left gutter makes room for the
     * vertical legend. A host stacking this chart beneath a time-depth section
     * overrides both to the section's own fixed margins, since two panels
     * sharing an x-axis have to share a plot area or the eye can't read across
     * them — and pairs it with `legendLayout: 'top'` so nothing needs the space.
     */
    gridLeft?: number
    gridRight?: number
    legendLayout?: 'left' | 'top'
}>();

const gridConfig = computed(() => ({
    left: props.gridLeft ?? 160,
    right: props.gridRight ?? 30,
    top: props.legendLayout === 'top' ? 34 : 30,
    bottom: 30,
}));

const legendConfig = computed(() => props.legendLayout === 'top'
    ? { show: true, orient: 'horizontal' as const, left: 'center', top: 0, itemWidth: 12, itemHeight: 8, textStyle: { fontSize: 9 } }
    : { show: true, orient: 'vertical' as const, left: 'left', top: 'center', itemWidth: 15, itemHeight: 10, textStyle: { fontSize: 10 } });

const DFN = computed(() => mainStore.dfnDays);
const midDate = computed(() => mainStore.midDate ?? moment.utc());

const TZ = 'America/Vancouver';
const windowStartLocal = computed(() => props.windowStart
    ? moment(props.windowStart).tz(TZ)
    : midDate.value.clone().tz(TZ).subtract(DFN.value, 'days'));
const windowEndLocal = computed(() => props.windowEnd
    ? moment(props.windowEnd).tz(TZ)
    : midDate.value.clone().tz(TZ).add(DFN.value, 'days'));
const dayNightVisible = computed(() => {
    const mode = props.dayNight ?? 'auto';
    if (mode !== 'auto') return mode === 'on';
    return windowEndLocal.value.diff(windowStartLocal.value, 'days') <= 40;
});

const showChartInfo = ref(false);

///////////////////////////////////  LIFECYCLE  ///////////////////////////////////

onMounted(() => {
    registerEchartsDarkTheme();
    initChart();
});

onBeforeUnmount(() => {
    if (chart && zrClickHandler) {
        try { chart.getZr().off('click', zrClickHandler); } catch (e) { }
    }
    if (chart) {
        try { chart.dispose(); } catch (e) { }
        chart = null;
    }
});

///////////////////////////////////  INIT  ///////////////////////////////////

function initChart() {
    if (!chartContainer.value) return;

    try { if (chart) { chart.dispose(); chart = null; } } catch (e) { }
    chart = echarts.init(chartContainer.value, 'dark', { renderer: 'canvas' });

    chart.setOption({
        tooltip: { trigger: 'axis' },
        toolbox: {
            feature: {
                dataZoom: { yAxisIndex: 'none' },
                dataView: { readOnly: true },
                saveAsImage: {}
            }
        },
        legend: legendConfig.value,
        xAxis: { type: 'time', axisLabel: { color: '#e0e0e0' } },
        yAxis: { type: 'value', min: 'dataMin', max: 'dataMax', axisLabel: { color: '#e0e0e0' } },
        grid: gridConfig.value,
        series: []
    });
    chart.resize();

    // ZRender click: snap to nearest model timestamp and update store dt
    try {
        let lastClickedX: number | null = null;
        zrClickHandler = (evt: any) => {
            if (!chart || !model_timestamps.length) return;

            const px = evt.event.zrX;
            const py = evt.event.zrY;
            if (!chart.containPixel('grid', [px, py])) return;
            const converted = chart.convertFromPixel('grid', [px, py]);
            if (!converted || converted[0] === undefined) return;

            const clickX = Number(converted[0]);

            // Binary search for nearest model timestamp
            let low = 0;
            let high = model_timestamps.length - 1;
            let bestIdx = 0;
            while (low <= high) {
                const mid = Math.floor((low + high) / 2);
                const midVal = model_timestamps[mid] as number;
                const bestVal = model_timestamps[bestIdx] as number;
                if (Math.abs(midVal - clickX) < Math.abs(bestVal - clickX)) bestIdx = mid;
                if (midVal < clickX) low = mid + 1;
                else if (midVal > clickX) high = mid - 1;
                else break;
            }

            let finalX = model_timestamps[bestIdx] as number;

            // `model_timestamps` isn't always a real model instant — ExplorePanel's
            // depth-section line chart feeds this component hour-floored bin starts
            // (its own aggregation grid, not SalishSeaCast's actual :30-past-the-hour
            // output times), which would otherwise send the map's clock to an hour
            // with no matching raster tile. Snap to the nearest timestamp the raster
            // layer actually has, when we know what those are.
            const rasterDts = mainStore.variables.find(
                v => v.source === mainStore.selected_variable.source && v.var === mainStore.selected_variable.var
            )?.dts;
            if (rasterDts?.length) {
                let best = 0;
                for (let i = 1; i < rasterDts.length; i++) {
                    if (Math.abs(rasterDts[i] - finalX) < Math.abs(rasterDts[best] - finalX)) best = i;
                }
                finalX = rasterDts[best];
            }

            if (finalX !== lastClickedX) {
                lastClickedX = finalX;
                mainStore.updateSelectedVariable({ dt: moment.utc(finalX) });
            }
        };
        chart.getZr().on('click', zrClickHandler);
    } catch (e) {
        console.warn('Failed to attach ZRender click handler:', e);
    }
}

///////////////////////////////////  EXPOSED  ///////////////////////////////////

async function fetchAndPlot(
    lat: number,
    lon: number,
    getModelData: () => Promise<any>,
    getClimData: () => Promise<any>,
    getSensorData: () => Promise<any>
) {
    loading.value = true;
    try {
        const [modelResp, climResp, sensorResp] = await Promise.allSettled([
            getModelData(),
            getClimData(),
            getSensorData(),
        ]);

        const model = modelResp.status === 'fulfilled' ? modelResp.value?.data ?? null : null;
        const clim = climResp.status === 'fulfilled' ? climResp.value?.data ?? null : null;
        const sensor = sensorResp.status === 'fulfilled' ? sensorResp.value?.data ?? null : null;

        if (modelResp.status === 'rejected' && modelResp.reason?.code !== 'ERR_CANCELED')
            console.error('Model timeseries error:', modelResp.reason);
        if (climResp.status === 'rejected' && climResp.reason?.code !== 'ERR_CANCELED')
            console.warn('Climate timeseries error:', climResp.reason);
        if (sensorResp.status === 'rejected' && sensorResp.reason?.code !== 'ERR_CANCELED')
            console.warn('Sensor timeseries error:', sensorResp.reason);

        plot(model, clim, sensor);
    } finally {
        loading.value = false;
    }
}

function plot(modelData: any, climateData: any, sensorData: any | null) {
    if (!chart) return;
    const tz = 'America/Vancouver';

    const lat = mainStore.lastClickedMapPoint?.lat;
    const lng = mainStore.lastClickedMapPoint?.lng;

    const hasModelData = modelData && Array.isArray(modelData.time) && modelData.time.length > 0;
    let __series_model: any[] = [];
    if (hasModelData) {
        model_timestamps = modelData.time.map((t: any) => moment.utc(t).valueOf());
        __series_model = model_timestamps.map((t: number, i: number) => [
            moment.utc(t).tz(tz).format(),
            modelData.value[i]
        ]);
    }

    const startLocal = windowStartLocal.value.clone();
    const endLocal = windowEndLocal.value.clone();

    // Night mark areas
    let markAreaData: any[] = [];
    if (!dayNightVisible.value) {
        // Left empty: across a multi-year window the bands collapse into noise.
    } else if (typeof lat === 'number' && typeof lng === 'number') {
        const nights = computeNightRanges({ lat, lng, tz, startLocalIso: startLocal.format(), endLocalIso: endLocal.format() });
        for (let i = 0; i < nights.length - 1; i++) {
            const nightEnd = nights[i]?.[1];
            const nextStart = nights[i + 1]?.[0];
            if (nightEnd && nextStart) markAreaData.push([{ xAxis: nightEnd }, { xAxis: nextStart }]);
        }
    } else {
        let day = startLocal.clone().startOf('day').subtract(1, 'day');
        const lastDay = endLocal.clone().endOf('day').add(1, 'day');
        while (day.isBefore(lastDay)) {
            const n1Start = day.clone().hour(0).minute(0).format();
            const n1End = day.clone().hour(6).minute(0).format();
            const n2Start = day.clone().hour(20).minute(0).format();
            const n2End = day.clone().add(1, 'day').startOf('day').format();
            if (moment(n1End).isAfter(startLocal) && moment(n1Start).isBefore(endLocal)) {
                markAreaData.push([
                    { xAxis: moment(n1Start).isBefore(startLocal) ? startLocal.format() : n1Start },
                    { xAxis: moment(n1End).isAfter(endLocal) ? endLocal.format() : n1End }
                ]);
            }
            if (moment(n2End).isAfter(startLocal) && moment(n2Start).isBefore(endLocal)) {
                markAreaData.push([
                    { xAxis: moment(n2Start).isBefore(startLocal) ? startLocal.format() : n2Start },
                    { xAxis: moment(n2End).isAfter(endLocal) ? endLocal.format() : n2End }
                ]);
            }
            day = day.add(1, 'day');
        }
    }

    const selectedXLocal = mainStore.selected_variable.dt
        ? moment.utc(mainStore.selected_variable.dt).tz(tz).format()
        : null;

    // Determine y-axis decimal precision from data range
    const axisDecimals = (() => {
        const values: number[] = [];
        const push = (arr: any[] | undefined) => { if (Array.isArray(arr)) arr.forEach(v => { const n = Number(v); if (Number.isFinite(n)) values.push(n); }); };
        push(modelData?.value);
        if (Array.isArray(climateData)) climateData.forEach(row => push([row?.mean, row?.min, row?.max]));
        if (sensorData?.value) push(sensorData.value);
        if (!values.length) return 0;
        // Reduce rather than Math.max(...values)/Math.min(...values): a variable-depth
        // ("profiler") sensor can return well over 100k points for a single depth band in
        // a normal date range, and spreading that many arguments into Math.max/min throws
        // "Maximum call stack size exceeded" (V8's argument-count limit, ~65k-125k).
        let vMin = Infinity, vMax = -Infinity;
        for (const v of values) { if (v < vMin) vMin = v; if (v > vMax) vMax = v; }
        const range = vMax - vMin;
        if (!Number.isFinite(range)) return 0;
        if (range < 1) return 3;
        if (range < 5) return 2;
        if (range < 10) return 1;
        return 0;
    })();

    const hasClimate = Array.isArray(climateData) && climateData.length > 0;
    const hasSensorData = sensorData && Array.isArray(sensorData.time) && sensorData.time.length > 0;

    // A variable-depth ("profiler") sensor queried fine but had no cast near the
    // currently selected depth in this window — distinct from "no sensor selected",
    // which just leaves sensorData null. See extractSensorTimeseries.py's profiler
    // branch, which returns this well-formed empty shape rather than a 404.
    const isProfilerNoCast = mainStore.selectedSensor?.depth === -1 && sensorData
        && Array.isArray(sensorData.time) && sensorData.time.length === 0;

    if (!hasModelData && !hasClimate && !hasSensorData) {
        chart.setOption({
            graphic: [{ type: 'text', left: 'center', top: 'middle', style: { text: 'No data available', fill: 'rgba(255,255,255,0.4)', fontSize: 16 } }],
            legend: { data: [] },
            series: []
        }, true);
        chart.resize();
        return;
    }

    const option: any = {
        graphic: [{
            type: 'text',
            right: 10,
            top: 6,
            style: isProfilerNoCast
                ? { text: `No sensor cast near ${sensorData.depth != null ? sensorData.depth.toFixed(1) + 'm' : 'this depth'} in this window`, fill: 'rgba(255,255,255,0.4)', fontSize: 11 }
                : { text: '' }
        }],
        legend: { ...legendConfig.value, icon: 'rect' },
        tooltip: {
            trigger: 'none',
            axisPointer: {
                type: 'cross',
                animation: true,
                label: {
                    backgroundColor: '#333333cc',
                    borderColor: '#aaaaaa88',
                    borderWidth: 1,
                    shadowBlur: 0,
                    shadowOffsetX: 0,
                    shadowOffsetY: 0,
                    color: '#e0e0e0'
                }
            },
            // formatter: (params: any) => {
            //     if (!Array.isArray(params)) return '';
            //     const timeStr = moment.parseZone(params[0]?.value?.[0] ?? params[0]?.axisValue).format('DD MMM, HH:mm');
            //     let out = `<b>${timeStr}</b><br/>`;
            //     for (const p of params) {
            //         const val = Array.isArray(p.value) ? p.value[1] : p.value;
            //         out += `<span style="color:${p.color}">●</span> ${Number(val).toFixed(3)}<br/>`;
            //     }
            //     return out;
            // }
        },
        toolbox: { feature: { saveAsImage: {} } },
        grid: gridConfig.value,
        xAxis: {
            type: 'time',
            min: startLocal.format(),
            max: endLocal.format(),
            axisLabel: { color: '#e0e0e0', formatter: (v: any) => moment.parseZone(v).format('DD MMM, HH:mm') }
        },
        yAxis: {
            type: 'value',
            min: 'dataMin',
            max: 'dataMax',
            splitLine: { show: false },
            name: (mainStore.variables as any[]).find((v: any) => v.var === mainStore.selected_variable.var)?.unit ?? '',
            nameLocation: 'center',
            nameTextStyle: { color: '#e0e0e0' },
            axisLabel: { color: '#e0e0e0', formatter: (v: any) => Number(v).toFixed(axisDecimals) }
        },
        series: []
    };

    const dayNightSeries: any = {
        name: 'Day/Night',
        type: 'line',
        data: [],
        markArea: {},
        itemStyle: { color: colors.yellow.accent2 },
        legendIcon: 'roundRect',
        markLine: {
            symbol: ['none', 'none'],
            data: [
                {
                    xAxis: moment.tz(moment(), tz).format(),
                    lineStyle: { color: colors.green.lighten2, width: 1, type: 'dashed' },
                    label: { show: true, position: 'insideStartBottom', formatter: 'NOW', color: colors.green.lighten2, backgroundColor: '', padding: [2, 4], borderRadius: 2, borderWidth: 1, borderColor: colors.green.lighten2 }
                },
                {
                    xAxis: selectedXLocal,
                    lineStyle: { color: colors.orange.lighten2, width: 1, type: 'dashed' },
                    label: { show: true, position: 'insideEndBottom', formatter: 'MAP', color: colors.orange.lighten2, backgroundColor: '', padding: [2, 4], borderRadius: 2, borderWidth: 1, borderColor: colors.orange.lighten2 }
                }
            ]
        },
        showSymbol: false
    };
    if (markAreaData.length > 0) {
        dayNightSeries.markArea = { silent: true, itemStyle: { color: colors.yellow.accent2, opacity: 0.05 }, data: markAreaData };
    }

    const seriesArr: any[] = [dayNightSeries];

    if (hasClimate) {
        const climate_ts = climateData.map((row: any) => moment.utc(row.requested_date).valueOf());
        const mean = climateData.map((row: any) => row.mean);
        const min = climateData.map((row: any) => row.min);
        const maxDiff = climateData.map((row: any) => row.max - row.min);

        // endLabel renders at each series' own last (for _stats_mean) or stacked-cumulative
        // (for the stack:'minmax' pair, where the rendered height is min, then min+range=max)
        // pixel position, so labelling "Min"/"Max"/"Mean" here lines up with the band's
        // actual top/bottom edges and the dashed mean line without any extra computation.
        const statsEndLabel = (formatter: string) => ({
            show: true, formatter, color: mainStore.colors.stats, fontSize: 14, distance: -28,
        });

        const fmt = (ts: number[], vals: any[]) => ts.map((t, i) => [moment.utc(t).tz(tz).format(), vals[i]]);
        seriesArr.push({ name: '_stats_min_base', type: 'line', data: fmt(climate_ts, min), lineStyle: { opacity: 0 }, stack: 'minmax', symbol: 'none', endLabel: statsEndLabel('Min') });
        seriesArr.push({ name: '_stats_max_range', type: 'line', data: fmt(climate_ts, maxDiff), lineStyle: { opacity: 0 }, areaStyle: { color: mainStore.colors.stats, opacity: 0.2 }, stack: 'minmax', symbol: 'none', endLabel: statsEndLabel('Max') });
        seriesArr.push({ name: '_stats_mean', type: 'line', data: fmt(climate_ts, mean), smooth: true, lineStyle: { color: mainStore.colors.stats, opacity: 0.8, width: 2, type: 'dashed' }, symbol: 'none', endLabel: statsEndLabel('Mean') });
        seriesArr.push({ name: 'Climatology', type: 'line', data: [], showSymbol: false, legendIcon: 'roundRect', lineStyle: { color: mainStore.colors.stats, opacity: 0 }, itemStyle: { color: mainStore.colors.stats } });
    }

    if (hasModelData) {
        seriesArr.push({
            name: 'Model',
            type: 'line',
            data: __series_model,
            smooth: true,
            lineStyle: { width: 1, color: mainStore.colors.model.line, shadowColor: mainStore.colors.model.shadow, shadowBlur: mainStore.colors.model.shadowBlur, opacity: 0.8 },
            itemStyle: { color: mainStore.colors.model.line },
            legendIcon: 'roundRect'
        });
    }

    if (hasSensorData) {
        const sensor_ts = sensorData.time.map((t: any) => moment.utc(t).valueOf());
        seriesArr.push({
            name: 'Sensor',
            type: 'line',
            data: sensor_ts.map((t: number, i: number) => [moment.utc(t).tz(tz).format(), sensorData.value[i]]),
            lineStyle: { width: 1, color: mainStore.colors.observation.line, opacity: 0.8, shadowColor: mainStore.colors.observation.shadow, shadowBlur: mainStore.colors.observation.shadowBlur },
            itemStyle: { color: mainStore.colors.observation.line },
            legendIcon: 'roundRect'
        });
    }

    option.series = seriesArr;
    option.legend.data = seriesArr.filter((s: any) => s.name && !s.name.startsWith('_')).map((s: any) => s.name);

    chart.setOption(option, true);
    console.log(option);
    chart.resize();

    // Stats legend toggle — clicking 'Climatology' shows/hides internal series.
    // Use setOption rather than legendSelect/legendUnSelect: those dispatch actions
    // only work reliably for series registered in legend.data; the _stats_* series
    // are intentionally excluded, so legendSelect would be a no-op on re-show.
    if (_statsLegendHandler) chart.off('legendselectchanged', _statsLegendHandler);
    if (hasClimate) {
        _statsLegendHandler = (params: any) => {
            if (params.name !== 'Climatology') return;
            const visible = params.selected['Climatology'];
            chart!.setOption({
                series: [
                    { name: '_stats_min_base', endLabel: { show: visible } },
                    { name: '_stats_max_range', areaStyle: { color: mainStore.colors.stats, opacity: visible ? 0.2 : 0 }, endLabel: { show: visible } },
                    { name: '_stats_mean', lineStyle: { color: mainStore.colors.stats, opacity: visible ? 0.8 : 0, width: 2, type: 'dashed' }, endLabel: { show: visible } },
                ]
            });
        };
        chart.on('legendselectchanged', _statsLegendHandler);
    } else {
        _statsLegendHandler = null;
    }
}

// Update only the vertical "Map" marker when selected dt changes (no full re-plot)
watch(() => mainStore.selected_variable.dt, (newDt) => {
    if (!chart) return;
    const tz = 'America/Vancouver';
    const sel = newDt ? moment.utc(newDt).tz(tz).format() : null;
    try {
        chart.setOption({
            series: [{
                name: 'Day/Night',
                markLine: {
                    symbol: ['none', 'none'],
                    data: [
                        {
                            xAxis: moment.tz(moment(), tz).format(),
                            lineStyle: { color: colors.green.lighten2, width: 1, type: 'dashed' },
                            label: { show: true, position: 'insideStartBottom', formatter: 'NOW', backgroundColor: '', padding: [2, 4], borderRadius: 2, borderWidth: 1, borderColor: colors.green.lighten2 }
                        },
                        {
                            xAxis: sel,
                            lineStyle: { color: colors.orange.lighten2, width: 1, type: 'dashed' },
                            label: { show: true, position: 'insideEndBottom', formatter: 'MAP', backgroundColor: '', padding: [2, 4], borderRadius: 2, borderWidth: 1, borderColor: colors.orange.lighten2 }
                        }
                    ]
                }
            }]
        });
    } catch (e) { /* ignore if chart has no series yet */ }
});

// Resize chart when container changes size
function resize() {
    chart?.resize();
}

defineExpose({ plot, fetchAndPlot, resize, loading });
</script>

<style scoped>
.global-chart-wrapper {
    position: relative;
}

.global-chart-overlay {
    position: absolute;
    inset: 0;
    display: flex;
    justify-content: center;
    align-items: center;
    z-index: 9999;
}

.progress {
    position: absolute;
    inset: 0;
    display: flex;
    justify-content: center;
    align-items: center;
    place-self: center;
}

.chart-info {
    position: absolute;
    bottom: 8px;
    left: 8px;
    z-index: 10;
    color: #e0e0e0;
    border-radius: 4px;
    padding: 4px;
    animation: glow 2s ease-in-out infinite;
}

@keyframes glow {
    0%, 100% {
        text-shadow: 0 0 5px #ffff00, 0 0 15px rgba(255, 255, 0, 0.5);
        filter: brightness(1.2);
    }
    50% {
        text-shadow: 0 0 15px #ffff00, 0 0 25px rgba(255, 255, 0, 0.7);
        filter: brightness(1.4);
    }
}
</style>
