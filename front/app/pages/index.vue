<template>
    <!-- <v-navigation-drawer expand-on-hover permanent rail>
        <v-list>
            <v-list-item prepend-avatar="https://randomuser.me/api/portraits/women/85.jpg"
                subtitle="sandra_a88@gmailcom" title="Sandra Adams"></v-list-item>
        </v-list>

        <v-divider></v-divider>

        <v-list density="compact" nav>
            <v-list-item prepend-icon="mdi-folder" title="My Files" value="myfiles"></v-list-item>
            <v-list-item prepend-icon="mdi-account-multiple" title="Shared with me" value="shared"></v-list-item>
            <v-list-item prepend-icon="mdi-star" title="Starred" value="starred"></v-list-item>
        </v-list>
    </v-navigation-drawer> -->

    <v-main>
        <BetaDisclaimerDialog />
        <!-- <div class="d-flex flex-column h-screen overflow-hidden"> -->
        <!-- Top: Map -->
        <div ref="mapContainer" class="flex-grow-1"
            :style="{ position: 'relative', height: `calc(100% - ${footerHeight})` }">
            <!-- <Layers @toggleLayer="onToggleLayer" /> -->

            <div class="selector">
                <ColorBarSelect v-if="mainStore.variables.length" />

                <Overlays class="my-2" @autorange="autorange" @toggle-vertical-profile="drawerOpen = !drawerOpen" />
            </div>

            <!-- <DepthSlider /> -->

            <!-- <div class="map-drawer-toggle" :style="{ right: drawerOpen ? '312px' : '12px' }">
                <v-btn size="24px" color="warning" class="ma-0 pa-0" @click="drawerOpen = !drawerOpen"
                    title="Vertical Profile">
                    <v-icon size="20px">mdi-chart-line</v-icon>
                </v-btn>
            </div> -->

            <SelectedVariableDrawer v-model="drawerOpen" :selected-point="lastClicked" :footer-height="footerHeight" />

            <v-snackbar-queue ref="snackbarQueue" v-model="snackMessages" :total-visible="3" closable
                contained></v-snackbar-queue>
        </div>

        <!-- Bottom: Global Chart Footer -->
        <v-footer class="ma-0 pa-0" :style="{ maxHeight: `${footerHeight}` }">
            <!-- <div ref="globalChartContainer" class="w-100" :style="{ height: `calc(${footerHeight} - 20px)` }"></div> -->
            <v-container minWidth="100%" class="ma-0 pa-0">
                <v-row class="ma-0 pa-0" style="height:20px; ">
                    <v-col cols="auto" class="my-0 mx-2 pa-0" style="height:20px">
                        <span class="footer-text">{{ var2name(selectedVariable.var) }}</span>
                    </v-col>
                    <v-divider vertical class="mx-2"></v-divider>
                    <v-col cols="auto" class="my-0 mx-2 pa-0" style="height:20px">
                        <span class="footer-text">{{ utc2pst(moment(selectedVariable.dt)) }}</span>
                    </v-col>
                    <v-divider vertical class="mx-2"></v-divider>
                    <v-col v-if="lastClicked" cols="auto" class="my-0 mx-2 pa-0" style="height:20px">
                        <span class="footer-text">{{ lastClicked?.lat.toFixed(5) }} , {{ lastClicked?.lng.toFixed(5)
                        }}</span>
                    </v-col>

                    <v-spacer></v-spacer>

                    <v-col cols="auto" class="my-0 mx-2 pa-0" style="height:20px">
                        <v-icon size="12px" class="mx-2">mdi-cursor-default-outline</v-icon>
                        <span class="footer-text">{{ mouseCoords.lat?.toFixed(5) }} , {{ mouseCoords.lng?.toFixed(5)
                        }}</span>
                    </v-col>
                </v-row>

                <v-row class="ma-0 pa-0" :style="{ height: `calc(${footerHeight} - 20px)` }"
                    style="position: relative;">
                    <TimeControls />

                    <div class="global-chart-wrapper" style="width: calc( 100% - 24px ); height: calc(100% - 32px);">
                        <div ref="globalChartContainer" style="width: 100%; height: 100%;"></div>
                        <div v-if="globalChartLoading" class="global-chart-overlay">
                            <v-progress-circular indeterminate color="warning" :size="64" :width="12"
                                class="progress" />
                        </div>
                    </div>

                    <div class="py-3"
                        style="position: absolute; width:40px; height: 100%; bottom: 0px; right: 0px; text-align:center; display:flex; flex-direction:column; align-items:center; gap:6px; padding-top:6px;">
                        <v-btn title="Long-term climatology" flat size="20px" :disabled="!lastClicked" icon
                            color="primary" @click="dialogOpen = true">
                            <v-icon size="14px">mdi-chart-line</v-icon>
                        </v-btn>
                    </div>
                </v-row>
            </v-container>

            <!-- Dialog component for monthly chart -->
            <EchartsLineDialog v-model="dialogOpen" :coord="lastClicked" :variable="selectedVariable.var"
                :depth="selectedVariable.depth" />

        </v-footer>
        <!-- <div class="footer-chart" style="height: 260px; border-top: 1px solid rgba(0,0,0,0.12);">
            <div ref="globalChartContainer" class="w-100 h-100"></div>
        </div> -->
        <!-- </div> -->


    </v-main>
</template>

<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount, watch, computed } from 'vue';
import { useRuntimeConfig } from '#app';
import mapboxgl from 'mapbox-gl';
import 'mapbox-gl/dist/mapbox-gl.css';
import * as echarts from 'echarts'
import { registerEchartsDarkTheme } from '../../composables/useEchartsTheme'
import axios from 'axios'
import moment from 'moment-timezone'
import DepthSlider from '../components/depth-slider.vue'
import ColorBarSelect from '../components/ColorBarSelect.vue'
import TimeControls from '../components/TimeControls.vue'
import SelectedVariableDrawer from '../components/SelectedVariableDrawer.vue'
import BetaDisclaimerDialog from '../components/BetaDisclaimerDialog.vue'
import type { FeatureCollection, Geometry, GeoJsonProperties } from 'geojson';

import { computeNightRanges } from '../../composables/useSunCalc'
import { var2name } from '../../composables/useVar2Name'
import { utc2pst } from '../../composables/useUTC2PST'
import { formatDepth } from '../../composables/useFormatDepth'
import { useCircleLayer } from '../../composables/useCircleLayer';
import useStationsInteraction from '../../composables/useStationsInteraction';
import getSensorTimeseries from '../../composables/useSensorTimeseries';
import EchartsLineDialog from '../components/EchartsLineDialog.vue'
import colors from 'vuetify/util/colors';

///////////////////////////////////  SETUP  ///////////////////////////////////

import { useMainStore } from '../stores/main'
const mainStore = useMainStore();

const config = useRuntimeConfig();
const apiBaseUrl = config.public.apiBaseUrl

// Colormaps cache
// const colormaps = ref<Record<string, any>>({});

async function getColormaps() {
    try {
        const r = await axios.get(`${apiBaseUrl}/colormaps`);
        const list = r.data;
        const map: Record<string, any> = {};
        for (const c of list) map[c.name] = c;
        // colormaps.value = map;
        mainStore.setColormaps(map);
        return map;
    } catch (e) {
        console.error('Failed to fetch colormaps:', e);
        mainStore.setColormaps({});
        return {};
    }
}

const mapContainer = ref<HTMLDivElement | null>(null);
const globalChartContainer = ref<HTMLDivElement | null>(null);
let map: mapboxgl.Map | null = null;
let globalChart: echarts.ECharts | null = null;
let model_timestamps: number[] = []; // Global cache for chart timestamps to support "click anywhere"
let _statsLegendHandler: ((params: any) => void) | null = null;
const meta = ref<any>(null);
const drawerOpen = ref(false);
// remember last clicked point (lat/lon) so chart can be refreshed when var/depth changes
const lastClicked = ref<{ lat: number; lng: number } | null>(null);
const footerHeight = '300px';

// [-126.4002914428711, 46.85966491699218, -121.31835174560548, 51.10480117797852]
const bounds = [[-126.4, 46.85], [-121.3, 51.1]] as [[number, number], [number, number]];

const mouseCoords = ref<{ lng: number | null, lat: number | null }>({ lng: null, lat: null });

const sensorData = ref<{ time: string, value: number }[]>([])

// Dialog for detailed timeseries
const dialogOpen = ref(false);

// Flags to coordinate initial click: mapLoaded becomes true when map 'load' fires;
// selectedReady becomes true when initial variables/selectedVariable are set.
const mapLoaded = ref(false);
const selectedReady = ref(false);
let didInitClick = false;

const clicked_sensor_id = ref<number | null>(null);

const globalChartLoading = ref(false);

/** 
 * Number of days from now to fetch for climate timeseries. This is used both for the API request parameter and for computing the x-axis range of the chart (now +/- DFN days). The API will return all available data within that range, which may be less than DFN if the model run does not extend that far into the future.
*/

const zoom = ref('');

const snackMessages = ref<object[]>([]);

///////////////////////////////////  COMPUTED  ///////////////////////////////////

const DFN = computed(() => mainStore.dfnDays);

const selectedVariable = computed(() => mainStore.selected_variable);

const showBathymetryContours = computed(() => mainStore.showBathymetryContours);

const selectedColormap = computed(() => {
    const name = mainStore.selected_variable.colormap;
    if (name) return mainStore.colormaps[name] ?? null;
    // Fallback to a default colormap (DB doesn't store colormap field)
    return null;
});

const midDate = computed(() => {
    return mainStore.midDate ?? moment.utc();
});

///////////////////////////////////  WATCHERS  ///////////////////////////////////

// When colormap, min, or max change in store, update overlay
watch([
    () => mainStore.selected_variable.colormap,
    () => mainStore.selected_variable.colormapMin,
    () => mainStore.selected_variable.colormapMax
], async () => {
    if (!map || !mapLoaded.value) return;
    if (mainStore.selected_variable.var === 'bathymetry') {
        updateBathymetryTilesLayerColorization();
    } else {
        try {
            await updatePngOverlay();
        } catch (e) {
            console.warn('Failed to update overlay after colormap/min/max change', e);
        }
    }
}, { immediate: false });

// Handler for time controls component
// function onTimeControlDt(dt: any) {
//     // dt is a moment object (UTC)
//     mainStore.updateSelectedVariable({ dt });
// }

///////////////////////////////////  HOOKS  ///////////////////////////////////
onMounted(async () => {
    registerEchartsDarkTheme();
    mapboxgl.accessToken = config.public.mapboxToken;
    if (!mapContainer.value) return;

    map = new mapboxgl.Map({
        container: mapContainer.value,
        // style: 'mapbox://styles/taimazb/cmk1jwu8o005101sv1j41cj6j?optimize=true&fresh=true',
        // style: 'mapbox://styles/taimazb/cmkcsejwe005m01ssgtdz3tgd?optimize=true&fresh=true',
        style: 'mapbox://styles/taimazb/cmkfvuotu00mw01svbld48v7y?optimize=true&fresh=true',
        // center: [-123.2, 48.8],
        bounds,
        // zoom: 9.5,
        // pitch: 45,
        minZoom: 7,
        maxZoom: 14,
        antialias: true,
        preserveDrawingBuffer: true, // needed for exporting canvas
    });
    console.log(map);

    // When the map finishes loading the style, add the PNG overlay and chart
    map.on('load', () => {
        map?.on('mousemove', (e) => {
            mouseCoords.value.lng = e.lngLat.lng;
            mouseCoords.value.lat = e.lngLat.lat;
        });
        // Fetch colormaps and variables in parallel
        Promise.all([getColormaps(), init()]).catch((e) => console.warn('init failed:', e));
        addSensors().catch((e) => console.warn('addSensors failed:', e));

        map?.on('zoom', () => {
            if (map) zoom.value = map.getZoom().toFixed(2);
        });

    });


})

onBeforeUnmount(() => {
    if (map) {
        const handlers = (map as any)?.__anchoredChartsHandlers;
        const refs = (map as any)?.__anchoredCharts as Array<any> | undefined;
        const svg = (map as any)?.__anchoredChartsSvg as SVGElement | undefined;
        const globalHandlers = (map as any)?.__globalChartHandlers;

        if (handlers) {
            map.off('move', handlers.updateAll);
            map.off('zoom', handlers.updateAll);
            map.off('rotate', handlers.updateAll);
            map.off('pitch', handlers.updateAll);
            map.off('resize', handlers.resizeHandler);
            window.removeEventListener('resize', handlers.updateAll);
        }

        if (globalHandlers) {
            map.off('resize', globalHandlers.resizeHandler);
            try { window.removeEventListener('resize', globalHandlers.resizeHandler); } catch (e) { }
            try { if (globalHandlers.onStationEnter) map.off('mouseenter', 'stations-circles', globalHandlers.onStationEnter); } catch (e) { }
            try { if (globalHandlers.onStationLeave) map.off('mouseleave', 'stations-circles', globalHandlers.onStationLeave); } catch (e) { }
            try { if (globalHandlers.onChartHighlight && globalChart) globalChart.off('highlight', globalHandlers.onChartHighlight); } catch (e) { }
            try { if (globalHandlers.onChartDownplay && globalChart) globalChart.off('downplay', globalHandlers.onChartDownplay); } catch (e) { }
            try { if (globalHandlers.onLegendSelectChanged && globalChart) globalChart.off('legendselectchanged', globalHandlers.onLegendSelectChanged); } catch (e) { }
            try { if (globalHandlers.onChartHighlight) delete (map as any).__globalChartHandlers.onChartHighlight; } catch (e) { }
        }
        // detach station handlers if attached
        try { const sd = (map as any).__stationsDetach; if (sd) sd(); } catch (e) { }

        // remove png overlay if present
        try { if (map.getLayer && map.getLayer('png-image-layer')) map.removeLayer('png-image-layer'); } catch (e) { }
        try { if (map.getSource && map.getSource('png-image')) map.removeSource('png-image'); } catch (e) { }
        try { const ov = (map as any).__activePngOverlay; if (ov && ov.clickHandler) map.off('click', ov.clickHandler); } catch (e) { }
        try { if ((map as any).__clickMarker) ((map as any).__clickMarker).remove(); } catch (e) { }
        if (refs) {
            for (const r of refs) {
                try { r.chart.dispose(); } catch { }
                try { r.chartMarker.remove(); } catch { }
                try { r.dotMarker.remove(); } catch { }
                try { if (r.line && r.line.parentNode) r.line.parentNode.removeChild(r.line); } catch { }
            }
        }

        // remove stations layer + source if present
        try { if (map.getLayer && map.getLayer('stations-circles')) map.removeLayer('stations-circles'); } catch (e) { }
        try { if (map.getSource && map.getSource('stations-points')) map.removeSource('stations-points'); } catch (e) { }

        if (svg && svg.parentNode) svg.parentNode.removeChild(svg);
        map.remove();
    }

    if (globalChart && zrClickHandler) {
        try { globalChart.getZr().off('click', zrClickHandler); } catch (e) { }
    }
});

///////////////////////////////////  WATCH  ///////////////////////////////////

// Watcher: add/update/remove overlay when selected variable or depth changes
watch(() => [mainStore.selected_variable.var, mainStore.selected_variable.depth, mainStore.midDate], async ([v, depth]) => {
    if (!map) return;

    if (!v) {
        removePngOverlay();
        removeBathymetryTilesLayer();
        return;
    }

    try {
        // Check if bathymetry is selected
        if (v === 'bathymetry') {
            removePngOverlay();
            addBathymetryTilesLayer();
        } else {
            removeBathymetryTilesLayer();
            await updatePngOverlay();
        }

        mapLoaded.value = true;

        // If the user previously clicked a point, refresh the timeseries chart for the new var/depth
        if (lastClicked.value && v !== 'bathymetry') {  // Skip API call for bathymetry
            try {
                // debounce rapid var/depth changes to avoid hammering the API
                if (tsRefreshTimer) clearTimeout(tsRefreshTimer);
                tsRefreshTimer = setTimeout(async () => {
                    try {
                        const lat = lastClicked.value!.lat;
                        const lon = lastClicked.value!.lng;
                        const varId = mainStore.selected_variable.var;

                        // abort previous request if any
                        try { if (tsRequestController) tsRequestController.abort(); } catch (e) { }
                        tsRequestController = new AbortController();

                        getTimeseriesPromises(lat, lon);
                    } catch (e) {
                        if (e && e.code === 'ERR_CANCELED') return; // aborted
                        console.warn('Failed to refresh timeseries after var/depth change', e);
                    } finally {
                        tsRequestController = null;
                    }
                }, 300);
            } catch (e) {
                console.warn('Failed to schedule timeseries refresh after var/depth change', e);
            }
        } else
            maybeInitClick();
    } catch (e) {
        console.error('Failed to load PNG for variable', v, e);
        removePngOverlay();
    }
}, { immediate: true });

// Watcher: when selected datetime changes (e.g. user clicks chart), update static overlay if not playing
watch(() => mainStore.selected_variable.dt, async (newDt) => {
    if (!map) return;
    try {
        // only update overlay image for the current dt
        await updatePngOverlay();
    } catch (e) {
        console.error('Failed to update PNG for dt change', e);
    }
});

watch(() => mainStore.showBathymetryContours, (show) => {
    if (!map) return;
    try {
        if (show) {
            if (!map.getSource('nonna'))
                map?.addSource('nonna', {
                    type: 'vector',
                    tiles: [`${apiBaseUrl}/vector/{z}/{x}/{y}.pbf`],
                });

            // Contour lines
            map?.addLayer({
                id: 'nonna-layer',
                type: 'line',
                source: 'nonna',
                'source-layer': 'nonna', // name of the layer in the vector tile source
                filter: [
                    "step",
                    ["zoom"],
                    [
                        "case",
                        ["==", ["%", ["to-number", ["get", "ELEV"]], 100], 0],
                        true,
                        false
                    ],
                    8,
                    [
                        "case",
                        ["==", ["%", ["to-number", ["get", "ELEV"]], 50], 0],
                        true,
                        false
                    ],
                    12,
                    true
                ],
                paint: {
                    "line-color": "#999",
                    "line-width": 1,
                    // "line-opacity": [
                    //     "step",
                    //     ["zoom"],
                    //     [
                    //         "case",
                    //         ["==", ["%", ["to-number", ["get", "ELEV"]], 100], 0],
                    //         1,
                    //         0
                    //     ],
                    //     8,
                    //     [
                    //         "case",
                    //         ["==", ["%", ["to-number", ["get", "ELEV"]], 50], 0],
                    //         1,
                    //         0
                    //     ],
                    //     12,
                    //     1
                    // ]
                }
            });

            // Labels for every 100m contour
            map?.addLayer({
                id: 'nonna-labels',
                type: 'symbol',
                source: 'nonna',
                'source-layer': 'nonna',
                // filter: ["==", ["%", ["to-number", ["get", "ELEV"]], 100], 0],
                filter: [
                    "step",
                    ["zoom"],
                    [
                        "case",
                        ["==", ["%", ["to-number", ["get", "ELEV"]], 100], 0],
                        true,
                        false
                    ],
                    8,
                    [
                        "case",
                        ["==", ["%", ["to-number", ["get", "ELEV"]], 50], 0],
                        true,
                        false
                    ],
                    12,
                    true
                ],
                layout: {
                    "symbol-placement": "line",
                    "text-field": ["to-string", ["get", "ELEV"]],
                    // "text-font": ["Inter Regular"],
                    "text-size": 12,
                    "text-allow-overlap": true,
                    // "symbol-spacing": 250
                },
                paint: {
                    "text-color": "#ccc",
                    "text-halo-color": "#333",
                    "text-halo-width": 1,
                    "text-halo-blur": 1
                }
            });

        } else {
            if (map.getLayer('nonna-layer')) map.removeLayer('nonna-layer');
            if (map.getLayer('nonna-labels')) map.removeLayer('nonna-labels');
            if (map.getSource('nonna')) map.removeSource('nonna');
        }
    } catch (e) {
        console.warn('Failed to toggle bathymetry contours layer visibility:', e);
    }
}, { immediate: true });

///////////////////////////////////  MEDTHODS  ///////////////////////////////////
async function getMetadata() {
    try {
        const varId = mainStore.selected_variable.var;
        const metaPath = `${apiBaseUrl}/metadata/${varId}`;

        const r = await axios.get(metaPath);
        meta.value = JSON.parse(r.data);
    } catch (e) {
        console.error('Failed to fetch metadata:', e);
    }
}

async function init() {
    if (!map) return;

    mainStore.setMidDate(moment.utc()); // Initialize midDate to now.

    getVariables();

    // For the new flow we don't use station points. Instead, we start with NO PNG overlay.

    // initialize the chart now with an empty series
    try { if (globalChart) { globalChart.dispose(); globalChart = null; } } catch (e) { /* ignore */ }
    if (!globalChartContainer.value) return;
    globalChart = echarts.init(globalChartContainer.value, 'dark', { renderer: 'canvas' });

    const option = {
        // title: { text: 'Timeseries', left: 'center' },
        tooltip: {
            trigger: 'axis'
        },
        toolbox: {
            feature: {
                dataZoom: {
                    yAxisIndex: 'none'
                },
                dataView: { readOnly: true },
                saveAsImage: {}
            }
        },
        legend: {
            show: true,
            orient: 'vertical',
            left: 'left',
            top: 'center',
            itemWidth: 15,
            itemHeight: 10,
            textStyle: { fontSize: 10 }
        },
        xAxis: {
            type: 'time'
        },
        yAxis: { type: 'value', min: 'dataMin', max: 'dataMax' },
        grid: { left: 160, right: 30, top: 30, bottom: 30 },
        series: []
    };
    globalChart.setOption(option);
    globalChart.resize();

    // Clicks anywhere on the chart area (using global canvas coordinate conversion)
    try {
        let lastClickedX: string | number | null = null;
        zrClickHandler = (evt: any) => {
            if (!globalChart || !model_timestamps.length) return;

            // ZRender coordinates for the click
            const px = evt.event.zrX;
            const py = evt.event.zrY;

            // Convert pixel point to chart coordinates (values)
            const converted = globalChart.convertFromPixel('grid', [px, py]);
            if (!converted || converted[0] === undefined) return;

            const clickX = Number(converted[0]);

            // Find nearest point in __series_model (assumed sorted timestamps)
            // Binary search for efficiency
            let low = 0;
            let high = model_timestamps.length - 1;
            let bestIdx = 0;

            while (low <= high) {
                const mid = Math.floor((low + high) / 2);
                if (Math.abs(model_timestamps[mid] - clickX) < Math.abs(model_timestamps[bestIdx] - clickX)) {
                    bestIdx = mid;
                }
                if (model_timestamps[mid] < clickX) low = mid + 1;
                else if (model_timestamps[mid] > clickX) high = mid - 1;
                else break;
            }

            const finalX = model_timestamps[bestIdx];
            if (finalX !== lastClickedX) {
                lastClickedX = finalX;
                mainStore.updateSelectedVariable({
                    dt: moment.utc(finalX)
                });
            }
        };
        globalChart.getZr().on('click', zrClickHandler);
    } catch (e) {
        console.warn('Failed to attach ECharts hover listeners:', e);
    }

    // No click handler registered until an overlay is added by selecting a variable.
}

async function getVariables() {
    try {
        const r = await axios.get(`${apiBaseUrl}/variables`);
        const data = r.data;

        // Convert datetimes to epoch ms numbers (plain numbers avoid deep Vue proxy overhead)
        data.forEach((v: any) => {
            v.dts = v.dts?.map((dtstr: string) => moment.utc(dtstr).valueOf());
        });

        mainStore.setVariables(data);

        if (data.length > 0) {
            const varId = 'temperature';
            const varMeta = data.find((v: any) => v.var === varId);
            const source = varMeta?.source ?? '';
            const dts = varMeta?.dts ?? [];
            const precision = varMeta?.precision || 0.1;
            const depth = (varMeta?.depths && varMeta.depths.length > 0) ? varMeta.depths[0].depth : 0.5;
            const colormap = varMeta?.colormap ?? null;
            const colormapMin = varMeta?.colormapMin ?? null;
            const colormapMax = varMeta?.colormapMax ?? null;
            if (dts.length > 0) {
                mainStore.updateSelectedVariable({
                    var: varId,
                    source: source,
                    dt: moment.utc(dts[dts.length - 1]),
                    depth: depth,
                    precision: precision,
                    colormap: colormap,
                    colormapMin: colormapMin,
                    colormapMax: colormapMax
                });
            }
        }

        selectedReady.value = true;
        maybeInitClick();
    } catch (e) {
        console.error('Failed to fetch variables:', e);
    }
}

function maybeInitClick() {
    // Call initClick only once both the map has finished loading and the selected variable has been initialized
    if (mapLoaded.value && selectedReady.value && !didInitClick) {
        didInitClick = true;
        initClick(49.2, -123.5); // Center of the map
    }
}

function initClick(lat: number, lng: number) {
    if (!map) return;

    lastClicked.value = { lat, lng };

    // Abort any in-flight timeseries requests and create a new controller
    try { if (tsRequestController) tsRequestController.abort(); } catch (e) { }
    tsRequestController = new AbortController();

    // trigger map click to load initial timeseries
    map.fire('click', { lngLat: { lat, lng } });

    // getTimeseriesFromApi(lat, lon);
}


async function getTimeseriesPromises(lat: number, lon: number) {
    // Fetch each data source independently so failures don't block others
    let modelResp = null;
    let climResp = null;
    let sensorResp = null;

    globalChartLoading.value = true;

    const fromDate = midDate.value.clone().subtract(DFN.value, 'days').format('YYYY-MM-DDTHHmmss');
    const toDate = midDate.value.clone().add(DFN.value, 'days').format('YYYY-MM-DDTHHmmss');

    try {
        modelResp = await getTimeseriesFromApi(lat, lon, fromDate, toDate);
    } catch (err: any) {
        if (err?.code !== 'ERR_CANCELED') {
            console.error('Failed to fetch model timeseries:', err);
        }
    }

    try {
        climResp = await getClimateTimeseries(lat, lon, fromDate, toDate);
    } catch (err: any) {
        if (err?.code !== 'ERR_CANCELED') {
            console.warn('Failed to fetch climate data (chart will show model only):', err);
        }
    }

    if (clicked_sensor_id.value) {
        try {
            sensorResp = await getSensorTimeseries(clicked_sensor_id.value, mainStore.selected_variable.var, fromDate, toDate);
        } catch (err: any) {
            if (err?.code !== 'ERR_CANCELED') {
                console.warn('Failed to fetch sensor data:', err);
            }
        }
    }

    const model = modelResp?.data || null;
    const clim = climResp?.data || null;
    const sensor = sensorResp?.data || null;

    // Plot whatever data we successfully retrieved
    if (model) {
        plotTimeseries(model, clim, sensor);
    }

    globalChartLoading.value = false;
}

async function getTimeseriesFromApi(lat: number, lon: number, fromDate: string, toDate: string) {
    return axios.post(`${apiBaseUrl}/extractTimeseries`, { var: mainStore.selected_variable.var, lat, lon, depth: mainStore.selected_variable.depth, fromDate, toDate }, { signal: tsRequestController.signal });
    // const r = await axios.post(`${apiBaseUrl}/extractTimeseries`, { var: mainStore.selected_variable.var, lat, lon, depth: mainStore.selected_variable.depth }, { signal: tsRequestController.signal });
    // const json = r.data;
    // if (json && Array.isArray(json.time) && Array.isArray(json.value)) {
    //     plotTimeseriesFromApi(json.time, json.value, lat, lon);
    // }
}

async function getClimateTimeseries(lat: number, lon: number, fromDate: string, toDate: string) {
    return axios.post(`${apiBaseUrl}/extract_climateTimeseries`, {
        var: mainStore.selected_variable.var,
        lat,
        lon,
        depth: formatDepth(mainStore.selected_variable.depth),
        fromDate,
        toDate
    });
};

async function addSensors() {
    const sensors = await getSensors();

    const features = sensors.map((s: any) => ({
        type: 'Feature',
        geometry: {
            type: 'Point',
            coordinates: [s.longitude, s.latitude]
        },
        properties: {
            id: s.id,
            name: s.name,
            depth: s.depth,
            variables: s.variables,
            active: s.active
        }
    }));
    const geojson: FeatureCollection<Geometry, GeoJsonProperties> = {
        type: 'FeatureCollection',
        features: features
    };

    const circle = useCircleLayer(() => map);
    // Color by `active` property: active -> yellow, inactive -> grey
    circle.addCircleLayer({
        sourceId: 'stations',
        layerId: 'stations-circles',
        radius: 6,
        color: ['case', ['==', ['get', 'active'], true], '#FFD700', '#888888']
    });
    circle.updateData(geojson);

    // Attach active-only click handlers via composable
    try {
        const stations = useStationsInteraction(() => map, async (sensor_id: number, depth: number) => {
            // Find the closest depth in depths and switch to that if not already there
            // Create a copy of the depths array before sorting to avoid mutating the store
            const depthsArray = mainStore.variables.find((v: any) => v.var === selectedVariable.value.var)?.depths;
            const closestDepth = depthsArray ? [...depthsArray].sort((a: any, b: any) => Math.abs(a.depth - depth) - Math.abs(b.depth - depth)) : [];
            if (closestDepth && closestDepth.length > 0) {
                const newDepth = closestDepth[0].depth;
                if (newDepth !== selectedVariable.value.depth) {
                    snackMessages.value.push({ color: 'warning', text: `Switched to closest available depth: ${formatDepth(newDepth)}` });
                    mainStore.updateSelectedVariable({ depth: newDepth });
                }
            }

            clicked_sensor_id.value = sensor_id;
            // show marker
            // try { if ((map as any).__clickMarker) ((map as any).__clickMarker).remove(); } catch (e) { }
            // const el = document.createElement('div'); el.style.width = '12px'; el.style.height = '12px'; el.style.borderRadius = '50%'; el.style.background = '#ff5722'; el.style.border = '2px solid white';
            // const marker = new mapboxgl.Marker({ element: el }).setLngLat([lon, lat]).addTo(map);
            // (map as any).__clickMarker = marker;

            // remember clicked point
            // lastClicked.value = { lat, lon };

            // Abort any in-flight timeseries requests and create new controller
            // try { if (tsRequestController) tsRequestController.abort(); } catch (e) { }
            // tsRequestController = new AbortController();

            // try {
            //     // fetch sensor telemetry and climate in parallel
            //     const [modelResp, climResp] = await Promise.all([
            //         getSensorTimeseries(sensor_id, selectedVariable.value.var),
            //         getClimateTimeseries(lat, lon)
            //     ]);

            //     // modelResp expected: { time: [...], value: [...] }
            //     const model = { data: modelResp };
            //     const clim = climResp.data;

            //     plotTimeseries(model.data, clim);
            // } catch (err: any) {
            //     if (err && err.code === 'ERR_CANCELED') return;
            //     console.error('Failed to fetch sensor timeseries:', err);
            // } finally {
            //     tsRequestController = null;
            // }
        });

        // attach and keep a reference for cleanup
        const detachStations = stations.attach(circle);
        (map as any).__stationsDetach = detachStations;
    } catch (e) {
        console.warn('Failed to attach station handlers:', e);
    }
    // circle.on('mouseenter', (e) => { /* ... */ });
}


async function getSensors() {
    try {
        const r = await axios.get(`${apiBaseUrl}/sensors`);
        const data = r.data;
        return data;
    } catch (e) {
        console.error('Failed to fetch sensors:', e);
        return [];
    }
}

// Add / update / remove PNG overlay for a given public PNG path
async function updatePngOverlay(sourceId = 'png-image', layerId = 'png-image-layer') {
    if (!map) throw new Error('map not initialized');
    // if (!meta.value || !meta.value.bounds) throw new Error('metadata not loaded');

    const varId = mainStore.selected_variable.var;
    const dt = mainStore.selected_variable.dt?.format('YYYY-MM-DDTHHmmss') || '';
    const depth = formatDepth(mainStore.selected_variable.depth);
    const pngPath = `${apiBaseUrl}/png/${varId}/${dt}/${depth}`;

    const varMeta = mainStore.variables.find(v => v.var === varId);
    const [lonmin, latmin, lonmax, latmax] = varMeta.bounds;
    const coords = [
        [lonmin, latmax], // top-left
        [lonmax, latmax], // top-right
        [lonmax, latmin], // bottom-right
        [lonmin, latmin], // bottom-left
    ] as [number, number][];

    // remove existing if present
    // try { if (map.getLayer(layerId)) map.removeLayer(layerId); } catch (e) { }
    // try { if (map.getSource(sourceId)) map.removeSource(sourceId); } catch (e) { }

    // prepare raster-color stops for Mapbox style
    const raster_values: any[] = [];
    // e.g.
    // 0, 'rgba(0, 0, 0, 0)',
    //             0.01, '#440154',
    //             0.25, '#00f',
    //             0.5, '#0f0',
    //             0.75, '#fde725',
    //             1.0, '#f00'

    const colormapMin = mainStore.selected_variable.colormapMin
    const colormapMax = mainStore.selected_variable.colormapMax

    // Get packing params from metadata, default to 0.1 precision and 0 base if missing
    // Note: base might be equal to colormapMin if it was dynamic
    const precision = mainStore.variables.find(v => v.var === varId)?.precision ?? 0.1;
    const base = 0

    // Use colormap if available, otherwise fall back to default ramp
    const cmap = selectedColormap.value;
    if (cmap && Array.isArray(cmap.stops) && cmap.stops.length > 0) {
        for (const s of cmap.stops) {
            const pos = s[0];
            const color = s[1];
            // pos may be normalized [0..1] or absolute depending on cmap.mode
            let val_phys = pos;
            if (!cmap.mode || cmap.mode === 'normalized') {
                val_phys = colormapMin + pos * (colormapMax - colormapMin);
            }
            const val_packed = (val_phys - base) / precision;
            raster_values.push(val_packed, color);
        }
    } else {
        const color_stops = [
            [0.0, 'rgba(0, 0, 0, 1)'],
            [0.001, '#440154'],
            [0.25, '#00f'],
            [0.5, '#0f0'],
            [0.75, '#fde725'],
            [1.0, '#f00']
        ];
        for (const stop of color_stops) {
            const val_phys = colormapMin + stop[0] * (colormapMax - colormapMin);
            // decode formula: q = (phys - base) / precision
            const val_packed = (val_phys - base) / precision;
            raster_values.push(val_packed, stop[1]);
        }
    }

    if (map.getSource(sourceId)) {
        map.getSource(sourceId)?.updateImage({
            type: 'image',
            url: pngPath,
            // coordinates: coords
        })
        map.setPaintProperty(layerId, 'raster-color', [
            'interpolate',
            ['linear'],
            ['raster-value'],
            ...raster_values
        ]);
        map.setPaintProperty(layerId, 'raster-color-range', [(colormapMin - base) / precision, (colormapMax - base) / precision]);
    }
    else {
        map.addSource(sourceId, { type: 'image', url: pngPath, coordinates: coords });
        map.addLayer({
            id: layerId, type: 'raster', source: sourceId, paint: {
                'raster-opacity': 1.0,
                'raster-color': [
                    'interpolate',
                    ['linear'],
                    ['raster-value'],
                    ...raster_values
                ],
                // Range of the packed integer values
                'raster-color-range': [(colormapMin - base) / precision, (colormapMax - base) / precision],
                // Mix to recover the 24-bit integer from normalized RGB [0..1]
                // R_int = R_norm * 255. Packed = R_int*65536 + G_int*256 + B_int
                // Coeffs: [255*65536, 255*256, 255, 0] -> [16711680, 65280, 255, 0]
                'raster-color-mix': [256 * 256 * 255, 256 * 255, 255, 0],
                'raster-fade-duration': 0
            },
        }, 'country-boundaries');
    }

    // save active overlay metadata on the map instance for access by click handler
    const overlayObj: any = { bounds: [lonmin, latmin, lonmax, latmax], coords, varId, depth: depth, pngPath, meta, clickHandler: null };
    // remove previous click handler if present
    const prev = (map as any).__activePngOverlay;
    if (prev && prev.clickHandler) {
        try { map.off('click', prev.clickHandler); } catch (e) { }
    }
    (map as any).__activePngOverlay = overlayObj;

    // register a click handler that queries the API for a timeseries at the clicked coordinate
    const onMapClick = async (evt: any) => {
        const { lng, lat } = evt.lngLat;

        // Check if click landed on a sensor feature
        const features = map.queryRenderedFeatures(evt.point, { layers: ['stations-circles'] });

        // Only clear sensor ID if we clicked empty map (not on a feature)
        if (features.length === 0) {
            clicked_sensor_id.value = null;
        }

        const overlay = (map as any).__activePngOverlay;
        if (!overlay) return;
        const [lon0, lat0, lon1, lat1] = overlay.bounds;
        if (!(lng >= lon0 && lng <= lon1 && lat >= lat0 && lat <= lat1)) return; // outside overlay
        // show a marker at clicked position
        try { if ((map as any).__clickMarker) ((map as any).__clickMarker).remove(); } catch (e) { }
        const el = document.createElement('div'); el.style.width = '12px'; el.style.height = '12px'; el.style.borderRadius = '50%'; el.style.background = '#ff5722'; el.style.border = '2px solid white';
        const marker = new mapboxgl.Marker({ element: el }).setLngLat([lng, lat]).addTo(map);
        (map as any).__clickMarker = marker;

        // remember clicked point so subsequent var/depth changes can refresh the chart
        lastClicked.value = { lat, lng };

        // Abort any in-flight timeseries requests and create a new controller
        try { if (tsRequestController) tsRequestController.abort(); } catch (e) { }
        tsRequestController = new AbortController();

        // POST to the API and expect {time: [...], value: [...]} in response
        try {
            await getTimeseriesPromises(lat, lng);
        } catch (err) {
            if (err && err.code === 'ERR_CANCELED') {
                // request was aborted; ignore
            } else {
                console.error('Failed to fetch timeseries:', err);
            }
        } finally {
            tsRequestController = null;
        }
    };

    // register click handler
    overlayObj.clickHandler = onMapClick;
    map.on('click', onMapClick);
}

// NOTE: stopping animator on every change to `mainStore.selected_variable` caused the animator to stop
// immediately after it updated the selected timestamp (dt). That logic is already handled by the
// var/depth watcher above. Removed the blanket watcher to avoid stopping playback when the animator
// updates the selected datetime.

function removePngOverlay(sourceId = 'png-image', layerId = 'png-image-layer') {
    if (!map) return;
    try { const ov = (map as any).__activePngOverlay; if (ov && ov.clickHandler) map.off('click', ov.clickHandler); } catch (e) { }
    try { if ((map as any).__clickMarker) ((map as any).__clickMarker).remove(); } catch (e) { }
    try { if (map.getLayer && map.getLayer(layerId)) map.removeLayer(layerId); } catch (e) { }
    try { if (map.getSource && map.getSource(sourceId)) map.removeSource(sourceId); } catch (e) { }
    try { delete (map as any).__activePngOverlay; } catch (e) { }
}

// Add bathymetry tiles layer based on raster tiles from backend
function addBathymetryTilesLayer(sourceId = 'bathymetry-tiles', layerId = 'bathymetry-tiles-layer') {
    if (!map) return;

    try {
        // Remove existing layer and source if they exist
        if (map.getLayer && map.getLayer(layerId)) map.removeLayer(layerId);
        if (map.getSource && map.getSource(sourceId)) map.removeSource(sourceId);

        // Add raster tile source for bathymetry
        map.addSource(sourceId, {
            type: 'raster',
            tiles: [`${apiBaseUrl}/raster_tiles/{z}/{x}/{y}.webp`],
            tileSize: 512,
        });

        // Build colorization stops like PNG layer
        const raster_values: any[] = [];
        const colormapMin = selectedVariable.value.colormapMin;
        const colormapMax = selectedVariable.value.colormapMax;
        const precision = mainStore.variables.find(v => v.var === 'bathymetry')?.precision ?? 1;
        const base = -3000;

        // Use the reactive selectedColormap computed like PNG layer does
        const cmap = selectedColormap.value;
        if (cmap && Array.isArray(cmap.stops) && cmap.stops.length > 0) {
            for (const s of cmap.stops) {
                const pos = s[0];
                const color = s[1];
                let val_phys = pos;
                if (!cmap.mode || cmap.mode === 'normalized') {
                    val_phys = colormapMin + pos * (colormapMax - colormapMin);
                }
                const val_packed = (val_phys - base) / precision;
                raster_values.push(val_packed, color);
            }
        } else {
            const color_stops = [
                [0.0, 'rgba(0, 0, 0, 1)'],
                [0.001, '#440154'],
                [0.25, '#00f'],
                [0.5, '#0f0'],
                [0.75, '#fde725'],
                [1.0, '#f00']
            ];
            for (const stop of color_stops) {
                const val_phys = colormapMin + stop[0] * (colormapMax - colormapMin);
                const val_packed = (val_phys - base) / precision;
                raster_values.push(val_packed, stop[1]);
            }
        }

        map.addLayer({
            id: layerId,
            type: 'raster',
            source: sourceId,
            paint: {
                // 'raster-opacity': 0.85,
                // Decode packed 24-bit integer: value = (R*m0 + G*m1 + B*m2) / 255 = R*65536 + G*256 + B
                'raster-color-mix': [16711680, 65280, 255, 0],
                // 'raster-color-offset': 0,
                'raster-color': [
                    'interpolate',
                    ['linear'],
                    ['raster-value'],
                    ...raster_values
                ],
                'raster-color-range': [(colormapMin - base) / precision, (colormapMax - base) / precision]
            }
        }, 'country-boundaries');

        // Store metadata for cleanup
        (map as any).__activeBathymetryLayer = {
            sourceId,
            layerId,
        };
    } catch (e) {
        console.error('Error adding bathymetry tiles layer:', e);
    }
}

// Update bathymetry tiles layer colorization
function updateBathymetryTilesLayerColorization(layerId = 'bathymetry-tiles-layer') {
    if (!map) return;

    try {
        if (!map.getLayer(layerId)) return;

        const colormapMin = selectedVariable.value.colormapMin;
        const colormapMax = selectedVariable.value.colormapMax;
        const precision = mainStore.variables.find(v => v.var === 'bathymetry')?.precision ?? 1;
        const base = -3000;

        const raster_values: any[] = [];
        // Use the reactive selectedColormap computed like PNG layer does
        const cmap = selectedColormap.value;

        if (cmap && Array.isArray(cmap.stops) && cmap.stops.length > 0) {
            for (const s of cmap.stops) {
                const pos = s[0];
                const color = s[1];
                let val_phys = pos;
                if (!cmap.mode || cmap.mode === 'normalized') {
                    val_phys = colormapMin + pos * (colormapMax - colormapMin);
                }
                const val_packed = (val_phys - base) / precision;
                raster_values.push(val_packed, color);
            }
        } else {
            const color_stops = [
                [0.0, 'rgba(0, 0, 0, 1)'],
                [0.001, '#440154'],
                [0.25, '#00f'],
                [0.5, '#0f0'],
                [0.75, '#fde725'],
                [1.0, '#f00']
            ];
            for (const stop of color_stops) {
                const val_phys = colormapMin + stop[0] * (colormapMax - colormapMin);
                const val_packed = (val_phys - base) / precision;
                raster_values.push(val_packed, stop[1]);
            }
        }

        // map.setPaintProperty(layerId, 'raster-color-offset', 0);
        map.setPaintProperty(layerId, 'raster-color', [
            'interpolate',
            ['linear'],
            ['raster-value'],
            ...raster_values
        ]);
        const raster_color_range = [(colormapMin - base) / precision, (colormapMax - base) / precision]
        map.setPaintProperty(layerId, 'raster-color-range', raster_color_range);
    } catch (e) {
        console.error('Error updating bathymetry tiles colorization:', e);
    }
}

// Remove bathymetry tiles layer
function removeBathymetryTilesLayer() {
    if (!map) return;

    try {
        const bathy = (map as any).__activeBathymetryLayer;
        if (!bathy) return;

        const { sourceId = 'bathymetry-tiles', layerId = 'bathymetry-tiles-layer' } = bathy;

        try { if (map.getLayer && map.getLayer(layerId)) map.removeLayer(layerId); } catch (e) { }
        try { if (map.getSource && map.getSource(sourceId)) map.removeSource(sourceId); } catch (e) { }
        try { delete (map as any).__activeBathymetryLayer; } catch (e) { }
    } catch (e) {
        console.error('Error removing bathymetry tiles layer:', e);
    }
}

// Plot timeseries returned from the API into the footer chart
function plotTimeseries(modelData: any, climateData: any, sensorData: any | null) {
    if (!globalChart) return;
    const tz = 'America/Vancouver';

    const lat = lastClicked.value?.lat;
    const lng = lastClicked.value?.lng;

    // Update global timestamp cache for the click handler
    model_timestamps = modelData.time.map((t: any) => moment.utc(t).valueOf());
    const values = modelData.value;
    const __series_model = model_timestamps.map((t: any, i: number) => [moment.utc(t).tz(tz).format(), values[i]]);

    const climate_timestamps = Array.isArray(climateData) ? climateData.map((row: any) => moment.utc(row.requested_date).valueOf()) : [];
    // const mean = Array.isArray(climateData) ? climateData.map((row: any) => row.mean) : [];
    // const median = Object.values(climateData.median);
    // const q1 = Array.isArray(climateData) ? climateData.map((row: any) => row.q1) : [];
    // const q3 = Array.isArray(climateData) ? climateData.map((row: any) => row.q3) : [];
    // const q3Diff = (Array.isArray(climateData) && q1.length === q3.length) ? q3.map((v: any, idx: number) => v - q1[idx]) : [];
    // const min = Array.isArray(climateData) ? climateData.map((row: any) => row.min) : [];
    // const maxDiff = (Array.isArray(climateData) && min.length > 0) ? climateData.map((row: any, idx: number) => row.max - min[idx]) : [];

    // const __series_mean = climate_timestamps.map((t: any, i: number) => [moment.utc(t).tz(tz).format(), mean[i]]);
    // const __series_q1 = climate_timestamps.map((t: any, i: number) => [moment.utc(t).tz(tz).format(), q1[i]]);
    // const __series_q3 = climate_timestamps.map((t: any, i: number) => [moment.utc(t).tz(tz).format(), q3Diff[i]]);
    // const __series_min = climate_timestamps.map((t: any, i: number) => [moment.utc(t).tz(tz).format(), min[i]]);
    // const __series_max = climate_timestamps.map((t: any, i: number) => [moment.utc(t).tz(tz).format(), maxDiff[i]]);

    // Combine time_model_local and times_clim_local into a single array for x-axis range calculation
    // const combinedTimes_timestamp = [...model_timestamps, ...climate_timestamps].sort((a, b) => a - b);
    // if (combinedTimes_timestamp.length === 0) {
    //     globalChart.setOption({ series: [] }, true);
    //     globalChart.resize();
    //     return;
    // }

    // Determine time range in local timezone
    // if (!localTimes || localTimes.length === 0) {
    //     globalChart.setOption({
    //         series: [
    //             {
    //                 name: mainStore.selected_variable.var || 'value',
    //                 type: 'line',
    //                 showSymbol: false,
    //                 smooth: true,
    //                 data: []
    //             }]
    //     });
    //     return;
    // }

    // const startLocal = moment.tz(combinedTimes_timestamp[0], tz).clone();
    // const endLocal = moment.tz(combinedTimes_timestamp[combinedTimes_timestamp.length - 1], tz).clone();

    //  Now +/- DFN days
    const startLocal = midDate.value.clone().tz(tz).subtract(DFN.value, 'days').clone();
    const endLocal = midDate.value.clone().tz(tz).add(DFN.value, 'days').clone();

    // Compute night mark areas using SunCalc (sunrise/sunset) if lat/lon provided, otherwise fall back to fixed night windows
    let markAreaData: any[] = [];
    if (typeof lat === 'number' && typeof lng === 'number') {
        const nights = computeNightRanges({ lat, lng, tz, startLocalIso: startLocal.format(), endLocalIso: endLocal.format() });
        // markAreaData = nights.map(([s, e]) => [{ xAxis: s }, { xAxis: e }]);
        for (let i = 0; i < nights.length - 1; i++) {
            // add a mark area between the end of one night and the start of the next to visually separate days
            markAreaData.push([{ xAxis: nights[i][1] }, { xAxis: nights[i + 1][0] }]);
        }
    } else {
        // fallback to previous simple night windows
        const tmp: any[] = [];
        let day = startLocal.clone().startOf('day').subtract(1, 'day');
        const lastDay = endLocal.clone().endOf('day').add(1, 'day');
        while (day.isBefore(lastDay)) {
            const n1Start = day.clone().hour(0).minute(0).format();
            const n1End = day.clone().hour(6).minute(0).format();
            const n2Start = day.clone().hour(20).minute(0).format();
            const n2End = day.clone().add(1, 'day').startOf('day').format();
            if (moment(n1End).isAfter(startLocal) && moment(n1Start).isBefore(endLocal)) {
                const cs = moment(n1Start).isBefore(startLocal) ? startLocal.format() : n1Start;
                const ce = moment(n1End).isAfter(endLocal) ? endLocal.format() : n1End;
                tmp.push([{ xAxis: cs }, { xAxis: ce }]);
            }
            if (moment(n2End).isAfter(startLocal) && moment(n2Start).isBefore(endLocal)) {
                const cs = moment(n2Start).isBefore(startLocal) ? startLocal.format() : n2Start;
                const ce = moment(n2End).isAfter(endLocal) ? endLocal.format() : n2End;
                tmp.push([{ xAxis: cs }, { xAxis: ce }]);
            }
            day = day.add(1, 'day');
        }
        markAreaData = tmp;
    }

    // selected time in chart timezone (to draw vertical marker)
    const selectedXLocal = mainStore.selected_variable.dt ? moment.utc(mainStore.selected_variable.dt).tz(tz).format() : null;

    // Build chart option with conditional inclusion of climate series
    const axisDecimals = (() => {
        const values: number[] = [];
        const pushValues = (arr: any[] | undefined) => {
            if (!Array.isArray(arr)) return;
            for (const v of arr) {
                const n = Number(v);
                if (Number.isFinite(n)) values.push(n);
            }
        };

        pushValues(modelData?.value);

        if (Array.isArray(climateData)) {
            for (const row of climateData) {
                pushValues([row?.mean, row?.q1, row?.q3, row?.min, row?.max]);
            }
        }

        if (sensorData && Array.isArray(sensorData.value)) {
            pushValues(sensorData.value);
        }

        if (values.length === 0) return 0;
        let min = values[0];
        let max = values[0];
        for (const v of values) {
            if (v < min) min = v;
            if (v > max) max = v;
        }
        const range = max - min;
        if (!Number.isFinite(range)) return 0;
        if (range < 1) return 3;
        if (range < 5) return 2;
        if (range < 10) return 1;
        return 0;
    })();

    const option: any = {
        // title: { text: varName ? `Timeseries — ${varName}` : 'Timeseries', left: 'center' },
        legend: {
            show: true,
            orient: 'vertical',
            left: 'left',
            top: 'center',
            itemWidth: 15,
            itemHeight: 10,
            textStyle: { fontSize: 10 },
            icon: 'rect'
        },
        tooltip: {
            trigger: 'axis', formatter: (params: any) => {
                if (!Array.isArray(params)) return '';
                const timeVal = params[0]?.value?.[0] ?? params[0]?.axisValue;
                // timeVal will be a local-time string with offset; parseZone keeps offset
                const timeStr = moment.parseZone(timeVal).format('DD MMM, HH:mm');
                let out = `<b>${timeStr}</b><br/>`;
                for (const p of params) {
                    const val = Array.isArray(p.value) ? p.value[1] : p.value;
                    out += `<span style="color:${p.color}">●</span> value: ${Number(val).toFixed(3)}<br/>`;
                }
                return out;
            }
        },
        toolbox: {
            feature: {
                saveAsImage: {}
            }
        },
        grid: { left: 160, right: 30, top: 30, bottom: 30 },
        xAxis: {
            type: 'time',
            min: startLocal.format(),
            max: endLocal.format(),
            axisLabel: {
                formatter: (value: any) => moment.parseZone(value).format('DD MMM, HH:mm')
            }
        },
        yAxis: {
            type: 'value',
            // min: (value) => {
            //     const padding = (value.max - value.min) * 0.1;
            //     return (value.min - padding).toFixed(1);
            // },
            // max: (value) => {
            //     const padding = (value.max - value.min) * 0.1;
            //     return (value.max + padding).toFixed(1);
            // },
            min: 'dataMin',
            max: 'dataMax',
            splitLine: { show: false },
            axisLabel: { formatter: (value: any) => Number(value).toFixed(axisDecimals) }
        },
        series: []
    };

    // Determine whether we have climate data to plot
    const hasClimate = Array.isArray(climateData) && climateData.length > 0;

    // Day/Night base series (always present)
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
                    label: { show: true, position: 'end', formatter: 'Now', color: colors.green.lighten2, backgroundColor: '', padding: [2, 4], borderRadius: 2, borderWidth: 1, borderColor: colors.green.lighten2 }
                },
                {
                    xAxis: selectedXLocal,
                    lineStyle: { color: colors.orange.lighten2, width: 1, type: 'dashed' },
                    label: { show: true, position: 'end', formatter: 'Map', color: colors.orange.lighten2, backgroundColor: '', padding: [2, 4], borderRadius: 2, borderWidth: 1, borderColor: colors.orange.lighten2 }
                }
            ]
        },
        showSymbol: false
    };

    const seriesArr: any[] = [dayNightSeries];

    // If climate present, compute series and push them
    if (hasClimate) {
        const climate_timestamps = climateData.map((row: any) => moment.utc(row.requested_date).valueOf());
        const mean = climateData.map((row: any) => row.mean);
        const q1 = climateData.map((row: any) => row.q1);
        const q3 = climateData.map((row: any) => row.q3);
        const min = climateData.map((row: any) => row.min);
        const q3Diff = q3.map((v: any, idx: number) => v - q1[idx]);
        const maxDiff = climateData.map((row: any, idx: number) => row.max - min[idx]);

        const __series_mean = climate_timestamps.map((t: any, i: number) => [moment.utc(t).tz(tz).format(), mean[i]]);
        const __series_q1 = climate_timestamps.map((t: any, i: number) => [moment.utc(t).tz(tz).format(), q1[i]]);
        const __series_q3 = climate_timestamps.map((t: any, i: number) => [moment.utc(t).tz(tz).format(), q3Diff[i]]);
        const __series_min = climate_timestamps.map((t: any, i: number) => [moment.utc(t).tz(tz).format(), min[i]]);
        const __series_max = climate_timestamps.map((t: any, i: number) => [moment.utc(t).tz(tz).format(), maxDiff[i]]);

        // base hidden series for stacking — all named with _ prefix so they're excluded from the legend UI
        seriesArr.push({ name: '_stats_min_base', type: 'line', data: __series_min, lineStyle: { opacity: 0 }, stack: 'minmax', symbol: 'none' });
        seriesArr.push({ name: '_stats_max_range', type: 'line', data: __series_max, lineStyle: { opacity: 0 }, areaStyle: { color: mainStore.colors.stats, opacity: 0.4 }, stack: 'minmax', symbol: 'none' });
        seriesArr.push({ name: '_stats_q1_base', type: 'line', data: __series_q1, stack: 'range', lineStyle: { opacity: 0 }, symbol: 'none' });
        seriesArr.push({ name: '_stats_iqr', type: 'line', data: __series_q3, stack: 'range', lineStyle: { opacity: 0 }, areaStyle: { color: mainStore.colors.stats, opacity: 0.4 }, symbol: 'none' });
        seriesArr.push({ name: '_stats_mean', type: 'line', data: __series_mean, smooth: true, lineStyle: { color: mainStore.colors.stats, opacity: 0.8, width: 4 }, symbol: 'none' });
        // Fake "Stats" series — empty data, exists only to provide the grouped legend entry
        seriesArr.push({ name: 'Stats', type: 'line', data: [], showSymbol: false, legendIcon: 'roundRect', lineStyle: { color: mainStore.colors.stats, opacity: 0 }, itemStyle: { color: mainStore.colors.stats } });
    }

    // model series (reuse previously computed __series_model)
    seriesArr.push({
        name: mainStore.selected_variable.var || 'value', type: 'line', showSymbol: false, data: __series_model, smooth: true, lineStyle: {
            width: 4, color: mainStore.colors.model.line, shadowColor: mainStore.colors.model.shadow,
            shadowBlur: 5, opacity: 0.8
        }, itemStyle: { color: mainStore.colors.model.line }, legendIcon: 'roundRect'
    });

    // sensor series will be optionally added below
    option.series = seriesArr;

    const hasSensorData = sensorData && Array.isArray(sensorData.time) && sensorData.time.length > 0;

    if (hasSensorData) {
        const sensor_timestamps = sensorData.time.map((t: any) => moment.utc(t).valueOf());
        const sensor_values = sensorData.value;
        const __series_sensor = sensor_timestamps.map((t: any, i: number) => [moment.utc(t).tz(tz).format(), sensor_values[i]]);

        option.series.push({
            name: 'Sensor Data',
            type: 'line',
            data: __series_sensor,
            symbol: 'none',
            lineStyle: { width: 4, color: mainStore.colors.observation.line, opacity: 0.8, shadowColor: mainStore.colors.observation.shadow, shadowBlur: 5 },
            itemStyle: { color: mainStore.colors.observation.line },
            legendIcon: 'roundRect'
        });
    } else {
        // Remove sensor series if previously present
        option.series = option.series.filter((s: any) => s.name !== 'Sensor Data');
    }

    // Add night mark areas if any
    if (markAreaData.length > 0) {
        (option.series[0] as any).markArea = { silent: true, itemStyle: { color: colors.yellow.accent2, opacity: 0.05 }, data: markAreaData };
    }

    // Build explicit legend.data: show all named series except internal underscore-prefixed stats series
    option.legend.data = (option.series as any[])
        .filter((s: any) => s.name && !s.name.startsWith('_'))
        .map((s: any) => s.name);

    // Use notMerge=true so removal of 'Sensor Data' is enforced (prevents stale series remaining)
    globalChart.setOption(option, true);
    globalChart.resize();

    // Register grouped 'Stats' legend toggle — clicking it shows/hides all 5 internal stats series
    const STATS_INTERNAL = ['_stats_min_base', '_stats_max_range', '_stats_q1_base', '_stats_iqr', '_stats_mean'];
    if (_statsLegendHandler) globalChart.off('legendselectchanged', _statsLegendHandler);
    if (hasClimate) {
        _statsLegendHandler = (params: any) => {
            if (params.name !== 'Stats') return;
            const action = params.selected['Stats'] ? 'legendSelect' : 'legendUnSelect';
            for (const name of STATS_INTERNAL) {
                globalChart!.dispatchAction({ type: action, name });
            }
        };
        globalChart.on('legendselectchanged', _statsLegendHandler);
    } else {
        _statsLegendHandler = null;
    }
}


async function autorange() {
    if (!map || !mapLoaded.value) {
        console.warn('Map not loaded yet');
        return;
    }

    try {
        const selectedVar = mainStore.selected_variable.var;
        const selectedDt = mainStore.selected_variable.dt;
        const selectedDepth = mainStore.selected_variable.depth;

        if (!selectedVar || !selectedDt) {
            console.warn('No variable or datetime selected');
            return;
        }

        // Get the visible map bounds
        const bounds = map.getBounds();
        const north = bounds.getNorth();
        const south = bounds.getSouth();
        const east = bounds.getEast();
        const west = bounds.getWest();

        // Format datetime as ISO string
        const dtStr = selectedDt.format('YYYY-MM-DDTHH:mm:ss');

        // Call the new getMinMax endpoint to extract min/max directly from the NC file
        const response = await axios.post(`${apiBaseUrl}/getMinMax`, {
            var: selectedVar,
            dt: dtStr,
            depth: selectedDepth,
            north: north,
            south: south,
            east: east,
            west: west
        });

        if (response.data && response.data.min !== null && response.data.max !== null) {
            let minVal = response.data.min;
            let maxVal = response.data.max;

            // Round using the precision from the selected variable
            const precision = mainStore.selected_variable.precision || 0;
            if (precision > 0) {
                minVal = Math.round(minVal / precision) * precision;
                maxVal = Math.round(maxVal / precision) * precision;
            }

            mainStore.updateSelectedVariable({
                colormapMin: minVal,
                colormapMax: maxVal
            });
        } else {
            console.warn('No valid min/max values in response');
        }
    } catch (e) {
        console.error('Error in autorange:', e);
    } finally {
        mainStore.setAutoRangeDisabled(false);
    }
}

// Watch selected timestamp and update vertical marker without replotting the series
try {
    watch(() => mainStore.selected_variable.dt, (newDt) => {
        if (!globalChart) return;
        const tz = 'America/Vancouver';
        const sel = newDt ? moment.utc(newDt).tz(tz).format() : null;
        try {
            globalChart.setOption({
                series: [
                    {
                        name: "Day/Night",
                        markLine: {
                            symbol: ['none', 'none'],
                            data: [
                                //  Now line
                                {
                                    xAxis: moment.tz(moment(), tz).format(),
                                    lineStyle: {
                                        color: colors.green.lighten2,
                                        width: 1,
                                        type: 'dashed'
                                    },
                                    label: {
                                        show: true,
                                        position: 'end',
                                        formatter: 'Now',
                                        backgroundColor: '',
                                        padding: [2, 4],
                                        borderRadius: 2,
                                        borderWidth: 1,
                                        borderColor: colors.green.lighten2
                                    },
                                },
                                // Selected time line
                                {
                                    xAxis: sel,
                                    lineStyle: { color: colors.orange.lighten2, width: 1, type: 'dashed' },
                                    label: {
                                        show: true,
                                        position: 'end',
                                        formatter: 'Map',
                                        backgroundColor: '',
                                        padding: [2, 4],
                                        borderRadius: 2,
                                        borderWidth: 1,
                                        borderColor: colors.orange.lighten2
                                    },
                                }
                            ]
                        }
                    }]
            });
        } catch (e) {
            // ignore if chart has no series yet
        }
    });
} catch (e) {
    console.warn('Failed to attach selected timestamp watcher for chart marker:', e);
}

// Abort controller for ongoing timeseries requests and debounce timer
let tsRequestController: AbortController | null = null;
let tsRefreshTimer: any = null;
let zrClickHandler: ((evt: any) => void) | null = null;


</script>

<style scoped>
.map-drawer-toggle {
    position: absolute;
    top: 12px;
    z-index: 2;
    /* background: rgba(255, 255, 255, 0.85); */
    border-radius: 8px;
    box-shadow: 0 2px 6px rgba(0, 0, 0, 0.15);
}
</style>

/* Shrink Mapbox bottom-left controls (logo + attribution) to reduce visual footprint */
<style>
.mapboxgl-ctrl-bottom-left .mapboxgl-ctrl-logo {
    transform: scale(0.5) translateY(1px) !important;
    transform-origin: left center !important;
}

/* make sure the controls remain clickable when scaled */
.mapboxgl-ctrl-bottom-left a {
    pointer-events: auto;
}

.footer-text {
    font-family: "Roboto Mono", monospace;
    font-size: 0.75rem;
    vertical-align: text-bottom;
}
</style>

<style scoped>
@import url('https://fonts.googleapis.com/css2?family=Roboto+Mono:ital,wght@0,100..700;1,100..700&display=swap');

.h-screen {
    height: calc(100vh - 48px);
}

.selector {
    position: absolute;
    width: 220px;
    z-index: 9998;
    top: 16px;
    left: 16px;
}

.global-chart-wrapper {
    position: relative;
}

.global-chart-overlay {
    position: absolute;
    inset: 0;
    background: #33333366;
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
</style>