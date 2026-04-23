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
        <howTo v-model="showHow" />
        <!-- <div class="d-flex flex-column h-screen overflow-hidden"> -->
        <!-- Top: Map -->
        <div ref="mapContainer" class="flex-grow-1"
            :style="{ position: 'relative', height: `calc(100% - ${footerHeight})` }">
            <!-- <Layers @toggleLayer="onToggleLayer" /> -->

            <div class="selector">
                <Overlays @toggle-vertical-profile="drawerOpen = !drawerOpen" @show-how="showHow = true"
                    @autorange="autorange" />
            </div>
            <ColorbarSettings v-if="showColorbarSettings" class="selector" :style="{ left: (mainStore.isControlPanelOpen ? mainStore.controlPanel_width + 16+50 : 16+50) + 'px' , transition: 'left 0.3s ease' }" />

            <controlPanel />

            <!-- <div class="map-drawer-toggle" :style="{ right: drawerOpen ? '312px' : '12px' }">
                <v-btn size="24px" color="warning" class="ma-0 pa-0" @click="drawerOpen = !drawerOpen"
                    title="Vertical Profile">
                    <v-icon size="20px">mdi-chart-line</v-icon>
                </v-btn>
            </div> -->

            <SelectedVariableDrawer v-model="drawerOpen" :selected-point="lastClicked" :footer-height="footerHeight" />

            <!-- Multi-sensor location picker -->
            <SensorPickerPopover :visible="sensorPicker.visible" :x="sensorPicker.x" :y="sensorPicker.y"
                :sensors="sensorPicker.sensors" @pick="(s) => clickSensor(s.id, s.depth)"
                @close="sensorPicker.visible = false" />

            <v-snackbar-queue ref="snackbarQueue" v-model="snackMessages" :total-visible="3" closable
                contained></v-snackbar-queue>
        </div>

        <!-- Bottom: Global Chart Footer -->
        <v-footer class="ma-0 pa-0" :style="{ maxHeight: footerHeight }">
            <!-- <div ref="globalChartContainer" class="w-100" :style="{ height: `calc(${footerHeight} - 20px)` }"></div> -->
            <v-container minWidth="100%" class="ma-0 pa-0">
                <v-row class="my-0 mx-2 pa-0" style="height:20px; ">
                    <v-col cols="auto" class="my-0 mx-1 pa-0" style="height:20px">
                        <span class="footer-text">{{ var2name(selectedVariable.var) }}</span>
                    </v-col>
                    <v-divider vertical class="mx-0"></v-divider>
                    <v-col cols="auto" class="my-0 mx-1 pa-0" style="height:20px">
                        <span class="footer-text">{{ utc2pst(moment(selectedVariable.dt)) }}</span>
                    </v-col>
                    <v-divider vertical class="mx-0"></v-divider>
                    <v-col cols="auto" class="my-0 mx-1 pa-0" style="height:20px">
                        <span class="footer-text">Depth {{ formatDepth(selectedVariable.depth) }} m</span>
                    </v-col>
                    <v-divider vertical class="mx-0"></v-divider>
                    <v-col v-if="lastClicked" cols="auto" class="my-0 mx-1 pa-0" style="height:20px">
                        <span class="footer-text">{{ lastClicked?.lat.toFixed(5) }} , {{ lastClicked?.lng.toFixed(5)
                        }}</span>
                    </v-col>
                    

                    <v-spacer></v-spacer>

                    <v-col cols="auto" class="my-0 mx-1 pa-0" style="height:20px">
                        <v-icon size="12px" class="mx-2">mdi-cursor-default-outline</v-icon>
                        <span class="footer-text">{{ mouseCoords.lat?.toFixed(5) }} , {{ mouseCoords.lng?.toFixed(5)
                        }}</span>
                    </v-col>
                </v-row>

                <v-row class="ma-0 pa-0" :style="{ height: `calc(${footerHeight} - 20px)`, position: 'relative' }"
                    gap="0">
                    <TimeControls />

                    <TimeseriesChart ref="timeseriesChart" style="width: 100%; height: calc(100% - 32px);" />

                    <!-- <div class="py-3"
                        style="position: absolute; width:40px; height: 100%; bottom: 0px; right: 0px; text-align:center; display:flex; flex-direction:column; align-items:center; gap:6px; padding-top:6px;">
                        <v-btn title="Long-term climatology" flat size="20px" :disabled="!lastClicked" icon
                            color="primary" @click="dialogOpen = true">
                            <v-icon size="14px">mdi-chart-line</v-icon>
                        </v-btn>
                    </div> -->
                </v-row>
            </v-container>

            <!-- Dialog component for monthly chart -->
            <!-- <EchartsLineDialog v-model="dialogOpen" :coord="lastClicked" :variable="selectedVariable.var"
                :depth="selectedVariable.depth" /> -->

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
import axios from 'axios'
import moment from 'moment-timezone'
import TimeControls from '../components/TimeControls.vue'
import SelectedVariableDrawer from '../components/SelectedVariableDrawer.vue'
import BetaDisclaimerDialog from '../components/BetaDisclaimerDialog.vue'
import type { FeatureCollection, Geometry, GeoJsonProperties } from 'geojson';
import { var2name } from '../../composables/useVar2Name'
import { utc2pst } from '../../composables/useUTC2PST'
import { formatDepth } from '../../composables/useFormatDepth'
import useStationsInteraction from '../../composables/useStationsInteraction';
import { addBuoyLayer, type MultiSensorCandidate } from '../../composables/useBuoyLayer';
import getSensorTimeseries from '../../composables/useSensorTimeseries';
import EchartsLineDialog from '../components/EchartsLineDialog.vue'
import TimeseriesChart from '../components/TimeseriesChart.vue';

///////////////////////////////////  SETUP  ///////////////////////////////////

import { useMainStore } from '../stores/main'
const mainStore = useMainStore();

const config = useRuntimeConfig();
const apiBaseUrl = config.public.apiBaseUrl

// Colormaps cache
// const colormaps = ref<Record<string, any>>({});



const mapContainer = ref<HTMLDivElement | null>(null);
const timeseriesChart = ref<InstanceType<typeof TimeseriesChart> | null>(null);
let map: mapboxgl.Map | null = null;
const meta = ref<any>(null);
const drawerOpen = ref(false);
const footerHeight = ref<string>('300px');

const sensorPicker = ref<{ visible: boolean; x: number; y: number; sensors: MultiSensorCandidate[] }>({
    visible: false,
    x: 0,
    y: 0,
    sensors: [],
});

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

/** 
 * Number of days from now to fetch for climate timeseries. This is used both for the API request parameter and for computing the x-axis range of the chart (now +/- DFN days). The API will return all available data within that range, which may be less than DFN if the model run does not extend that far into the future.
*/

const zoom = ref('');

const snackMessages = computed({
    get: () => mainStore.snackMessages,
    set: (val) => { mainStore.snackMessages = val; },
});

const showHow = ref(false);

///////////////////////////////////  COMPUTED  ///////////////////////////////////

const DFN = computed(() => mainStore.dfnDays);

const selectedVariable = computed(() => mainStore.selected_variable);

const showBathymetryContours = computed(() => mainStore.showBathymetryContours);

const lastClicked = computed(() => mainStore.lastClickedMapPoint);

const selectedColormap = computed(() => {
    const name = mainStore.selected_variable.colormap;
    if (name) return mainStore.colormaps[name] ?? null;
    // Fallback to a default colormap (DB doesn't store colormap field)
    return null;
});

const midDate = computed(() => {
    return mainStore.midDate ?? moment.utc();
});

const mapCenter = computed(() => mainStore.mapCenter);

const showColorbarSettings = computed(()=> mainStore.showColorbarSettings);

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

watch(() => mapCenter.value, (newCenter) => {
    if (!map || !newCenter) return;
    map.easeTo({ center: [newCenter.lng, newCenter.lat] });
}, { immediate: true })

///////////////////////////////////  HOOKS  ///////////////////////////////////
onMounted(async () => {
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
        Promise.all([init()]).catch((e) => console.warn('init failed:', e));
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

        // remove stations layers + source if present
        try { if (map.getLayer && map.getLayer('stations-badge')) map.removeLayer('stations-badge'); } catch (e) { }
        try { if (map.getLayer && map.getLayer('stations-circles')) map.removeLayer('stations-circles'); } catch (e) { }
        try { if (map.getSource && map.getSource('stations-points')) map.removeSource('stations-points'); } catch (e) { }

        if (svg && svg.parentNode) svg.parentNode.removeChild(svg);
        map.remove();
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
                // Skip if a sensor click is already handling the fetch via lastClickedMapPoint watcher
                if (_sensorClickPending) return;
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

watch(() => mainStore.lastClickedMapPoint, (point) => {
    if (!point) return;
    trigger_mapClick(point.lat, point.lng);
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

    selectedReady.value = true;
    maybeInitClick();

    // For the new flow we don't use station points. Instead, we start with NO PNG overlay.

    // Chart is initialized by the TimeseriesChart component itself
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

    mainStore.setLastClickedMapPoint({ lat, lng });

    // Abort any in-flight timeseries requests and create a new controller
    try { if (tsRequestController) tsRequestController.abort(); } catch (e) { }
    tsRequestController = new AbortController();

    // trigger map click to load initial timeseries
    map.fire('click', { lngLat: { lat, lng } });

    // getTimeseriesFromApi(lat, lon);
}


async function getTimeseriesPromises(lat: number, lon: number) {
    if (!timeseriesChart.value) return;
    const fromDate = midDate.value.clone().subtract(DFN.value, 'days').format('YYYY-MM-DDTHHmmss');
    const toDate   = midDate.value.clone().add(DFN.value, 'days').format('YYYY-MM-DDTHHmmss');

    await timeseriesChart.value.fetchAndPlot(
        lat, lon,
        () => getTimeseriesFromApi(lat, lon, fromDate, toDate),
        () => getClimateTimeseries(lat, lon, fromDate, toDate),
        () => mainStore.selectedSensorID
            ? getSensorTimeseries(mainStore.selectedSensorID, mainStore.selected_variable.var, fromDate, toDate)
            : Promise.resolve(null)
    );
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

    // Group sensors by coordinate key so co-located sensors share one marker
    const locationMap = new Map<string, any[]>();
    for (const s of sensors) {
        const key = `${s.longitude},${s.latitude}`;
        if (!locationMap.has(key)) locationMap.set(key, []);
        locationMap.get(key)!.push(s);
    }

    const features = Array.from(locationMap.entries()).map(([, group]) => {
        const first = group[0];
        const anyActive = group.some((s: any) => s.active);
        return {
            type: 'Feature',
            geometry: {
                type: 'Point',
                coordinates: [first.longitude, first.latitude]
            },
            properties: {
                sensorCount: group.length,
                active: anyActive,
                // Embed all sensors as JSON string (Mapbox flattens properties to primitives)
                sensorsJson: JSON.stringify(
                    group
                        .sort((a: any, b: any) => a.depth - b.depth)
                        .map((s: any) => ({ id: s.id, name: s.name, depth: s.depth }))
                ),
            }
        };
    });

    const geojson: FeatureCollection<Geometry, GeoJsonProperties> = {
        type: 'FeatureCollection',
        features: features
    };

    try {
        const detach = await addBuoyLayer(map, geojson, clickSensor, openSensorPicker);
        (map as any).__stationsDetach = detach;
    } catch (e) {
        console.warn('Failed to add buoy layer:', e);
    }
}

function openSensorPicker(sensors: MultiSensorCandidate[], screenX: number, screenY: number) {
    sensorPicker.value = { visible: true, x: screenX, y: screenY, sensors };
}

function clickSensor(sensor_id: number, depth: number) {
    sensorPicker.value.visible = false;
    // Set flag BEFORE selectSensor so the var/depth watcher skips its own fetch.
    // The lastClickedMapPoint watcher (via setLastClickedMapPoint below) will
    // trigger the single authoritative fetch.
    _sensorClickPending = true;
    mainStore.selectSensor(sensor_id, depth);
    const sensor = mainStore.sensors.find((s: any) => s.id === sensor_id);
    if (sensor) {
        mainStore.setLastClickedMapPoint({ lat: sensor.latitude, lng: sensor.longitude });
    } else {
        // Sensor not in store — fall back to current point
        const pt = lastClicked.value;
        if (pt) getTimeseriesPromises(pt.lat, pt.lng);
    }
    // Clear after this tick — the var/depth watcher has already seen the flag.
    nextTick(() => { _sensorClickPending = false; });
}


async function getSensors() {
    try {
        const r = await axios.get(`${apiBaseUrl}/sensors`);
        const data = r.data;
        mainStore.setSensors(data);
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

        // Check if click landed on a sensor feature (layer may not exist yet)
        const stationLayers = map.getLayer('stations-circles') ? ['stations-circles'] : [];
        const features = stationLayers.length
            ? map.queryRenderedFeatures(evt.point, { layers: stationLayers })
            : [];

        if (features.length > 0) {
            // Sensor click is fully handled by useStationsInteraction → clickSensor.
            // Returning here prevents a duplicate getTimeseriesPromises call.
            return;
        }

        mainStore.setSelectedSensorID(null);
        sensorPicker.value.visible = false;
        mainStore.setLastClickedMapPoint({ lat, lng });
    };

    // register click handler
    overlayObj.clickHandler = onMapClick;
    map.on('click', onMapClick);
}

async function trigger_mapClick(lat: number, lng: number) {
    const overlay = (map as any).__activePngOverlay;
    if (!overlay) return;
    // show a marker at clicked position
    try { if ((map as any).__clickMarker) ((map as any).__clickMarker).remove(); } catch (e) { }
    const el = document.createElement('div');
    el.className = 'map-click-marker';
    const marker = new mapboxgl.Marker({ element: el, anchor: 'center' }).setLngLat([lng, lat]).addTo(map);
    (map as any).__clickMarker = marker;

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

// Abort controller for ongoing timeseries requests and debounce timer
let tsRequestController: AbortController | null = null;
let tsRefreshTimer: any = null;
// Prevents the var/depth watcher from scheduling a duplicate fetch when a sensor
// click already triggers one via the lastClickedMapPoint watcher path.
let _sensorClickPending = false;


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

@keyframes map-click-pulse {
    0% {
        box-shadow: 0 0 0 0 rgba(255, 87, 34, 0.7);
    }

    70% {
        box-shadow: 0 0 0 14px rgba(255, 87, 34, 0);
    }

    100% {
        box-shadow: 0 0 0 0 rgba(255, 87, 34, 0);
    }
}

.map-click-marker {
    width: 12px;
    height: 12px;
    border-radius: 50%;
    background: #ff5722;
    border: 2px solid white;
    animation: map-click-pulse 1.5s ease-out infinite;
}
</style>

<style scoped>
@import url('https://fonts.googleapis.com/css2?family=Roboto+Mono:ital,wght@0,100..700;1,100..700&display=swap');

.h-screen {
    height: calc(100vh - 48px);
}

.selector {
    position: absolute;
    width: 0;
    z-index: 9998;
    top: 16px;
    left: 0;
}
</style>