<template>
    <div class="d-flex flex-column h-screen overflow-hidden">
        <!-- Top: Map -->
        <div ref="mapContainer" class="flex-grow-1 map-container">
            <Layers @toggleLayer="onToggleLayer" />
            <DepthSlider />
        </div>

        <!-- Bottom: Global Chart Footer -->
        <v-footer class="ma-0 pa-0" :style="{ maxHeight: `${footerHeight}` }">
            <!-- <div ref="globalChartContainer" class="w-100" :style="{ height: `calc(${footerHeight} - 20px)` }"></div> -->
            <v-container minWidth="100%" class="ma-0 pa-0">
                <v-row class="ma-0 pa-0" :style="{ height: `calc(${footerHeight} - 20px)` }">
                    <div ref="globalChartContainer" style="width: 100%; height: 100%;"></div>
                </v-row>
                <v-row class="ma-0 pa-0" style="height:20px; background-color: #ccc;">
                    <v-col cols="auto" class="my-0 mx-2 pa-0" style="height:20px">
                        <span class="footer-text" style="font-weight: bold;">SELECTED</span>
                    </v-col>
                    <v-col cols="auto" class="my-0 mx-2 pa-0" style="height:20px">
                        <span class="footer-text">{{ var2name(selectedVariable.var) }}</span>
                    </v-col>
                    <v-divider vertical class="mx-2"></v-divider>
                    <v-col cols="auto" class="my-0 mx-2 pa-0" style="height:20px">
                        <span class="footer-text">{{ utc2pst(moment(selectedVariable.dt)) }}</span>
                    </v-col>

                    <v-spacer></v-spacer>

                    <v-col cols="auto" class="my-0 mx-2 pa-0" style="height:20px">
                        <span class="footer-text">{{ mouseCoords.lat }} , {{ mouseCoords.lng }}</span>
                    </v-col>
                </v-row>
            </v-container>

        </v-footer>
        <!-- <div class="footer-chart" style="height: 260px; border-top: 1px solid rgba(0,0,0,0.12);">
            <div ref="globalChartContainer" class="w-100 h-100"></div>
        </div> -->
    </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount, watch, reactive, computed } from 'vue';
import { useRuntimeConfig } from '#app';
import mapboxgl from 'mapbox-gl';
import 'mapbox-gl/dist/mapbox-gl.css';
import * as echarts from 'echarts'
import axios from 'axios'
import moment from 'moment-timezone'
import Layers from '../components/layers.vue'
import DepthSlider from '../components/depth-slider.vue'
import type { FeatureCollection, Geometry, GeoJsonProperties } from 'geojson';

import { computeNightRanges } from '../../composables/useSunCalc'
import { var2name } from '../../composables/useVar2Name'
import { utc2pst } from '../../composables/useUTC2PST'
import { formatDepth } from '../../composables/useFormatDepth'
import { useCircleLayer } from '../../composables/useCircleLayer';


///////////////////////////////////  SETUP  ///////////////////////////////////

import { useMainStore } from '../stores/main'
import { no } from 'vuetify/locale';
const mainStore = useMainStore();

const config = useRuntimeConfig();
const apiBaseUrl = config.public.apiBaseUrl

const mapContainer = ref<HTMLDivElement | null>(null);
const globalChartContainer = ref<HTMLDivElement | null>(null);
let map: mapboxgl.Map | null = null;
let globalChart: echarts.ECharts | null = null;
let __seriesTs: number[] = []; // Global cache for chart timestamps to support "click anywhere"
const meta = ref<any>(null);
// remember last clicked point (lat/lon) so chart can be refreshed when var/depth changes
const lastClicked = ref<{ lat: number; lon: number } | null>(null);
const footerHeight = '200px';

// [-126.4002914428711, 46.85966491699218, -121.31835174560548, 51.10480117797852]
const bounds = [[-126.4, 46.85], [-121.3, 51.1]] as [[number, number], [number, number]];

const mouseCoords = ref<{ lng: number | null, lat: number | null }>({ lng: null, lat: null });

const sensorData = ref<{time:string, value: number}[]>([])

///////////////////////////////////  COMPUTED  ///////////////////////////////////

const selectedVariable = computed(() => mainStore.selected_variable);

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
        antialias: true,
        preserveDrawingBuffer: true, // needed for exporting canvas
    });
    console.log(map);

    // When the map finishes loading the style, add the PNG overlay and chart
    map.on('load', () => {
        map?.on('mousemove', (e) => {
            mouseCoords.value.lng = e.lngLat.lng.toFixed(4) as unknown as number;
            mouseCoords.value.lat = e.lngLat.lat.toFixed(4) as unknown as number;
        });
        init().catch((e) => console.warn('init failed:', e));
        addSensors().catch((e) => console.warn('addSensors failed:', e));
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
});

///////////////////////////////////  WATCH  ///////////////////////////////////

// Watcher: add/update/remove overlay when selected variable or depth changes
watch(() => [mainStore.selected_variable.var, mainStore.selected_variable.depth], async ([v, depth]) => {
    if (!map) return;

    if (!v) { removePngOverlay(); return; }
    try {
        await getMetadata();
        await updatePngOverlay();

        // If the user previously clicked a point, refresh the timeseries chart for the new var/depth
        if (lastClicked.value) {
            try {
                // debounce rapid var/depth changes to avoid hammering the API
                if (tsRefreshTimer) clearTimeout(tsRefreshTimer);
                tsRefreshTimer = setTimeout(async () => {
                    try {
                        const lat = lastClicked.value!.lat;
                        const lon = lastClicked.value!.lon;
                        const varId = mainStore.selected_variable.var;
                        console.log('Refreshing timeseries for last clicked point due to var/depth change', { varId, lat, lon, depth: mainStore.selected_variable.depth });

                        // abort previous request if any
                        try { if (tsRequestController) tsRequestController.abort(); } catch (e) { }
                        tsRequestController = new AbortController();

                        const r = await axios.post(`${apiBaseUrl}/extractTimeseries`, { var: varId, lat, lon, depth: mainStore.selected_variable.depth }, { signal: tsRequestController.signal });
                        const json = r.data;
                        if (json && Array.isArray(json.time) && Array.isArray(json.value)) {
                            const vname = mainStore.variables.find(x => x.var === varId)?.name || varId;
                            plotTimeseriesFromApi(vname, json.time, json.value, lat, lon);
                        }
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
        }
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

///////////////////////////////////  MEDTHODS  ///////////////////////////////////
async function getMetadata() {
    try {
        const varId = mainStore.selected_variable.var;
        const metaPath = `${apiBaseUrl}/metadata/${varId}`;
        console.log('metaPath: ', metaPath);

        const r = await axios.get(metaPath);
        meta.value = JSON.parse(r.data);
    } catch (e) {
        console.error('Failed to fetch metadata:', e);
    }
}

async function init() {
    if (!map) return;

    getVariables()

    // For the new flow we don't use station points. Instead, we start with NO PNG overlay.

    // initialize the chart now with an empty series
    try { if (globalChart) { globalChart.dispose(); globalChart = null; } } catch (e) { /* ignore */ }
    if (!globalChartContainer.value) return;
    globalChart = echarts.init(globalChartContainer.value, undefined, { renderer: 'canvas' });

    const option = {
        // title: { text: 'Timeseries', left: 'center' },
        tooltip: {
            trigger: 'axis'
        },
        xAxis: {
            type: 'time'
        },
        yAxis: { type: 'value', min: 'dataMin', max: 'dataMax' },
        grid: { left: 50, right: 30, top: 30, bottom: 30 },
        series: []
    };
    globalChart.setOption(option);
    globalChart.resize();

    // Clicks anywhere on the chart area (using global canvas coordinate conversion)
    try {
        let lastClickedX: string | number | null = null;
        globalChart.getZr().on('click', (evt: any) => {
            console.log(globalChart, evt);
            if (!globalChart || !__seriesTs.length) return;

            // ZRender coordinates for the click
            const px = evt.event.zrX;
            const py = evt.event.zrY;

            // Convert pixel point to chart coordinates (values)
            const converted = globalChart.convertFromPixel('grid', [px, py]);
            console.log('converted: ', converted);

            if (!converted || converted[0] === undefined) return;

            const clickX = Number(converted[0]);
            console.log('clickX: ', clickX);

            // Find nearest point in __seriesTs (assumed sorted timestamps)
            // Binary search for efficiency
            let low = 0;
            let high = __seriesTs.length - 1;
            let bestIdx = 0;

            while (low <= high) {
                const mid = Math.floor((low + high) / 2);
                if (Math.abs(__seriesTs[mid] - clickX) < Math.abs(__seriesTs[bestIdx] - clickX)) {
                    bestIdx = mid;
                }
                if (__seriesTs[mid] < clickX) low = mid + 1;
                else if (__seriesTs[mid] > clickX) high = mid - 1;
                else break;
            }

            const finalX = __seriesTs[bestIdx];

            if (finalX !== lastClickedX) {
                lastClickedX = finalX;
                console.log('ZR clicked nearest x:', finalX, 'moment:', moment(finalX).utc().format('YYYY-MM-DDTHHmmss'));
                mainStore.setSelectedVariable(
                    mainStore.selected_variable.var,
                    moment.utc(finalX),
                    mainStore.selected_variable.depth
                );
            }
        });
    } catch (e) {
        console.warn('Failed to attach ECharts hover listeners:', e);
    }

    // No click handler registered until an overlay is added by selecting a variable.
}

async function getVariables() {
    try {
        const r = await axios.get(`${apiBaseUrl}/variables`);
        const data = r.data;

        // Convert datetimes from string to moment objects
        data.forEach((v: any) => {
            v.dts = v.dts.map((dtstr: string) => moment.utc(dtstr));
        });

        mainStore.setVariables(data);
        console.log(data);

        if (data.length > 0) {
            const dts = data[0].dts
            const precision = data[0].precision;
            const depth = data[0].depths && data[0].depths.length > 0 ? data[0].depths[0] : 0.5;
            mainStore.setSelectedVariable(data[0].var, dts[dts.length - 1], depth, precision);
        }
    } catch (e) {
        console.error('Failed to fetch variables:', e);
    }
}

async function addSensors() {
    const sensors = await getSensors();
    console.log('sensors: ', sensors);

    const features = sensors.map((s: any) => ({
        type: 'Feature',
        geometry: {
            type: 'Point',
            coordinates: [s.longitude, s.latitude]
        },
        properties: {
            name: s.name,
            depths: s.depths,
            variables: s.variables
        }
    }));
    const geojson: FeatureCollection<Geometry, GeoJsonProperties> = {
        type: 'FeatureCollection',
        features: features
    };

    const circle = useCircleLayer(() => map);
    circle.addCircleLayer({ sourceId: 'stations', layerId: 'stations-circles', radius: 6, color: '#FFD700' });
    circle.updateData(geojson);
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

    const varId = mainStore.selected_variable.var;
    const dt = mainStore.selected_variable.dt?.format('YYYY-MM-DDTHHmmss') || '';
    const depth = formatDepth(mainStore.selected_variable.depth);
    console.log(mainStore.selected_variable.depth, depth);

    const pngPath = `${apiBaseUrl}/png/${varId}/${dt}/${depth}`;

    const [lonmin, latmin, lonmax, latmax] = meta.value.bounds;
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
    const vmin = mainStore.variables.find(v => v.var === varId)?.min
    const vmax = mainStore.variables.find(v => v.var === varId)?.max

    // Get packing params from metadata, default to 0.1 precision and 0 base if missing
    // Note: base might be equal to vmin if it was dynamic
    const precision = mainStore.variables.find(v => v.var === varId).precision;
    const base = 0

    const color_stops = [
        [0.0, 'rgba(0, 0, 0, 1)'],
        [0.001, '#440154'],
        [0.25, '#00f'],
        [0.5, '#0f0'],
        [0.75, '#fde725'],
        [1.0, '#f00']
    ];
    for (const stop of color_stops) {
        const val_phys = vmin + stop[0] * (vmax - vmin);
        // decode formula: q = (phys - base) / precision
        const val_packed = (val_phys - base) / precision;
        raster_values.push(val_packed, stop[1]);
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
        map.setPaintProperty(layerId, 'raster-color-range', [(vmin - base) / precision, (vmax - base) / precision]);
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
                'raster-color-range': [(vmin - base) / precision, (vmax - base) / precision],
                // Mix to recover the 24-bit integer from normalized RGB [0..1]
                // R_int = R_norm * 255. Packed = R_int*65536 + G_int*256 + B_int
                // Coeffs: [255*65536, 255*256, 255, 0] -> [16711680, 65280, 255, 0]
                'raster-color-mix': [16711680, 65280, 255, 0],
                'raster-fade-duration': 0
            },
        }, 'country-boundaries');
    }
    console.log(vmin, vmax, base, precision, raster_values);

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
        const overlay = (map as any).__activePngOverlay;
        if (!overlay) return;
        const [lon0, lat0, lon1, lat1] = overlay.bounds;
        if (!(lng >= lon0 && lng <= lon1 && lat >= lat0 && lat <= lat1)) return; // outside overlay
        // show a marker at clicked position
        try { if ((map as any).__clickMarker) ((map as any).__clickMarker).remove(); } catch (e) { }
        const el = document.createElement('div'); el.style.width = '12px'; el.style.height = '12px'; el.style.borderRadius = '50%'; el.style.background = '#ff5722'; el.style.border = '2px solid white';
        const marker = new mapboxgl.Marker({ element: el }).setLngLat([lng, lat]).addTo(map);
        (map as any).__clickMarker = marker;

        // determine variable name
        const vname = overlay.varName || inferVarNameFromPath(overlay.publicPngPath);

        // remember clicked point so subsequent var/depth changes can refresh the chart
        lastClicked.value = { lat, lon: lng };

        // Abort any in-flight timeseries requests and create a new controller
        try { if (tsRequestController) tsRequestController.abort(); } catch (e) { }
        tsRequestController = new AbortController();

        // POST to the API and expect {time: [...], value: [...]} in response
        try {
            console.log({ var: mainStore.selected_variable.var, lat: lat, lon: lng, depth: mainStore.selected_variable.depth });
            const r = await axios.post(`${apiBaseUrl}/extractTimeseries`, { var: mainStore.selected_variable.var, lat: lat, lon: lng, depth: mainStore.selected_variable.depth }, { signal: tsRequestController.signal });
            console.log(r);

            const json = r.data;
            if (!json || !Array.isArray(json.time) || !Array.isArray(json.value)) throw new Error('Invalid API response');
            plotTimeseriesFromApi(vname, json.time, json.value, lat, lng);
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

    // Optionally zoom to the image bounds for visibility during testing
    // try { map.fitBounds([[lonmin, latmin], [lonmax, latmax]], { padding: 40, duration: 800 }); } catch (e) { }
    console.info(`Added PNG overlay ${pngPath} as source='${sourceId}', layer='${layerId}'`);

    // Do not auto-initialize animator here; animation starts when Play pressed from footer controls
    // We keep the static PNG overlay active by default.
}

// Called when component or overlay destroyed
onBeforeUnmount(() => {

});

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

// Plot timeseries returned from the API into the footer chart
function plotTimeseriesFromApi(varName: string | null, times: string[], values: number[], lat?: number, lon?: number) {
    if (!globalChart) return;
    const tz = 'America/Vancouver';

    // Update global timestamp cache for the click handler
    __seriesTs = (times || []).map(t => moment.utc(t).valueOf());

    // Convert the incoming UTC times to Vancouver local times (strings with offset)
    const localTimes = (times || []).map((t) => moment.utc(t).tz(tz).format());
    const seriesData = localTimes.map((lt, i) => [lt, values[i]]);

    // Determine time range in local timezone
    if (!localTimes || localTimes.length === 0) {
        globalChart.setOption({
            series: [
                {
                    name: varName || 'value',
                    type: 'line',
                    showSymbol: false,
                    smooth: true,
                    data: []
                }]
        });
        return;
    }
    const startLocal = moment.tz(localTimes[0], tz).clone();
    const endLocal = moment.tz(localTimes[localTimes.length - 1], tz).clone();

    // Compute night mark areas using SunCalc (sunrise/sunset) if lat/lon provided, otherwise fall back to fixed night windows
    let markAreaData: any[] = [];
    if (typeof lat === 'number' && typeof lon === 'number') {
        const nights = computeNightRanges({ lat, lon, tz, startLocalIso: startLocal.format(), endLocalIso: endLocal.format() });
        markAreaData = nights.map(([s, e]) => [{ xAxis: s }, { xAxis: e }]);
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

    const option: any = {
        // title: { text: varName ? `Timeseries — ${varName}` : 'Timeseries', left: 'center' },
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
        xAxis: {
            type: 'time',
            axisLabel: {
                formatter: (value: any) => moment.parseZone(value).format('DD MMM, HH:mm')
            }
        },
        yAxis: {
            type: 'value',
            min: 'dataMin',
            max: 'dataMax',
            axisLabel: {
                formatter: (value: any) => Number(value).toFixed()
            }
        },
        series: [{
            name: varName || 'value',
            type: 'line',
            showSymbol: false,
            data: seriesData,
            smooth: true,
            lineStyle: { width: 4 },
            markLine: selectedXLocal ? {
                silent: true,
                symbol: 'none',
                lineStyle: { color: '#ff5722', width: 2, type: 'dashed' },
                label: { show: false },
                data: [{ xAxis: selectedXLocal }]
            } : { data: [] }
        }],
    };

    // Add night mark areas if any
    if (markAreaData.length > 0) {
        (option.series[0] as any).markArea = { silent: true, itemStyle: { color: 'rgba(20,30,70,0.08)' }, data: markAreaData };
    }

    globalChart.setOption(option);
    globalChart.resize();
}

// Watch selected timestamp and update vertical marker without replotting the series
try {
    watch(() => mainStore.selected_variable.dt, (newDt) => {
        if (!globalChart) return;
        const tz = 'America/Vancouver';
        const sel = newDt ? moment.utc(newDt).tz(tz).format() : null;
        const data = sel ? [{ xAxis: sel }] : [];
        try {
            globalChart.setOption({ series: [{ markLine: { silent: true, symbol: 'none', lineStyle: { color: '#ff5722', width: 2 }, label: { show: false }, data } }] });
        } catch (e) {
            // ignore if chart has no series yet
        }
    });
} catch (e) {
    console.warn('Failed to attach selected timestamp watcher for chart marker:', e);
}

function inferVarNameFromPath(path: string) {
    try { const parts = path.split('/').filter(Boolean); return parts[1] || null; } catch (e) { return null; }
}

// Abort controller for ongoing timeseries requests and debounce timer
let tsRequestController: AbortController | null = null;
let tsRefreshTimer: any = null;


// async function loadData(url: string): Promise<{ hindcast: [number, number][], forecast: [number, number][] }> {
async function loadData(url: string): Promise<[number, number][]> {
    const data = await $fetch<any[]>(url);
    const array: [number, number][] = [];
    // const hindcast: [number, number][] = [];
    // const forecast: [number, number][] = [];
    // const now = Date.now();

    for (const item of data) {
        if (!item || item.time === undefined || item.ssh === undefined || item.ssh === null) continue;
        const ts = new Date(item.time).getTime();
        const ssh = typeof item.ssh === 'string' ? parseFloat(item.ssh) : item.ssh;
        if (isNaN(ts) || isNaN(ssh)) continue;
        // if (ts >= now) forecast.push([ts, ssh]); else hindcast.push([ts, ssh]);
        array.push([ts, ssh]);
    }

    // hindcast.sort((a, b) => a[0] - b[0]);
    // forecast.sort((a, b) => a[0] - b[0]);

    // return { hindcast, forecast };

    return array
}

function onToggleLayer(variable: string) {
    // toggle: clicking the currently-selected variable will deselect it
    if (mainStore.selected_variable.var === variable) {
        mainStore.setSelectedVariable('', null, null);
    } else {
        mainStore.setSelectedVariable(variable, mainStore.selected_variable.dt, mainStore.selected_variable.depth);
    }
}


</script>

<style scoped>
.map-container {
    position: relative;
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
    font-family: 'Courier New', Courier, monospace;
    font-size: 0.75rem;
    vertical-align: middle;
}
</style>

<style scoped>
.h-screen {
    height: 100vh;
}
</style>