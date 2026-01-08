<template>
    <div class="d-flex flex-column h-screen overflow-hidden">
        <!-- Top: Map -->
        <div ref="mapContainer" class="flex-grow-1"></div>

        <!-- Bottom: Global Chart Footer -->
        <div class="footer-chart" style="height: 260px; border-top: 1px solid rgba(0,0,0,0.12);">
            <div ref="globalChartContainer" class="w-100 h-100"></div>
        </div>
    </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount } from 'vue';
import { useRuntimeConfig } from '#app';
import mapboxgl from 'mapbox-gl';
import 'mapbox-gl/dist/mapbox-gl.css';
import * as echarts from 'echarts'
import axios from 'axios'
import moment from 'moment-timezone'

const config = useRuntimeConfig();
const mapContainer = ref<HTMLDivElement | null>(null);
const globalChartContainer = ref<HTMLDivElement | null>(null);
let map: mapboxgl.Map | null = null;
let globalChart: echarts.ECharts | null = null;

///////////////////////////////////  HOOKS  ///////////////////////////////////
onMounted(async () => {
    mapboxgl.accessToken = config.public.mapboxToken;
    if (!mapContainer.value) return;

    map = new mapboxgl.Map({
        container: mapContainer.value,
        style: 'mapbox://styles/taimazb/cmk1jwu8o005101sv1j41cj6j?optimize=true&fresh=true',
        center: [-123.2, 48.8],
        zoom: 9.5,
        pitch: 45,
    });

    // When the map finishes loading the style, add the PNG overlay and chart
    map.on('load', () => {
        addStations().catch((e) => console.warn('addStations failed:', e));
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


///////////////////////////////////  MEDTHODS  ///////////////////////////////////
async function addStations() {
    if (!map) return;

    // For the new flow we don't use station points. Instead, add a PNG overlay and
    // register a click handler that queries the API for a timeseries at the clicked coordinate.

    // Example PNG — change this to the variable you want to display
    const examplePng = '/png/dissolved_inorganic_carbon/dissolved_inorganic_carbon_depth0p5000003_2026-01-06T123000.png';
    try {
        await addPngOverlay(examplePng);
    } catch (e) {
        console.warn('Failed to add PNG overlay:', e);
    }

    // initialize the chart now with an empty series
    try { if (globalChart) { globalChart.dispose(); globalChart = null; } } catch (e) { /* ignore */ }
    if (!globalChartContainer.value) return;
    globalChart = echarts.init(globalChartContainer.value, undefined, { renderer: 'canvas' });

    const option = {
        title: { text: 'Timeseries', left: 'center' },
        tooltip: { trigger: 'axis' },
        xAxis: { type: 'time' },
        yAxis: { type: 'value', min: 'dataMin', max: 'dataMax' },
        series: []
    };
    globalChart.setOption(option);
    globalChart.resize();

    // click handler for the map will be registered by addPngOverlay once the meta is available

    async function addPngOverlay(publicPngPath: string, sourceId = 'png-image', layerId = 'png-image-layer') {
        if (!map) throw new Error('map not initialized');
        // First try per-image sidecar JSON (use axios)
        const jsonPath = publicPngPath + '.json';
        let meta: any = null;
        let varName: string | null = null;
        try {
            const r = await axios.get(jsonPath);
            meta = r.data;
        } catch (e) {
            // fall back to a variable-level meta.json located at /png/<var>/meta.json
            const parts = publicPngPath.split('/').filter(Boolean);
            if (parts.length >= 2) {
                varName = parts[1];
                const metaPath = `/png/${varName}/meta.json`;
                try {
                    const r2 = await axios.get(metaPath);
                    meta = r2.data;
                } catch (e2) {
                    // ignore and let later validation fail
                }
            }
        }
        if (!meta || !meta.bounds || meta.bounds.length !== 4) throw new Error(`Invalid or missing meta JSON for: ${publicPngPath}`);
        const [lonmin, latmin, lonmax, latmax] = meta.bounds;

        const coords = [
            [lonmin, latmax], // top-left
            [lonmax, latmax], // top-right
            [lonmax, latmin], // bottom-right
            [lonmin, latmin], // bottom-left
        ];

        // remove existing if present
        try { if (map.getLayer(layerId)) map.removeLayer(layerId); } catch (e) { }
        try { if (map.getSource(sourceId)) map.removeSource(sourceId); } catch (e) { }

        map.addSource(sourceId, { type: 'image', url: publicPngPath, coordinates: coords });
        map.addLayer({
            id: layerId, type: 'raster', source: sourceId, paint: {
                'raster-opacity': 1.0
            }
        });

        // save active overlay metadata on the map instance for access by click handler
        const overlayObj: any = { bounds: [lonmin, latmin, lonmax, latmax], varName, publicPngPath, meta, clickHandler: null };
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
            const apiBase = config.public.apiBase || 'http://localhost:9011';

            // default time range: last 30 days
            const to_dt = new Date().toISOString().replace('Z', '');
            const from_dt = new Date(Date.now() - 30 * 24 * 3600 * 1000).toISOString().replace('Z', '');

            // POST to the API and expect {time: [...], value: [...]} in response
            try {
                console.log({ var: vname, from_dt, to_dt, lat: lat, lon: lng });
                const r = await axios.post(`${apiBase}/extractTimeseries`, { var: vname, from_dt, to_dt, lat: lat, lon: lng });
                console.log(r);
                
                const json = r.data;
                if (!json || !Array.isArray(json.time) || !Array.isArray(json.value)) throw new Error('Invalid API response');
                plotTimeseriesFromApi(vname, json.time, json.value, lat, lng);
            } catch (err) {
                console.error('Failed to fetch timeseries:', err);
            }
        };

        // register click handler
        overlayObj.clickHandler = onMapClick;
        map.on('click', onMapClick);

        // Optionally zoom to the image bounds for visibility during testing
        try { map.fitBounds([[lonmin, latmin], [lonmax, latmax]], { padding: 40, duration: 800 }); } catch (e) { }
        console.info(`Added PNG overlay ${publicPngPath} as source='${sourceId}', layer='${layerId}'`);
    }
}
// Plot timeseries returned from the API into the footer chart
import { computeNightRanges } from '../../composables/useSunCalc'

function plotTimeseriesFromApi(varName: string | null, times: string[], values: number[], lat?: number, lon?: number) {
    if (!globalChart) return;
    const tz = 'America/Vancouver';

    // Convert the incoming UTC times to Vancouver local times (strings with offset)
    const localTimes = (times || []).map((t) => moment.utc(t).tz(tz).format());
    const seriesData = localTimes.map((lt, i) => [lt, values[i]]);

    // Determine time range in local timezone
    if (!localTimes || localTimes.length === 0) {
        globalChart.setOption({ series: [{ name: varName || 'value', type: 'line', showSymbol: false, data: [] }] });
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

    const option: any = {
        title: { text: varName ? `Timeseries — ${varName}` : 'Timeseries', left: 'center' },
        tooltip: { trigger: 'axis', formatter: (params: any) => {
            if (!Array.isArray(params)) return '';
            const timeVal = params[0]?.value?.[0] ?? params[0]?.axisValue;
            // timeVal will be a local-time string with offset; parseZone keeps offset
            const timeStr = moment.parseZone(timeVal).format('YYYY-MM-DD HH:mm');
            let out = `<b>${timeStr}</b><br/>`;
            for (const p of params) {
                const val = Array.isArray(p.value) ? p.value[1] : p.value;
                out += `<span style="color:${p.color}">●</span> ${p.seriesName}: ${Number(val).toFixed(3)}<br/>`;
            }
            return out;
        }},
        xAxis: {
            type: 'time',
            axisLabel: {
                formatter: (value: any) => moment.parseZone(value).format('YYYY-MM-DD HH:mm')
            }
        },
        yAxis: { type: 'value' },
        series: [{ name: varName || 'value', type: 'line', showSymbol: false, data: seriesData }],
    };

    // Add night mark areas if any
    if (markAreaData.length > 0) {
        (option.series[0] as any).markArea = { silent: true, itemStyle: { color: 'rgba(20,30,70,0.08)' }, data: markAreaData };
    }

    globalChart.setOption(option);
    globalChart.resize();
}

function inferVarNameFromPath(path: string) {
    try { const parts = path.split('/').filter(Boolean); return parts[1] || null; } catch (e) { return null; }
}



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
</script>

<style scoped>
.h-screen {
    height: 100vh;
}
</style>