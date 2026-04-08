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

const config = useRuntimeConfig();
const mapContainer = ref<HTMLDivElement | null>(null);
const globalChartContainer = ref<HTMLDivElement | null>(null);
let map: mapboxgl.Map | null = null;
let globalChart: echarts.ECharts | null = null;

const stations = [
    {
        id: 'squamish',
        name: 'Squamish',
        coord: [-123.15395, 49.693733],
        dataUrl: '/ubcSSfSquamishSSH10m_3015_60ed_36f4.json',
        offset: [-190, -140],
        color: '#ff0',
    },
    {
        id: 'BoundaryBay',
        name: 'Boundary Bay',
        coord: [-122.9225, 48.998367],
        dataUrl: '/ubcSSfBoundaryBaySSH10m_8f27_0e0a_d6d9.json',
        offset: [-190, -140],
        color: '#0ff',
    },
    {
        id: 'CampbellRiver',
        name: 'Campbell River',
        coord: [-125.220505, 50.019947],
        dataUrl: '/ubcSSfCampbellRiverSSH10m_d176_7761_25dc.json',
        offset: [-190, -140],
        color: '#f0f',
    },
    {
        id: 'CherryPoint',
        name: 'Cherry Point',
        coord: [-122.75, 48.86],
        dataUrl: '/ubcSSfCherryPointSSH10m_697c_12db_8d83.json',
        offset: [-190, -140],
        color: '#ff8000',
    },
    {
        id: 'FridayHarbor',
        name: 'Friday Harbor',
        coord: [-123.008446, 48.553696],
        dataUrl: '/ubcSSfFridayHarborSSH10m_0e8e_1203_3641.json',
        offset: [-190, -140],
        color: '#0f0',
    },
    {
        id: 'HalfmoonBay',
        name: 'Halfmoon Bay',
        coord: [-123.9098, 49.50789],
        dataUrl: '/ubcSSfHalfmoonBaySSH10m_5540_d751_f957.json',
        offset: [-190, -140],
        color: '#f88',
    },
    {
        id: 'Nanaimo',
        name: 'Nanaimo',
        coord: [-123.93079, 49.166595],
        dataUrl: '/ubcSSfNanaimoSSH10m_86c0_9337_8cbf.json',
        offset: [-190, -140],
        color: '#88f',
    },
    {
        id: 'NeahBay',
        name: 'Neah Bay',
        coord: [-124.597946, 48.399952],
        dataUrl: '/ubcSSfNeahBaySSH10m_a80d_d710_63b2.json',
        offset: [-190, -140],
        color: '#ff0',
    },
    {
        id: 'NewWestminster',
        name: 'New Westminster',
        coord: [-122.90665, 49.20258],
        dataUrl: '/ubcSSfNewWestminsterSSH10m_5bbc_7059_fbbc.json',
        offset: [-190, -140],
        color: '#fff',
    },
    {
        id: 'PatriciaBay',
        name: 'Patricia Bay',
        coord: [-123.45493, 48.654064],
        dataUrl: '/ubcSSfPatriciaBaySSH10m_e186_e01e_3520.json',
        offset: [-190, -140],
        color: '#aaa',
    }
]

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

    map.on('load', addStations);
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

    // Load all data in parallel
    const loadingPromises = stations.map(async (s) => {
        try {
            const data = await loadData(s.dataUrl);
            return { s, data };
        } catch (e) {
            console.error(`Failed to load data for ${s.name}:`, e);
            return null;
        }
    });

    const loaded = (await Promise.all(loadingPromises)).filter(r => r !== null) as Array<any>;
    if (loaded.length === 0) return;

    // Build series: two series per station (hindcast = solid, forecast = dashed)
    const series: any[] = [];
    for (const { s, data } of loaded) {
        // Ensure the combined array is sorted by time
        const sorted = (data || []).slice().sort((a: [number, number], b: [number, number]) => a[0] - b[0]);
        // convert timestamps to ISO strings for robust time parsing in ECharts
        const seriesData = sorted.map(([ts, ssh]: [number, number]) => [new Date(ts).toISOString(), ssh]);

        // Use a single legend name per station so legend shows one entry
        const seriesName = s.name;

        series.push({
            id: s.id,
            name: seriesName,
            type: 'line',
            showSymbol: false,
            data: seriesData,
            lineStyle: { width: 1.5, color: s.color },
            emphasis: { focus: 'series' },
            // helpful metadata for future interactions
            // encode: { value: 1 },
            // custom: { stationId: s.id, part: 'past' }
        });

        // series.push({
        //     id: `${s.id}-forecast`,
        //     name: seriesName,
        //     type: 'line',
        //     showSymbol: false,
        //     data: data.forecast,
        //     lineStyle: { width: 1.5, color: s.color, type: 'dashed' },
        //     emphasis: { focus: 'series' },
        //     custom: { stationId: s.id, part: 'forecast' }
        // });
    }

    // Build a GeoJSON source for station points (used for the Mapbox circle layer)
    const features = loaded.map(({ s }) => ({
        type: 'Feature',
        geometry: { type: 'Point', coordinates: s.coord },
        properties: { id: s.id, name: s.name, color: s.color }
    }));
    const geojson = { type: 'FeatureCollection', features };

    // Remove existing layer/source if present and add/update
    try {
        if (map.getLayer && map.getLayer('stations-circles')) map.removeLayer('stations-circles');
        if (map.getSource && map.getSource('stations-points')) map.removeSource('stations-points');
    } catch (e) { /* ignore */ }

    if (map.addSource) {
        map.addSource('stations-points', { type: 'geojson', data: geojson });
        map.addLayer({
            id: 'stations-circles',
            type: 'circle',
            source: 'stations-points',
            paint: {
                'circle-radius': 6,
                'circle-color': ['get', 'color'],
                'circle-stroke-color': '#ffffff',
                'circle-stroke-width': 2,
                'circle-opacity': 0.95
            }
        });
    }

    // Initialize or replace the global footer chart
    try { if (globalChart) { globalChart.dispose(); globalChart = null; } } catch (e) { /* ignore */ }
    if (!globalChartContainer.value) return;
    globalChart = echarts.init(globalChartContainer.value, undefined, { renderer: 'canvas' });

    // add 'now' markers and shaded future region
    const nowTs = Date.now();
    const nowISO = new Date(nowTs).toISOString();
    if (series.length > 0) {
        series[0].markLine = {
            silent: true,
            symbol: 'none',
            lineStyle: { color: '#666', width: 1 },
            data: [{ xAxis: nowISO }]
        };
        series[0].markArea = {
            silent: true,
            itemStyle: { color: 'rgba(200,200,200,0.3)' },
            data: [[{ xAxis: nowISO }, { xAxis: 'max' }]]
        };
    }

    const option = {
        title: { text: 'SSH — All Stations', left: 'center' },
        tooltip: {
            trigger: 'axis',
            formatter: (params: any) => {
                if (!Array.isArray(params)) return '';
                const timeVal = params[0]?.value?.[0] ?? params[0]?.axisValue;
                const timeStr = new Date(timeVal).toLocaleString();
                let out = `<b>${timeStr}</b><br/>`;
                for (const p of params) {
                    const val = Array.isArray(p.value) ? p.value[1] : p.value;
                    const t = Array.isArray(p.value) ? p.value[0] : timeVal;
                    const status = new Date(t).getTime() >= nowTs ? 'forecast' : 'past';
                    out += `<span style="color:${p.color}">●</span> ${p.seriesName} (${status}): ${Number(val).toFixed(3)}<br/>`;
                }
                return out;
            }
        },
        legend: { type: 'scroll', orient: 'vertical', right: '3%', top: '10%', bottom: '18%', textStyle: { fontSize: 11 } },
        grid: { left: '8%', right: '22%', top: '10%', bottom: '18%' },
        xAxis: { type: 'time' },
        yAxis: { type: 'value', name: 'm' },
        series,
    };

    globalChart.setOption(option);
    globalChart.resize();
    // DEBUG: log series counts and a sample to help catch empty-data regressions
    try {
        const opt = globalChart.getOption();
        console.debug('globalChart series count:', (opt.series || []).length);
        if (opt.series && opt.series.length) {
            console.debug('sample points for first series:', (opt.series[0].data || []).slice(0, 3));
        }
    } catch (e) { console.debug('globalChart debug read failed', e); }

    // Helper: apply highlight by station name (series names start with station name)
    const applyStationHighlight = (stationName: string | null) => {
        if (!globalChart) return;
        const opt: any = globalChart.getOption();
        const existingSeries = (opt && opt.series) || [];
        const mutedOpacity = 0.25;
        const baseWidth = 1.5;
        const activeWidth = 3;

        const updates = existingSeries.map((s: any) => {
            const seriesName = s.name || '';
            const isActive = stationName ? seriesName === stationName : false;
            const prevStyle = s.lineStyle || {};
            return {
                name: seriesName,
                lineStyle: {
                    ...prevStyle,
                    opacity: isActive ? 1 : (stationName ? mutedOpacity : 1),
                    width: isActive ? activeWidth : (prevStyle.width ?? baseWidth)
                }
            };
        });
        // Apply updates in a merged way
        globalChart.setOption({ series: updates });
    };

    const onStationEnter = (e: any) => {
        if (!e || !e.features || !e.features.length) return;
        const props = e.features[0].properties || {};
        const stationName = props.name || props.id || null;
        try { map.getCanvas().style.cursor = 'pointer'; } catch (e) { }
        applyStationHighlight(stationName);
    };

    const onStationLeave = () => {
        try { map.getCanvas().style.cursor = ''; } catch (e) { }
        applyStationHighlight(null);
    };

    const resizeHandler = () => { globalChart?.resize(); };
    map.on('resize', resizeHandler);
    window.addEventListener('resize', resizeHandler);

    // register hover handlers on the circles layer
    try {
        map.on('mouseenter', 'stations-circles', onStationEnter);
        map.on('mouseleave', 'stations-circles', onStationLeave);
    } catch (e) { /* ignore if layer isn't present yet */ }

    // Helper to add a single PNG image overlay given its public path (and sidecar .json bounds)
    async function addPngOverlay(publicPngPath: string, sourceId = 'png-image', layerId = 'png-image-layer') {
        if (!map) throw new Error('map not initialized');
        // First try per-image sidecar JSON (use axios)
        const jsonPath = publicPngPath + '.json';
        let meta: any = null;
        try {
            const r = await axios.get(jsonPath);
            meta = r.data;
        } catch (e) {
            // fall back to a variable-level meta.json located at /png/<var>/meta.json
            const parts = publicPngPath.split('/').filter(Boolean);
            // expect ['png', '<var>', '<filename>']
            if (parts.length >= 2) {
                const varName = parts[1];
                const metaPath = `/png/${varName}/meta.json`;
                try {
                    const r2 = await axios.get(metaPath);
                    meta = r2.data;
                } catch (e2) {
                    // ignore
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
                'raster-opacity': 1.0,
                'raster-color': [
                    'interpolate',
                    ['linear'],
                    ['raster-value'],
                    0, 'rgba(0, 0, 0, 0)',
                    0.5, '#3b528b',
                    0.67, '#21918c',
                    0.87, '#5ec962',
                    1.0, '#fde725'
                ],
                'raster-color-range': [0, 255],
                'raster-color-mix': [1, 0, 0, 0]
            }
        });

        // Optionally zoom to the image bounds for visibility during testing
        try { map.fitBounds([[lonmin, latmin], [lonmax, latmax]], { padding: 40, duration: 800 }); } catch (e) { }
        console.info(`Added PNG overlay ${publicPngPath} as source='${sourceId}', layer='${layerId}'`);
    }

    // Chart -> Map sync: when a series is highlighted (e.g., legend hover), fly to station
    const onChartHighlight = (params: any) => {
        const stationName = params && params.seriesName;
        if (!stationName) return;
        const s = stations.find(st => st.name === stationName);
        if (s && map) {
            try { map.flyTo({ center: s.coord, zoom: 11, essential: true }); } catch (e) { }
        }
    };
    const onChartDownplay = (_params: any) => {
        // No-op for now; could restore previous view if desired
    };
    const onLegendSelectChanged = (params: any) => {
        // On click toggle selection, if selected then fly to that station
        if (!params || !params.name) return;
        const s = stations.find(st => st.name === params.name);
        if (s && map) {
            try { map.flyTo({ center: s.coord, zoom: 11, essential: true }); } catch (e) { }
        }
    };

    try {
        globalChart?.on('highlight', onChartHighlight);
        globalChart?.on('downplay', onChartDownplay);
        globalChart?.on('legendselectchanged', onLegendSelectChanged);
    } catch (e) { /* ignore */ }

    (map as any).__globalChartHandlers = { resizeHandler, onStationEnter, onStationLeave, onChartHighlight, onChartDownplay, onLegendSelectChanged };

    // Add a test PNG overlay (from the public folder) — change path as needed
    try {
        await addPngOverlay('/png/dissolved_inorganic_carbon/dissolved_inorganic_carbon_depth0p5000003_2026-01-06T123000.png');
    } catch (e) {
        console.warn('Failed to add PNG overlay:', e);
    }
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