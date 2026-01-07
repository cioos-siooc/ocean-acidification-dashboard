<template>
    <div ref="mapContainer" class="w-100 h-screen"></div>
</template>

<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount } from 'vue';
import { useRuntimeConfig } from '#app';
import mapboxgl from 'mapbox-gl';
import 'mapbox-gl/dist/mapbox-gl.css';
import * as echarts from 'echarts'

const config = useRuntimeConfig();
const mapContainer = ref<HTMLDivElement | null>(null);
let map: mapboxgl.Map | null = null;

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
        
        if (handlers) {
            map.off('move', handlers.updateAll);
            map.off('zoom', handlers.updateAll);
            map.off('rotate', handlers.updateAll);
            map.off('pitch', handlers.updateAll);
            map.off('resize', handlers.resizeHandler);
            window.removeEventListener('resize', handlers.updateAll);
        }
        
        if (refs) {
            for (const r of refs) {
                try { r.chart.dispose(); } catch { }
                try { r.chartMarker.remove(); } catch { }
                try { r.dotMarker.remove(); } catch { }
                try { if (r.line && r.line.parentNode) r.line.parentNode.removeChild(r.line); } catch { }
            }
        }
        
        if (svg && svg.parentNode) svg.parentNode.removeChild(svg);
        map.remove();
    }
});


///////////////////////////////////  MEDTHODS  ///////////////////////////////////
async function addStations() {
    if (!map) return;
    
    // 0. Cleanup existing refs if any
    const existingRefs = (map as any).__anchoredCharts;
    if (existingRefs) {
        for (const r of existingRefs) {
            try { r.chart.dispose(); } catch {}
            try { r.chartMarker.remove(); } catch {}
            try { r.dotMarker.remove(); } catch {}
            try { if (r.line && r.line.parentNode) r.line.parentNode.removeChild(r.line); } catch {}
        }
    }
    (map as any).__anchoredCharts = [];

    // create a single SVG overlay for all lines if it doesn't exist
    const svgNS = 'http://www.w3.org/2000/svg';
    let svg = (map as any).__anchoredChartsSvg as SVGElement | null;
    if (!svg) {
        svg = document.createElementNS(svgNS, 'svg');
        svg.style.position = 'absolute';
        svg.style.top = '0';
        svg.style.left = '0';
        svg.style.width = '100%';
        svg.style.height = '100%';
        svg.style.pointerEvents = 'none';
        svg.style.zIndex = '5'; 
        map.getContainer().appendChild(svg);
        (map as any).__anchoredChartsSvg = svg;
    }

    const refs: Array<any> = (map as any).__anchoredCharts;

    // Load all data in parallel
    const loadingPromises = (stations || []).map(async (s) => {
        try {
            const data = await loadData(s.dataUrl);
            return { s, data };
        } catch (e) {
            console.error(`Failed to load data for ${s.name}:`, e);
            return null;
        }
    });

    const loadedStations = (await Promise.all(loadingPromises)).filter(st => st !== null);

    for (const { s, data } of loadedStations) {
        console.log(s, data);
        
        const coord = s.coord as [number, number];

        // 1. Create dot marker
        const dot = document.createElement('div');
        dot.style.width = '10px';
        dot.style.height = '10px';
        dot.style.background = s.color ?? '#ff0';
        dot.style.border = '1.5px solid white';
        dot.style.borderRadius = '50%';
        dot.style.boxShadow = '0 0 4px rgba(0,0,0,0.5)';
        const dotMarker = new mapboxgl.Marker({ element: dot, anchor: 'center' })
            .setLngLat(coord)
            .addTo(map);

        // 2. Create chart container DOM
        const chartEl = document.createElement('div');
        chartEl.style.width = '320px';
        chartEl.style.height = '220px';
        chartEl.style.background = 'rgba(255,255,255,0.95)';
        chartEl.style.borderRadius = '8px';
        chartEl.style.boxShadow = '0 6px 16px rgba(0,0,0,0.25)';
        chartEl.style.pointerEvents = 'auto';
        chartEl.style.border = `2px solid ${s.color ?? '#ff0'}`;

        const chartOffset: [number, number] = s.offset ?? [-160, -110];
        const chartMarker = new mapboxgl.Marker({ element: chartEl, offset: chartOffset, anchor: 'center' })
            .setLngLat(coord)
            .addTo(map);

        // 3. Initialize ECharts AFTER adding to DOM
        const chart = echarts.init(chartEl);
        chart.setOption({
            animation: false,
            title: { text: `SSH: ${s.name}`, left: 'center', top: '5px', textStyle: { fontSize: 13, fontWeight: 'bold' } },
            grid: { left: '35px', right: '15px', top: '40px', bottom: '25px' },
            tooltip: { trigger: 'axis', backgroundColor: 'rgba(255,255,255,0.9)' },
            xAxis: { type: 'time', axisLabel: { fontSize: 9 } },
            yAxis: { type: 'value', axisLabel: { fontSize: 9 }, scale: true, splitLine: { lineStyle: { type: 'dashed' } } },
            series: [
                { name: 'Hindcast', type: 'line', data: data.hindcast, showSymbol: false, lineStyle: { width: 2, color: '#2196F3' } },
                { name: 'Forecast', type: 'line', data: data.forecast, showSymbol: false, lineStyle: { width: 2, color: '#f44336' } },
            ],
        });

        // 4. Create line
        const line = document.createElementNS(svgNS, 'line');
        line.setAttribute('stroke', s.color ?? '#ff0');
        line.setAttribute('stroke-width', '1.5');
        line.setAttribute('stroke-dasharray', '4 2');
        svg.appendChild(line);

        refs.push({ id: s.id, coord, chart, chartMarker, dotMarker, line, chartEl });
    }

    const updateAll = () => {
        if (!map) return;
        const mapContainer = map.getContainer();
        const mapRect = mapContainer.getBoundingClientRect();
        
        for (const r of refs) {
            const dotRect = r.dotMarker.getElement().getBoundingClientRect();
            const chartRect = r.chartEl.getBoundingClientRect();
            
            // Dot center relative to map viewport
            const x1 = (dotRect.left + dotRect.width / 2) - mapRect.left;
            const y1 = (dotRect.top + dotRect.height / 2) - mapRect.top;
            
            // Chart corners relative to map viewport
            const tl = { x: chartRect.left - mapRect.left, y: chartRect.top - mapRect.top };
            const tr = { x: tl.x + chartRect.width, y: tl.y };
            const bl = { x: tl.x, y: tl.y + chartRect.height };
            const br = { x: tl.x + chartRect.width, y: tl.y + chartRect.height };

            const corners = [tl, tr, bl, br];
            let minDist = Infinity;
            let closest = tl;
            
            for (const c of corners) {
                const dx = c.x - x1;
                const dy = c.y - y1;
                const d2 = dx * dx + dy * dy;
                if (d2 < minDist) {
                    minDist = d2;
                    closest = c;
                }
            }

            r.line.setAttribute('x1', String(x1));
            r.line.setAttribute('y1', String(y1));
            r.line.setAttribute('x2', String(closest.x));
            r.line.setAttribute('y2', String(closest.y));
            
            // Culling
            const margin = 20;
            const isOutside = (dotRect.right < mapRect.left - margin || dotRect.left > mapRect.right + margin ||
                               dotRect.bottom < mapRect.top - margin || dotRect.top > mapRect.bottom + margin);
            r.line.style.visibility = isOutside ? 'hidden' : 'visible';
            r.chartEl.style.visibility = isOutside ? 'hidden' : 'visible';
            r.dotMarker.getElement().style.visibility = isOutside ? 'hidden' : 'visible';
        }
    };

    const resizeHandler = () => { 
        for (const r of refs) r.chart.resize(); 
        updateAll(); 
    };

    // Ensure initial sync
    requestAnimationFrame(() => {
        resizeHandler();
        updateAll();
    });

    // Subscriptions
    map.on('move', updateAll);
    map.on('zoom', updateAll);
    map.on('rotate', updateAll);
    map.on('pitch', updateAll);
    map.on('resize', resizeHandler);
    window.addEventListener('resize', updateAll);

    (map as any).__anchoredChartsHandlers = { updateAll, resizeHandler };
}


async function loadData(url: string): Promise<{ hindcast: [Date, number][], forecast: [Date, number][] }> {
    const data = await $fetch<any[]>(url);
    const hindcast: [Date, number][] = [];
    const forecast: [Date, number][] = [];
    const now = Date.now();
    
    for (const item of data) {
        const time = new Date(item.time);
        if (time.getTime() >= now)
            forecast.push([time, item.ssh]);
        else
            hindcast.push([time, item.ssh]);
    }
    return { hindcast, forecast };
}
</script>

<style scoped>
.h-screen {
    height: 100vh;
}
</style>