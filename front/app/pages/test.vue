<template>
    <v-main>
        <div ref="mapContainer" class="flex-grow-1" style="position: relative; height: 100%; width:100%">
        </div>
    </v-main>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue';
import mapboxgl from 'mapbox-gl';


let map: mapboxgl.Map;
const mapContainer = ref<HTMLDivElement | null>(null);
const bounds = [[-126.4, 46.85], [-121.3, 51.1]] as [[number, number], [number, number]];
const config = useRuntimeConfig();
const apiBaseUrl = config.public.apiBaseUrl;

onMounted(async () => {
    mapboxgl.accessToken = config.public.mapboxToken;
    if (!mapContainer.value) return;

    map = new mapboxgl.Map({
        container: mapContainer.value,
        // style: 'mapbox://styles/taimazb/cmk1jwu8o005101sv1j41cj6j?optimize=true&fresh=true',
        // style: 'mapbox://styles/taimazb/cmkcsejwe005m01ssgtdz3tgd?optimize=true&fresh=true',
        style: 'mapbox://styles/taimazb/cmkcsejwe005m01ssgtdz3tgd?optimize=true&fresh=true',
        // center: [-123.2, 48.8],
        // bounds,
        zoom: 1,
        // pitch: 45,
        minZoom: 0,
        maxZoom: 14,
        antialias: true,
        preserveDrawingBuffer: true, // needed for exporting canvas
        projection: 'mercator',
    });
    console.log(map);

    map.on('load', () => {
        addBathymetryTilesLayer();
    })
});


function addBathymetryTilesLayer(sourceId = 'bathymetry-tiles', layerId = 'bathymetry-tiles-layer') {
    if (!map) return;

    try {
        // Remove existing layer and source if they exist
        if (map.getLayer && map.getLayer(layerId)) map.removeLayer(layerId);
        if (map.getSource && map.getSource(sourceId)) map.removeSource(sourceId);

        // Add raster tile source for bathymetry
        map.addSource(sourceId, {
            type: 'raster',
            // tiles: [`/0.webp`],
            tiles: ["/5639.webp"],
            tileSize: 512,
        });

        const base = -3000;
        const precision = 1;

        // G+B decode with WIDE range (same as diagnostic that worked)
        // Values land at 2922..2999. Wide range [0,4000] avoids narrow-range issues.
        map.addLayer({
            id: layerId,
            type: 'raster',
            source: sourceId,
            paint: {
                'raster-color-mix': [0, 65280, 255, 0],
                'raster-color': [
                    'interpolate',
                    ['linear'],
                    ['raster-value'],
                    2950, '#440154',
                    2960, '#3b528b',
                    2970, '#21908d',
                    2980, '#5dc863',
                    2990, '#fde725',
                    3000, '#f00'
                ],
                'raster-color-range': [0, 4000],
                'raster-resampling': 'nearest',
            }
        });

        // Store metadata for cleanup
        (map as any).__activeBathymetryLayer = {
            sourceId,
            layerId,
        };
    } catch (e) {
        console.error('Error adding bathymetry tiles layer:', e);
    }
}
</script>
