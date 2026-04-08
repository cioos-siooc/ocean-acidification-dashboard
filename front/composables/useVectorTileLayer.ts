import { ref, onBeforeUnmount } from 'vue';

export function useVectorTileLayer(getMap: () => any) {
    const added = ref(false);
    const current = {
        sourceId: '' as string,
        layerId: '' as string,
    };

    const listeners: Array<{ event: string; layer: string; handler: any }> = [];

    function ensureMap() {
        const map = getMap();
        if (!map) throw new Error('Map not available');
        return map;
    }

    function addVectorTileLayer(opts: {
        sourceId?: string;
        layerId?: string;
        tileUrl: string; // e.g., 'mapbox://tileset-id' or 'pmtiles://tiles.pmtiles'
        sourceType?: 'vector' | 'raster'; // default: 'vector'
        layerType?: string; // 'fill', 'line', 'symbol', 'circle', etc. (required if sourceType='vector')
        minzoom?: number;
        maxzoom?: number;
        paint?: Record<string, any>; // Mapbox paint properties
        layout?: Record<string, any>; // Mapbox layout properties
        beforeLayerId?: string; // Insert before this layer ID
    }) {
        const map = ensureMap();
        const sourceId = opts.sourceId ?? 'vector-tiles-source';
        const layerId = opts.layerId ?? 'vector-tiles-layer';
        const sourceType = opts.sourceType ?? 'vector';
        const layerType = opts.layerType ?? 'fill';

        // Remove existing layer and source
        try { if (map.getLayer(layerId)) map.removeLayer(layerId); } catch (e) { }
        try { if (map.getSource(sourceId)) map.removeSource(sourceId); } catch (e) { }

        // Add vector tile source
        const sourceConfig: any = {
            type: sourceType,
            tiles: [opts.tileUrl],
        };

        // Add attribution if needed
        // sourceConfig.attribution = opts.attribution ?? '';

        map.addSource(sourceId, sourceConfig);

        // Build layer config
        const layer: any = {
            id: layerId,
            type: layerType,
            source: sourceId,
        };

        // Add source-layer if it's a vector tile (comes from vector tilesets with multiple layers)
        // You may need to adjust this based on your tileset structure
        // if (sourceType === 'vector' && opts.sourceLayer) {
        //   layer['source-layer'] = opts.sourceLayer;
        // }

        if (opts.minzoom !== undefined) layer.minzoom = opts.minzoom;
        if (opts.maxzoom !== undefined) layer.maxzoom = opts.maxzoom;

        // Apply paint and layout properties
        if (opts.paint) layer.paint = opts.paint;
        if (opts.layout) layer.layout = opts.layout;

        // Default paint properties if none provided
        if (!opts.paint) {
            layer.paint = {
                'fill-color': '#088',
                'fill-opacity': 0.8,
            };
        }

        // Add layer
        if (opts.beforeLayerId) {
            map.addLayer(layer, opts.beforeLayerId);
        } else {
            map.addLayer(layer);
        }

        added.value = true;
        current.sourceId = sourceId;
        current.layerId = layerId;

        return { sourceId, layerId };
    }

    function updateTileUrl(newUrl: string) {
        const map = getMap();
        if (!map) return;
        const sid = current.sourceId || 'vector-tiles-source';
        try {
            const source = map.getSource(sid) as any;
            if (source && source.setTiles) {
                source.setTiles([newUrl]);
            }
        } catch (e) {
            console.warn('Failed to update tile URL:', e);
        }
    }

    function setVisibility(visible: boolean) {
        const map = getMap();
        const lid = current.layerId;
        if (!lid || !map) return;
        try {
            map.setLayoutProperty(lid, 'visibility', visible ? 'visible' : 'none');
        } catch (e) {
            console.warn('Failed to set visibility:', e);
        }
    }

    function setStyle(paintProps: Record<string, any>) {
        const map = getMap();
        const lid = current.layerId;
        if (!lid || !map) return;
        try {
            Object.entries(paintProps).forEach(([key, value]) => {
                map.setPaintProperty(lid, key, value);
            });
        } catch (e) {
            console.warn('Failed to set paint properties:', e);
        }
    }

    function on(layerEvent: string, handler: (evt: any) => void) {
        const map = getMap();
        const lid = current.layerId;
        if (!lid || !map) return;
        map.on(layerEvent, lid, handler);
        listeners.push({ event: layerEvent, layer: lid, handler });
        return () => {
            try {
                map.off(layerEvent, lid, handler);
            } catch (e) { }
        };
    }

    function clearListeners() {
        const map = getMap();
        if (!map) return;
        listeners.forEach(({ event, layer, handler }) => {
            try {
                map.off(event, layer, handler);
            } catch (e) { }
        });
        listeners.length = 0;
    }

    function removeLayer() {
        const map = getMap();
        if (!map) return;
        try {
            if (map.getLayer(current.layerId)) map.removeLayer(current.layerId);
        } catch (e) { }
        try {
            if (map.getSource(current.sourceId)) map.removeSource(current.sourceId);
        } catch (e) { }
        clearListeners();
        added.value = false;
    }

    onBeforeUnmount(() => {
        removeLayer();
    });

    return {
        added,
        addVectorTileLayer,
        updateTileUrl,
        setVisibility,
        setStyle,
        on,
        removeLayer,
    };
}
