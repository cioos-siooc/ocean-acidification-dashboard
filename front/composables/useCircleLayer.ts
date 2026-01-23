import { ref, onBeforeUnmount } from 'vue';

export function useCircleLayer(getMap: () => any) {
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

  function addCircleLayer(opts: {
    sourceId?: string;
    layerId?: string;
    data?: GeoJSON.GeoJSON | null;
    radius?: number | any; // number or Mapbox expression
    color?: string | any; // color string or expression
    stroke?: string | any;
    strokeWidth?: number | any;
    opacity?: number | any;
    minzoom?: number;
    maxzoom?: number;
  }) {
    const map = ensureMap();
    const sourceId = opts.sourceId ?? 'circles-source';
    const layerId = opts.layerId ?? 'circles-circles';

    // remove existing
    try { if (map.getLayer(layerId)) map.removeLayer(layerId); } catch (e) { }
    try { if (map.getSource(sourceId)) map.removeSource(sourceId); } catch (e) { }

    map.addSource(sourceId, { type: 'geojson', data: opts.data ?? { type: 'FeatureCollection', features: [] } });

    const paint: any = {
      'circle-radius': opts.radius ?? 6,
      'circle-color': opts.color ?? ['get', 'color'],
      'circle-stroke-color': opts.stroke ?? '#0028ff ',
      'circle-stroke-width': opts.strokeWidth ?? 3,
      'circle-opacity': opts.opacity ?? 0.95,
    };

    const layer: any = {
      id: layerId,
      type: 'circle',
      source: sourceId,
      paint,
    };
    if (opts.minzoom !== undefined) layer.minzoom = opts.minzoom;
    if (opts.maxzoom !== undefined) layer.maxzoom = opts.maxzoom;

    map.addLayer(layer);

    added.value = true;
    current.sourceId = sourceId;
    current.layerId = layerId;
    return { sourceId, layerId };
  }

  function updateData(data: GeoJSON.GeoJSON) {
    const map = getMap();
    if (!map) return;
    const src = current.sourceId || 'circles-source';
    try {
      const s = map.getSource(src);
      if (s && typeof (s as any).setData === 'function') {
        (s as any).setData(data);
      }
    } catch (e) {
      // ignore
    }
  }

  function setVisibility(visible: boolean) {
    const map = getMap();
    const lid = current.layerId;
    if (!lid || !map) return;
    try {
      map.setLayoutProperty(lid, 'visibility', visible ? 'visible' : 'none');
    } catch (e) { }
  }

  function on(layerEvent: string, handler: (evt: any) => void) {
    const map = getMap();
    const lid = current.layerId;
    if (!lid || !map) return;
    map.on(layerEvent, lid, handler);
    listeners.push({ event: layerEvent, layer: lid, handler });
    return () => {
      try { map.off(layerEvent, lid, handler); } catch (e) { }
    };
  }

  function clearListeners() {
    const map = getMap();
    for (const l of listeners) {
      try { map.off(l.event, l.layer, l.handler); } catch (e) { }
    }
    listeners.splice(0, listeners.length);
  }

  function remove() {
    const map = getMap();
    clearListeners();
    try { if (current.layerId && map.getLayer(current.layerId)) map.removeLayer(current.layerId); } catch (e) { }
    try { if (current.sourceId && map.getSource(current.sourceId)) map.removeSource(current.sourceId); } catch (e) { }
    added.value = false;
    current.layerId = '';
    current.sourceId = '';
  }

  onBeforeUnmount(() => {
    try { remove(); } catch (e) { }
  });

  return {
    addCircleLayer,
    updateData,
    setVisibility,
    on,
    remove,
    added,
    current,
  };
}

export default useCircleLayer;