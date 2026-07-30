import type { MultiSensorCandidate } from './useBuoyLayer';

export type CircleLayer = {
  on: (layerEvent: string, handler: (evt: any) => void) => (() => void) | undefined;
};

export function useStationsInteraction(
  getMap: () => any,
  layerId: string,
  onFetchTimeseries: (sensor_id: string, depth: number) => void,
  onMultiSensorClick: (sensors: MultiSensorCandidate[], screenX: number, screenY: number) => void,
  onSpiderfyClick: (sensors: MultiSensorCandidate[], screenX: number, screenY: number) => void,
) {
  let _detach: (() => void) | null = null;

  function _isActive(raw: any) {
    return raw === true || raw === 'true' || raw === 't' || raw === '1' || raw === 1;
  }

  function _parseSensors(feature: any): MultiSensorCandidate[] {
    try {
      return JSON.parse(feature.properties?.sensorsJson || '[]');
    } catch {
      return [];
    }
  }

  function attach(circle: CircleLayer) {
    const map = getMap();
    if (!map) throw new Error('Map not available');

    const offClick = circle.on('click', (evt: any) => {
      try {
        const { x, y } = evt.point;
        // Query all buoy features within a 20 px radius of the click
        const bbox: [[number, number], [number, number]] = [[x - 20, y - 20], [x + 20, y + 20]];
        const nearby: any[] = map.queryRenderedFeatures(bbox, { layers: [layerId] });

        const activeFeatures = nearby.filter(f => _isActive(f.properties?.active));
        if (!activeFeatures.length) return;

        // Deduplicate by feature id so a symbol rendered twice doesn't double-count
        const seen = new Set<string>();
        const uniqueFeatures = activeFeatures.filter(f => {
          const key = f.properties?.sensorsJson ?? JSON.stringify(f.geometry);
          if (seen.has(key)) return false;
          seen.add(key);
          return true;
        });

        const allSensors: MultiSensorCandidate[] = uniqueFeatures.flatMap(_parseSensors);
        if (allSensors.length === 0) return;

        if (allSensors.length === 1) {
          onFetchTimeseries(allSensors[0].id, allSensors[0].depth);
          return;
        }

        // Always use spiderfy — consistent for both same-position (depth variants) and nearby sensors
        onSpiderfyClick(allSensors, x, y);
      } catch (e) {
        // swallow
      }
    });

    // hover handlers (cursor)
    const offEnter = circle.on('mouseenter', (evt: any) => {
      try {
        const feature = (evt.features && evt.features[0]) || null;
        if (!feature) return;
        const rawActive = feature.properties?.active;
        if (_isActive(rawActive)) map.getCanvas().style.cursor = 'pointer';
      } catch (e) {}
    });

    const offLeave = circle.on('mouseleave', () => {
      try { map.getCanvas().style.cursor = ''; } catch (e) {}
    });

    _detach = () => {
      try { if (offClick) offClick(); } catch (e) {}
      try { if (offEnter) offEnter(); } catch (e) {}
      try { if (offLeave) offLeave(); } catch (e) {}
      _detach = null;
    };

    return _detach;
  }

  function detach() {
    if (_detach) _detach();
  }

  return { attach, detach };
}

export default useStationsInteraction;
