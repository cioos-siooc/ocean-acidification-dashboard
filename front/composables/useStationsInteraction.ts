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

        // Resolve to a SINGLE station: the query box is only a fat-finger tolerance,
        // so picking the feature whose icon is closest to the click point keeps a click
        // on one buoy from pulling in an adjacent buoy's sensors (which produced a
        // confusing spiderfy mixing two physically distinct stations).
        const nearest = uniqueFeatures.reduce((best, f) => {
          const coords = f.geometry?.coordinates;
          if (!Array.isArray(coords)) return best;
          const p = map.project(coords as [number, number]);
          const d = (p.x - x) ** 2 + (p.y - y) ** 2;
          return best && best.d <= d ? best : { f, d };
        }, null as { f: any; d: number } | null);
        if (!nearest) return;

        const sensors: MultiSensorCandidate[] = _parseSensors(nearest.f);
        if (sensors.length === 0) return;

        if (sensors.length === 1) {
          onFetchTimeseries(sensors[0].id, sensors[0].depth);
          return;
        }

        // Spiderfy this station's own sensors (same-position depth variants and its
        // sub-100 m grouped members) — consistently, whatever the count.
        onSpiderfyClick(sensors, x, y);
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
