import type { Ref } from 'vue';
import type { MultiSensorCandidate } from './useBuoyLayer';

export type CircleLayer = {
  on: (layerEvent: string, handler: (evt: any) => void) => (() => void) | undefined;
};

export function useStationsInteraction(
  getMap: () => any,
  onFetchTimeseries: (sensor_id: number, depth: number) => void,
  onMultiSensorClick: (sensors: MultiSensorCandidate[], screenX: number, screenY: number) => void,
) {
  let _detach: (() => void) | null = null;

  function _isActive(raw: any) {
    return raw === true || raw === 'true' || raw === 't' || raw === '1' || raw === 1;
  }

  function attach(circle: CircleLayer) {
    const map = getMap();
    if (!map) throw new Error('Map not available');

    // click handler
    const offClick = circle.on('click', (evt: any) => {
      try {
        const feature = (evt.features && evt.features[0]) || null;
        if (!feature) return;

        const rawActive = feature.properties?.active;
        if (!_isActive(rawActive)) return;

        // Multi-sensor location: parse embedded sensors list
        const sensorsJson = feature.properties?.sensorsJson;
        if (sensorsJson) {
          const sensors: MultiSensorCandidate[] = JSON.parse(sensorsJson);
          if (sensors.length > 1) {
            const point = map.project(evt.lngLat);
            onMultiSensorClick(sensors, point.x, point.y);
            return;
          }
          // Single sensor embedded in sensorsJson
          onFetchTimeseries(sensors[0].id, sensors[0].depth);
          return;
        }

        // Fallback: flat properties (legacy)
        const sensor_id = feature?.properties?.id;
        const depth = feature?.properties?.depth;
        if (sensor_id !== undefined) onFetchTimeseries(sensor_id, depth);
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
