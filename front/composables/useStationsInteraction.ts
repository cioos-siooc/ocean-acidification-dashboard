import type { Ref } from 'vue';

export type CircleLayer = {
  on: (layerEvent: string, handler: (evt: any) => void) => (() => void) | undefined;
};

export function useStationsInteraction(getMap: () => any, onFetchTimeseries: (sensor_id: number) => void) {
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
        const sensor_id = feature?.properties?.id;
        const lng = evt.lngLat?.lng ?? feature?.geometry?.coordinates?.[0];
        const lat = evt.lngLat?.lat ?? feature?.geometry?.coordinates?.[1];
        if (!feature || !sensor_id || lng === undefined || lat === undefined) return;
        const rawActive = feature.properties?.active;
        if (!_isActive(rawActive)) return; // ignore inactive stations

        // Provide sensor id + coords to caller
        onFetchTimeseries(sensor_id);
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
