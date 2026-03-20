import { ref, watch } from 'vue'
import mapboxgl from 'mapbox-gl'

export function useRasterLayer(getMap: () => mapboxgl.Map | null) {
  const layerId = 'raster-tiles'
  const sourceId = 'raster-source'
  const isVisible = ref(true)

  const addRasterLayer = (sourceUrl: string) => {
    const map = getMap()
    if (!map) return

    if (!map.getSource(sourceId)) {
      map.addSource(sourceId, {
        type: 'raster',
        tiles: [sourceUrl],
        tileSize: 512,
      })
    }

    if (!map.getLayer(layerId)) {
      map.addLayer({
        id: layerId,
        type: 'raster',
        source: sourceId,
        paint: { 'raster-opacity': 1 }
      })
    }
  }

  const setVisibility = (visible: boolean) => {
    const map = getMap()
    if (!map?.getLayer(layerId)) return
    map.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none')
    isVisible.value = visible
  }

  const removeRasterLayer = () => {
    const map = getMap()
    if (!map) return
    if (map.getLayer(layerId)) map.removeLayer(layerId)
    if (map.getSource(sourceId)) map.removeSource(sourceId)
  }

  return { addRasterLayer, setVisibility, removeRasterLayer, isVisible }
}