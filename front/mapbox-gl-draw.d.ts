// @mapbox/mapbox-gl-draw ships no type declarations and no @types package is
// installed — this is the minimal surface the app actually calls.
declare module '@mapbox/mapbox-gl-draw' {
  import type { IControl, Map as MapboxMap } from 'mapbox-gl'

  export default class MapboxDraw implements IControl {
    constructor(options?: Record<string, any>)
    onAdd(map: MapboxMap): HTMLElement
    onRemove(map: MapboxMap): void
    changeMode(mode: string, options?: Record<string, any>): void
    deleteAll(): this
    getAll(): GeoJSON.FeatureCollection
    getMode(): string
  }
}
