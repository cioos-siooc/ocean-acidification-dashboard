import type { FeatureCollection, Geometry, GeoJsonProperties } from 'geojson';
import useStationsInteraction from './useStationsInteraction';

// Marker sprites (v2 "Instrument"). Shape says what kind of sensor sits at a
// location — square for fixed depth, diamond for a profiler — and the fill
// says whether its data is live, so colour never carries meaning alone.
// Drawn at 2x and added with pixelRatio 2 so they stay crisp.
const INK = '#d4d9de';
const GROUND = '#13161a';
const LIVE = '#5cc28a';
const MUTED = '#7d868f';
const ACCENT = '#e2a336';

function makeMarkerSvg(shape: 'square' | 'diamond', status: 'live' | 'historic' | 'inactive') {
    const stroke = status === 'inactive' ? MUTED : INK;
    const body = shape === 'square'
        ? `<rect x="7" y="7" width="14" height="14" fill="${GROUND}" stroke="${stroke}" stroke-width="2.4"/>`
        : `<rect x="7" y="7" width="14" height="14" transform="rotate(45 14 14)" fill="${GROUND}" stroke="${stroke}" stroke-width="2.4"/>`;
    const dot = status === 'live' ? `<circle cx="14" cy="14" r="3.2" fill="${LIVE}"/>` : '';
    // A dark outline under everything keeps the marker legible over a bright raster.
    const halo = shape === 'square'
        ? `<rect x="5.5" y="5.5" width="17" height="17" fill="none" stroke="rgba(0,0,0,0.55)" stroke-width="1.5"/>`
        : `<rect x="5.5" y="5.5" width="17" height="17" transform="rotate(45 14 14)" fill="none" stroke="rgba(0,0,0,0.55)" stroke-width="1.5"/>`;
    return `<svg xmlns="http://www.w3.org/2000/svg" width="28" height="28" viewBox="0 0 28 28">${halo}${body}${dot}</svg>`;
}

// Selection: accent crosshair ticks around whichever marker holds the selected sensor.
function makeSelectedSvg() {
    return `<svg xmlns="http://www.w3.org/2000/svg" width="56" height="56" viewBox="0 0 56 56">
        <g stroke="rgba(0,0,0,0.6)" stroke-width="5" stroke-linecap="square">
            <path d="M4 28h10M42 28h10M28 4v10M28 42v10"/>
        </g>
        <g stroke="${ACCENT}" stroke-width="2.6" stroke-linecap="square">
            <path d="M4 28h10M42 28h10M28 4v10M28 42v10"/>
        </g>
        <rect x="16" y="16" width="24" height="24" fill="none" stroke="${ACCENT}" stroke-width="2.4"/>
    </svg>`;
}

export const STATIONS_LAYER_ID = 'stations-circles';
export const SOURCE_ID = 'stations';
const LAYER_BADGE_ID = 'stations-badge';
const LAYER_SELECTED_ID = 'stations-selected';

// Every layer the buoy overlay owns — the Overlay rail's "Sensor Layer" toggle
// hides them as one group.
export const BUOY_LAYER_IDS = [STATIONS_LAYER_ID, LAYER_BADGE_ID, LAYER_SELECTED_ID];

export function setBuoyLayerVisibility(map: any, visible: boolean) {
    if (!map) return;
    const visibility = visible ? 'visible' : 'none';
    for (const id of BUOY_LAYER_IDS) {
        try { if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', visibility); } catch (e) { }
    }
}
const IMG = {
    fixedLive: 'buoy-fixed-live',
    fixedHistoric: 'buoy-fixed-historic',
    fixedInactive: 'buoy-fixed-inactive',
    profilerLive: 'buoy-profiler-live',
    profilerHistoric: 'buoy-profiler-historic',
    profilerInactive: 'buoy-profiler-inactive',
    selected: 'buoy-selected',
} as const;

async function loadImage(map: any, id: string, svg: string, size: number): Promise<void> {
    if (map.hasImage(id)) map.removeImage(id);
    const dataUrl = 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svg);
    return new Promise<void>((resolve) => {
        const img = new Image(size, size);
        img.onload = () => {
            const canvas = document.createElement('canvas');
            canvas.width = size;
            canvas.height = size;
            canvas.getContext('2d')!.drawImage(img, 0, 0);
            map.addImage(id, canvas.getContext('2d')!.getImageData(0, 0, size, size), { pixelRatio: 2 });
            resolve();
        };
        img.onerror = () => resolve();
        img.src = dataUrl;
    });
}

/** Ring the marker holding `sensorId` (or clear the ring with null). */
export function setSelectedBuoy(map: any, sensorId: string | null) {
    if (!map?.getLayer(LAYER_SELECTED_ID)) return;
    map.setFilter(LAYER_SELECTED_ID, sensorId
        ? ['in', `|${sensorId}|`, ['get', 'sensorIds']]
        : ['==', ['get', 'sensorIds'], '__none__']);
}

export type MultiSensorCandidate = {
    id: string;
    name: string;
    depth: number;
    depth_min?: number | null;
    depth_max?: number | null;
    isRealtime: boolean;
    lat: number;
    lon: number;
};

/**
 * Add (or replace) the buoy symbol layer for mooring stations.
 * Returns a detach function that removes event listeners when called.
 */
export async function addBuoyLayer(
    map: any,
    geojson: FeatureCollection<Geometry, GeoJsonProperties>,
    onSensorClick: (sensor_id: string, depth: number) => void,
    onMultiSensorClick: (sensors: MultiSensorCandidate[], screenX: number, screenY: number) => void,
    onSpiderfyClick: (sensors: MultiSensorCandidate[], screenX: number, screenY: number) => void,
): Promise<() => void> {
    await Promise.all([
        loadImage(map, IMG.fixedLive, makeMarkerSvg('square', 'live'), 28),
        loadImage(map, IMG.fixedHistoric, makeMarkerSvg('square', 'historic'), 28),
        loadImage(map, IMG.fixedInactive, makeMarkerSvg('square', 'inactive'), 28),
        loadImage(map, IMG.profilerLive, makeMarkerSvg('diamond', 'live'), 28),
        loadImage(map, IMG.profilerHistoric, makeMarkerSvg('diamond', 'historic'), 28),
        loadImage(map, IMG.profilerInactive, makeMarkerSvg('diamond', 'inactive'), 28),
        loadImage(map, IMG.selected, makeSelectedSvg(), 56),
    ]);

    // Remove existing layers/source before re-adding
    try { if (map.getLayer(LAYER_SELECTED_ID)) map.removeLayer(LAYER_SELECTED_ID); } catch (e) { }
    try { if (map.getLayer(LAYER_BADGE_ID)) map.removeLayer(LAYER_BADGE_ID); } catch (e) { }
    try { if (map.getLayer(STATIONS_LAYER_ID)) map.removeLayer(STATIONS_LAYER_ID); } catch (e) { }
    try { if (map.getSource(SOURCE_ID)) map.removeSource(SOURCE_ID); } catch (e) { }

    map.addSource(SOURCE_ID, { type: 'geojson', data: geojson });
    map.addLayer({
        id: STATIONS_LAYER_ID,
        type: 'symbol',
        source: SOURCE_ID,
        layout: {
            // A location with any profiler draws as a diamond; live wins over
            // historic when a location mixes the two.
            'icon-image': [
                'case',
                ['!', ['get', 'active']],
                ['case', ['get', 'isVariableDepth'], IMG.profilerInactive, IMG.fixedInactive],
                ['get', 'isRealtime'],
                ['case', ['get', 'isVariableDepth'], IMG.profilerLive, IMG.fixedLive],
                ['case', ['get', 'isVariableDepth'], IMG.profilerHistoric, IMG.fixedHistoric],
            ],
            'icon-size': 1,
            'icon-allow-overlap': true,
            'icon-anchor': 'center',
        },
    });

    // Badge layer: count on top-right for multi-sensor locations
    map.addLayer({
        id: LAYER_BADGE_ID,
        type: 'symbol',
        source: SOURCE_ID,
        filter: ['>', ['get', 'sensorCount'], 1],
        layout: {
            'text-field': ['to-string', ['get', 'sensorCount']],
            'text-size': 10,
            'text-font': ['DIN Offc Pro Medium', 'Arial Unicode MS Regular'],
            'text-offset': [1.1, -1.0],
            'text-allow-overlap': true,
            'text-ignore-placement': true,
        },
        paint: {
            'text-color': '#d4d9de',
            'text-halo-color': '#0d0f11',
            'text-halo-width': 2,
        },
    });

    // Selection ring, drawn under the marker it surrounds. Starts empty;
    // setSelectedBuoy() points its filter at the selected sensor.
    map.addLayer({
        id: LAYER_SELECTED_ID,
        type: 'symbol',
        source: SOURCE_ID,
        filter: ['==', ['get', 'sensorIds'], '__none__'],
        layout: {
            'icon-image': IMG.selected,
            'icon-size': 1,
            'icon-allow-overlap': true,
            'icon-ignore-placement': true,
        },
    }, STATIONS_LAYER_ID);

    const layerAdapter = {
        on: (event: string, handler: (evt: any) => void) => {
            map.on(event, STATIONS_LAYER_ID, handler);
            return () => { try { map.off(event, STATIONS_LAYER_ID, handler); } catch (e) { } };
        },
    };

    const stations = useStationsInteraction(() => map, STATIONS_LAYER_ID, onSensorClick, onMultiSensorClick, onSpiderfyClick);
    return stations.attach(layerAdapter) ?? (() => {});
}
