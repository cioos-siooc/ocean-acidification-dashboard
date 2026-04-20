import type { FeatureCollection, Geometry, GeoJsonProperties } from 'geojson';
import useStationsInteraction from './useStationsInteraction';

const PATH = `M9.68 13c-.08 0-.16 0-.24.01c-.14.03-.27.1-.38.21c-.14.12-.27.24-.42.36c-.73.57-1.78.55-2.48-.07l-.39-.34a.784.784 0 0 0-1 0c-.19.15-.36.31-.55.46c-.7.51-1.67.49-2.35-.06l-.31-.26A.92.92 0 0 0 1 13v-.97c.26-.04.52.01.75.13c.28.17.54.36.79.57a.84.84 0 0 0 1.12.09c.16-.12.29-.26.45-.38c.24-.2.52-.33.8-.39c.15-.07.32-.08.48-.05c.42.02.83.16 1.17.44c.16.12.31.27.47.4c.18.15.43.21.65.17l-.09-.03c.11-.01.21-.03.32-.06c.04-.02.08-.05.12-.08c.16-.13.31-.28.47-.4c.23-.19.48-.31.75-.38c.13-.05.26-.07.4-.06c.46-.02.93.13 1.31.44c.15.12.29.26.44.38c.29.24.71.24 1 0l.47-.4c.32-.26.72-.4 1.13-.39V13c-.26.02-.51.14-.68.34c-.27.28-.62.48-1 .58c-.61.18-1.28.05-1.77-.36l-.39-.34c-.13-.13-.3-.2-.48-.22m2.09-1a.83.83 0 0 1-.42-.18c-.15-.12-.29-.26-.44-.38a1.93 1.93 0 0 0-2.46 0c-.16.12-.31.27-.47.4c-.29.24-.71.24-1 0c-.16-.13-.31-.28-.47-.4c-.71-.58-1.74-.58-2.45 0c-.13.1-.23.2-.35.3a2.002 2.002 0 0 1 1.81-3.46l2.26-4.57l-.48-.13a.5.5 0 0 1-.36-.61c.07-.26.35-.42.61-.35l.49.13l.13-.48C8.38 1.46 9.2.99 10 1.2c.8.22 1.28 1.04 1.06 1.84l-.13.48l.49.13c.26.08.42.35.35.62a.5.5 0 0 1-.61.35l-.48-.13l-.33 5.09A2 2 0 0 1 11.77 12M9.23 4.1l-.49-.13l-2.26 4.57l1.45.39z`;

function makeSvg(fill: string) {
    return `<svg xmlns="http://www.w3.org/2000/svg" width="30" height="30" viewBox="0 0 15 15"><path fill="${fill}" stroke="black" stroke-width="1" stroke-linejoin="round" paint-order="stroke fill" d="${PATH}"/></svg>`;
}

const SOURCE_ID = 'stations';
const LAYER_ID = 'stations-circles';
const LAYER_BADGE_ID = 'stations-badge';
const IMAGE_ACTIVE = 'buoy-active';
const IMAGE_INACTIVE = 'buoy-inactive';

async function loadImage(map: any, id: string, svg: string): Promise<void> {
    if (map.hasImage(id)) return;
    const dataUrl = 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svg);
    return new Promise<void>((resolve) => {
        const img = new Image(30, 30);
        img.onload = () => {
            const canvas = document.createElement('canvas');
            canvas.width = 30;
            canvas.height = 30;
            canvas.getContext('2d')!.drawImage(img, 0, 0);
            map.addImage(id, canvas.getContext('2d')!.getImageData(0, 0, 30, 30));
            resolve();
        };
        img.onerror = () => resolve();
        img.src = dataUrl;
    });
}

export type MultiSensorCandidate = { id: number; name: string; depth: number };

/**
 * Add (or replace) the buoy symbol layer for mooring stations.
 * Returns a detach function that removes event listeners when called.
 */
export async function addBuoyLayer(
    map: any,
    geojson: FeatureCollection<Geometry, GeoJsonProperties>,
    onSensorClick: (sensor_id: number, depth: number) => void,
    onMultiSensorClick: (sensors: MultiSensorCandidate[], screenX: number, screenY: number) => void,
): Promise<() => void> {
    await Promise.all([
        loadImage(map, IMAGE_ACTIVE,   makeSvg('#FFD700')),
        loadImage(map, IMAGE_INACTIVE, makeSvg('#888888')),
    ]);

    // Remove existing layers/source before re-adding
    try { if (map.getLayer(LAYER_BADGE_ID)) map.removeLayer(LAYER_BADGE_ID); } catch (e) { }
    try { if (map.getLayer(LAYER_ID)) map.removeLayer(LAYER_ID); } catch (e) { }
    try { if (map.getSource(SOURCE_ID)) map.removeSource(SOURCE_ID); } catch (e) { }

    map.addSource(SOURCE_ID, { type: 'geojson', data: geojson });
    map.addLayer({
        id: LAYER_ID,
        type: 'symbol',
        source: SOURCE_ID,
        layout: {
            'icon-image': [
                'case',
                ['==', ['get', 'active'], true], IMAGE_ACTIVE,
                IMAGE_INACTIVE,
            ],
            'icon-size': 1,
            'icon-allow-overlap': true,
            'icon-anchor': 'center',
        },
        paint: {
            'icon-opacity': 0.95,
        },
    });

    // Badge layer: number label on top for multi-sensor locations
    map.addLayer({
        id: LAYER_BADGE_ID,
        type: 'symbol',
        source: SOURCE_ID,
        filter: ['>', ['get', 'sensorCount'], 1],
        layout: {
            'text-field': ['to-string', ['get', 'sensorCount']],
            'text-size': 9,
            'text-font': ['DIN Offc Pro Bold', 'Arial Unicode MS Bold'],
            'text-offset': [0.75, -0.75],
            'text-allow-overlap': true,
            'text-ignore-placement': true,
        },
        paint: {
            'text-color': '#ffffff',
            'text-halo-color': '#E64A19',
            'text-halo-width': 4,
        },
    });

    const layerAdapter = {
        on: (event: string, handler: (evt: any) => void) => {
            map.on(event, LAYER_ID, handler);
            return () => { try { map.off(event, LAYER_ID, handler); } catch (e) { } };
        },
    };

    const stations = useStationsInteraction(() => map, onSensorClick, onMultiSensorClick);
    return stations.attach(layerAdapter) ?? (() => {});
}

