import { ref, reactive, computed, onBeforeUnmount } from 'vue';
import { useRuntimeConfig } from '#app';
import moment from 'moment-timezone';
import { useMainStore } from '../app/stores/main';

export function useMapAnimator(getMap: () => any) { // Using any for mapboxgl.Map to avoid strict type issues if not imported, or import it.
    
    const config = useRuntimeConfig();
    const apiBaseUrl = config.public.apiBaseUrl;
    const mainStore = useMainStore();

    const isPlaying = ref(false);
    const animFps = ref(2);
    const bufferSize = 12; // frames to keep decoded
    const startThreshold = 6; // frames needed before starting playback
    const fetchConcurrency = 6;
    const playRequested = ref(false);

    const _anim = reactive({
        urls: [] as string[],
        varId: '',
        depth: null as number | null,
        canv: null as HTMLCanvasElement | null,
        ctx: null as CanvasRenderingContext2D | null,
        bitmaps: new Map<number, ImageBitmap>(),
        loading: new Set<number>(),
        nextToLoad: 0,
        currentIndex: 0,
        lastDrawTime: 0,
        rafId: 0,
        playStartPerf: 0,
        startedAtFrame: 0,
        coords: [] as any,
        total: 0,
        dts: [] as any[],
        loadOrder: [] as number[],
    });

    const bufferProgress = computed(() => {
        if (!_anim.total) return 0;
        const loaded = _anim.bitmaps.size;
        return Math.round((loaded / _anim.total) * 100);
    });

    const currentFrameLabel = computed(() => {
        return _anim.total ? `${_anim.currentIndex + 1}/${_anim.total}` : '';
    });

    function makeFrameUrls(varId: string, depth: number | null) {
        const v = mainStore.variables.find((vv: any) => vv.var === varId);
        if (!v || !v.dts) return [];
        // store datetimes for mapping index -> datetime
        _anim.dts = v.dts.map((dt: any) => dt);
        return v.dts.map((dt: any) => `${apiBaseUrl}/png/${varId}/${dt.format('YYYY-MM-DDTHHmmss')}/${depth}`);
    }

    async function initAnimatorForOverlay(coords: [number, number][], varId: string, depth: number | null) {
        stopAnimator();
        const map = getMap();
        if (!map) return;

        _anim.varId = varId;
        _anim.depth = depth;
        _anim.urls = makeFrameUrls(varId, depth);
        _anim.total = _anim.urls.length;
        _anim.coords = coords;
        _anim.bitmaps.clear();
        _anim.loading.clear();
        _anim.nextToLoad = 0;
        _anim.currentIndex = 0;
        _anim.loadOrder = [] as number[];

        if (_anim.total === 0) return;

        // set start index to currently selected datetime if available
        try {
            const selDt = mainStore.selected_variable.dt;
            let startIdx = 0;
            if (selDt && _anim.dts && _anim.dts.length) {
                // find nearest index
                let best = 0;
                let bestDiff = Infinity;
                const selTs = moment.utc(selDt).valueOf();
                for (let i = 0; i < _anim.dts.length; i++) {
                    const t = moment.utc(_anim.dts[i]).valueOf();
                    const diff = Math.abs(t - selTs);
                    if (diff < bestDiff) {
                        bestDiff = diff; best = i;
                    }
                }
                _anim.currentIndex = best;
                _anim.startedAtFrame = best;
                startIdx = best;
            }
            // build prioritized load order starting from startIdx and wrapping around
            for (let k = 0; k < _anim.total; k++) {
                _anim.loadOrder.push((startIdx + k) % _anim.total);
            }
        } catch (e) {
            // fallback: default load order 0..N-1
            for (let k = 0; k < _anim.total; k++) _anim.loadOrder.push(k);
        }

        // create canvas element and canvas source for map
        const canvasId = 'anim-canvas';
        // remove previous canvas if present
        const existing = document.getElementById(canvasId) as HTMLCanvasElement | null;
        if (existing && existing.parentNode) existing.parentNode.removeChild(existing);

        const canvas = document.createElement('canvas');
        canvas.id = canvasId;
        canvas.style.width = '512px';
        canvas.style.height = '512px';
        canvas.style.display = 'block';
        canvas.style.visibility = 'hidden'; // not shown directly; Mapbox uses it as source
        document.body.appendChild(canvas);
        _anim.canv = canvas;
        _anim.ctx = canvas.getContext('2d');

        // add canvas source and layer (remove old one first if exists)
        try { if (map.getLayer('anim-layer')) map.removeLayer('anim-layer'); } catch (e) { }
        try { if (map.getSource('anim-canvas')) map.removeSource('anim-canvas'); } catch (e) { }

        map.addSource('anim-canvas', { type: 'canvas', canvas: canvasId, coordinates: coords, animate: false });
        map.addLayer({ id: 'anim-layer', type: 'raster', source: 'anim-canvas', paint: { 'raster-opacity': 1.0, 'raster-fade-duration': 0 } });

        // Apply mapbox colorization paint using vmin/vmax/precision/base if available
        try {
            const v = mainStore.variables.find((vv: any) => vv.var === varId);
            const precision = v?.precision ?? 0.1;
            const base = 0;
            const vmin_local = v?.min ?? 0;
            const vmax_local = v?.max ?? 1;
            const stops = [] as any[];
            const color_stops = [
                [0.0, 'rgba(0, 0, 0, 1)'],
                [0.001, '#440154'],
                [0.25, '#00f'],
                [0.5, '#0f0'],
                [0.75, '#fde725'],
                [1.0, '#f00']
            ];
            for (const stop of color_stops) {
                const val_phys = vmin_local + stop[0] * (vmax_local - vmin_local);
                const val_packed = (val_phys - base) / precision;
                stops.push(val_packed, stop[1]);
            }
            map.setPaintProperty('anim-layer', 'raster-color', ['interpolate', ['linear'], ['raster-value'], ...stops]);
            map.setPaintProperty('anim-layer', 'raster-color-range', [(vmin_local - base) / precision, (vmax_local - base) / precision]);
            map.setPaintProperty('anim-layer', 'raster-color-mix', [16711680, 65280, 255, 0]);
        } catch (e) {
            console.warn('Failed to set anim layer paint props', e);
        }
        scheduleLoads();
    }

    function stopAnimator() {
        const map = getMap();
        isPlaying.value = false;
        if (_anim.rafId) {
            cancelAnimationFrame(_anim.rafId);
            _anim.rafId = 0;
        }
        // remove canvas source / layer
        try { if (map && map.getLayer && map.getLayer('anim-layer')) map.removeLayer('anim-layer'); } catch (e) { }
        try { if (map && map.getSource && map.getSource('anim-canvas')) map.removeSource('anim-canvas'); } catch (e) { }
        if (_anim.canv && _anim.canv.parentNode) {
            _anim.canv.parentNode.removeChild(_anim.canv);
        }
        _anim.canv = null;
        _anim.ctx = null;
        _anim.bitmaps.forEach((bm) => { try { (bm as any).close?.(); } catch { } });
        _anim.bitmaps.clear();
        _anim.loading.clear();
        _anim.nextToLoad = 0;
    }

    function scheduleLoads() {
        while (_anim.loading.size < fetchConcurrency && _anim.nextToLoad < _anim.total) {
            // pick next index from loadOrder (prioritized)
            const idx = _anim.loadOrder && _anim.loadOrder.length ? _anim.loadOrder[_anim.nextToLoad++] : _anim.nextToLoad++;
            loadFrame(idx).catch(e => { console.warn('frame load failed', idx, e); });
        }
    }

    async function loadFrame(i: number) {
        if (_anim.bitmaps.has(i) || _anim.loading.has(i)) return;
        _anim.loading.add(i);
        const url = _anim.urls[i];
        if (!url) {
             _anim.loading.delete(i);
            return;
        }

        try {
            const resp = await fetch(url, { cache: 'force-cache' });
            if (!resp.ok) throw new Error(`Failed to fetch ${url}: ${resp.status}`);
            const blob = await resp.blob();
            let bitmap: ImageBitmap | null = null;
            try {
                bitmap = await createImageBitmap(blob);
            } catch (e) {
                // fallback: use Image element draw to canvas
                const img = new Image();
                img.src = URL.createObjectURL(blob);
                await new Promise((res, rej) => { img.onload = res; img.onerror = rej; });
                const c = document.createElement('canvas'); c.width = img.width; c.height = img.height; c.getContext('2d')!.drawImage(img, 0, 0);
                bitmap = await createImageBitmap(c);
                URL.revokeObjectURL(img.src);
            }
            _anim.bitmaps.set(i, bitmap!);
        } catch (e) {
            console.warn('loadFrame error', e);
        } finally {
            _anim.loading.delete(i);
            // If enough frames buffered, start playback
            // Start playback automatically only if the user has requested it (pressed Play)
            if (playRequested.value && _anim.bitmaps.size >= startThreshold) {
                startPlayback();
            }
            // continue scheduling
            scheduleLoads();
        }
    }

    function drawToCanvas(atIndex: number, alpha = 1.0) {
        const map = getMap();
        if (!_anim.ctx || !_anim.canv) return;
        const ctx = _anim.ctx;
        const bm = _anim.bitmaps.get(atIndex);
        if (!bm) {
            console.warn('drawToCanvas: bitmap not found for index', atIndex);
            return;
        }
        // ensure canvas size matches image
        if (_anim.canv.width !== bm.width || _anim.canv.height !== bm.height) {
            _anim.canv.width = bm.width;
            _anim.canv.height = bm.height;
        }
        ctx.clearRect(0, 0, _anim.canv.width, _anim.canv.height);
        // Draw the current frame only (no crossfade)
        ctx.globalAlpha = 1.0;
        ctx.drawImage(bm, 0, 0);
        ctx.globalAlpha = 1.0;
        
        // Hide the static layer if it's still visible (smooth transition)
        try {
            if (map && map.getLayoutProperty('png-image-layer', 'visibility') !== 'none') {
                map.setLayoutProperty('png-image-layer', 'visibility', 'none');
            }
        } catch (e) { }

        // Tell mapbox the canvas source updated; mapbox will re-render automatically
        if (map) map.triggerRepaint();
    }

    function startPlayback() {
        if (isPlaying.value) return;
        if (!_anim.total || _anim.bitmaps.size < 1) {
             console.warn('startPlayback aborted: no frames or total 0');
             return;
        }
        isPlaying.value = true;
        _anim.playStartPerf = performance.now();
        _anim.startedAtFrame = _anim.currentIndex;
        _anim.lastDrawTime = 0;
        _anim.rafId = requestAnimationFrame(playLoop);
    }

    function pausePlayback() {
        isPlaying.value = false;
        if (_anim.rafId) { cancelAnimationFrame(_anim.rafId); _anim.rafId = 0; }
    }

    function playLoop(now: number) {
        if (!isPlaying.value) return;
        try {
            if (!_anim.total) return;
            const elapsed = now - _anim.playStartPerf;
            const frameFloat = (elapsed / 1000) * animFps.value + _anim.startedAtFrame;
            const frameIndex = Math.floor(frameFloat) % _anim.total;
            const frac = frameFloat - Math.floor(frameFloat);

            let drawnIndex = -1;
            // Determine which frame to draw: prefer the target frame if decoded, else pick the nearest decoded frame
            if (_anim.bitmaps.has(frameIndex)) {
                drawnIndex = frameIndex;
            } else {
                // search forward and backward up to bufferSize for nearest loaded frame
                for (let off = 1; off <= Math.min(bufferSize, _anim.total); off++) {
                    const forward = (frameIndex + off) % _anim.total;
                    if (_anim.bitmaps.has(forward)) { drawnIndex = forward; break; }
                    const backward = (frameIndex - off + _anim.total) % _anim.total;
                    if (_anim.bitmaps.has(backward)) { drawnIndex = backward; break; }
                }
                // if none found but we have some bitmaps, pick currentIndex or any available bitmap
                if (drawnIndex === -1 && _anim.bitmaps.size > 0) {
                    if (_anim.currentIndex !== undefined && _anim.bitmaps.has(_anim.currentIndex)) {
                        drawnIndex = _anim.currentIndex;
                    } else {
                        const first = _anim.bitmaps.keys().next().value;
                        if (first !== undefined) drawnIndex = first;
                    }
                }
            }

            if (drawnIndex >= 0) {
                // draw the selected bitmap
                drawToCanvas(drawnIndex, 1.0);
                if (drawnIndex !== _anim.currentIndex) {
                    _anim.currentIndex = drawnIndex;
                    // update selected datetime in store so chart marker follows animation
                    try {
                        const dt = _anim.dts && _anim.dts[drawnIndex];
                        if (dt) {
                            mainStore.setSelectedVariable(_anim.varId, moment.utc(dt), _anim.depth);
                        }
                    } catch (e) {
                        console.warn('Failed to update selected dt from animator', e);
                    }
                }
            }

            // keep prefetching while playing
            scheduleLoads();
        } catch (e) {
            console.error('Animator playLoop error:', e);
            // Stop playback to avoid repeated errors
            pausePlayback();
            stopAnimator();
            return;
        }

        _anim.rafId = requestAnimationFrame(playLoop);
    }

    async function togglePlay() {
        const map = getMap();
        if (!map) return;
        const ov = (map as any).__activePngOverlay;
        if (!ov) return;

        if (isPlaying.value) {
            // Stop playback and restore static overlay
            playRequested.value = false;
            // before destroying animator, set selected dt to current frame so static overlay uses it
            try {
                if (_anim.currentIndex !== undefined && _anim.dts && _anim.dts[_anim.currentIndex]) {
                    mainStore.setSelectedVariable(_anim.varId, moment.utc(_anim.dts[_anim.currentIndex]), _anim.depth);
                }
            } catch (e) { /* ignore */ }
            pausePlayback();
            stopAnimator();
            try { if (map.getLayer('png-image-layer')) map.setLayoutProperty('png-image-layer', 'visibility', 'visible'); } catch (e) { }
        } else {
            // Start: initialize animator if needed
            // We do NOT hide the static layer yet; we wait until the first frame is drawn in drawToCanvas
            playRequested.value = true;
            // Initialize animator if not already initialized for this overlay or if canvas/anim layer is missing
            if (!_anim.canv || !_anim.total || _anim.urls.length === 0 || !map.getSource('anim-canvas')) {
                await initAnimatorForOverlay(ov.coords, ov.varId, ov.depth);
            }
            // Ensure loading starts
            scheduleLoads();
            // start playback immediately (will wait inside playLoop for frames to become available)
            startPlayback();
        }
    }

    onBeforeUnmount(() => {
        stopAnimator();
    });

    return {
        isPlaying,
        playRequested,
        bufferProgress,
        currentFrameLabel,
        togglePlay,
        stopAnimator,
        pausePlayback
    };
}
