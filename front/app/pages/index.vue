<template>
  <main class="grow min-h-0">
    <BetaDisclaimerDialog />
    <howTo v-model="showHow" />
    <!-- <div class="flex flex-col h-screen overflow-hidden"> -->
    <!-- Top: Map -->
    <div
      ref="mapContainer"
      class="grow"
      :style="{ position: 'relative', height: `calc(100% - ${footerHeight})` }"
    >
      <!-- <Layers @toggleLayer="onToggleLayer" /> -->

      <Overlays
        @show-how="showHow = true"
        @autorange="autorange"
        class="overlay"
        :style="{
          top: `${overlayGap}px`,
          left:
            (mainStore.isControlPanelOpen
              ? mainStore.controlPanel_width + overlayGap
              : overlayGap) + 'px',
        }"
      />

      <ColorbarSettings
        v-if="showColorbarSettings"
        class="overlay"
        :style="{
          left:
            (mainStore.isControlPanelOpen
              ? mainStore.controlPanel_width + overlayGap + 50
              : overlayGap + 50) + 'px',
          transition: 'left 0.3s ease',
        }"
      />

      <selectedInfo
        class="overlay"
        :style="{
          bottom: `${overlayGap}px`,
          left:
            (mainStore.isControlPanelOpen
              ? mainStore.controlPanel_width + overlayGap
              : overlayGap) + 'px',
        }"
      />

      <controlPanel />

      <!-- <div class="map-drawer-toggle" :style="{ right: drawerOpen ? '312px' : '12px' }">
                <UButton color="warning" class="size-[24px] p-0 justify-center m-0 p-0" @click="drawerOpen = !drawerOpen" title="Vertical Profile">
                    <UIcon name="i-mdi-chart-line" class="size-[20px]" />
                </UButton>
            </div> -->

      <SelectedVariableDrawer
        v-model="drawerOpen"
        :selected-point="lastClicked"
        :footer-height="footerHeight"
      />

      <!-- Query mode toggle -->
      <!-- <div style="position: absolute; top: 10px; right: 10px; z-index: 10">
        <SegmentedControl
          :model-value="mainStore.queryMode"
          :items="queryModeItems"
          size="sm"
          aria-label="Map query mode"
          @update:model-value="(v) => mainStore.setQueryMode(v as string)"
        />
      </div> -->

      <!-- Multi-sensor location picker (exact same coordinate) -->
      <SensorPickerPopover
        :visible="sensorPicker.visible"
        :x="sensorPicker.x"
        :y="sensorPicker.y"
        :sensors="sensorPicker.sensors"
        @pick="(s) => clickSensor(s.id, s.depth)"
        @close="sensorPicker.visible = false"
      />

      <!-- Spiderfy overlay (nearby sensors at different coordinates) -->
      <div
        v-if="spiderfy.visible"
        style="position: absolute; inset: 0; z-index: 5000; pointer-events: all"
        @click.self="spiderfy.visible = false"
      >
        <svg
          style="
            position: absolute;
            inset: 0;
            width: 100%;
            height: 100%;
            pointer-events: none;
          "
        >
          <line
            v-for="(spoke, i) in spiderfy.spokes"
            :key="`sl-${i}`"
            :x1="spiderfy.centerX"
            :y1="spiderfy.centerY"
            :x2="spoke.x"
            :y2="spoke.y"
            :stroke="
              spoke.sensor.isRealtime ? 'rgba(102,187,106,0.75)' : 'rgba(255,167,38,0.75)'
            "
            stroke-width="1.5"
          />
          <circle
            :cx="spiderfy.centerX"
            :cy="spiderfy.centerY"
            r="5"
            fill="#aaaaaa"
            stroke="#333"
            stroke-width="1.5"
          />
        </svg>
        <div
          v-for="(spoke, i) in spiderfy.spokes"
          :key="`sn-${i}`"
          class="spiderfy-node"
          :style="{ left: spoke.x + 'px', top: spoke.y + 'px' }"
          @click.stop="clickSensorFromSpiderfy(spoke.sensor)"
        >
          <div
            class="spiderfy-dot"
            :style="{ background: spoke.sensor.isRealtime ? '#66BB6A' : '#FFA726' }"
          />
          <div class="spiderfy-label">{{ spoke.sensor.name }}</div>
        </div>
      </div>

      <div
        class="px-2 pt-2"
        style="
          width: 250px;
          position: absolute;
          bottom: 0;
          z-index: 999;
          background-color: #11111199;
          border-top-left-radius: 20px;
          border-top-right-radius: 20px;
          margin: auto;
          right: 0;
        "
        :style="{
          left:
            (mainStore.isControlPanelOpen
              ? mainStore.controlPanel_width + overlayGap + 50
              : overlayGap + 50) + 'px',
          transition: 'left 0.3s ease',
        }"
      >
        <ColormapBar class="m-2" />
      </div>

      <!-- Cursor coordinate readout, follows the mouse over the map -->
      <div
        class="bg-elevated rounded-lg cursor-coord-label"
        v-if="mainStore.showCursorCoords && mouseCoords.visible"
        :style="{ left: mouseCoords.x + 'px', top: mouseCoords.y + 'px' }"
      >
        {{ mouseCoords.lat?.toFixed(5) }}, {{ mouseCoords.lng?.toFixed(5) }}
      </div>
    </div>

    <!-- Bottom: Global Chart Footer -->
    <footer
      class="m-0 p-0 footer-resizable"
      :style="{ height: footerHeight, maxHeight: footerHeight }"
    >
      <!-- Drag handle: resize the bottom sheet by dragging this top edge up/down -->
      <div
        class="footer-resize-handle"
        :class="{ 'is-resizing': isResizingFooter }"
        title="Drag to resize"
        @pointerdown="startFooterResize"
      >
        <div class="footer-resize-grip"></div>
      </div>
      <div class="flex footer-content" style="width: 100%">
        <!-- Vertical tab rail. Plain buttons with explicit active classes
                     rather than v-btn-toggle: Explore's sub-list is nested directly
                     beneath it and only shown while Explore is active, which makes
                     the rail's item heights variable — incompatible with the sliding
                     "pill" a uniform-height toggle group can animate. -->
        <div class="footer-rail flex flex-col shrink-0">
          <div class="footer-rail-track">
            <UButton
              variant="ghost"
              class="footer-rail-item"
              block
              :class="{ 'footer-rail-item--active': activeTab === 'explore' }"
              @click="activeTab = 'explore'"
            >
              Explore
            </UButton>
            <div v-if="activeTab === 'explore'" class="footer-rail-sublist">
              <UButton
                variant="ghost"
                class="footer-rail-subitem"
                block
                v-for="sv in exploreSubViews"
                :key="sv.value"
                :class="{
                  'footer-rail-subitem--active': mainStore.exploreView === sv.value,
                }"
                @click="mainStore.setExploreView(sv.value)"
              >
                {{ sv.label }}
              </UButton>
            </div>

            <USeparator v-if="remainingFooterTabs.length > 0" />

            <UButton
              variant="ghost"
              class="footer-rail-item"
              block
              v-for="t in remainingFooterTabs"
              :key="t.value"
              :class="{ 'footer-rail-item--active': activeTab === t.value }"
              @click="activeTab = t.value"
            >
              {{ t.label }}
            </UButton>

            <USeparator />

            <!-- Analysis is a fullscreen dialog with no rail of its own once
                             open, so Model/Sensor is picked here rather than inside it —
                             a sub-item both sets the source and opens the workspace. The
                             top-level row only expands the sub-list (rather than opening
                             straight away, as Explore's does) since opening immediately
                             would cover the rail before the sub-list was ever clickable. -->
            <UButton
              variant="ghost"
              class="footer-rail-item"
              block
              :class="{ 'footer-rail-item--active': activeTab === 'analysis' }"
            >
              Analysis
            </UButton>
            <div class="footer-rail-sublist">
              <UButton
                variant="ghost"
                class="footer-rail-subitem"
                block
                v-for="sv in analysisSubViews"
                :key="sv.value"
                :disabled="sv.disabled"
                :title="sv.title"
                @click="
                  mainStore.setAnalysisSource(sv.value);
                  activeTab = 'analysis';
                "
              >
                {{ sv.label }}
              </UButton>
            </div>
          </div>
        </div>

        <!-- Content area. Both panes stay mounted (like the fullscreen
                     workspaces below) so switching tabs doesn't refetch or reset
                     whatever each pane had already loaded. -->
        <div class="grow" style="min-width: 0; height: 100%; overflow: hidden">
          <!-- Timeseries tab -->
          <!-- The one map-synced pane: the clicked point's data at the
                         map's depth, over a paged window. -->
          <div v-show="activeTab === 'explore'" style="height: 100%">
            <ExplorePanel :active="activeTab === 'explore'" />
          </div>
          <!-- Cross-Section tab: reads a drawn line instead of a clicked
                         point, so it gets its own pane rather than living inside
                         ExplorePanel's point-shaped view. -->
          <div v-show="activeTab === 'crossSection'" style="height: 100%">
            <CrossSectionPanel :active="activeTab === 'crossSection'" />
          </div>
        </div>
      </div>
    </footer>

    <!-- Fullscreen workspaces. Kept mounted so closing and reopening one
             doesn't refetch everything it had already loaded. -->
    <AnalysisWorkspace v-model="analysisOpen" />
    <ComparisonWorkspace v-model="comparisonOpen" />
    <!-- <div class="footer-chart" style="height: 260px; border-top: 1px solid rgba(0,0,0,0.12);">
            <div ref="globalChartContainer" class="w-full h-full"></div>
        </div> -->
    <!-- </div> -->
  </main>
</template>

<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount, watch, computed } from 'vue';
import { useRuntimeConfig } from '#app';
import mapboxgl from 'mapbox-gl';
import 'mapbox-gl/dist/mapbox-gl.css';
import MapboxDraw from '@mapbox/mapbox-gl-draw';
import '@mapbox/mapbox-gl-draw/dist/mapbox-gl-draw.css';
import axios from 'axios'
import moment from 'moment-timezone'
import SelectedVariableDrawer from '../components/SelectedVariableDrawer.vue'
import BetaDisclaimerDialog from '../components/BetaDisclaimerDialog.vue'
import type { FeatureCollection, Geometry, GeoJsonProperties } from 'geojson';
import { resolveColormap } from '~~/composables/useColormapResolver'
import { MAP_BOUNDS, MAP_MIN_ZOOM, MAP_MAX_ZOOM, MAP_STYLE } from '@/config/app'
import { utc2pst } from '~~/composables/useUTC2PST'
import useStationsInteraction from '~~/composables/useStationsInteraction';
import { addBuoyLayer, SOURCE_ID, STATIONS_LAYER_ID, type MultiSensorCandidate } from '~~/composables/useBuoyLayer';
import getSensorTimeseries from '~~/composables/useSensorTimeseries';
import AnalysisWorkspace from '../components/AnalysisWorkspace.vue'
import ExplorePanel from '../components/ExplorePanel.vue'
import ComparisonWorkspace from '../components/ComparisonWorkspace.vue'
import CrossSectionPanel from '../components/crossSection/CrossSectionPanel.vue'
import SegmentedControl from '../components/ui/SegmentedControl.vue'

///////////////////////////////////  SETUP  ///////////////////////////////////

import { useMainStore } from '../stores/main'
import { trackEvent } from '~~/composables/useAnalytics'

const queryModeItems = [
    { value: 'point', label: 'Point', icon: 'i-mdi-map-marker', title: 'Point query' },
    { value: 'area', label: 'Area', icon: 'i-mdi-vector-square', title: 'Area query' },
]

const mainStore = useMainStore();

const config = useRuntimeConfig();
const apiBaseUrl = config.public.apiBaseUrl

// Colormaps cache
// const colormaps = ref<Record<string, any>>({});



const mapContainer = ref<HTMLDivElement | null>(null);
let map: mapboxgl.Map | null = null;
let crossSectionDraw: MapboxDraw | null = null;
const meta = ref<any>(null);
const drawerOpen = computed({
    get: () => mainStore.isVerticalProfileOpen,
    set: (v: boolean) => mainStore.setIsVerticalProfileOpen(v),
});

// Auto-open the vertical profile drawer on switching into either depth
// section — the section is a time-depth slice at one instant per column, the
// drawer is that same coordinate's full water column at one instant, so the
// two naturally belong side by side. One-directional on purpose: leaving a
// depth view never force-closes a drawer the user opened deliberately.
watch(() => mainStore.exploreView, (view) => {
    if (view === 'model-depth' || view === 'sensor-depth') mainStore.setIsVerticalProfileOpen(true);
});

const activeTab = computed({
    get: () => mainStore.activeBottomTab,
    set: (v: 'explore' | 'analysis' | 'comparison' | 'crossSection') => mainStore.setActiveBottomTab(v),
});
const footerTabs = computed(() => [
    { value: 'explore' as const, icon: 'mdi-chart-line', label: 'Explore' },
    { value: 'crossSection' as const, icon: 'mdi-vector-polyline', label: 'Cross-Section' },
    { value: 'analysis' as const, icon: 'mdi-poll', label: 'Analysis' },
    ...(mainStore.selectedSensor?.id
        ? [{ value: 'comparison' as const, icon: 'mdi-compare-horizontal', label: 'Comparison' }]
        : []),
]);
// Explore and Analysis are rendered separately (each has its own sub-list
// nested directly beneath it), so the rail's trailing loop only needs the rest
// — just Comparison, when a sensor is selected.
const remainingFooterTabs = computed(() => footerTabs.value.filter(t => t.value !== 'explore' && t.value !== 'analysis'));

// Explore's own sub-views. Sensor depth only exists once a profiler sensor is
// selected — `selectedProfilerSensorId` is the same store getter ExplorePanel
// itself uses to decide whether there's a section to fetch, so the rail and
// the panel never disagree about whether the option should be offered.
const exploreSubViews = computed(() => [
    { value: 'series' as const, label: 'Timeseries' },
    { value: 'model-depth' as const, label: 'Model depth' },
    ...(mainStore.selectedProfilerSensorId
        ? [{ value: 'sensor-depth' as const, label: 'Sensor depth' }]
        : []),
]);

// Analysis's own sub-views. Sensor stays visible but disabled with no sensor
// selected (rather than disappearing like Explore's Sensor depth) — Model vs
// Sensor is a binary choice worth always showing, not a capability that only
// exists once something else happens to be selected.
const analysisSubViews = computed(() => {
    const hasSensor = !!mainStore.selectedSensor?.id;
    return [
        { value: 'model' as const, label: 'Model', disabled: false, title: '' },
        {
            value: 'sensor' as const, label: 'Sensor', disabled: !hasSensor,
            title: hasSensor ? "Analyse the sensor's own record" : 'Select a sensor first',
        },
    ];
});

// Analysis and Comparison are fullscreen workspaces rather than footer panes.
// Picking them in the rail opens the workspace; closing it drops back to Explore,
// the only tab whose content is tied to the map behind it.
const analysisOpen = computed({
    get: () => activeTab.value === 'analysis',
    set: (open: boolean) => { if (!open) activeTab.value = 'explore'; },
});
const comparisonOpen = computed({
    get: () => activeTab.value === 'comparison',
    set: (open: boolean) => { if (!open) activeTab.value = 'explore'; },
});

// Comparison has nothing to show without a sensor — close it if one is cleared.
watch(() => mainStore.selectedSensor, (sensor) => {
    if (!sensor?.id && activeTab.value === 'comparison') {
        activeTab.value = 'explore';
    }
});

// Bottom-sheet height is user-resizable via the drag handle on the footer's top edge.
const MIN_FOOTER_PX = 160;
const footerHeightPx = ref<number>(440);
const footerHeight = computed<string>(() => `${footerHeightPx.value}px`);
const isResizingFooter = ref<boolean>(false);

/** Largest the footer may grow to, always leaving room for the map above it. */
function maxFooterPx(): number {
    return Math.max(MIN_FOOTER_PX, window.innerHeight - 160);
}

/** Drag handle on the footer's top edge: dragging up grows the sheet, down shrinks it. */
function startFooterResize(e: PointerEvent) {
    e.preventDefault();
    isResizingFooter.value = true;
    const startY = e.clientY;
    const startH = footerHeightPx.value;
    let rafId = 0;

    const onMove = (ev: PointerEvent) => {
        // Dragging upward (smaller clientY) increases the footer height.
        const next = startH + (startY - ev.clientY);
        footerHeightPx.value = Math.min(maxFooterPx(), Math.max(MIN_FOOTER_PX, next));
        if (!rafId) {
            rafId = requestAnimationFrame(() => {
                rafId = 0;
                map?.resize();
            });
        }
    };
    const onUp = () => {
        isResizingFooter.value = false;
        window.removeEventListener('pointermove', onMove);
        window.removeEventListener('pointerup', onUp);
        if (rafId) cancelAnimationFrame(rafId);
        // Final sync so map/charts settle exactly on the released size.
        // The footer panes carry their own ResizeObservers, so only the map
        // needs telling explicitly.
        map?.resize();
    };
    window.addEventListener('pointermove', onMove);
    window.addEventListener('pointerup', onUp);
}

const sensorPicker = ref<{ visible: boolean; x: number; y: number; sensors: MultiSensorCandidate[] }>({
    visible: false,
    x: 0,
    y: 0,
    sensors: [],
});

const spiderfy = ref<{
    visible: boolean;
    centerX: number;
    centerY: number;
    spokes: Array<{ sensor: MultiSensorCandidate; x: number; y: number }>;
}>({ visible: false, centerX: 0, centerY: 0, spokes: [] });

// [-126.4002914428711, 46.85966491699218, -121.31835174560548, 51.10480117797852]
const bounds = MAP_BOUNDS;

const mouseCoords = ref<{ lng: number | null, lat: number | null, x: number, y: number, visible: boolean }>({ lng: null, lat: null, x: 0, y: 0, visible: false });

const sensorData = ref<{ time: string, value: number }[]>([])

// Dialog for detailed timeseries
const dialogOpen = ref(false);

// Flags to coordinate initial click: mapLoaded becomes true when map 'load' fires;
// selectedReady becomes true when initial variables/selectedVariable are set.
const mapLoaded = ref(false);
const selectedReady = ref(false);
let didInitClick = false;

/**
 * Number of days from now to fetch for climate timeseries. This is used both for the API request parameter and for computing the x-axis range of the chart (now +/- DFN days). The API will return all available data within that range, which may be less than DFN if the model run does not extend that far into the future.
*/

const zoom = ref('');

// Nuxt UI has no queue component: UApp renders a single Toaster and `toast.add`
// pushes into it. Drain the store's queue as messages arrive so every existing
// `pushSnack` caller keeps working unchanged.
const toast = useToast();
watch(() => mainStore.snackMessages.length, (n) => {
    if (!n) return;
    for (const m of mainStore.snackMessages.splice(0)) {
        toast.add({ title: m.text, color: (m.color as never) ?? 'info' });
    }
});

const snackMessages = computed({
    get: () => mainStore.snackMessages,
    set: (val) => { mainStore.snackMessages = val; },
});

const showHow = ref(false);

const overlayGap = 8; // gap in px between map edge and overlays (info, colorbar, buttons)

///////////////////////////////////  COMPUTED  ///////////////////////////////////

const DFN = computed(() => mainStore.dfnDays);

const selectedVariable = computed(() => mainStore.selected_variable);

const showBathymetryContours = computed(() => mainStore.showBathymetryContours);

const lastClicked = computed(() => mainStore.lastClickedMapPoint);

const selectedColormap = computed(() => {
    const name = mainStore.selected_variable.colormap;
    if (name) return resolveColormap(mainStore.colormaps, name);
    // Fallback to a default colormap (DB doesn't store colormap field)
    return null;
});

const midDate = computed(() => {
    return mainStore.midDate ?? moment.utc();
});

const mapCenter = computed(() => mainStore.mapCenter);

const showColorbarSettings = computed(() => mainStore.showColorbarSettings);


///////////////////////////////////  HOOKS  ///////////////////////////////////
onMounted(async () => {
    mapboxgl.accessToken = config.public.mapboxToken;
    if (!mapContainer.value) return;

    map = new mapboxgl.Map({
        container: mapContainer.value,
        // style: 'mapbox://styles/taimazb/cmk1jwu8o005101sv1j41cj6j?optimize=true&fresh=true',
        // style: 'mapbox://styles/taimazb/cmkcsejwe005m01ssgtdz3tgd?optimize=true&fresh=true',
        style: MAP_STYLE,
        // center: [-123.2, 48.8],
        bounds,
        // zoom: 9.5,
        // pitch: 45,
        minZoom: MAP_MIN_ZOOM,
        maxZoom: MAP_MAX_ZOOM,
        antialias: true,
        preserveDrawingBuffer: true, // needed for exporting canvas
    });
    console.log(map);

    // When the map finishes loading the style, add the PNG overlay and chart
    map.on('load', () => {
        // Every other layer added below/elsewhere on this page (PNG overlay,
        // bathymetry, stations, analysis box, cross-section...) can land
        // above the style's built-in "water names" labels depending on add
        // order. Re-raising on 'idle' (fires once per settled render, not
        // per frame) keeps it on top of whatever's underneath without every
        // layer-adding function here needing to know it exists.
        map?.on('idle', raiseWaterNamesLayer);

        map?.on('mousemove', (e) => {
            mouseCoords.value.lng = e.lngLat.lng;
            mouseCoords.value.lat = e.lngLat.lat;
            mouseCoords.value.x = e.point.x;
            mouseCoords.value.y = e.point.y;
            mouseCoords.value.visible = true;
        });
        map?.getContainer().addEventListener('mouseleave', () => {
            mouseCoords.value.visible = false;
        });
        // Fetch colormaps and variables in parallel
        Promise.all([init()]).catch((e) => console.warn('init failed:', e));
        addSensors().catch((e) => console.warn('addSensors failed:', e));

        map?.on('zoom', () => {
            if (map) zoom.value = map.getZoom().toFixed(2);
        });

        // Publish the camera so the Share button — mounted in the app header,
        // nowhere near this page's `map` instance — can capture it. `moveend`
        // rather than `move`: one write per settled gesture, not per frame.
        map?.on('moveend', publishMapView);
        publishMapView();

        applyPendingMapView();
        restoreSharedCrossSection();

        updateAnalysisBox();

        // Cross-Section tab's line-drawing control. Added once and left on the
        // map permanently (mode is toggled by the activeTab watcher below)
        // rather than added/removed per tab switch — mapbox-gl-draw's own
        // draw_line_string mode already handles double-click-zoom
        // suspension and Enter/Escape-to-finish, so there's nothing else to
        // hand-roll here.
        crossSectionDraw = new MapboxDraw({ displayControlsDefault: false, controls: {} });
        map?.addControl(crossSectionDraw);
        map?.on('draw.create', onCrossSectionDrawChange);
        map?.on('draw.update', onCrossSectionDrawChange);
        map?.on('draw.delete', () => mainStore.setCrossSectionLine(null));
    });


})

onBeforeUnmount(() => {
    if (map) {
        map.off('idle', raiseWaterNamesLayer);
        map.off('moveend', publishMapView);

        const handlers = (map as any)?.__anchoredChartsHandlers;
        const refs = (map as any)?.__anchoredCharts as Array<any> | undefined;
        const svg = (map as any)?.__anchoredChartsSvg as SVGElement | undefined;
        const globalHandlers = (map as any)?.__globalChartHandlers;

        if (handlers) {
            map.off('move', handlers.updateAll);
            map.off('zoom', handlers.updateAll);
            map.off('rotate', handlers.updateAll);
            map.off('pitch', handlers.updateAll);
            map.off('resize', handlers.resizeHandler);
            window.removeEventListener('resize', handlers.updateAll);
        }

        if (globalHandlers) {
            map.off('resize', globalHandlers.resizeHandler);
            try { window.removeEventListener('resize', globalHandlers.resizeHandler); } catch (e) { }
            try { if (globalHandlers.onStationEnter) map.off('mouseenter', 'stations-circles', globalHandlers.onStationEnter); } catch (e) { }
            try { if (globalHandlers.onStationLeave) map.off('mouseleave', 'stations-circles', globalHandlers.onStationLeave); } catch (e) { }
        }
        // detach station handlers if attached
        try { const sd = (map as any).__stationsDetach; if (sd) sd(); } catch (e) { }

        // remove png overlay if present
        try { if (map.getLayer && map.getLayer('png-image-layer')) map.removeLayer('png-image-layer'); } catch (e) { }
        try { if (map.getSource && map.getSource('png-image')) map.removeSource('png-image'); } catch (e) { }
        try { const ov = (map as any).__activePngOverlay; if (ov && ov.clickHandler) map.off('click', ov.clickHandler); } catch (e) { }
        try { if ((map as any).__clickMarker) ((map as any).__clickMarker).remove(); } catch (e) { }
        if (refs) {
            for (const r of refs) {
                try { r.chart.dispose(); } catch { }
                try { r.chartMarker.remove(); } catch { }
                try { r.dotMarker.remove(); } catch { }
                try { if (r.line && r.line.parentNode) r.line.parentNode.removeChild(r.line); } catch { }
            }
        }

        // remove analysis region box
        try { if (map.getLayer(ABOX_FILL)) map.removeLayer(ABOX_FILL); } catch (e) { }
        try { if (map.getLayer(ABOX_LINE)) map.removeLayer(ABOX_LINE); } catch (e) { }
        try { if (map.getSource(ABOX_SOURCE)) map.removeSource(ABOX_SOURCE); } catch (e) { }

        // remove stations layers + source if present
        try { if (map.getLayer && map.getLayer('stations-badge')) map.removeLayer('stations-badge'); } catch (e) { }
        try { if (map.getLayer && map.getLayer('stations-circles')) map.removeLayer('stations-circles'); } catch (e) { }
        try { if (map.getSource && map.getSource('stations-points')) map.removeSource('stations-points'); } catch (e) { }

        if (svg && svg.parentNode) svg.parentNode.removeChild(svg);
        map.remove();
    }

});

///////////////////////////////////  WATCH  ///////////////////////////////////

// Watcher: add/update/remove overlay when selected variable, depth or datetime
// changes. `dt` shares this one watcher rather than getting its own — a depth
// section cell click (or the variable picker's source switch) sets `depth`
// and `dt` together in the same tick, and two separate watchers each calling
// updatePngOverlay() would independently fire off the same request, doubling
// the image fetch. One watcher covering every source that can move the
// overlay collapses that to a single call no matter how many of them changed
// at once.
watch(() => [mainStore.selected_variable.source, mainStore.selected_variable.var, mainStore.selected_variable.depth, mainStore.selected_variable.dt, mainStore.midDate], async ([v, depth]: [string, string, string | null, moment.Moment | null, moment.Moment | null]) => {
    if (!map) return;

    if (!v) {
        removePngOverlay();
        removeBathymetryTilesLayer();
        return;
    }

    try {
        // Check if bathymetry is selected
        if (v === 'bathymetry') {
            removePngOverlay();
            addBathymetryTilesLayer();
        } else {
            removeBathymetryTilesLayer();
            await updatePngOverlay();
        }

        mapLoaded.value = true;

        // Nothing to refetch here on a var/depth change any more: the footer
        // panes watch `selected_variable` themselves. Only the "no point picked
        // yet" bootstrap still belongs to the page.
        if (!lastClicked.value || v === 'bathymetry') maybeInitClick();
    } catch (e) {
        console.error('Failed to load PNG for variable', v, e);
        removePngOverlay();
    }
}, { immediate: true });

// Watcher: Explore panel's bin-mode toggle changes which dt resolution the raster tile URL
// requests (hourly/daily/monthly — see updatePngOverlay), so the map layer needs its own refresh.
watch(() => mainStore.exploreBinMode, async () => {
    // A shared link restores the bin mode before `/variables` has landed, so
    // this can fire with no variable metadata (and therefore no bounds) to
    // render against. Nothing is lost by skipping: the selected_variable
    // watcher above is `immediate` and draws the overlay once the list arrives.
    if (!map || !mainStore.variables.length) return;
    try {
        await updatePngOverlay();
    } catch (e) {
        console.error('Failed to update PNG for bin mode change', e);
    }
});

watch(() => mainStore.showBathymetryContours, (show) => {
    if (!map) return;
    try {
        if (show) {
            if (!map.getSource('nonna'))
                map?.addSource('nonna', {
                    type: 'vector',
                    tiles: [`${apiBaseUrl}/vector/{z}/{x}/{y}.pbf`],
                });

            // Contour lines
            map?.addLayer({
                id: 'nonna-layer',
                type: 'line',
                source: 'nonna',
                'source-layer': 'nonna', // name of the layer in the vector tile source
                filter: [
                    "step",
                    ["zoom"],
                    [
                        "case",
                        ["==", ["%", ["to-number", ["get", "ELEV"]], 100], 0],
                        true,
                        false
                    ],
                    8,
                    [
                        "case",
                        ["==", ["%", ["to-number", ["get", "ELEV"]], 50], 0],
                        true,
                        false
                    ],
                    12,
                    true
                ],
                paint: {
                    "line-color": "#999",
                    "line-width": 1,
                    // "line-opacity": [
                    //     "step",
                    //     ["zoom"],
                    //     [
                    //         "case",
                    //         ["==", ["%", ["to-number", ["get", "ELEV"]], 100], 0],
                    //         1,
                    //         0
                    //     ],
                    //     8,
                    //     [
                    //         "case",
                    //         ["==", ["%", ["to-number", ["get", "ELEV"]], 50], 0],
                    //         1,
                    //         0
                    //     ],
                    //     12,
                    //     1
                    // ]
                }
            });

            // Labels for every 100m contour
            map?.addLayer({
                id: 'nonna-labels',
                type: 'symbol',
                source: 'nonna',
                'source-layer': 'nonna',
                // filter: ["==", ["%", ["to-number", ["get", "ELEV"]], 100], 0],
                filter: [
                    "step",
                    ["zoom"],
                    [
                        "case",
                        ["==", ["%", ["to-number", ["get", "ELEV"]], 100], 0],
                        true,
                        false
                    ],
                    8,
                    [
                        "case",
                        ["==", ["%", ["to-number", ["get", "ELEV"]], 50], 0],
                        true,
                        false
                    ],
                    12,
                    true
                ],
                layout: {
                    "symbol-placement": "line",
                    "text-field": ["to-string", ["get", "ELEV"]],
                    // "text-font": ["Inter Regular"],
                    "text-size": 12,
                    "text-allow-overlap": true,
                    // "symbol-spacing": 250
                },
                paint: {
                    "text-color": "#ccc",
                    "text-halo-color": "#333",
                    "text-halo-width": 1,
                    "text-halo-blur": 1
                }
            });

        } else {
            if (map.getLayer('nonna-layer')) map.removeLayer('nonna-layer');
            if (map.getLayer('nonna-labels')) map.removeLayer('nonna-labels');
            if (map.getSource('nonna')) map.removeSource('nonna');
        }
    } catch (e) {
        console.warn('Failed to toggle bathymetry contours layer visibility:', e);
    }
}, { immediate: true });

watch(() => mainStore.showMapLabels, (visible: boolean) => {
    try {
        setMapLabelsVisibility(visible);
    } catch (e) {
        console.warn('Failed to toggle map labels visibility:', e);
    }
});

watch(() => mainStore.lastClickedMapPoint, (point) => {
    if (!point) return;
    trigger_mapClick(point.lat, point.lng);
}, { immediate: true });

// When colormap, min, or max change in store, update overlay
watch([
    () => mainStore.selected_variable.colormap,
    () => mainStore.selected_variable.colormapMin,
    () => mainStore.selected_variable.colormapMax
], async () => {
    if (!map || !mapLoaded.value) return;
    if (mainStore.selected_variable.var === 'bathymetry') {
        updateBathymetryTilesLayerColorization();
    } else {
        try {
            await updatePngOverlay();
        } catch (e) {
            console.warn('Failed to update overlay after colormap/min/max change', e);
        }
    }
}, { immediate: false });

// Handler for time controls component
// function onTimeControlDt(dt: any) {
//     // dt is a moment object (UTC)
//     mainStore.updateSelectedVariable({ dt });
// }

watch(() => mapCenter.value, (newCenter) => {
    if (!map || !newCenter) return;
    map.easeTo({ center: [newCenter.lng, newCenter.lat] });
}, { immediate: true })

///////////////////////////////////  MEDTHODS  ///////////////////////////////////
async function getMetadata() {
    try {
        const varName = mainStore.selected_variable.var;
        const metaPath = `${apiBaseUrl}/metadata/${varName}`;

        const r = await axios.get(metaPath);
        meta.value = JSON.parse(r.data);
    } catch (e) {
        console.error('Failed to fetch metadata:', e);
    }
}

async function init() {
    if (!map) return;

    mainStore.setMidDate(moment.utc()); // Initialize midDate to now.

    selectedReady.value = true;
    maybeInitClick();

    // For the new flow we don't use station points. Instead, we start with NO PNG overlay.

    // Chart is initialized by the TimeseriesChart component itself
}


function publishMapView() {
    if (!map) return;
    const c = map.getCenter();
    mainStore.setMapView({
        center: [c.lng, c.lat],
        zoom: map.getZoom(),
        bearing: map.getBearing(),
        pitch: map.getPitch(),
    });
}

/**
 * A shared link's camera. Applied here rather than in the Map constructor
 * because the payload is decoded asynchronously in app.vue and may still be in
 * flight when this page mounts — hence the watcher below as well, which covers
 * a decode that lands after the map is already up.
 */
function applyPendingMapView() {
    const view = mainStore.takePendingMapView();
    if (!map || !view) return;
    map.jumpTo({ center: view.center, zoom: view.zoom, bearing: view.bearing, pitch: view.pitch });
    publishMapView();
}
watch(() => mainStore.pendingMapView, () => { if (mapLoaded.value || map) applyPendingMapView(); });

/**
 * Put a shared cross-section line back on the map. `mainStore.crossSectionLine`
 * alone only feeds the panel's fetch — the line itself lives in
 * mapbox-gl-draw's own store, which starts empty on a fresh load, so the
 * geometry has to be handed back to it explicitly.
 */
let crossSectionRestored = false;
function restoreSharedCrossSection() {
    if (!crossSectionDraw || crossSectionRestored) return;
    const line = mainStore.crossSectionLine;
    if (line?.length) {
        crossSectionRestored = true;
        crossSectionDraw.add({
            type: 'Feature',
            properties: {},
            geometry: { type: 'LineString', coordinates: line.map(p => [p.lng, p.lat]) },
        } as GeoJSON.Feature<GeoJSON.LineString>);
        updateCrossSectionVertexLabels();
    } else if (!mainStore.shareRestorePending) {
        // No line to put back (and none still arriving). Arm drawing if the
        // app opened straight into this tab — the activeTab watcher only fires
        // on a *change* of tab, so a shared link that lands here never triggers it.
        crossSectionRestored = true;
        if (activeTab.value === 'crossSection') crossSectionDraw.changeMode('draw_line_string');
    }
}
// The payload finishes decoding after the map is built, so the line can arrive
// later than `map.on('load')` — retry once it does (the flag above makes this
// idempotent, and leaves the user's own subsequent drawing alone).
watch([() => mainStore.shareRestorePending, () => mainStore.crossSectionLine], () => {
    if (mapLoaded.value || map) restoreSharedCrossSection();
});

function maybeInitClick() {
    // Call initClick only once both the map has finished loading and the selected variable has been initialized
    // — and, for a shared link, once the payload has been decoded, so the
    // shared coordinate isn't overwritten by the default bootstrap point.
    if (mainStore.shareRestorePending) return;
    if (mapLoaded.value && selectedReady.value && !didInitClick) {
        didInitClick = true;
        const shared = mainStore.lastClickedMapPoint;
        if (shared) {
            // Already the app's selected point (applyShareState set it); all
            // that's missing is the marker. Deliberately not initClick(): that
            // reports a `model_point_queried` the user never performed.
            trigger_mapClick(shared.lat, shared.lng);
        } else {
            initClick(49.2, -123.5); // Center of the map
        }
    }
}

// A share payload finishes decoding after this page has mounted, so the
// bootstrap click above has to be retried once the restore releases it.
watch(() => mainStore.shareRestorePending, (pending) => { if (!pending) maybeInitClick(); });

function initClick(lat: number, lng: number) {
    if (!map) return;

    trackEvent('model_point_queried', {
        lat, lon: lng,
        source: mainStore.selected_variable.source,
        variable: mainStore.selected_variable.var,
        query_mode: mainStore.queryMode,
    });
    mainStore.setLastClickedMapPoint({ lat, lng });
    map.fire('click', { lngLat: { lat, lng } });
}


// The page no longer fetches timeseries itself. Each footer pane owns its own
// data: the Depth tab pulls the model section (and slices the single-depth line
// out of it) plus the climatology envelope, and the Comparison pane fetches its
// own model/sensor pair. This page's remaining job around a click is to place
// the marker and publish the coordinate on the store.

const REALTIME_THRESHOLD_MS = 14 * 24 * 60 * 60 * 1000; // 14 days
const PROXIMITY_M = 100; // sensors within 100 m share one map marker

function haversineM(lat1: number, lon1: number, lat2: number, lon2: number): number {
    const R = 6371000;
    const φ1 = lat1 * Math.PI / 180, φ2 = lat2 * Math.PI / 180;
    const Δφ = (lat2 - lat1) * Math.PI / 180;
    const Δλ = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(Δφ / 2) ** 2 + Math.cos(φ1) * Math.cos(φ2) * Math.sin(Δλ / 2) ** 2;
    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

let _sensorGroups: any[][] = [];
let _sensorIsRealtime: (s: any) => boolean = () => false;
let _visibleSensorGroups: any[][] = []; // _sensorGroups filtered to mainStore.filteredSensors

function coordKey(lat: number, lon: number): string {
    return `${lat.toFixed(6)},${lon.toFixed(6)}`;
}

function buildSensorGeoJSON(groups: any[][], sensorIsRealtime: (s: any) => boolean): FeatureCollection<Geometry, GeoJsonProperties> {
    const features = groups.map(group => {
        const first = group[0];
        const anyActive = group.some((s: any) => s.active);
        const isRealtime = group.some(sensorIsRealtime);
        const allRealtime = group.every(sensorIsRealtime);
        const hasMixed = group.length > 1 && isRealtime && !allRealtime;
        return {
            type: 'Feature',
            geometry: { type: 'Point', coordinates: [first.longitude, first.latitude] },
            properties: {
                coordKey: coordKey(first.latitude, first.longitude),
                sensorCount: group.length,
                active: anyActive,
                isRealtime,
                hasMixed,
                isVariableDepth: group.some((s: any) => s.depth < 0),
                sensorsJson: JSON.stringify(
                    group.map((s: any) => ({
                        id: s.id,
                        name: s.name,
                        depth: s.depth,
                        depth_min: s.depth_min ?? null,
                        depth_max: s.depth_max ?? null,
                        isRealtime: sensorIsRealtime(s),
                        lat: s.latitude,
                        lon: s.longitude,
                    }))
                ),
            }
        };
    });
    return { type: 'FeatureCollection', features };
}

function updateBuoyVarOpacity(selectedVar: string | null) {
    if (!map || !_visibleSensorGroups.length) return;
    const keysWithVar = _visibleSensorGroups
        .filter(g => !selectedVar || g.some((s: any) => selectedVar in (s.variables ?? {})))
        .map(g => coordKey(g[0].latitude, g[0].longitude));
    const expr: any = (!selectedVar || keysWithVar.length === _visibleSensorGroups.length)
        ? 0.95
        : ['case', ['in', ['get', 'coordKey'], ['literal', keysWithVar]], 0.95, 0.2];
    try { (map as any).setPaintProperty(STATIONS_LAYER_ID, 'icon-opacity', expr); } catch (_) { }
}

// Recomputes which station groups are visible on the map from mainStore.filteredSensors
// (the same filter criteria — search/organization/variable — driving the sensorInfo.vue list),
// and pushes the resulting geometry into the existing source via setData.
// Each group is trimmed down to only its filter-matching members, so clusters that survive
// the filter don't still expose non-matching sensors when spiderfied.
function applyBuoyFilters() {
    if (!map || !_sensorGroups.length) return;
    const filteredIds = new Set(mainStore.filteredSensors.map((s: any) => s.id));
    _visibleSensorGroups = _sensorGroups
        .map(g => g.filter((s: any) => filteredIds.has(s.id)))
        .filter(g => g.length > 0);
    const geojson = buildSensorGeoJSON(_visibleSensorGroups, _sensorIsRealtime);
    const source = map.getSource(SOURCE_ID) as mapboxgl.GeoJSONSource | undefined;
    if (source) source.setData(geojson);
    updateBuoyVarOpacity(mainStore.selected_variable?.var ?? null);
}

watch(() => mainStore.filteredSensors, applyBuoyFilters);

async function addSensors() {
    const sensors = await getSensors();
    const now = Date.now();

    const groups: any[][] = [];
    for (const s of sensors) {
        const existing = groups.find(g =>
            haversineM(g[0].latitude, g[0].longitude, s.latitude, s.longitude) < PROXIMITY_M
        );
        if (existing) existing.push(s);
        else groups.push([s]);
    }

    const sensorIsRealtime = (s: any) =>
        s.latest_data_at && (now - new Date(s.latest_data_at).getTime()) < REALTIME_THRESHOLD_MS;

    _sensorGroups = groups;
    _sensorIsRealtime = sensorIsRealtime;

    const filteredIds = new Set(mainStore.filteredSensors.map((s: any) => s.id));
    _visibleSensorGroups = groups
        .map(g => g.filter((s: any) => filteredIds.has(s.id)))
        .filter(g => g.length > 0);

    const geojson = buildSensorGeoJSON(_visibleSensorGroups, sensorIsRealtime);

    try {
        const detach = await addBuoyLayer(map, geojson, clickSensor, openSensorPicker, openSpiderfy);
        (map as any).__stationsDetach = detach;
    } catch (e) {
        console.warn('Failed to add buoy layer:', e);
    }

    updateBuoyVarOpacity(mainStore.selected_variable?.var ?? null);
}

watch(() => mainStore.selected_variable.var, (newVar) => {
    updateBuoyVarOpacity(newVar ?? null);
});

function openSensorPicker(sensors: MultiSensorCandidate[], screenX: number, screenY: number) {
    openSpiderfy(sensors, screenX, screenY);
}

function openSpiderfy(sensors: MultiSensorCandidate[], screenX: number, screenY: number) {
    sensorPicker.value.visible = false;
    const radius = 70;
    const startAngle = -Math.PI / 2;
    const spokes = sensors.map((sensor, i) => {
        const angle = startAngle + (2 * Math.PI / sensors.length) * i;
        return { sensor, x: screenX + radius * Math.cos(angle), y: screenY + radius * Math.sin(angle) };
    });
    spiderfy.value = { visible: true, centerX: screenX, centerY: screenY, spokes };
    map.once('movestart', () => { spiderfy.value.visible = false; });
}

function clickSensorFromSpiderfy(sensor: MultiSensorCandidate) {
    spiderfy.value.visible = false;
    clickSensor(sensor.id, sensor.depth);
}

function clickSensor(sensor_id: string, depth: number) {
    trackEvent('sensor_selected', { sensor_id, source: 'map' });
    sensorPicker.value.visible = false;
    mainStore.selectSensor(sensor_id, depth);
    // Publishing the coordinate is the whole job — the panes fetch off the store.
    // (No fetch to de-duplicate here any more, so the old _sensorClickPending
    // guard against a double request from the var/depth watcher is gone too.)
    const sensor = mainStore.sensors.find((s: any) => s.id === sensor_id);
    if (sensor) {
        mainStore.setLastClickedMapPoint({ lat: sensor.latitude, lng: sensor.longitude });
    }
}


async function getSensors() {
    try {
        const r = await axios.get(`${apiBaseUrl}/sensors`);
        const data = r.data;
        mainStore.setSensors(data);
        return data;
    } catch (e) {
        console.error('Failed to fetch sensors:', e);
        return [];
    }
}

// Add / update / remove PNG overlay for a given public PNG path
async function updatePngOverlay(sourceId = 'png-image', layerId = 'png-image-layer') {
    if (!map) throw new Error('map not initialized');
    // if (!meta.value || !meta.value.bounds) throw new Error('metadata not loaded');

    const source = mainStore.selected_variable.source.replace(/\s+/g, ''); // Remove spaces from source name for URL
    const varName = mainStore.selected_variable.var;
    // dt's shape tells the API which bin mode to render (see extract_image.resolve_bin_mode):
    // full timestamp -> hourly, YYYY-MM-DD -> daily, YYYY-MM -> monthly. Keeps the map layer's
    // resolution in sync with the Explore panel's bin-mode toggle used by the timeseries/depth charts.
    const dtFormat = mainStore.exploreBinMode === 'daily' ? 'YYYY-MM-DD'
        : mainStore.exploreBinMode === 'monthly' ? 'YYYY-MM'
            : 'YYYY-MM-DDTHHmmss';
    const dt = mainStore.selected_variable.dt?.format(dtFormat) || '';
    const depth = mainStore.selected_variable.depth
    const pngPath = `${apiBaseUrl}/png/${source}/${varName}/${dt}/${depth}`;

    const bounds = mainStore.variables.find(v => v.source === mainStore.selected_variable.source && v.var === varName)?.bounds;
    if (!bounds) throw new Error('bounds not found');
    const [lonmin, latmin, lonmax, latmax] = bounds;
    const coords = [
        [lonmin, latmax], // top-left
        [lonmax, latmax], // top-right
        [lonmax, latmin], // bottom-right
        [lonmin, latmin], // bottom-left
    ] as [number, number][];

    // remove existing if present
    // try { if (map.getLayer(layerId)) map.removeLayer(layerId); } catch (e) { }
    // try { if (map.getSource(sourceId)) map.removeSource(sourceId); } catch (e) { }

    // prepare raster-color stops for Mapbox style
    const raster_values: any[] = [];
    // e.g.
    // 0, 'rgba(0, 0, 0, 0)',
    //             0.01, '#440154',
    //             0.25, '#00f',
    //             0.5, '#0f0',
    //             0.75, '#fde725',
    //             1.0, '#f00'

    const colormapMin = mainStore.selected_variable.colormapMin
    const colormapMax = mainStore.selected_variable.colormapMax

    // Get packing params from metadata, default to 0.1 precision and 0 base if missing
    // Note: base might be equal to colormapMin if it was dynamic
    const precision = mainStore.variables.find(v => v.var === varName)?.precision ?? 0.1;
    const base = 0

    // Use colormap if available, otherwise fall back to default ramp
    const cmap = selectedColormap.value;
    if (cmap && Array.isArray(cmap.stops) && cmap.stops.length > 0) {
        for (const s of cmap.stops) {
            const pos = s[0];
            const color = s[1];
            // pos may be normalized [0..1] or absolute depending on cmap.mode
            let val_phys = pos;
            if (!cmap.mode || cmap.mode === 'normalized') {
                val_phys = colormapMin + pos * (colormapMax - colormapMin);
            }
            const val_packed = (val_phys - base) / precision;
            raster_values.push(val_packed, color);
        }
    } else {
        const color_stops = [
            [0.0, 'rgba(0, 0, 0, 1)'],
            [0.001, '#440154'],
            [0.25, '#00f'],
            [0.5, '#0f0'],
            [0.75, '#fde725'],
            [1.0, '#f00']
        ];
        for (const stop of color_stops) {
            const val_phys = colormapMin + stop[0] * (colormapMax - colormapMin);
            // decode formula: q = (phys - base) / precision
            const val_packed = (val_phys - base) / precision;
            raster_values.push(val_packed, stop[1]);
        }
    }

    if (map.getSource(sourceId)) {
        map.getSource(sourceId)?.updateImage({
            type: 'image',
            url: pngPath,
            coordinates: coords
        })
        map.setPaintProperty(layerId, 'raster-color', [
            'interpolate',
            ['linear'],
            ['raster-value'],
            ...raster_values
        ]);
        map.setPaintProperty(layerId, 'raster-color-range', [(colormapMin - base) / precision, (colormapMax - base) / precision]);
    }
    else {
        map.addSource(sourceId, { type: 'image', url: pngPath, coordinates: coords });
        map.addLayer({
            id: layerId, type: 'raster', source: sourceId, paint: {
                'raster-opacity': 1.0,
                'raster-color': [
                    'interpolate',
                    ['linear'],
                    ['raster-value'],
                    ...raster_values
                ],
                // Range of the packed integer values
                'raster-color-range': [(colormapMin - base) / precision, (colormapMax - base) / precision],
                // Mix to recover the 24-bit integer from normalized RGB [0..1]
                // R_int = R_norm * 255. Packed = R_int*65536 + G_int*256 + B_int
                // Coeffs: [255*65536, 255*256, 255, 0] -> [16711680, 65280, 255, 0]
                'raster-color-mix': [256 * 256 * 255, 256 * 255, 255, 0],
                'raster-fade-duration': 0
            },
        }, 'country-boundaries');
    }

    // save active overlay metadata on the map instance for access by click handler
    const overlayObj: any = { bounds: [lonmin, latmin, lonmax, latmax], coords, varName, depth: depth, pngPath, meta, clickHandler: null };
    // remove previous click handler if present
    const prev = (map as any).__activePngOverlay;
    if (prev && prev.clickHandler) {
        try { map.off('click', prev.clickHandler); } catch (e) { }
    }
    (map as any).__activePngOverlay = overlayObj;

    // register a click handler that queries the API for a timeseries at the clicked coordinate
    const onMapClick = async (evt: any) => {
        // While drawing a cross-section line, clicks add vertices via
        // crossSectionDraw (see onCrossSectionDrawChange) — they aren't a
        // point query, and must not clear the sensor selection mid-draw.
        if (mainStore.activeBottomTab === 'crossSection') return;

        const { lng, lat } = evt.lngLat;

        // Check if click landed on a sensor feature (layer may not exist yet)
        const stationLayers = map.getLayer('stations-circles') ? ['stations-circles'] : [];
        const features = stationLayers.length
            ? map.queryRenderedFeatures(evt.point, { layers: stationLayers })
            : [];

        if (features.length > 0) {
            // Sensor click is fully handled by useStationsInteraction → clickSensor.
            // Returning here prevents a duplicate getTimeseriesPromises call.
            return;
        }

        mainStore.setSelectedSensor(null); // Clear selected sensor on map click
        sensorPicker.value.visible = false;
        mainStore.setLastClickedMapPoint({ lat, lng });
    };

    // register click handler
    overlayObj.clickHandler = onMapClick;
    map.on('click', onMapClick);
}

/** Drops the click marker at a coordinate. Data fetching is the panes' job. */
function trigger_mapClick(lat: number, lng: number) {
    const overlay = (map as any).__activePngOverlay;
    if (!overlay) return;
    try { if ((map as any).__clickMarker) ((map as any).__clickMarker).remove(); } catch (e) { }
    if (mainStore.queryMode === 'point') {
        const el = document.createElement('div');
        el.className = 'map-click-marker';
        const marker = new mapboxgl.Marker({ element: el, anchor: 'center' }).setLngLat([lng, lat]).addTo(map);
        (map as any).__clickMarker = marker;
    }
}

// NOTE: stopping animator on every change to `mainStore.selected_variable` caused the animator to stop
// immediately after it updated the selected timestamp (dt). That logic is already handled by the
// var/depth watcher above. Removed the blanket watcher to avoid stopping playback when the animator
// updates the selected datetime.

// Cached id of the style's built-in water-name-labels layer (added directly
// in Mapbox Studio, e.g. "Puget Sound", "Strait of Georgia" — not something
// this app adds itself). Studio slugifies a layer's display name into its
// id, so resolve by fuzzy match rather than hardcoding a guess at the id.
let waterNamesLayerId: string | null = null;
let warnedMissingWaterNamesLayer = false;

function resolveWaterNamesLayer(): string | null {
    if (!map) return null;
    if (waterNamesLayerId && map.getLayer(waterNamesLayerId)) return waterNamesLayerId;
    const normalize = (s: string) => s.toLowerCase().replace(/[^a-z0-9]/g, '');
    const layers = map.getStyle()?.layers ?? [];
    const match = layers.find((l: { id: string }) => normalize(l.id) === normalize('water names'));
    waterNamesLayerId = match ? match.id : null;
    if (!waterNamesLayerId && !warnedMissingWaterNamesLayer) {
        warnedMissingWaterNamesLayer = true;
        console.warn('Could not find a "water names" layer in the map style; labels will not be forced on top.');
    }
    return waterNamesLayerId;
}

// Re-raise the water-name labels to the very top of the style's layer stack.
// Bound to the map's 'idle' event so every current and future layer-adding
// function on this page stays below it without each needing to know this
// layer exists. Skips the move when already topmost so it can't loop.
function raiseWaterNamesLayer() {
    if (!map) return;
    const layerId = resolveWaterNamesLayer();
    if (!layerId) return;
    try {
        const layers = map.getStyle()?.layers;
        if (!layers || !layers.length) return;
        if (layers[layers.length - 1].id !== layerId) {
            map.moveLayer(layerId);
        }
    } catch (e) { /* map may be mid-teardown */ }
}

// Base style's "Place labels" group (settlement/state/country/continent
// names) — a fixed, standard set of Mapbox Streets layer ids, unlike the
// water-names layer which is custom and has to be resolved by name.
const PLACE_LABEL_LAYER_IDS = [
    'settlement-subdivision-label',
    'settlement-minor-label',
    'settlement-major-label',
    'state-label',
    'country-label',
    'continent-label',
];

// Drives the "Map Labels" overlay button: shows/hides the water-name labels
// together with the place-name labels as one group.
function setMapLabelsVisibility(visible: boolean) {
    if (!map) return;
    const visibility = visible ? 'visible' : 'none';
    const waterLayerId = resolveWaterNamesLayer();
    const targetIds = waterLayerId ? [waterLayerId, ...PLACE_LABEL_LAYER_IDS] : PLACE_LABEL_LAYER_IDS;
    for (const id of targetIds) {
        try {
            if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', visibility);
        } catch (e) { /* layer not present in this style version */ }
    }
}

function removePngOverlay(sourceId = 'png-image', layerId = 'png-image-layer') {
    if (!map) return;
    try { const ov = (map as any).__activePngOverlay; if (ov && ov.clickHandler) map.off('click', ov.clickHandler); } catch (e) { }
    try { if ((map as any).__clickMarker) ((map as any).__clickMarker).remove(); } catch (e) { }
    try { if (map.getLayer && map.getLayer(layerId)) map.removeLayer(layerId); } catch (e) { }
    try { if (map.getSource && map.getSource(sourceId)) map.removeSource(sourceId); } catch (e) { }
    try { delete (map as any).__activePngOverlay; } catch (e) { }
}

// Add bathymetry tiles layer based on raster tiles from backend
function addBathymetryTilesLayer(sourceId = 'bathymetry-tiles', layerId = 'bathymetry-tiles-layer') {
    if (!map) return;

    try {
        // Remove existing layer and source if they exist
        if (map.getLayer && map.getLayer(layerId)) map.removeLayer(layerId);
        if (map.getSource && map.getSource(sourceId)) map.removeSource(sourceId);

        // Add raster tile source for bathymetry
        map.addSource(sourceId, {
            type: 'raster',
            tiles: [`${apiBaseUrl}/raster_tiles/{z}/{x}/{y}.webp`],
            tileSize: 512,
        });

        // Build colorization stops like PNG layer
        const raster_values: any[] = [];
        const colormapMin = selectedVariable.value.colormapMin;
        const colormapMax = selectedVariable.value.colormapMax;
        const precision = mainStore.variables.find(v => v.var === 'bathymetry')?.precision ?? 1;
        const base = -3000;

        // Use the reactive selectedColormap computed like PNG layer does
        const cmap = selectedColormap.value;
        if (cmap && Array.isArray(cmap.stops) && cmap.stops.length > 0) {
            for (const s of cmap.stops) {
                const pos = s[0];
                const color = s[1];
                let val_phys = pos;
                if (!cmap.mode || cmap.mode === 'normalized') {
                    val_phys = colormapMin + pos * (colormapMax - colormapMin);
                }
                const val_packed = (val_phys - base) / precision;
                raster_values.push(val_packed, color);
            }
        } else {
            const color_stops = [
                [0.0, 'rgba(0, 0, 0, 1)'],
                [0.001, '#440154'],
                [0.25, '#00f'],
                [0.5, '#0f0'],
                [0.75, '#fde725'],
                [1.0, '#f00']
            ];
            for (const stop of color_stops) {
                const val_phys = colormapMin + stop[0] * (colormapMax - colormapMin);
                const val_packed = (val_phys - base) / precision;
                raster_values.push(val_packed, stop[1]);
            }
        }

        map.addLayer({
            id: layerId,
            type: 'raster',
            source: sourceId,
            paint: {
                // 'raster-opacity': 0.85,
                // Decode packed 24-bit integer: value = (R*m0 + G*m1 + B*m2) / 255 = R*65536 + G*256 + B
                'raster-color-mix': [16711680, 65280, 255, 0],
                // 'raster-color-offset': 0,
                'raster-color': [
                    'interpolate',
                    ['linear'],
                    ['raster-value'],
                    ...raster_values
                ],
                'raster-color-range': [(colormapMin - base) / precision, (colormapMax - base) / precision]
            }
        }, 'country-boundaries');

        // Store metadata for cleanup
        (map as any).__activeBathymetryLayer = {
            sourceId,
            layerId,
        };
    } catch (e) {
        console.error('Error adding bathymetry tiles layer:', e);
    }
}

// Update bathymetry tiles layer colorization
function updateBathymetryTilesLayerColorization(layerId = 'bathymetry-tiles-layer') {
    if (!map) return;

    try {
        if (!map.getLayer(layerId)) return;

        const colormapMin = selectedVariable.value.colormapMin;
        const colormapMax = selectedVariable.value.colormapMax;
        const precision = mainStore.variables.find(v => v.var === 'bathymetry')?.precision ?? 1;
        const base = -3000;

        const raster_values: any[] = [];
        // Use the reactive selectedColormap computed like PNG layer does
        const cmap = selectedColormap.value;

        if (cmap && Array.isArray(cmap.stops) && cmap.stops.length > 0) {
            for (const s of cmap.stops) {
                const pos = s[0];
                const color = s[1];
                let val_phys = pos;
                if (!cmap.mode || cmap.mode === 'normalized') {
                    val_phys = colormapMin + pos * (colormapMax - colormapMin);
                }
                const val_packed = (val_phys - base) / precision;
                raster_values.push(val_packed, color);
            }
        } else {
            const color_stops = [
                [0.0, 'rgba(0, 0, 0, 1)'],
                [0.001, '#440154'],
                [0.25, '#00f'],
                [0.5, '#0f0'],
                [0.75, '#fde725'],
                [1.0, '#f00']
            ];
            for (const stop of color_stops) {
                const val_phys = colormapMin + stop[0] * (colormapMax - colormapMin);
                const val_packed = (val_phys - base) / precision;
                raster_values.push(val_packed, stop[1]);
            }
        }

        // map.setPaintProperty(layerId, 'raster-color-offset', 0);
        map.setPaintProperty(layerId, 'raster-color', [
            'interpolate',
            ['linear'],
            ['raster-value'],
            ...raster_values
        ]);
        const raster_color_range = [(colormapMin - base) / precision, (colormapMax - base) / precision]
        map.setPaintProperty(layerId, 'raster-color-range', raster_color_range);
    } catch (e) {
        console.error('Error updating bathymetry tiles colorization:', e);
    }
}

// Remove bathymetry tiles layer
function removeBathymetryTilesLayer() {
    if (!map) return;

    try {
        const bathy = (map as any).__activeBathymetryLayer;
        if (!bathy) return;

        const { sourceId = 'bathymetry-tiles', layerId = 'bathymetry-tiles-layer' } = bathy;

        try { if (map.getLayer && map.getLayer(layerId)) map.removeLayer(layerId); } catch (e) { }
        try { if (map.getSource && map.getSource(sourceId)) map.removeSource(sourceId); } catch (e) { }
        try { delete (map as any).__activeBathymetryLayer; } catch (e) { }
    } catch (e) {
        console.error('Error removing bathymetry tiles layer:', e);
    }
}



async function autorange() {
    if (!map || !mapLoaded.value) {
        console.warn('Map not loaded yet');
        return;
    }

    try {
        const selectedSource = mainStore.selected_variable.source;
        const selectedVar = mainStore.selected_variable.var;
        const selectedDt = mainStore.selected_variable.dt;
        const selectedDepth = mainStore.selected_variable.depth_nc;

        if (!selectedVar || !selectedDt) {
            console.warn('No variable or datetime selected');
            return;
        }

        // Get the visible map bounds
        const bounds = map.getBounds();
        const north = bounds.getNorth();
        const south = bounds.getSouth();
        const east = bounds.getEast();
        const west = bounds.getWest();

        // Format datetime as ISO string
        const dtStr = selectedDt.format('YYYY-MM-DDTHH:mm:ss');

        // Call the new getMinMax endpoint to extract min/max directly from the NC file
        const response = await axios.post(`${apiBaseUrl}/getMinMax`, {
            source: selectedSource,
            var: selectedVar,
            dt: dtStr,
            depth: selectedDepth,
            bin_mode: mainStore.exploreBinMode,
            north: north,
            south: south,
            east: east,
            west: west
        });

        console.log(response.data);

        if (response.data && response.data.min !== null && response.data.max !== null) {
            let minVal = response.data.min;
            let maxVal = response.data.max;

            // Round using the precision from the selected variable
            const precision = mainStore.selected_variable.precision || 0;
            if (precision > 0) {
                minVal = Math.round(minVal / precision) * precision;
                maxVal = Math.round(maxVal / precision) * precision;
            }

            mainStore.updateSelectedVariable({
                colormapMin: minVal,
                colormapMax: maxVal
            });
        } else {
            console.warn('No valid min/max values in response');
        }
    } catch (e) {
        console.error('Error in autorange:', e);
    } finally {
        mainStore.setAutoRangeDisabled(false);
    }
}

// --- ANALYSIS REGION BOX ---
const ABOX_SOURCE = 'analysis-region'
const ABOX_FILL = 'analysis-region-fill'
const ABOX_LINE = 'analysis-region-line'

function updateAnalysisBox() {
    if (!map || !map.isStyleLoaded()) return
    const pt = lastClicked.value
    const show = mainStore.queryMode === 'area' && !!pt

    const data: GeoJSON.Feature<GeoJSON.Polygon> = show
        ? {
            type: 'Feature', properties: {}, geometry: {
                type: 'Polygon', coordinates: [[
                    [pt!.lng - 0.05, pt!.lat - 0.05],
                    [pt!.lng + 0.05, pt!.lat - 0.05],
                    [pt!.lng + 0.05, pt!.lat + 0.05],
                    [pt!.lng - 0.05, pt!.lat + 0.05],
                    [pt!.lng - 0.05, pt!.lat - 0.05],
                ]]
            }
        }
        : { type: 'Feature', properties: {}, geometry: { type: 'Polygon', coordinates: [[]] } }

    if (map.getSource(ABOX_SOURCE)) {
        (map.getSource(ABOX_SOURCE) as mapboxgl.GeoJSONSource).setData(data)
        map.setLayoutProperty(ABOX_FILL, 'visibility', show ? 'visible' : 'none')
        map.setLayoutProperty(ABOX_LINE, 'visibility', show ? 'visible' : 'none')
    } else {
        map.addSource(ABOX_SOURCE, { type: 'geojson', data })
        map.addLayer({
            id: ABOX_FILL, type: 'fill', source: ABOX_SOURCE,
            paint: { 'fill-color': 'rgba(255,193,7,0.08)' },
            layout: { visibility: show ? 'visible' : 'none' }
        })
        map.addLayer({
            id: ABOX_LINE, type: 'line', source: ABOX_SOURCE,
            paint: { 'line-color': '#ff5722', 'line-width': 1.5, 'line-dasharray': [4, 2] },
            layout: { visibility: show ? 'visible' : 'none' }
        })
    }
}

watch([lastClicked, activeTab], updateAnalysisBox)
watch(() => mainStore.queryMode, () => {
    updateAnalysisBox()
    if (lastClicked.value) trigger_mapClick(lastClicked.value.lat, lastClicked.value.lng)
})

// No activeTab watcher any more: panes that were hidden used to miss updates
// because this page fetched on their behalf and gated on which tab was showing.
// Each pane now watches the store itself and re-measures when it becomes active.

// --- CROSS-SECTION LINE DRAWING ---
// This one *is* an activeTab watcher, unlike the panes above — it isn't data
// fetching done on a pane's behalf, it's map-control-mode plumbing that has to
// live here because `crossSectionDraw`/`map` are page-scoped, not reachable
// from CrossSectionPanel.vue.
function onCrossSectionDrawChange() {
    const line = crossSectionDraw?.getAll().features
        .find((f) => f.geometry?.type === 'LineString') as GeoJSON.Feature<GeoJSON.LineString> | undefined;
    if (!line) { mainStore.setCrossSectionLine(null); return; }
    // GeoJSON coordinates are [lng, lat]; the store (like lastClickedMapPoint) uses {lat, lng}.
    mainStore.setCrossSectionLine(line.geometry.coordinates.map(([lng, lat]) => ({ lat: lat!, lng: lng! })));
}

// mapbox-gl-draw only renders vertex handles while the line is the *active*
// selection (mid-draw, or selected in simple_select) — once you click away it
// reverts to a plain line, so there's nothing on the map to tell "dashed line
// #2 in the chart" apart from "dashed line #3". This numbered overlay stays
// pinned to the vertices regardless of draw/selection state, matching the
// 1-based numbering CrossSectionPanel.vue's own vertex markers use.
const CS_VERTEX_SOURCE = 'cross-section-vertices'
const CS_VERTEX_CIRCLES = 'cross-section-vertex-circles'
const CS_VERTEX_LABELS = 'cross-section-vertex-labels'

function updateCrossSectionVertexLabels() {
    if (!map) return
    const line = mainStore.crossSectionLine
    const data: GeoJSON.FeatureCollection<GeoJSON.Point> = {
        type: 'FeatureCollection',
        features: (line ?? []).map((pt, i) => ({
            type: 'Feature',
            properties: { label: String(i + 1) },
            geometry: { type: 'Point', coordinates: [pt.lng, pt.lat] },
        })),
    }
    const existingSource = map.getSource(CS_VERTEX_SOURCE) as mapboxgl.GeoJSONSource | undefined;
    if (existingSource) {
        // Updating an existing source's data doesn't require the style to be
        // fully loaded — only creating new sources/layers below does. Gating
        // this on isStyleLoaded() left stale vertex markers on the map when a
        // tab switch fired mid-style-update (e.g. right after deleteAll()).
        existingSource.setData(data)
    } else if (map.isStyleLoaded()) {
        map.addSource(CS_VERTEX_SOURCE, { type: 'geojson', data })
        map.addLayer({
            id: CS_VERTEX_CIRCLES, type: 'circle', source: CS_VERTEX_SOURCE,
            paint: {
                'circle-radius': 8, 'circle-color': '#12181f',
                'circle-stroke-width': 1.5, 'circle-stroke-color': '#eef3f7',
            },
        })
        map.addLayer({
            id: CS_VERTEX_LABELS, type: 'symbol', source: CS_VERTEX_SOURCE,
            layout: {
                'text-field': ['get', 'label'], 'text-size': 10, 'text-font': ['Open Sans Bold'],
                'text-allow-overlap': true, 'text-ignore-placement': true,
            },
            paint: { 'text-color': '#eef3f7' },
        })
    }
}

watch(() => mainStore.crossSectionLine, updateCrossSectionVertexLabels, { deep: true })

watch(activeTab, (tab) => {
    if (!crossSectionDraw) return;
    crossSectionDraw.deleteAll();
    mainStore.setCrossSectionLine(null);
    crossSectionDraw.changeMode(tab === 'crossSection' ? 'draw_line_string' : 'simple_select');

    // The point-click marker and the cross-section line/vertices are mutually
    // exclusive map decorations — only one mode's input should be visible at a time.
    if (tab === 'crossSection') {
        try { if ((map as any).__clickMarker) ((map as any).__clickMarker).remove(); } catch (e) { }
    } else if (lastClicked.value) {
        trigger_mapClick(lastClicked.value.lat, lastClicked.value.lng);
    }
});

// A finished line leaves mapbox-gl-draw in simple_select mode (its own
// draw_line_string mode exits there once you click back on the last vertex or
// press Enter) — clicking the map again would just select/drag the existing
// line, not start a new one. CrossSectionPanel's "New line" button bumps this
// token to re-arm drawing without waiting for a tab round-trip.
watch(() => mainStore.crossSectionRedrawToken, () => {
    if (!crossSectionDraw) return;
    crossSectionDraw.deleteAll();
    crossSectionDraw.changeMode('draw_line_string');
});
</script>

<style scoped>
.footer-resizable {
  position: relative;
  /* `flex-direction`/`align-items` below always assumed a flex box — Vuetify's
     v-footer supplied `display: flex` itself. A plain <footer> is block, which
     left .footer-content unbounded and let the chart grow to its content height. */
  display: flex;
  overflow: hidden;
  flex-direction: column;
  align-items: stretch;
  border-top: 1px solid rgba(255, 255, 255, 0.16);
  box-shadow: 0 -4px 12px rgba(0, 0, 0, 0.35);
}

/* Handle sits in the layout flow as a reserved band above the content,
   so it never overlaps the tab controls (playback buttons, etc.). */
.footer-resize-handle {
  flex: 0 0 12px;
  height: 12px;
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: ns-resize;
  touch-action: none;
  background: transparent;
  transition: background 0.15s ease;
}

.footer-content {
  flex: 1 1 auto;
  min-height: 0;
}

.footer-resize-handle:hover,
.footer-resize-handle.is-resizing {
  background: rgba(255, 255, 255, 0.06);
}

.footer-resize-grip {
  width: 44px;
  height: 4px;
  border-radius: 2px;
  background: rgba(255, 255, 255, 0.25);
  transition: background 0.15s ease;
}

.footer-resize-handle:hover .footer-resize-grip,
.footer-resize-handle.is-resizing .footer-resize-grip {
  background: rgba(255, 255, 255, 0.55);
}

.map-drawer-toggle {
  position: absolute;
  top: 12px;
  z-index: 2;
  border-radius: 8px;
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.15);
}

.footer-rail {
  width: 150px;
  background: rgba(255, 255, 255, 0.03);
  border-right: 1px solid rgba(255, 255, 255, 0.08);
}

.footer-rail-track {
  display: flex;
  flex-direction: column;
  width: 100%;
  gap: 2px;
  padding: 6px;
}

.footer-rail-item {
  height: 32px;
  padding: 0 10px;
  border-radius: 8px !important;
  font-size: 0.75rem;
  letter-spacing: 0;
  opacity: 0.65;
  transition: opacity 150ms ease, color 150ms ease, background 150ms ease;
  /* UButton centres its leading icon + label as a group by default; override so all rows share a left edge regardless of label length */
  justify-content: flex-start;
}

.footer-rail-item {
  justify-content: flex-start;
}

.footer-rail-item:hover {
  opacity: 0.9;
}

.footer-rail-item--active {
  opacity: 1;
  font-weight: 600;
  color: rgb(var(--v-theme-primary));
  background: rgba(var(--v-theme-primary), 0.16);
}

/* Explore's sub-views, nested directly beneath the Explore row while it's active. */
.footer-rail-sublist {
  display: flex;
  flex-direction: column;
  gap: 1px;
  padding-left: 14px;
  margin: 1px 0 3px;
  border-left: 1px solid rgba(255, 255, 255, 0.1);
}

.footer-rail-subitem {
  height: 26px;
  padding: 0 8px;
  border-radius: 6px !important;
  font-size: 0.7rem;
  letter-spacing: 0;
  opacity: 0.55;
  transition: opacity 150ms ease, color 150ms ease, background 150ms ease;
  justify-content: flex-start;
}

.footer-rail-subitem {
  justify-content: flex-start;
}

.footer-rail-subitem:hover {
  opacity: 0.85;
}

.footer-rail-subitem--active {
  opacity: 1;
  font-weight: 600;
  color: rgb(var(--v-theme-primary));
  background: rgba(var(--v-theme-primary), 0.12);
}

.cursor-coord-label {
  position: absolute;
  z-index: 998;
  pointer-events: none;
  transform: translate(-14px, 14px);
  width: fit-content;
  padding: 3px 6px;
  border-radius: 6px;
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.2);
  font-family: monospace;
  font-size: 11px;
  color: #ccc;
  white-space: nowrap;
}
</style>

/* Shrink Mapbox bottom-left controls (logo + attribution) to reduce visual footprint */
<style>
.mapboxgl-ctrl-bottom-left .mapboxgl-ctrl-logo {
  transform: scale(0.5) translateY(1px) !important;
  transform-origin: left center !important;
}

/* make sure the controls remain clickable when scaled */
.mapboxgl-ctrl-bottom-left a {
  pointer-events: auto;
}

.footer-text {
  font-family: "Roboto Mono", monospace;
  font-size: 0.75rem;
  vertical-align: text-bottom;
}

@keyframes map-click-pulse {
  0% {
    box-shadow: 0 0 0 0 rgba(255, 87, 34, 0.7);
  }

  70% {
    box-shadow: 0 0 0 14px rgba(255, 87, 34, 0);
  }

  100% {
    box-shadow: 0 0 0 0 rgba(255, 87, 34, 0);
  }
}

.map-click-marker {
  width: 12px;
  height: 12px;
  border-radius: 50%;
  background: #ff5722;
  border: 2px solid white;
  animation: map-click-pulse 1.5s ease-out infinite;
}
</style>

<style scoped>
.h-screen {
  height: calc(100vh - 48px);
}

.overlay {
  position: absolute;
  z-index: 998;
}

.spiderfy-node {
  position: absolute;
  transform: translate(-50%, -50%);
  cursor: pointer;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 3px;
}

.spiderfy-dot {
  width: 13px;
  height: 13px;
  border: 2px solid #333;
  border-radius: 50%;
  transition: transform 0.12s ease;
}

.spiderfy-node:hover .spiderfy-dot {
  transform: scale(1.35);
}

.spiderfy-label {
  color: #fff;
  font-size: 10px;
  white-space: nowrap;
  max-width: 110px;
  overflow: hidden;
  text-overflow: ellipsis;
  text-shadow: 0 1px 3px #000, 0 0 5px #000;
  pointer-events: none;
}
</style>
