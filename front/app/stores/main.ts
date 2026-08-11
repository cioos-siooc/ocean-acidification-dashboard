import type moment from 'moment';
import { defineStore } from 'pinia'
import colors from 'vuetify/util/colors';
import { trackEvent } from '~~/composables/useAnalytics';

// Depths are plain numbers (the WebP filename stem too, e.g. 18.0 -> "18.0.webp")
// — -1 is the synthetic bottom-layer sentinel ("bottom.webp").
export function formatDepthLabel(depth: number): string {
    return depth === -1 ? 'bottom' : depth.toFixed(1);
}

export const useMainStore = defineStore('main', {
    state: () => ({
        colors: {
            model: {
                line: colors.red.lighten3,
                shadow: colors.red.lighten4,
                shadowBlur: 3
            },
            observation: {
                line: colors.green.lighten3,
                shadow: colors.green.lighten4,
                shadowBlur: 3
            },
            stats: colors.blue.darken2,
        },

        dfnDays: 14, // days from now for climate timeseries
        variables: [] as Array<{ var: string, name: string, source: string, dts: number[], colormap: string | null, colormapMin: number, colormapMax: number, depths: number[], precision: number, bounds: [number, number, number, number] }>,
        selected_variable: { var: '', source: '', dt: null as moment.Moment | null, depth: null as string | null, depth_nc: null as number | null, precision: null as number | null, colormap: null as string | null, colormapMin: null as number | null, colormapMax: null as number | null, colormapStops: [null, null, null] as (number | null)[] },
        showBathymetryContours: false,
        colormaps: {} as Record<string, any>,
        autoRangeDisabled: false,

        /**
         * The midDate is used to determine the middle point of the time range the footer chart displays. It is set to now initially.
         */
        midDate: null as moment.Moment | null,

        sensors: [] as Array<{ id: string, name: string, latitude: number, longitude: number, depth: number, depth_min: number | null, depth_max: number | null, device_config: {}, variables: {}, active: boolean, first_data_at: string | null, latest_data_at: string | null, source: { api?: string, link?: string, description?: string }, organization: string }>,
        selectedSensor: {} as { id: string, depth: number } | null,

        // Sensor panel filters — shared with the map so markers stay in sync with the sensor list.
        sensorSearchQuery: '' as string,
        sensorOrganizationFilter: [] as string[],
        sensorVariableFilter: [] as string[],

        lastClickedMapPoint: null as { lat: number, lng: number } | null,

        mapCenter: null as { lat: number, lng: number } | null,

        snackMessages: [] as Array<{ color: string, text: string }>,

        controlPanel_width: 300,
        isControlPanelOpen: true,

        showColorbarSettings: false,

        queryMode: 'point' as 'point' | 'area',

        showCursorCoords: false,

        // 'explore' is the only footer pane — the one view that reads the map's
        // coordinate, depth and clock. 'analysis'/'comparison' are fullscreen
        // workspaces: they take a coordinate as input and then have nothing more
        // to say to the map, so showing the map behind them is wasted space.
        activeBottomTab: 'explore' as 'explore' | 'analysis' | 'comparison',

        // Which record the Analysis tab runs against. Was a whole separate tab
        // ("Sensor Analysis") until the two were merged behind one surface.
        analysisSource: 'model' as 'model' | 'sensor',

        // Which of Explore's sub-views is showing: the single-depth timeseries,
        // or a full-water-column section for the model or a profiler sensor.
        // Lives in the store (not a local ExplorePanel ref) because the footer's
        // nav rail in index.vue is what drives it now, one level up from the panel.
        exploreView: 'series' as 'series' | 'model-depth' | 'sensor-depth',

        // ExplorePanel's own bin-mode toggle (1H/1D/1M), mirrored here so the
        // vertical profile drawer — a sibling of ExplorePanel under index.vue,
        // not a child of it — can request a profile aggregated the same way
        // the depth section currently on screen is.
        exploreBinMode: 'hourly' as 'hourly' | 'daily' | 'monthly',
    }),

    getters: {
        // A *profiler* sensor (registry depth === -1) is the only kind that casts
        // through the whole water column, so it's the only kind with a "Sensor
        // depth" section to show. Centralised here rather than re-derived per
        // component (ExplorePanel's section fetch, index.vue's nav rail) so both
        // agree on the exact same definition of "profiler selected".
        selectedProfilerSensorId(state): string | null {
            const sel = state.selectedSensor;
            if (!sel?.id) return null;
            return state.sensors.find((s) => s.id === sel.id)?.depth === -1 ? sel.id : null;
        },

        filteredSensors(state) {
            const q = state.sensorSearchQuery.trim().toLowerCase();
            return state.sensors.filter((sensor) => {
                if (q && !sensor.name.toLowerCase().includes(q) && !(sensor.organization ?? '').toLowerCase().includes(q)) {
                    return false;
                }
                if (state.sensorOrganizationFilter.length && !state.sensorOrganizationFilter.includes(sensor.organization)) {
                    return false;
                }
                if (state.sensorVariableFilter.length) {
                    const sensorVars = Object.keys(sensor.variables ?? {});
                    if (!state.sensorVariableFilter.some(v => sensorVars.includes(v))) return false;
                }
                return true;
            });
        },
    },

    actions: {
        setVariables(vars: Array<{ var: string, name: string, source: string, dts: number[], colormap: string | null, colormapMin: number, colormapMax: number, depths: number[], precision: number, bounds: [number, number, number, number] }>) {
            this.variables = vars;
        },

        updateSelectedVariable(partial: Partial<typeof this.selected_variable>) {
            // Use individual property assignment to ensure Pinia tracks mutations properly
            for (const [key, value] of Object.entries(partial)) {
                (this.selected_variable as any)[key] = value;
            }
        },

        setShowBathymetryContours(value: boolean) {
            this.showBathymetryContours = value;
        },

        setShowCursorCoords(value: boolean) {
            this.showCursorCoords = value;
        },

        setColormaps(colormaps: Record<string, any>) {
            this.colormaps = colormaps;
        },

        setAutoRangeDisabled(value: boolean) {
            this.autoRangeDisabled = value;
        },

        setMidDate(date: moment.Moment) {
            this.midDate = date;
        },

        setSensors(sensors: Array<{ id: string, name: string, latitude: number, longitude: number, depth: number, depth_min: number | null, depth_max: number | null, device_config: {}, variables: {}, active: boolean, first_data_at: string | null, latest_data_at: string | null, source: { api?: string, link?: string, description?: string }, organization: string }>) {
            this.sensors = sensors;
        },
        setSelectedSensor(sensor: { id: string, depth: number } | null) {
            this.selectedSensor = sensor;
        },
        setSensorSearchQuery(query: string) {
            this.sensorSearchQuery = query;
        },
        setSensorOrganizationFilter(orgs: string[]) {
            this.sensorOrganizationFilter = orgs;
        },
        setSensorVariableFilter(vars: string[]) {
            this.sensorVariableFilter = vars;
        },
        /**
         * Select a sensor: snap to closest available depth and set as active sensor.
         * Can be called from any component (sensorInfo, map click handler, etc.)
         */
        selectSensor(sensor_id: string, depth: number) {
            console.log("Selecting sensor", sensor_id, "at depth", depth);
            // depth === -1 here is the sensor registry's "profiles the water column"
            // sentinel (see sensorComparison.vue's isVariableDepth) — a different meaning
            // than the model's own -1 "bottom" pseudo-level in `depthsArray` below. There's
            // no single fixed depth to snap the model view to for a profiler, so skip it;
            // the model's currently-selected depth is left untouched.
            if (depth !== -1) {
                const variable = this.selected_variable.var;
                const depthsArray = this.variables.find((v) => v.var === variable)?.depths;
                const closestDepth = depthsArray
                    ? [...depthsArray].sort((a, b) => Math.abs(a - depth) - Math.abs(b - depth))
                    : [];
                if (closestDepth.length > 0) {
                    const newDepthNc = closestDepth[0] ?? null;
                    const newDepth = newDepthNc !== null ? formatDepthLabel(newDepthNc) : null;
                    if (newDepth && newDepth !== this.selected_variable.depth) {
                        this.snackMessages.push({ color: 'warning', text: `Switched to closest available depth: ${newDepth}m` });
                        this.updateSelectedVariable({ depth: newDepth, depth_nc: newDepthNc });
                    }
                }
            }
            // Deliberately does not navigate. The sensor's series appears overlaid
            // on Explore's timeseries, which is the map-synced answer to "what did
            // I just click"; opening a fullscreen workspace would cover the map the
            // user is still working with.
            this.setSelectedSensor({ id: sensor_id, depth: depth });
        },

        setLastClickedMapPoint(point: { lat: number, lng: number } | null) {
            this.lastClickedMapPoint = point;
        },

        setMapCenter(center: { lat: number, lng: number } | null) {
            this.mapCenter = center;
        },

        pushSnack(message: { color: string, text: string }) {
            this.snackMessages.push(message);
        },

        toggleIsControlPanelOpen() {
            this.isControlPanelOpen = !this.isControlPanelOpen;
        },

        setShowColorbarSettings(value: boolean) {
            this.showColorbarSettings = value;
        },

        setQueryMode(mode: 'point' | 'area') {
            if (mode !== this.queryMode) {
                trackEvent('query_mode_changed', { mode });
            }
            this.queryMode = mode;
        },

        setActiveBottomTab(tab: 'explore' | 'analysis' | 'comparison') {
            if (tab !== this.activeBottomTab) {
                trackEvent('tab_switched', { from_tab: this.activeBottomTab, to_tab: tab });
            }
            this.activeBottomTab = tab;
        },

        setAnalysisSource(source: 'model' | 'sensor') {
            if (source !== this.analysisSource) {
                trackEvent('analysis_source_changed', { source });
            }
            this.analysisSource = source;
        },

        setExploreView(view: 'series' | 'model-depth' | 'sensor-depth') {
            if (view !== this.exploreView) {
                trackEvent('explore_view_changed', { view });
            }
            this.exploreView = view;
        },

        // No analytics event here, unlike the setters above — this fires on
        // every bin-mode toggle click, a display-density choice rather than a
        // discrete user action worth counting (same reasoning PostHog's own
        // exclusions use for high-frequency, non-navigational state changes).
        setExploreBinMode(mode: 'hourly' | 'daily' | 'monthly') {
            this.exploreBinMode = mode;
        },
    }
})