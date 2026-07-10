import type moment from 'moment';
import { defineStore } from 'pinia'
import colors from 'vuetify/util/colors';

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
        variables: [] as Array<{ var: string, source: string, dts: number[], colormap: string | null, colormapMin: number, colormapMax: number, depths: number[], precision: number, bounds: [number, number, number, number] }>,
        selected_variable: { var: '', source: '', dt: null as moment.Moment | null, depth: null as string | null, depth_nc: null as number | null, precision: null as number | null, colormap: null as string | null, colormapMin: null as number | null, colormapMax: null as number | null, colormapStops: [null, null, null] as (number | null)[] },
        showBathymetryContours: false,
        colormaps: {} as Record<string, any>,
        autoRangeDisabled: false,

        /**
         * The midDate is used to determine the middle point of the time range the footer chart displays. It is set to now initially.
         */
        midDate: null as moment.Moment | null,

        sensors: [] as Array<{ id: string, name: string, latitude: number, longitude: number, depth: number, device_config: {}, variables: {}, active: boolean, first_data_at: string | null, latest_data_at: string | null, source: { api?: string }, organization: string }>,
        selectedSensor: {} as { id: string, depth: number } | null,

        lastClickedMapPoint: null as { lat: number, lng: number } | null,

        mapCenter: null as { lat: number, lng: number } | null,

        snackMessages: [] as Array<{ color: string, text: string }>,

        controlPanel_width: 300,
        isControlPanelOpen: true,

        showColorbarSettings: false,
        showClimatologyDialog: false,

        queryMode: 'point' as 'point' | 'area',

        showCursorCoords: true,

        activeBottomTab: 'timeseries' as 'timeseries' | 'analysis' | 'comparison',
    }),

    actions: {
        setVariables(vars: Array<{ var: string, source: string, dts: number[], colormap: string | null, colormapMin: number, colormapMax: number, depths: number[], precision: number, bounds: [number, number, number, number] }>) {
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

        setSensors(sensors: Array<{ id: string, name: string, latitude: number, longitude: number, depth: number, device_config: {}, variables: {}, active: boolean, first_data_at: string | null, latest_data_at: string | null, source: { api?: string }, organization: string }>) {
            this.sensors = sensors;
        },
        setSelectedSensor(sensor: { id: string, depth: number } | null) {
            this.selectedSensor = sensor;
        },
        /**
         * Select a sensor: snap to closest available depth and set as active sensor.
         * Can be called from any component (sensorInfo, map click handler, etc.)
         */
        selectSensor(sensor_id: string, depth: number) {
            console.log("Selecting sensor", sensor_id, "at depth", depth);
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

        setShowClimatologyDialog(value: boolean) {
            this.showClimatologyDialog = value;
        },

        setQueryMode(mode: 'point' | 'area') {
            this.queryMode = mode;
        },

        setActiveBottomTab(tab: 'timeseries' | 'analysis' | 'comparison') {
            this.activeBottomTab = tab;
        },
    }
})