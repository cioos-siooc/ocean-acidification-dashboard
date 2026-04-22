import type moment from 'moment';
import { defineStore } from 'pinia'
import colors from 'vuetify/util/colors';

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

        dfnDays: 5, // days from now for climate timeseries
        variables: [] as Array<{ var: string, source: string, dts: number[], colormap: string | null, colormapMin: number, colormapMax: number, depths: { depth: number, hasImage: boolean }[], precision: number }>,
        selected_variable: { var: '', source: '', dt: null as moment.Moment | null, depth: null as number | null, precision: null as number | null, colormap: null as string | null, colormapMin: null as number | null, colormapMax: null as number | null, colormapStops: [null, null, null] as (number | null)[] },
        showBathymetryContours: false,
        colormaps: {} as Record<string, any>,
        autoRangeDisabled: false,

        /**
         * The midDate is used to determine the middle point of the time range the footer chart displays. It is set to now initially.
         */
        midDate: null as moment.Moment | null,

        sensors : [] as Array<{ id: number, name: string, latitude: number, longitude: number, depth: number, variables: string[], active: boolean }>,
        selectedSensorID: null as number | null,

        lastClickedMapPoint: null as { lat: number, lng: number } | null,

        mapCenter: null as { lat: number, lng: number } | null,

        snackMessages: [] as Array<{ color: string, text: string }>,

        controlPanel_width: 300,
        isControlPanelOpen: true,

        showColorbarSettings: false,
    }),

    actions: {
        setVariables(vars: Array<{ var: string, source: string, dts: number[], colormap: string | null, colormapMin: number, colormapMax: number, depths: { depth: number, hasImage: boolean }[], precision: number }>) {
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

        setColormaps(colormaps: Record<string, any>) {
            this.colormaps = colormaps;
        },

        setAutoRangeDisabled(value: boolean) {
            this.autoRangeDisabled = value;
        },

        setMidDate(date: moment.Moment) {
            this.midDate = date;
        },

        setSensors(sensors: Array<{ id: number, name: string, latitude: number, longitude: number, depth: number, variables: string[], active: boolean }>) {
            this.sensors = sensors;
        },
        setSelectedSensorID(sensorID: number | null) {
            this.selectedSensorID = sensorID;
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

        /**
         * Select a sensor: snap to closest available depth and set as active sensor.
         * Can be called from any component (sensorInfo, map click handler, etc.)
         */
        selectSensor(sensor_id: number, depth: number) {
            const variable = this.selected_variable.var;
            const depthsArray = this.variables.find((v) => v.var === variable)?.depths;
            const closestDepth = depthsArray
                ? [...depthsArray].sort((a, b) => Math.abs(a.depth - depth) - Math.abs(b.depth - depth))
                : [];
            if (closestDepth.length > 0) {
                const newDepth = closestDepth[0].depth;
                if (newDepth !== this.selected_variable.depth) {
                    this.snackMessages.push({ color: 'warning', text: `Switched to closest available depth: ${newDepth}m` });
                    this.updateSelectedVariable({ depth: newDepth });
                }
            }
            this.setSelectedSensorID(sensor_id);
        },

        toggleIsControlPanelOpen() {
            this.isControlPanelOpen = !this.isControlPanelOpen;
        },

        setShowColorbarSettings(value: boolean) {
            this.showColorbarSettings = value;
        }
    }
})