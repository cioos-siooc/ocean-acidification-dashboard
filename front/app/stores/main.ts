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
        selected_variable: { var: '', source: '', dt: null as moment.Moment | null, depth: null as number | null, precision: null as number | null, colormap: null as string | null, colormapMin: null as number | null, colormapMax: null as number | null },
        showBathymetryContours: false,
        colormaps: {} as Record<string, any>,
        autoRangeDisabled: false,

        /**
         * The midDate is used to determine the middle point of the time range the footer chart displays. It is set to now initially.
         */
        midDate: null as moment.Moment | null
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
        }
    }
})