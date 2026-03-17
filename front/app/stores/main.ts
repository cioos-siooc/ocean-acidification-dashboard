import type moment from 'moment';
import { defineStore } from 'pinia'

export const useMainStore = defineStore('main', {
    state: () => ({
        variables: [] as Array<{ var: string, dts: moment.Moment[], min: number, max: number, depths: number[], precision: number }>,
        selected_variable: { var: '', dt: null as moment.Moment | null, depth: null as number | null, precision: null as number | null },
        showBathymetryContours: false,
    }),

    actions: {
        setVariables(vars: Array<{ var: string, dts: moment.Moment[], min: number, max: number, depths: number[], precision: number }>) {
            this.variables = vars;
        },

        setSelectedVariable(varId: string, dt: moment.Moment | null, depth: number | null, precision: number | null) {
            this.selected_variable.var = varId;
            this.selected_variable.dt = dt;
            this.selected_variable.depth = depth;
            this.selected_variable.precision = precision;
        },

        setShowBathymetryContours(value: boolean) {
            this.showBathymetryContours = value;
        }
    }
})