import type moment from 'moment';
import { defineStore } from 'pinia'

export const useMainStore = defineStore('main', {
    state: () => ({
        variables: [] as Array<{ var: string, dts: moment.Moment[], min: number, max: number, depths: number[] }>,
        selected_variable: { var: '', dt: null as moment.Moment | null, depth: null as number | null },
    }),

    actions: {
        setVariables(vars: Array<{ var: string, dts: moment.Moment[], min: number, max: number, depths: number[] }>) {
            this.variables = vars;
        },

        setSelectedVariable(varId: string, dt: moment.Moment | null, depth: number | null) {
            this.selected_variable = { var: varId, dt: dt, depth: depth };
            console.log('this.selected_variable: ', this.selected_variable);
        }
    }
})