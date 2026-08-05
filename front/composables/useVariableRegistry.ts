// The app's variable vocabulary, sourced from `shared/variable_config.yml`
// via `GET /variables` (already loaded into `mainStore.variables`).
//
// Two jobs, deliberately answered from the same place so they can't disagree:
//
//   - What is this variable called?  `variableLabel()` returns the config's
//     `name`, so no component carries its own label table. A registry miss
//     falls back to Title Case rather than rendering a raw snake_case key.
//   - Should the app show it at all?  `isModelVariable()`. Sensor ingestion
//     stores whatever a source publishes — `chlorophyll` and
//     `co2_partial_pressure` have no SSC model counterpart, and `time`/`depth`
//     are axis entries that only exist to tell the ingester which column to
//     read (see `sensors/erddap_to_ch.py`'s `depth_axis_unit`, which needs the
//     `depth` entry's `dbar` unit to trigger the pressure→depth conversion).
//     None of them belong in a variable list, and none are in the config, so
//     membership is the whole test — no separate exclusion list to maintain.

import { computed } from 'vue';
import { useMainStore } from '@/stores/main';
import { var2name } from './useVar2Name';

export function useVariableRegistry() {
    const mainStore = useMainStore();

    // var -> display name. Built from every source's variable list; the
    // canonical IDs are shared across sources, so later entries just re-assert
    // the same label.
    const labels = computed(() => {
        const map = new Map<string, string>();
        for (const v of mainStore.variables) {
            if (v?.var) map.set(v.var, v.name || var2name(v.var));
        }
        return map;
    });

    /** True when the variable is one the app models and displays. */
    function isModelVariable(varId: string): boolean {
        return labels.value.has(varId);
    }

    function variableLabel(varId: string): string {
        return labels.value.get(varId) ?? var2name(varId);
    }

    /**
     * A sensor's variables narrowed to the ones the app shows, in config order
     * so every sensor lists them consistently rather than in per-source
     * ingestion order.
     */
    function modelVariablesOf(variables: Record<string, unknown> | null | undefined): string[] {
        const present = new Set(Object.keys(variables ?? {}));
        return mainStore.variables
            .map(v => v.var)
            .filter((varId, i, arr) => arr.indexOf(varId) === i && present.has(varId));
    }

    return { labels, isModelVariable, variableLabel, modelVariablesOf };
}
