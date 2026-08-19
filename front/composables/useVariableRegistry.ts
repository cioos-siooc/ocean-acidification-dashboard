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

    // var -> unit (e.g. "°C", "mmol/m³"), same config-sourced list as `labels`.
    // Unitless variables (pH, Omega) resolve to '' — callers skip the suffix rather
    // than rendering an empty parenthetical.
    const units = computed(() => {
        const map = new Map<string, string>();
        for (const v of mainStore.variables) {
            if (v?.var) map.set(v.var, (v as { unit?: string }).unit ?? '');
        }
        return map;
    });

    function variableUnit(varId: string): string {
        return units.value.get(varId) ?? '';
    }

    // Alternate display units for a variable (e.g. DO's mg/L, mL/L; temperature's
    // °F), sourced from variable_config.yml's `alt_units`. Empty for the other
    // variables — those keep a single, non-toggleable canonical unit.
    function variableAltUnits(varId: string): Array<{ unit: string; scale: number; offset: number }> {
        const v = mainStore.variables.find(v => v.var === varId);
        return (v?.alt_units ?? []).map(u => ({ unit: u.unit, scale: u.scale, offset: u.offset ?? 0 }));
    }

    // The unit currently chosen for display — the user's preference if they've
    // toggled one, otherwise the canonical unit. This is what every chart/label
    // should render, in place of `variableUnit()`.
    function displayUnit(varId: string): string {
        return mainStore.unitPreference[varId] ?? variableUnit(varId);
    }

    /** Canonical (ClickHouse/model) value -> the unit `displayUnit()` currently reports. */
    function toDisplayValue(varId: string, canonicalValue: number | null | undefined): number | null {
        if (canonicalValue === null || canonicalValue === undefined) return null;
        const target = mainStore.unitPreference[varId];
        const alt = target ? variableAltUnits(varId).find(u => u.unit === target) : undefined;
        return alt ? canonicalValue * alt.scale + alt.offset : canonicalValue;
    }

    /** Inverse of `toDisplayValue()` — a value typed/dragged in the display unit, back to canonical. */
    function toCanonicalValue(varId: string, displayValue: number | null | undefined): number | null {
        if (displayValue === null || displayValue === undefined) return null;
        const target = mainStore.unitPreference[varId];
        const alt = target ? variableAltUnits(varId).find(u => u.unit === target) : undefined;
        return alt ? (displayValue - alt.offset) / alt.scale : displayValue;
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

    return {
        labels, isModelVariable, variableLabel, variableUnit, modelVariablesOf,
        variableAltUnits, displayUnit, toDisplayValue, toCanonicalValue,
    };
}
