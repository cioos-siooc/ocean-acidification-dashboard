<template>
    <div style="display: flex; align-items: flex-end; gap: 6px;">
        <div style="flex: 1; min-width: 0;">
            <div class="bar" :style="barStyle"></div>
            <div class="ticks">
                <div class="tick left text-label-small">{{ displayMin?.toFixed(precisionDigits) }}</div>
                <div class="tick center text-label-small">{{ colormapAvg }}</div>
                <div class="tick right text-label-small">{{ displayMax?.toFixed(precisionDigits) }}</div>
            </div>
        </div>
        <div style="flex: 0 0 auto; padding-bottom: 2px;">
            <v-menu v-if="unitOptions.length > 1" location="top">
                <template #activator="{ props: menuProps }">
                    <button v-bind="menuProps" type="button" class="unit-label unit-label--clickable">
                        {{ unit }}<v-icon size="10px" class="ml-1">mdi-menu-down</v-icon>
                    </button>
                </template>
                <v-list density="compact" min-width="0">
                    <v-list-item v-for="u in unitOptions" :key="u" :active="u === unit"
                        @click="mainStore.setUnitPreference(selectedVariable.var, u)">
                        <v-list-item-title class="text-caption">{{ u }}</v-list-item-title>
                    </v-list-item>
                </v-list>
            </v-menu>
            <span v-else class="unit-label">{{ unit }}</span>
        </div>
    </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { useMainStore } from '../stores/main';
import { resolveColormap } from '~~/composables/useColormapResolver';
import { useVariableRegistry } from '~~/composables/useVariableRegistry';

const mainStore = useMainStore();
const { displayUnit, toDisplayValue, variableUnit, variableAltUnits } = useVariableRegistry();

const selectedVariable = computed(() => mainStore.selected_variable);

const unit = computed(() => displayUnit(selectedVariable.value.var));

// Canonical unit + whatever alternates variable_config.yml defines (empty
// for the other 6 variables) — clicking the unit label opens this list
// directly on the legend, rather than the toggle living behind Color Settings.
const unitOptions = computed(() => [
    variableUnit(selectedVariable.value.var),
    ...variableAltUnits(selectedVariable.value.var).map(u => u.unit),
]);

const colormapMin = computed(() => selectedVariable.value.colormapMin);
const colormapMax = computed(() => selectedVariable.value.colormapMax);
const displayMin = computed(() => toDisplayValue(selectedVariable.value.var, colormapMin.value));
const displayMax = computed(() => toDisplayValue(selectedVariable.value.var, colormapMax.value));
const precisionDigits = computed(() => -Math.log10(selectedVariable.value.precision));
const colormapAvg = computed(() => {
    if (!colormapMin.value || !colormapMax.value) return '';
    const avg = (colormapMin.value + colormapMax.value) / 2;
    return toDisplayValue(selectedVariable.value.var, avg)?.toFixed(precisionDigits.value);
});

const selectedColormap = computed(() => selectedVariable.value.colormap);
const colormaps = computed(() => mainStore.colormaps);

const barStyle = computed(() => {
    const palette = resolveColormap(colormaps.value, selectedColormap.value)?.stops;
    const stops = palette?.map(s => `${s[1]} ${Math.round(s[0] * 100)}%`).join(', ');
    return {
        background: `linear-gradient(90deg, ${stops})`,
    };
});
</script>

<style scoped>
.bar {
    height: 14px;
    border-radius: 4px;
    border: 1px solid rgba(0, 0, 0, 0.08);
}

.ticks {
    display: flex;
    justify-content: space-between;
    margin-top: 6px;
}

.tick {
    color: #ccc;
}

.unit-label {
    display: inline-flex;
    align-items: center;
    font-size: x-small;
    color: #aaa;
}

.unit-label--clickable {
    background: none;
    border: none;
    padding: 0;
    cursor: pointer;
}

.unit-label--clickable:hover {
    color: #e0e0e0;
}
</style>
