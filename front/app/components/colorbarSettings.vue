<template>
    <div class="bg-elevated rounded-lg m-0 py-3 px-5" style="width:420px">
        <!-- RANGE — the unit toggle itself lives on the map's colorbar legend
             (ColormapBar.vue), which is always visible; these fields just
             follow whatever unit is chosen there via toDisplayValue/toCanonicalValue. -->
        <div class="flex items-center gap-2 mt-4 mb-1">
            <UInput v-model="minText" type="number" :step="numberStep" class="range-input" @blur="commitMin" @keyup.enter="commitMin" />
            <!-- USlider has no per-thumb label slot, so the old #thumb-label (which
                 mapped the 0-100 position back to a display value) is gone. The
                 min/max number inputs either side already show those values. -->
            <USlider v-model="sliderEnds" :min="0" :max="100" class="grow" />
            <UInput v-model="maxText" type="number" :step="numberStep" class="range-input" @blur="commitMax" @keyup.enter="commitMax" />
        </div>

        <div class="bar-track">
            <div class="bar" :style="barStyle"></div>
        </div>

        <!-- PALETTE -->
        <PalettePicker v-model="selectedColormap" class="mt-4" />

        <div class="flex items-center gap-2 p-0 mt-2">
            <div class="grow" />
            <UButton variant="subtle" color="error" @click="resetToDefaults">
                Reset to Defaults
            </UButton>
            <UButton variant="subtle" color="primary" @click="showColorbarSettings = false">
                Close
            </UButton>
        </div>
    </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { useMainStore } from '../stores/main'
import { resolveColormap } from '~~/composables/useColormapResolver';
import { useVariableRegistry } from '~~/composables/useVariableRegistry';
const mainStore = useMainStore();
const { toDisplayValue, toCanonicalValue } = useVariableRegistry();

////////////////////////////////////////  COMPUTED  ///////////////////////////////////

const sliderEnds = computed({
    get() {
        const mn = default_colormapMin.value;
        const mx = default_colormapMax.value;
        const rng = mx - mn || 1.0;
        return [
            ((colormapMin.value ?? mn) - mn) / rng * 100,
            ((colormapMax.value ?? mx) - mn) / rng * 100
        ] as [number, number];
    },
    set([min, max]: [number, number]) {
        const mn = default_colormapMin.value;
        const mx = default_colormapMax.value;
        const rng = mx - mn || 1.0;
        mainStore.updateSelectedVariable({
            colormapMin: mn + rng * (min / 100),
            colormapMax: mn + rng * (max / 100)
        });
    }
});

const barWidth = computed(() => {
    return sliderEnds.value[1] - sliderEnds.value[0] + '%'
});
const barLeft = computed(() => {
    return sliderEnds.value[0] + '%'
});

const showColorbarSettings = computed({
    get: () => mainStore.showColorbarSettings,
    set: (val: boolean) => mainStore.setShowColorbarSettings(val)
});

const selectedVariable = computed(() => mainStore.selected_variable);

const colormaps = computed(() => mainStore.colormaps);

const barStyle = computed(() => {
    const palette = resolveColormap(colormaps.value, selectedColormap.value)?.stops
    const stops = palette?.map(s => `${s[1]} ${Math.round(s[0] * 100)}%`).join(', ');
    return {
        position: 'absolute' as const,
        width: barWidth.value,
        left: barLeft.value,
        background: `linear-gradient(90deg, ${stops})`,
    };
});

const precisionDigits = computed(() => -Math.log10(selectedVariable.value.precision));

const numberStep = computed(() => selectedVariable.value.precision ?? 'any');

const selectedColormap = computed({
    get() { return selectedVariable.value.colormap },
    set(v: string | null) { mainStore.updateSelectedVariable({ colormap: v }) }
});

const colormapMin = computed({
    get() { return selectedVariable.value.colormapMin },
    set(v: number | null) { mainStore.updateSelectedVariable({ colormapMin: v }) }
});
const colormapMax = computed({
    get() { return selectedVariable.value.colormapMax },
    set(v: number | null) { mainStore.updateSelectedVariable({ colormapMax: v }) }
});

const variables = computed(() => mainStore.variables);

// Default variable bounds
const default_colormapMin = computed(() => variables.value.find(v => v.var === selectedVariable.value.var && v.source === selectedVariable.value.source)?.colormapMin ?? 0);
const default_colormapMax = computed(() => variables.value.find(v => v.var === selectedVariable.value.var && v.source === selectedVariable.value.source)?.colormapMax ?? 1);

///////////////////////////////////  RANGE TEXT INPUTS  ///////////////////////////////////

function formatDisplay(canonicalValue: number | null) {
    const v = toDisplayValue(selectedVariable.value.var, canonicalValue);
    return v === null ? '' : v.toFixed(precisionDigits.value);
}

const minText = ref(formatDisplay(colormapMin.value));
const maxText = ref(formatDisplay(colormapMax.value));

watch(colormapMin, (v) => { minText.value = formatDisplay(v); });
watch(colormapMax, (v) => { maxText.value = formatDisplay(v); });
// Same canonical range, different label, when the unit toggle (on the
// colorbar legend) changes.
watch(() => mainStore.unitPreference[selectedVariable.value.var], () => {
    minText.value = formatDisplay(colormapMin.value);
    maxText.value = formatDisplay(colormapMax.value);
});

function commitMin() {
    const v = parseFloat(minText.value);
    if (!Number.isNaN(v)) colormapMin.value = toCanonicalValue(selectedVariable.value.var, v);
    else minText.value = formatDisplay(colormapMin.value);
}

function commitMax() {
    const v = parseFloat(maxText.value);
    if (!Number.isNaN(v)) colormapMax.value = toCanonicalValue(selectedVariable.value.var, v);
    else maxText.value = formatDisplay(colormapMax.value);
}

///////////////////////////////////  METHODS  ///////////////////////////////////

function resetToDefaults() {
    // Reset adjustable bounds to the variable's default bounds
    mainStore.updateSelectedVariable({
        colormapMin: default_colormapMin.value,
        colormapMax: default_colormapMax.value
    });
    sliderEnds.value = [0, 100];
}
</script>


<style scoped>
.range-input {
    max-width: 92px;
}

.range-input :deep(input) {
    text-align: center;
    padding-top: 6px;
    padding-bottom: 6px;
    padding-left: 4px;
    padding-right: 4px;
}

.bar-track {
    position: relative;
    height: 14px;
    margin-top: 4px;
}

.bar {
    height: 14px;
    border-radius: 4px;
    border: 1px solid rgba(0, 0, 0, 0.08);
}
</style>
