<template>
    <!-- <v-dialog v-model="showColorbarSettings" max-width="500px" transition="dialog-transition"> -->
    <v-card width="420px" class="ma-0 py-3 px-5">
        <!-- RANGE -->
        <div class="d-flex align-center ga-2 mt-4 mb-1">
            <v-text-field v-model="minText" type="number" :step="numberStep"
                hide-details class="range-input" @blur="commitMin" @keyup.enter="commitMin" />
            <v-range-slider v-model="sliderEnds" thumb-label strict hide-details class="flex-grow-1">
                <template #thumb-label="{ modelValue }">
                    {{ (default_colormapMin + (default_colormapMax - default_colormapMin) * (modelValue / 100)).toFixed(precisionDigits) }}
                </template>
            </v-range-slider>
            <v-text-field v-model="maxText" type="number" :step="numberStep"
                hide-details class="range-input" @blur="commitMax" @keyup.enter="commitMax" />
        </div>

        <div class="bar-track">
            <div class="bar" :style="barStyle"></div>
        </div>

        <!-- PALETTE -->
        <PalettePicker v-model="selectedColormap" class="mt-4" />

        <v-card-actions class="pa-0 mt-2">
            <v-spacer></v-spacer>
            <v-btn color="error" variant="tonal" @click="resetToDefaults">
                Reset to Defaults
            </v-btn>
            <v-btn color="primary" variant="tonal" @click="showColorbarSettings = false">
                Close
            </v-btn>
        </v-card-actions>
    </v-card>
    <!-- </v-dialog> -->
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { useMainStore } from '../stores/main'
import { resolveColormap } from '../../composables/useColormapResolver';
const mainStore = useMainStore();

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

const minText = ref(formatValue(colormapMin.value));
const maxText = ref(formatValue(colormapMax.value));

watch(colormapMin, (v) => { minText.value = formatValue(v); });
watch(colormapMax, (v) => { maxText.value = formatValue(v); });

function formatValue(v: number | null) {
    return v === null || v === undefined ? '' : Number(v).toFixed(precisionDigits.value);
}

function commitMin() {
    const v = parseFloat(minText.value);
    if (!Number.isNaN(v)) colormapMin.value = v;
    else minText.value = formatValue(colormapMin.value);
}

function commitMax() {
    const v = parseFloat(maxText.value);
    if (!Number.isNaN(v)) colormapMax.value = v;
    else maxText.value = formatValue(colormapMax.value);
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
