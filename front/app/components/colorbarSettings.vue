<template>
    <!-- <v-dialog v-model="showColorbarSettings" max-width="500px" transition="dialog-transition"> -->
    <v-card width="400px" class="ma-0 py-3 px-7" >
        <v-row class="ma-0 pa-0 mt-6">
            <v-col cols="11">
                <v-range-slider v-model="sliderEnds" thumb-label="always" strict density="compact" hide-details >
                    <template #thumb-label="{ modelValue }">
                        <!-- <div style="background: white; padding: 2px 4px; border-radius: 3px; font-size: 10px; border: 1px solid rgba(0,0,0,0.1);"> -->
                        {{ (default_colormapMin + (default_colormapMax - default_colormapMin) * (modelValue /
                            100)).toFixed(precisionDigits) }}
                        <!-- </div> -->
                    </template>
                </v-range-slider>
            </v-col>
        </v-row>

        <v-row class="ma-0 pa-0">
            <v-col cols="11">
                <div :style="{ position: 'relative', width: barWidth, left: barLeft, height: '100%' }">
                    <div class="bar" :style="barStyle"></div>
                </div>
            </v-col>

            <v-col cols="1">
                <v-menu height="50%">
                    <template v-slot:activator="{ props }">
                        <v-btn flat size="16px" icon v-bind="props">
                            <v-icon>mdi-menu-down</v-icon>
                        </v-btn>
                    </template>
                    <v-list>
                        <v-list-item v-for="(item, index) in colormaps" :key="index" :value="index"
                            @click="mainStore.updateSelectedVariable({ colormap: index })">
                            <v-list-item-title>
                                <div :style="colormapStyle(item)"></div> {{ item.name }}
                            </v-list-item-title>
                        </v-list-item>
                    </v-list>
                </v-menu>
            </v-col>
        </v-row>

        <v-card-actions class="pa-0">
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
import { computed, ref, onMounted } from 'vue';
import { useMainStore } from '../stores/main'
const mainStore = useMainStore();

const test = ref(0);
watch(test, (newVal) => {
    console.log(newVal);
});

////////////////////////////////////////  COMPUTED  ///////////////////////////////////

// const sliderEnds = ref<[number, number]>([0, 100]);
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
    const palette = colormaps.value[selectedColormap.value]?.stops
    const stops = palette?.map(s => `${s[1]} ${Math.round(s[0] * 100)}%`).join(', ');
    return {
        background: `linear-gradient(90deg, ${stops})`,
        height: '100%'
    };
});

const precisionDigits = computed(() => -Math.log10(selectedVariable.value.precision));

const unit = computed(() => variables.value.find(v => v.var === selectedVariable.value.var)?.unit ?? '');

const selectedColormap = computed({
    get() { return selectedVariable.value.colormap },
    set(v: string | null) { mainStore.updateSelectedVariable({ colormap: v }) }
});

// const colormapMin = computed({
//     get() {
//         return selectedVariable.value.colormapMin
//     },
//     set(v: number | null) { mainStore.updateSelectedVariable({ colormapMin: v }) }
// });
// const colormapMax = computed({
//     get() { return selectedVariable.value.colormapMax },
//     set(v: number | null) { mainStore.updateSelectedVariable({ colormapMax: v }) }
// });
const colormapMin = computed(() => selectedVariable.value.colormapMin);
const colormapMax = computed(() => selectedVariable.value.colormapMax);

const variables = computed(() => mainStore.variables);

// Default variable bounds
const default_colormapMin = computed(() => variables.value.find(v => v.var === selectedVariable.value.var && v.source === selectedVariable.value.source)?.colormapMin ?? 0);
const default_colormapMax = computed(() => variables.value.find(v => v.var === selectedVariable.value.var && v.source === selectedVariable.value.source)?.colormapMax ?? 1);

///////////////////////////////////  METHODS  ///////////////////////////////////

// function updateMinMax([min, max]: [number, number]) {
//     console.log(min,max);
//     const mn = default_colormapMin.value;
//     const mx = default_colormapMax.value;
//     const rng = mx - mn || 1.0;
//     mainStore.updateSelectedVariable({
//         colormapMin: mn + rng * (min / 100),
//         colormapMax: mn + rng * (max / 100)
//     });
// }

function colormapStyle(item: any) {
    if (!item || !Array.isArray(item.stops)) return {};
    const raw = item;
    // Normalize stops (if absolute mode, convert to normalized 0..1 using current min/max)
    const stops = (raw.mode === 'absolute')
        ? (raw.stops || []).map((s: any) => {
            const mn = colormapMin.value;
            const mx = colormapMax.value;
            const rng = mx - mn || 1.0;
            return [Math.max(0, Math.min(1, (s[0] - mn) / rng)), s[1]];
        })
        : raw.stops;
    const stopsStr = stops.map((s: any) => `${s[1]} ${Math.round(s[0] * 100)}%`).join(', ');
    return {
        background: `linear-gradient(90deg, ${stopsStr})`,
        width: '48px',
        height: '12px',
        borderRadius: '3px',
        border: '1px solid rgba(0,0,0,0.08)',
        marginRight: '8px',
        display: 'inline-block'
    };
}

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
</style>