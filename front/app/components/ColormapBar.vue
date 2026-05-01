<template>
    <div style="display: flex; align-items: flex-end; gap: 6px;">
        <div style="flex: 1; min-width: 0;">
            <div class="bar" :style="barStyle"></div>
            <div class="ticks">
                <div class="tick left text-label-small">{{ colormapMin?.toFixed(precisionDigits) }}</div>
                <div class="tick center text-label-small">{{ colormapAvg }}</div>
                <div class="tick right text-label-small">{{ colormapMax?.toFixed(precisionDigits) }}</div>
            </div>
        </div>
        <div style="flex: 0 0 auto; font-size: x-small; color: #aaa; padding-bottom: 2px;">{{ unit }}</div>
    </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { useMainStore } from '../stores/main';
import { color } from 'echarts';

const mainStore = useMainStore();

const selectedVariable = computed(() => mainStore.selected_variable);

const unit = computed(() =>
    mainStore.variables.find(v => v.var === selectedVariable.value.var)?.unit ?? ''
);

const colormapMin = computed(() => selectedVariable.value.colormapMin);
const colormapMax = computed(() => selectedVariable.value.colormapMax);
const precisionDigits = computed(() => -Math.log10(selectedVariable.value.precision));
const colormapAvg = computed(() => {
    if (!colormapMin.value || !colormapMax.value) return '';
    const avg = (colormapMin.value + colormapMax.value) / 2;
    console.log(colormapMin.value, colormapMax.value, avg, precisionDigits.value);
    return avg.toFixed(precisionDigits.value);
});

const selectedColormap = computed(() => selectedVariable.value.colormap);
const colormaps = computed(() => mainStore.colormaps);

const barStyle = computed(() => {
    const palette = colormaps.value[selectedColormap.value]?.stops;
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
</style>
