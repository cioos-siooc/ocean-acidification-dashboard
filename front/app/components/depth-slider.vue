<template>
    <v-card v-if="depths && depths.length > 0" rounded="pill"
        class="depth-control pa-2 d-flex flex-column align-center">
        <v-slider v-model="index" thumb-label="always" direction="vertical" :min="0"
            :max="Math.max(0, depths.length - 1)" step="1" reverse class="depth-slider-widget"
            @end="onIndexChange">
            <template #thumb-label="{ value }">
                <div class="depth-readout">
                    {{ currentDepthDisplay }} m
                </div>
            </template>
        </v-slider>
    </v-card>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import axios from 'axios'
import { useMainStore } from '../stores/main'
import { useRuntimeConfig } from '#app'
import { formatDepth } from '../../composables/useFormatDepth'

const mainStore = useMainStore()

// const depths = ref<number[]>([])
const index = ref(0)

// Derived display of current depth
const selectedVariable = computed(() => mainStore.selected_variable.var)
const depths = computed(() => mainStore.variables.find(v => v.var === selectedVariable.value)?.depths ?? [])
const currentDepth = computed(() => depths.value[index.value] ?? null)
const currentDepthDisplay = computed(() => currentDepth.value === null ? '-' : `${formatDepth(currentDepth.value)}`)



// Watch selected variable changes to fetch metadata (depths)
watch(() => mainStore.selected_variable.var, async (newVar) => {
    if (!newVar) {
        depths.value = []
        return
    }
    try {
        // Prefer runtime-config api base if available
        const config: any = useRuntimeConfig ? useRuntimeConfig() : undefined;
        const apiBase = config?.public?.apiBaseUrl ?? '';
        const res = await axios.get(`${apiBase}/metadata/${newVar}`)
        const meta = JSON.parse(res.data)
        depths.value = Array.isArray(meta.depths) && meta.depths.length > 0 ? meta.depths : []
        // pick nearest index to the currently-selected depth in store
        const curDepth = mainStore.selected_variable.depth
        if (curDepth !== null && depths.value.length > 0) {
            index.value = nearestIndex(depths.value, curDepth)
        } else {
            index.value = 0
        }
    } catch (e) {
        console.warn('Failed to fetch metadata for depth slider:', e)
        depths.value = []
    }
}, { immediate: true })

// When the slider index changes, update the selected variable depth in store
function onIndexChange(v: number) {
    mainStore.setSelectedVariable(mainStore.selected_variable.var, mainStore.selected_variable.dt, depths.value[v])
}

function nearestIndex(arr: number[], target: number) {
    let best = 0
    let bestDiff = Infinity
    arr.forEach((v, i) => {
        const diff = Math.abs(v - target)
        if (diff < bestDiff) { bestDiff = diff; best = i }
    })
    return best
}

</script>

<style scoped>
.depth-control {
    position: absolute;
    left: 8px;
    bottom: 8px;
    /* top: 50%; */
    /* transform: translateY(-50%); */
    width: 40px;
    height: 200px;
    z-index: 1100;
    overflow: visible;
    /* allow visibility outside the card */
}

.depth-readout {
    /* font-weight: 600; */
    text-align: center;
    width: max-content;
}

.depth-slider-widget {
    /* height: 200px; */
    width: 32px;
}

.depth-markers {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 6px;
    margin-top: 8px;
}

.depth-marker {
    font-size: 12px;
}

/* Style the Vuetify slider thumb label to be smaller and bold */
/* .depth-control :deep(.v-slider__thumb-label),
.depth-control :deep(.v-slider__thumb-label__content), */
.depth-control :deep(.v-slider .v-label) {
    font-size: 11px !important;
    font-weight: bold !important;
    line-height: 1 !important;
    margin: 0 !important;
}

/* Target Vuetify internals from scoped CSS using :deep() so the rule actually takes effect */
.depth-control :deep(.v-slider__container) {
    min-height: 150px !important;
    max-height: 150px !important;
    height: 150px !important;
    overflow: visible !important;
}

.depth-control :deep(.v-slider__thumb) {
    font-size: 11px !important;
    font-weight: 700 !important;
    z-index: 2000 !important;
}

/* In some Vuetify versions the container may use a different internal class - include a fallback */
/* .depth-control :deep(.v-slider) {
  min-height: 150px !important;
  max-height: 150px !important;
  height: 150px !important;
} */

/* If Vuetify uses a different class name in some versions, cover common alternatives */
/* .depth-control :deep(.v-slider-thumb-label) {
    font-size: 11px !important;
    font-weight: 700 !important;
} */
</style>
