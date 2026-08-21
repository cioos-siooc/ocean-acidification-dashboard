<template>
    <UModal v-model:open="showDialog" :ui="{ content: 'max-w-[75vw]' }">
        <template #content>
        <div class="bg-elevated rounded-lg">
            <div class="text-lg beta-header p-6">
                <div class="text-2xl font-bold text-white">Visual Guide to OceanECO</div>
            </div>

            <div class="flex flex-wrap -m-3">
                <div class="p-3 w-1/2" v-for="video in videos" :key="video.filename">
                    <div class="rounded-lg m-3 p-5" :title="video.title">
                        <video :src="`/guide/${video.filename}.webm`" :poster="`/guide/${video.filename}.webp`" controls playsinline class="block w-full aspect-video rounded" />
                    </div>
                </div>
            </div>


        </div>
        </template>
    </UModal>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'

// The parent drives this: `<howTo v-model="showHow" />` in pages/index.vue, set
// by the overlay's @show-how. Previously this was a local `ref(true)` and the
// parent's v-model only worked by accident — it fell through as a `modelValue`
// attr onto Vuetify's v-dialog, which happens to accept that prop. UModal names
// it `open`, so the fallthrough stopped working and the dialog opened on load.
const showDialog = defineModel<boolean>({ default: false });

const videos = [
    { "filename": "01", "title": "Select a Variable" },
    { "filename": "02", "title": "Get Timeseries for a Point" },
    { "filename": "03", "title": "Change Depth" },
    { "filename": "04", "title": "Select Color Palette" },
    { "filename": "05", "title": "Auto-scale Color Palette" },
    { "filename": "06", "title": "Adjust Time" },
    { "filename": "07", "title": "Check Historical Data" },
    { "filename": "08", "title": "Plot Sensor Data" },
    { "filename": "09", "title": "Plot Vertical Profile" },
    { "filename": "10", "title": "Bathymetry Layer and Contours" },
    { "filename": "11", "title": "Use the Model Evaluation Page" },
]
</script>

<style scoped>
.bg-opacity-10 {
    background-color: rgba(var(--v-theme-warning-rgb), 0.1);
}

.beta-header {
    background: linear-gradient(135deg, #0098ff 0%, #fb8c00 100%);
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.25);
    border-radius: 4px 4px 0 0;
}
</style>
