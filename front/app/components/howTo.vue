<template>
    <v-dialog v-model="showDialog" width="75%" height="75%"  >
        <v-card>
            <v-card-title class="beta-header pa-6">
                <div class="text-h5 font-weight-bold text-white">Visual Guide to OAH</div>
            </v-card-title>

            <v-row>
                <v-col cols="6" v-for="video in videos" :key="video.filename">
                    <v-card  :title="video.title" class="ma-3 pa-5" flat variant="outlined">
                        <v-video :src="`/guide/${video.filename}.webm`" :image="`/guide/${video.filename}.webp`" theme="dark" color="primary" aspect-ratio="16/9"
                            hide-volume  />
                    </v-card>
                </v-col>
            </v-row>


        </v-card>
    </v-dialog>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'

const showDialog = ref(true);

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
