<template>
    <v-card flat class="ma-0 pa-0">
        <v-card-text>
            <div v-if="sensors.length === 0">
                No sensors found.
            </div>
            <div v-else>
                <!-- FILTERS -->
                <!-- <v-row class="ma-1 pa-0">
                    <v-col class="ma-0 pa-0">
                        <v-btn size="16px" icon flat disabled title="Filter by visible area">
                            <iconsMap />
                        </v-btn>
                    </v-col>
                </v-row> -->

                <!-- SEARCH BAR -->
                <!-- <v-text-field v-model="searchQuery" disabled label="Search Sensors" variant="outlined" density="compact"
                    hide-details clearable rounded=""></v-text-field> -->

                <!-- SENSOR LIST -->
                <v-list-item v-for="(sensor, i) in sensors" :key="sensor.id" :active="sensor.id === selectedSensor?.id"
                    @click="selectSensor(sensor.id)" variant="text" class="rounded my-3" color="yellow"
                    :style="{ backgroundColor: '#33333399' }">
                    <v-list-item-content>
                        <v-list-item-title class="text-body-medium">
                            <v-icon size="12px" :color="sensor.active ? 'green' : 'grey'">mdi-circle</v-icon>
                            {{ sensor.name }}
                        </v-list-item-title>

                        <div class="ml-4">
                            <v-list-item-subtitle class="text-label-small">
                                <!-- ({{ sensor.latitude.toFixed(2) }}, {{ sensor.longitude.toFixed(2) }}) | -->
                                {{ depth2txt(sensor.depth) }}
                            </v-list-item-subtitle>
                            <!-- <v-label v-else-if="sensor.depth.length > 1" class="text-label-small" style="cursor:pointer" @click.stop="depthDialogSensor = sensor">
                                {{ depth2txt(sensor.depth) }}
                                <v-icon size="10px">mdi-chevron-down</v-icon>
                            </v-label> -->

                            <v-list-item-subtitle class="text-label-small">{{ formatDataRange(sensor) }}</v-list-item-subtitle>
                            <v-row class="ma-0 pa-0">
                                <v-spacer></v-spacer>
                                <v-col cols="auto" class="ma-0 pa-0">
                                    <v-btn flat icon size="12px" title="Model Evaluation" @click.stop="openHeatmapDialog(sensor.id)">
                                        <icons-heatmap :color="colors.orange.lighten3" />
                                    </v-btn>
                                </v-col>
                                <v-col cols="auto" class="ma-0 pa-0" disabled>
                                    <v-btn flat icon size="12px" title="Model Evaluation">
                                        <icons-compare :color="colors.blue.lighten3" />
                                    </v-btn>
                                </v-col>
                            </v-row>

                        </div>
                    </v-list-item-content>
                </v-list-item>
            </div>
        </v-card-text>
    </v-card>


    <!-- DIALOGS -->
    <!-- DEPTH PICKER (reserved for future variable-depth/profiler sensors) -->

    <!-- HEATMATP -->
    <v-dialog v-model="showHeatmapDialog" width="85%" height="85%" transition="dialog-transition">
        <HeatmapChart :sensor-id="heatmap_sensorId" :model-variable="heatmap_variable"/>
    </v-dialog>
</template>

<script setup lang="ts">
import { useMainStore } from '@/stores/main';
import { ref, computed } from 'vue';
import colors from 'vuetify/util/colors';

const mainStore = useMainStore();

///////////////////////////////////  PROPS & STATE  ///////////////////////////////////

const sensors = computed(() => mainStore.sensors.sort((a, b) => a.active === b.active ? 0 : a.active ? -1 : 1)); // active sensors first
const selectedSensor = computed(() => mainStore.selectedSensor);
const searchQuery = ref('');
const depthDialogSensor = ref<typeof mainStore.sensors[number] | null>(null);
const depthDialogOpen = computed({
    get: () => depthDialogSensor.value !== null,
    set: (v) => { if (!v) depthDialogSensor.value = null; }
});

const showHeatmapDialog = ref(false);
const heatmap_sensorId = ref<string | null>(null);
const heatmap_variable = computed(() => mainStore.selected_variable?.var ?? null);
const heatmap_minDate = ref<string | null>(null);
const heatmap_maxDate = ref<string | null>(null);

///////////////////////////////// METHODS  ///////////////////////////////////

function selectSensor(sensorID: string) {
    const sensor = sensors.value.find(s => s.id === sensorID);
    if (sensor) {
        mainStore.selectSensor(sensorID, sensor.depth);
        mainStore.setLastClickedMapPoint({ lat: sensor.latitude, lng: sensor.longitude });
        mainStore.setMapCenter({ lat: sensor.latitude, lng: sensor.longitude });
    }
}

function depth2txt(depth: number): string {
    if (depth == null || depth < 0) return 'Variable depth';
    if (depth === 0) return 'Surface';
    return depth.toFixed(0) + ' m';
}

function formatDataRange(sensor: any): string {
    const fmt = (iso: string) => new Date(iso).toLocaleDateString('en-CA', { year: 'numeric', month: 'short' });
    const { first_data_at, latest_data_at } = sensor;
    if (!first_data_at && !latest_data_at) return 'No data';
    if (!first_data_at) return `Up to ${fmt(latest_data_at)}`;
    if (!latest_data_at) return `From ${fmt(first_data_at)}`;
    const isRecent = Date.now() - new Date(latest_data_at).getTime() < 14 * 86400_000;
    return `${fmt(first_data_at)} – ${isRecent ? 'present' : fmt(latest_data_at)}`;
}

function openHeatmapDialog(sensorId: string) {
    const sensor = sensors.value.find(s => s.id === sensorId);
    heatmap_sensorId.value = sensorId;
    showHeatmapDialog.value = true;
}

</script>