<template>
    <v-card flat class="ma-0 pa-0">
        <v-card-text>
            <div v-if="sensors.length === 0">
                No sensors found.
            </div>
            <div v-else>
                <!-- FILTERS -->
                <v-row class="ma-1 pa-0">
                    <v-col class="ma-0 pa-0">
                        <v-btn size="16px" icon flat disabled title="Filter by visible area">
                            <iconsMap />
                        </v-btn>
                    </v-col>
                </v-row>

                <!-- SEARCH BAR -->
                <v-text-field v-model="searchQuery" disabled label="Search Sensors" variant="outlined" density="compact"
                    hide-details clearable rounded=""></v-text-field>

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

                            <v-list-item-subtitle class="text-label-small">Last Updated: 4 hours
                                ago</v-list-item-subtitle>
                            <v-row>
                                <v-spacer></v-spacer>
                                <v-btn flat icon size="12px" title="Model Evaluation">
                                    <icons-compare />
                                </v-btn>
                            </v-row>

                        </div>
                    </v-list-item-content>
                </v-list-item>
            </div>
        </v-card-text>
    </v-card>

    <!-- Depth picker dialog -->
    <v-dialog v-model="depthDialogOpen" max-width="280" max-height="300">
        <v-card v-if="depthDialogSensor">
            <v-card-title class="text-body-1">{{ depthDialogSensor.name }}</v-card-title>
            <v-card-subtitle>Select a depth</v-card-subtitle>
            <v-list density="compact">
                <v-list-item v-for="d in depthDialogSensor.depth" :key="d" :title="d + ' m'"
                    @click="pickDepth(depthDialogSensor.id, d)" />
            </v-list>
        </v-card>
    </v-dialog>
</template>

<script setup lang="ts">
import { useMainStore } from '@/stores/main';
import { ref, computed } from 'vue';
import { formatDepth } from '../../composables/useFormatDepth';
import { el } from 'vuetify/locale';

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

///////////////////////////////// METHODS  ///////////////////////////////////

function selectSensor(sensorID: number) {
    const sensor = sensors.value.find(s => s.id === sensorID);
    console.log(sensor);
    if (sensor) {
        if (sensor.depth.length === 1) {
            console.log(sensor.depth[0]);
            mainStore.selectSensor(sensorID, sensor.depth[0]);
            mainStore.setLastClickedMapPoint({ lat: sensor.latitude, lng: sensor.longitude });
            mainStore.setMapCenter({ lat: sensor.latitude, lng: sensor.longitude });
        }
        else {
            depthDialogSensor.value = sensor;
            return; // wait for depth selection
        }
    }
}

function pickDepth(sensorID: number, depth: number) {
    depthDialogSensor.value = null;
    const sensor = sensors.value.find(s => s.id === sensorID);
    if (sensor) {
        mainStore.selectSensor(sensorID, depth);
        mainStore.setLastClickedMapPoint({ lat: sensor.latitude, lng: sensor.longitude });
        mainStore.setMapCenter({ lat: sensor.latitude, lng: sensor.longitude });
    }
}

function depth2txt(depth: number[]): string {
    if (depth === null || depth.length === 0) return '';

    if (depth.length === 1) {
        if (depth[0] === 0) return 'Surface';
        else return depth[0].toFixed(0) + ' m';
    } else {
        return 'Profile'
    }
}

function sensorDepths(depth: number[]): Array<{ label: string, value: number }> {
    if (depth === null || depth.length === 0) return [];

    return depth.map(d => ({ label: d === 0 ? 'Surface' : d.toFixed(0) + ' m', value: d }));
}

</script>