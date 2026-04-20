<template>
    <v-card flat class="ma-0 pa-0">
        <v-card-text>
            <div v-if="sensors.length === 0">
                No sensors found.
            </div>
            <div v-else>
                <!-- SEARCH BAR -->
                <v-text-field v-model="searchQuery" label="Search Sensors" variant="outlined" density="compact"
                    clearable rounded=""></v-text-field>

                <!-- SENSOR LIST -->
                <v-list-item v-for="(sensor, i) in sensors" :key="sensor.id" :active="sensor.id === selectedSensorID"
                    @click="selectSensor(sensor.id)" variant="text" class="rounded my-3"
                    color="yellow" :style="{ backgroundColor: i % 2 === 0 ? '#33333399' : 'transparent' }">
                    <v-list-item-content>
                        <v-list-item-title class="text-body-medium">
                            <v-icon size="12px" :color="sensor.active ? 'green' : 'grey'">mdi-circle</v-icon>
                            {{ sensor.name }}
                        </v-list-item-title>
                        <div class="ml-4">
                            <v-list-item-subtitle>({{ sensor.latitude.toFixed(2) }}, {{ sensor.longitude.toFixed(2) }})
                                | {{ formatDepth(sensor.depth) }} m</v-list-item-subtitle>
                            <v-list-item-subtitle>Last Updated: 13 hours ago</v-list-item-subtitle>
                        </div>
                    </v-list-item-content>
                </v-list-item>
            </div>
        </v-card-text>
    </v-card>
</template>

<script setup lang="ts">
import { useMainStore } from '@/stores/main';
import { ref, computed } from 'vue';
import { formatDepth } from '../../composables/useFormatDepth';

const mainStore = useMainStore();

///////////////////////////////////  PROPS & STATE  ///////////////////////////////////

const sensors = computed(() => mainStore.sensors.sort((a, b) => a.active === b.active ? 0 : a.active ? -1 : 1)); // active sensors first
const selectedSensorID = computed(() => mainStore.selectedSensorID);
const searchQuery = ref('');

const selectedVariable = computed(() => mainStore.selected_variable);

///////////////////////////////// METHODS  ///////////////////////////////////

function selectSensor(sensorID: number) {
    const sensor = sensors.value.find(s => s.id === sensorID);
    if (sensor) {
        mainStore.selectSensor(sensorID, sensor.depth);
        mainStore.setLastClickedMapPoint({ lat: sensor.latitude, lng: sensor.longitude });
        mainStore.setMapCenter({ lat: sensor.latitude, lng: sensor.longitude });
    }
} 
</script>