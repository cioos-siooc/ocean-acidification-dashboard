<template>
    <v-card flat class="ma-0 pa-0">
        <v-card-text>
            <div v-if="sensors.length === 0">
                No sensors found.
            </div>
            <div v-else>
                <!-- FILTERS -->
                <v-row class="ma-0 pa-0" dense>
                    <v-col cols="12" class="pa-1">
                        <v-text-field v-model="searchQuery" label="Search sensors" hide-details clearable
                            prepend-inner-icon="mdi-magnify" rounded></v-text-field>
                    </v-col>
                    <v-col cols="12" class="pa-1">
                        <v-select v-model="organizationFilter" :items="organizationOptions" label="Organization"
                            hide-details clearable multiple chips closable-chips></v-select>
                    </v-col>
                    <v-col cols="12" class="pa-1">
                        <v-select v-model="variableFilter" :items="variableOptions" item-title="label"
                            item-value="value" label="Variable" hide-details clearable multiple chips
                            closable-chips></v-select>
                    </v-col>
                </v-row>

                <div v-if="filteredSensors.length === 0" class="text-center text-medium-emphasis pa-4">
                    No sensors match your filters.
                </div>

                <!-- SENSOR LIST -->
                <v-list-item v-for="(sensor, i) in filteredSensors" :key="sensor.id" :ref="setSensorRef(sensor.id)"
                    :active="sensor.id === selectedSensor?.id" @click="selectSensor(sensor.id)" variant="text"
                    class="rounded my-3" color="yellow" :style="{ backgroundColor: '#33333399' }">
                    <v-list-item-content>
                        <v-list-item-title class="text-body-medium">
                            <v-icon size="12px" :color="sensorStatusColor(sensor)">mdi-circle</v-icon>
                            {{ sensor.name }}
                        </v-list-item-title>

                        <div class="ml-4">
                            <v-list-item-subtitle class="text-label-small">
                                <span v-if="sensor.organization" class="sensor-org">{{ sensor.organization }} </span>
                                {{ depth2txt(sensor) }}
                            </v-list-item-subtitle>

                            <v-list-item-subtitle class="text-label-small">
                                {{ coordTxt(sensor.latitude, sensor.longitude) }}
                            </v-list-item-subtitle>

                            <v-list-item-subtitle class="text-label-small">{{ formatDataRange(sensor)
                            }}</v-list-item-subtitle>

                            <div class="mt-1 d-flex flex-wrap gap-1">
                                <v-chip v-for="varKey in modelVariablesOf(sensor.variables)" :key="varKey"
                                    size="x-small" variant="tonal" color="grey">
                                    {{ variableLabel(varKey) }}
                                </v-chip>
                            </div>

                            <v-row class="mt-2 d-flex gap-1">
                                <v-spacer />
                                <v-col cols="1">
                                    <v-btn icon size="x-small" variant="tonal" color="yellow"
                                        @click.stop="openInfoDialog(sensor)">
                                        <v-icon size="12px">mdi-information-variant</v-icon>
                                    </v-btn>
                                </v-col>
                                <v-col cols="1">
                                    <v-btn v-if="sensor.id === selectedSensor?.id" icon size="x-small" variant="tonal"
                                        color="red-lighten-2" @click.stop="mainStore.setActiveBottomTab('comparison')">
                                        <v-icon size="12px">mdi-chart-bar</v-icon>
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
        <HeatmapChart :sensor-id="heatmap_sensorId" :model-variable="heatmap_variable" />
    </v-dialog>

    <!-- SENSOR INFO -->
    <v-dialog v-model="showInfoDialog" width="480" transition="dialog-transition">
        <v-card v-if="infoDialogSensor">
            <v-card-title class="d-flex align-center">
                <v-icon size="14px" :color="sensorStatusColor(infoDialogSensor)" class="mr-2">mdi-circle</v-icon>
                {{ infoDialogSensor.name }}
            </v-card-title>
            <v-card-subtitle v-if="infoDialogSensor.organization">{{ infoDialogSensor.organization }}</v-card-subtitle>
            <v-card-text>
                <v-list>
                    <v-list-item>
                        <v-list-item-title class="text-caption text-medium-emphasis">Location</v-list-item-title>
                        <v-list-item-subtitle>{{ coordTxt(infoDialogSensor.latitude, infoDialogSensor.longitude)
                            }}</v-list-item-subtitle>
                    </v-list-item>
                    <v-list-item>
                        <v-list-item-title class="text-caption text-medium-emphasis">Depth</v-list-item-title>
                        <v-list-item-subtitle>{{ depth2txt(infoDialogSensor) }}</v-list-item-subtitle>
                    </v-list-item>
                    <v-list-item>
                        <v-list-item-title class="text-caption text-medium-emphasis">Data range</v-list-item-title>
                        <v-list-item-subtitle>{{ formatDataRange(infoDialogSensor) }}</v-list-item-subtitle>
                    </v-list-item>
                    <v-list-item>
                        <v-list-item-title class="text-caption text-medium-emphasis">Variables</v-list-item-title>
                        <!-- Sensors with nothing to download (ONC, or an ERDDAP link of an
                             unrecognized layout) fall back to a plain list. -->
                        <v-list-item-subtitle v-if="!infoDownloads?.variables.length">
                            {{ infoDialogVariables.map(variableLabel).join(', ') }}
                        </v-list-item-subtitle>
                        <div v-else>
                            <div v-for="v in infoDownloads.variables" :key="v.canonical"
                                class="d-flex align-center gap-1 mt-2">
                                <span class="var-name">{{ variableLabel(v.canonical) }}</span>
                                <v-btn v-if="v.nc" size="x-small" variant="tonal" color="yellow"
                                    prepend-icon="mdi-download" :href="v.nc" target="_blank" rel="noopener">
                                    NetCDF
                                </v-btn>
                                <v-btn v-if="v.csv" size="x-small" variant="tonal" color="yellow"
                                    prepend-icon="mdi-download" :href="v.csv" target="_blank" rel="noopener">
                                    CSV
                                </v-btn>
                                <v-btn v-if="v.page" size="x-small" variant="tonal" color="yellow"
                                    prepend-icon="mdi-open-in-new" :href="v.page" target="_blank" rel="noopener">
                                    Oceans 3.0
                                </v-btn>
                            </div>
                            <!-- ONC has no direct-download URL: Oceans 3.0 is a cart/order
                                 flow, so say so rather than let the buttons imply a file. -->
                            <div v-if="infoDownloads.api === 'ONC'" class="mt-3 var-note">
                                ONC data is ordered through Oceans 3.0 (account required). Each link opens Data
                                Search with that instrument selected.
                            </div>
                        </div>
                    </v-list-item>

                </v-list>
            </v-card-text>
            <v-divider v-if="infoDownloads"></v-divider>
            <v-card-actions>
                <v-btn v-if="infoDownloads?.dataset" size="x-small" variant="tonal" prepend-icon="mdi-open-in-new"
                    :href="infoDownloads.dataset" target="_blank" rel="noopener">
                    Go to {{ infoDownloads.api }}
                </v-btn>
                <v-spacer />
                <v-btn size="small" variant="text" @click="showInfoDialog = false">Close</v-btn>
            </v-card-actions>
        </v-card>
    </v-dialog>
</template>

<script setup lang="ts">
import { useMainStore } from '@/stores/main';
import { storeToRefs } from 'pinia';
import { ref, computed, watch, nextTick, type ComponentPublicInstance } from 'vue';
import colors from 'vuetify/util/colors';
import { sensorStatusColor } from '~~/composables/useSensorStatus';
import { sensorDownloads } from '~~/composables/useSensorDownloadLinks';
import { useVariableRegistry } from '~~/composables/useVariableRegistry';
import { trackEvent } from '~~/composables/useAnalytics';

const mainStore = useMainStore();
// Labels and the "is this a variable we show?" test both come from
// variable_config.yml — see useVariableRegistry for why they share a source.
const { variableLabel, isModelVariable, modelVariablesOf } = useVariableRegistry();

type Sensor = typeof mainStore.sensors[number];

///////////////////////////////////  PROPS & STATE  ///////////////////////////////////

const sensors = computed(() => mainStore.sensors.sort((a, b) => a.active === b.active ? 0 : a.active ? -1 : 1)); // active sensors first
const selectedSensor = computed(() => mainStore.selectedSensor);
// Filter state lives in the store so the map layer can apply the same filters to its markers.
const {
    sensorSearchQuery: searchQuery,
    sensorOrganizationFilter: organizationFilter,
    sensorVariableFilter: variableFilter,
} = storeToRefs(mainStore);

const organizationOptions = computed(() => {
    const orgs = new Set(mainStore.sensors.map((s: Sensor) => s.organization).filter(Boolean));
    return Array.from(orgs).sort();
});

const variableOptions = computed(() => {
    const vars = new Set<string>();
    mainStore.sensors.forEach((s: Sensor) => modelVariablesOf(s.variables).forEach(v => vars.add(v)));
    return Array.from(vars).map(v => ({ label: variableLabel(v), value: v }));
});

// Filtering itself lives in mainStore.filteredSensors so the map layer applies the same criteria.
const filteredSensors = computed(() =>
    [...mainStore.filteredSensors].sort((a, b) => a.active === b.active ? 0 : a.active ? -1 : 1)
);
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

const showInfoDialog = ref(false);
const infoDialogSensor = ref<typeof mainStore.sensors[number] | null>(null);
const infoDialogVariables = computed(() => modelVariablesOf(infoDialogSensor.value?.variables));
// null for non-ERDDAP sensors (e.g. ONC), which have no direct download URLs to offer.
// Restricted to the same variables the dialog lists, so a sensor never offers a
// download for something the app doesn't otherwise acknowledge.
const infoDownloads = computed(() => sensorDownloads(infoDialogSensor.value, infoDialogVariables.value));

const sensorRefs = new Map<string, Element | ComponentPublicInstance>();
function setSensorRef(id: string) {
    return (el: Element | ComponentPublicInstance | null) => {
        if (el) sensorRefs.set(id, el);
        else sensorRefs.delete(id);
    };
}

watch(() => selectedSensor.value?.id, async (id: string | undefined) => {
    if (!id) return;
    await nextTick();
    const el = sensorRefs.get(id);
    const target = (el as any)?.$el ?? el;
    if (target instanceof HTMLElement) {
        target.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
});

///////////////////////////////// METHODS  ///////////////////////////////////

function selectSensor(sensorID: string) {
    const sensor = sensors.value.find(s => s.id === sensorID);
    if (sensor) {
        trackEvent('sensor_selected', { sensor_id: sensorID, source: 'list' });
        mainStore.selectSensor(sensorID, sensor.depth);
        mainStore.setLastClickedMapPoint({ lat: sensor.latitude, lng: sensor.longitude });
        mainStore.setMapCenter({ lat: sensor.latitude, lng: sensor.longitude });
    }
}

function coordTxt(lat: number, lng: number): string {
    const latStr = `${Math.abs(lat).toFixed(2)}°${lat >= 0 ? 'N' : 'S'}`;
    const lngStr = `${Math.abs(lng).toFixed(2)}°${lng >= 0 ? 'E' : 'W'}`;
    return `${latStr}, ${lngStr}`;
}

function depth2txt(sensor: { depth: number, depth_min?: number | null, depth_max?: number | null }): string {
    const { depth, depth_min, depth_max } = sensor;
    if (depth == null || depth < 0) {
        if (depth_min != null && depth_max != null) {
            return `Variable depth (${depth_min.toFixed(0)}–${depth_max.toFixed(0)} m)`;
        }
        return 'Variable depth';
    }
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

function openInfoDialog(sensor: typeof mainStore.sensors[number]) {
    infoDialogSensor.value = sensor;
    showInfoDialog.value = true;
}

</script>

<style scoped>
.sensor-org {
    opacity: 0.6;
    font-size: 0.75em;
}

.gap-1 {
    gap: 4px;
}

/* Keeps the per-variable download buttons aligned in a column. */
.var-name {
    /* Wide enough for the longest label a sensor actually carries ("Dissolved
       Oxygen") so the download buttons line up in a column across rows. */
    min-width: 9rem;
    font-size: 0.75rem;
    opacity: var(--v-medium-emphasis-opacity);
}

.var-note {
    font-size: 0.75rem;
    line-height: 1.1rem;
    opacity: var(--v-medium-emphasis-opacity);
}
</style>