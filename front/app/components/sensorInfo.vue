<template>
    <div class="rounded-lg m-0 p-0">
        <div class="px-4 py-3">
            <div v-if="sensors.length === 0">
                No sensors found.
            </div>
            <div v-else>
                <!-- FILTERS -->
                <div class="flex flex-wrap m-0 p-0">
                    <div class="w-full p-1">
                        <UFormField label="Search sensors">
  <UInput v-model="searchQuery" icon="i-mdi-magnify" />
</UFormField>
                    </div>
                    <div class="w-full p-1">
                        <UFormField label="Organization">
  <USelectMenu v-model="organizationFilter" :items="organizationOptions" clearable multiple class="w-full" />
</UFormField>
                    </div>
                    <div class="w-full p-1">
                        <UFormField label="Variable">
  <USelectMenu v-model="variableFilter" :items="variableOptions" label-key="label" value-key="value" clearable multiple class="w-full" />
</UFormField>
                    </div>
                </div>

                <div v-if="filteredSensors.length === 0" class="text-center text-muted p-4">
                    No sensors match your filters.
                </div>

                <!-- SENSOR LIST -->
                <div v-for="(sensor, i) in filteredSensors" :key="sensor.id" :ref="setSensorRef(sensor.id)"
                    role="button" tabindex="0" @click="selectSensor(sensor.id)" @keydown.enter="selectSensor(sensor.id)"
                    class="rounded my-3 px-3 py-1 cursor-pointer hover:bg-white/5"
                    :class="sensor.id === selectedSensor?.id ? 'ring-1 ring-yellow-400' : ''"
                    :style="{ backgroundColor: '#33333399' }">
                    <div>
                        <div class="text-sm">
                            <UIcon name="i-mdi-circle" :style="{ color: sensorStatusColor(sensor) }" class="size-[12px]" />
                            {{ sensor.name }}
                        </div>

                        <div class="ml-4">
                            <div class="text-[11px] font-medium text-muted">
                                <span v-if="sensor.organization" class="sensor-org">{{ sensor.organization }} </span>
                                {{ depth2txt(sensor) }}
                            </div>

                            <div class="text-[11px] font-medium text-muted">
                                {{ coordTxt(sensor.latitude, sensor.longitude) }}
                            </div>

                            <div class="text-[11px] font-medium text-muted">{{ formatDataRange(sensor)
                            }}</div>

                            <div class="mt-1 flex flex-wrap gap-1">
                                <UBadge size="xs" color="neutral" variant="subtle" class="rounded-full" v-for="varKey in modelVariablesOf(sensor.variables)" :key="varKey">
                                    {{ variableLabel(varKey) }}
                                </UBadge>
                            </div>

                            <div class="flex flex-wrap mt-2 flex gap-1">
                                <div class="grow" />
                                <div class="p-3 w-1/12">
                                    <UButton variant="subtle" size="xs" color="neutral" class="shrink-0" @click.stop="openInfoDialog(sensor)">
                                        <UIcon name="i-mdi-information-variant" class="size-[12px]" />
                                    </UButton>
                                </div>
                                <div class="p-3 w-1/12">
                                    <UButton variant="subtle" size="xs" color="neutral" class="shrink-0" v-if="sensor.id === selectedSensor?.id" @click.stop="mainStore.setActiveBottomTab('comparison')">
                                        <UIcon name="i-mdi-chart-bar" class="size-[12px]" />
                                    </UButton>
                                </div>
                            </div>

                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>


    <!-- DIALOGS -->
    <!-- DEPTH PICKER (reserved for future variable-depth/profiler sensors) -->

    <!-- HEATMATP -->
    <UModal v-model:open="showHeatmapDialog" :ui="{ content: 'max-w-[85vw]' }">
        <template #content>
            <HeatmapChart :sensor-id="heatmap_sensorId" :model-variable="heatmap_variable" />
        </template>
    </UModal>

    <!-- SENSOR INFO -->
    <UModal v-model:open="showInfoDialog" :ui="{ content: 'max-w-[480px]' }">
        <template #content>
        <div class="bg-elevated rounded-lg" v-if="infoDialogSensor">
            <div class="px-4 pt-4 pb-2 text-lg flex items-center">
                <UIcon name="i-mdi-circle" :style="{ color: sensorStatusColor(infoDialogSensor) }" class="size-[14px] mr-2" />
                {{ infoDialogSensor.name }}
            </div>
            <div class="px-4 pb-2 text-sm text-muted" v-if="infoDialogSensor.organization">{{ infoDialogSensor.organization }}</div>
            <div class="px-4 py-3">
                <div>
                    <div class="px-4 py-1"><div class="text-muted text-xs">Location</div>
                        <div class="text-sm">{{ coordTxt(infoDialogSensor.latitude, infoDialogSensor.longitude)
                            }}</div>
                    </div>
                    <div class="px-4 py-1"><div class="text-muted text-xs">Depth</div>
                        <div class="text-sm">{{ depth2txt(infoDialogSensor) }}</div>
                    </div>
                    <div class="px-4 py-1"><div class="text-muted text-xs">Data range</div>
                        <div class="text-sm">{{ formatDataRange(infoDialogSensor) }}</div>
                    </div>
                    <div class="px-4 py-1"><div class="text-muted text-xs">Variables</div>
                        <!-- Sensors with nothing to download (ONC, or an ERDDAP link of an
                             unrecognized layout) fall back to a plain list. -->
                        <div class="text-sm" v-if="!infoDownloads?.variables.length">
                            {{ infoDialogVariables.map(variableLabel).join(', ') }}
                        </div>
                        <div v-else>
                            <div v-for="v in infoDownloads.variables" :key="v.canonical"
                                class="flex items-center gap-1 mt-2">
                                <span class="var-name">{{ variableLabel(v.canonical) }}</span>
                                <UButton variant="subtle" size="xs" color="neutral" v-if="v.nc" leading-icon="i-mdi-download" :href="v.nc" target="_blank" rel="noopener">
                                    NetCDF
                                </UButton>
                                <UButton variant="subtle" size="xs" color="neutral" v-if="v.csv" leading-icon="i-mdi-download" :href="v.csv" target="_blank" rel="noopener">
                                    CSV
                                </UButton>
                                <UButton variant="subtle" size="xs" color="neutral" v-if="v.page" leading-icon="i-mdi-open-in-new" :href="v.page" target="_blank" rel="noopener">
                                    Oceans 3.0
                                </UButton>
                            </div>
                            <!-- ONC has no direct-download URL: Oceans 3.0 is a cart/order
                                 flow, so say so rather than let the buttons imply a file. -->
                            <div v-if="infoDownloads.api === 'ONC'" class="mt-3 var-note">
                                ONC data is ordered through Oceans 3.0 (account required). Each link opens Data
                                Search with that instrument selected.
                            </div>
                        </div>
                    </div>

                </div>
            </div>
            <USeparator v-if="infoDownloads" />
            <div class="flex items-center gap-2 px-2 py-2">
                <UButton variant="subtle" size="xs" v-if="infoDownloads?.dataset" leading-icon="i-mdi-open-in-new" :href="infoDownloads.dataset" target="_blank" rel="noopener">
                    Go to {{ infoDownloads.api }}
                </UButton>
                <div class="grow" />
                <UButton variant="ghost" size="sm" @click="showInfoDialog = false">Close</UButton>
            </div>
        </div>
        </template>
    </UModal>
</template>

<script setup lang="ts">
import { useMainStore } from '@/stores/main';
import { storeToRefs } from 'pinia';
import { ref, computed, watch, nextTick, type ComponentPublicInstance } from 'vue';
import colors from '@/config/palette';
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