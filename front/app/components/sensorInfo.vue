<template>
    <div class="rounded-lg m-0 p-0 h-full flex flex-col min-h-0">
        <div class="px-4 py-3 flex-1 min-h-0 flex flex-col">
            <div v-if="sensors.length === 0">
                No sensors found.
            </div>
            <div v-else class="flex-1 min-h-0 flex flex-col">
                <!-- FILTERS -->
                <!-- Search stays on the surface (it's the one control used on most
                     visits); the two dropdowns live in a popover so the sensor list
                     starts higher up the panel. The badge is what tells the user a
                     filter is on while the popover is shut. -->
                <div class="flex items-center gap-1 p-1 shrink-0">
                    <UInput v-model="searchQuery" icon="i-mdi-magnify" placeholder="Search sensors" size="sm" class="grow" />
                    <UPopover v-model:open="showFilters" :content="{ side: 'bottom', align: 'end' }">
                        <UButton
                            :variant="activeFilterCount ? 'solid' : 'ghost'"
                            :color="activeFilterCount ? 'primary' : 'neutral'"
                            size="sm"
                            class="shrink-0 relative"
                            aria-label="Filter sensors"
                            title="Filter sensors"
                        >
                            <UIcon name="i-mdi-filter-variant" class="size-[16px]" />
                            <UBadge
                                v-if="activeFilterCount"
                                :label="String(activeFilterCount)"
                                color="primary"
                                size="xs"
                                class="rounded-full absolute -top-1 -right-1 px-1"
                            />
                        </UButton>
                        <template #content>
                            <div class="p-3 w-[260px] flex flex-col gap-3">
                                <UFormField label="Organization">
                                    <USelectMenu v-model="organizationFilter" :items="organizationOptions" clearable multiple class="w-full" />
                                </UFormField>
                                <UFormField label="Variable">
                                    <USelectMenu v-model="variableFilter" :items="variableOptions" label-key="label" value-key="value" clearable multiple class="w-full" />
                                </UFormField>
                                <div class="flex justify-end">
                                    <UButton variant="ghost" size="xs" :disabled="!activeFilterCount" @click="clearFilters">
                                        Clear filters
                                    </UButton>
                                </div>
                            </div>
                        </template>
                    </UPopover>
                </div>

                <div v-if="filteredSensors.length === 0" class="text-center text-muted p-4">
                    No sensors match your filters.
                </div>

                <!-- SENSOR LIST -->
                <!-- Only this list scrolls; the search + filter row above it stays
                     pinned at the top of the Sensors block. -->
                <div v-else class="flex-1 min-h-0 overflow-y-auto">
                <div v-for="(sensor, i) in filteredSensors" :key="sensor.id" :ref="setSensorRef(sensor.id)"
                    role="button" tabindex="0" @click="selectSensor(sensor.id)" @keydown.enter="selectSensor(sensor.id)"
                    class="rounded my-2 px-3 py-2 cursor-pointer hover:bg-white/5"
                    :class="sensor.id === selectedSensor?.id ? 'ring-1 ring-yellow-400' : ''"
                    :style="{ backgroundColor: '#33333399' }">
                    <div class="ml-4 flex items-center gap-1">
                        <UBadge v-if="sensor.organization" color="primary" variant="solid" size="xs"
                            class="rounded-sm px-1.5 py-0 text-[10px] leading-4 font-semibold tracking-wide bg-(--ui-color-primary-700) text-white">
                            {{ sensor.organization }}
                        </UBadge>
                        <div class="grow" />
                        <UButton v-if="sensor.id === selectedSensor?.id" variant="ghost" size="xs" color="neutral" class="p-0.5"
                            aria-label="Compare with model" @click.stop="mainStore.setActiveBottomTab('comparison')">
                            <UIcon name="i-mdi-chart-bar" class="size-[14px]" />
                        </UButton>
                        <UButton variant="ghost" size="xs" color="neutral" class="p-0.5"
                            aria-label="Sensor details" @click.stop="openInfoDialog(sensor)">
                            <UIcon name="i-mdi-information-outline" class="size-[14px]" />
                        </UButton>
                    </div>

                    <div class="text-sm leading-5">
                        <UIcon name="i-mdi-circle" :style="{ color: sensorStatusColor(sensor) }" class="size-[12px]" />
                        {{ sensor.name }}
                    </div>

                    <div class="ml-4 text-[11px] leading-4 font-medium text-muted">
                        <div>{{ depth2txt(sensor) }} · {{ coordTxt(sensor.latitude, sensor.longitude) }}</div>
                        <div>{{ formatDataRange(sensor) }}</div>

                        <div class="mt-1.5 flex flex-wrap gap-1">
                            <UBadge size="xs" color="neutral" variant="subtle" class="rounded-full" v-for="varKey in modelVariablesOf(sensor.variables)" :key="varKey">
                                {{ variableLabel(varKey) }}
                            </UBadge>
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
    <SensorInfoDialog v-model:open="showInfoDialog" :sensor="infoDialogSensor" />
</template>

<script setup lang="ts">
import { useMainStore } from '@/stores/main';
import { storeToRefs } from 'pinia';
import { ref, computed, watch, nextTick, type ComponentPublicInstance } from 'vue';
import colors from '@/config/palette';
import { sensorStatusColor } from '~~/composables/useSensorStatus';
import { coordTxt, depth2txt, formatDataRange } from '~~/composables/useSensorFormat';
import SensorInfoDialog from './SensorInfoDialog.vue';
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

const showFilters = ref(false);

// Only the two popover dropdowns count — the search box is always visible, so it
// needs no badge to announce itself.
const activeFilterCount = computed(
    () => (organizationFilter.value?.length ?? 0) + (variableFilter.value?.length ?? 0)
);

function clearFilters() {
    organizationFilter.value = [];
    variableFilter.value = [];
}

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
.gap-1 {
    gap: 4px;
}
</style>