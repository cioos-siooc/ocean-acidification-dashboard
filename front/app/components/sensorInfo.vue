<template>
    <div class="sensors">
        <div v-if="sensors.length === 0" class="p-4 text-muted">No sensors found.</div>
        <template v-else>
            <!-- Search, filters and sort stay pinned; only the list scrolls. The
                 two dropdown filters live in a popover so the list starts higher,
                 and the badge says a filter is on while it's shut. -->
            <div class="sensors__head">
                <div class="flex items-center justify-between">
                    <span class="oe-label">Sensors · {{ filteredSensors.length }}</span>
                    <USelect v-model="sortMode" :items="sortItems" size="xs" variant="ghost"
                        class="sensors__sort" aria-label="Sort sensors" />
                </div>
                <div class="flex items-center gap-1">
                    <UInput v-model="searchQuery" icon="i-mdi-magnify" placeholder="Search sensors" size="sm" class="grow" />
                    <UPopover v-model:open="showFilters" :content="{ side: 'bottom', align: 'end' }">
                        <UButton
                            variant="outline"
                            color="neutral"
                            size="sm"
                            class="shrink-0 relative"
                            :class="activeFilterCount ? 'oe-on' : ''"
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
            </div>

            <div v-if="filteredSensors.length === 0" class="text-center text-muted p-4">
                No sensors match your filters.
            </div>

            <div v-else class="sensors__list">
                <div v-for="sensor in sortedSensors" :key="sensor.id" :ref="setSensorRef(sensor.id)"
                    role="button" tabindex="0" class="srow" :class="{ 'srow--on': sensor.id === selectedSensor?.id }"
                    @click="selectSensor(sensor.id)" @keydown.enter="selectSensor(sensor.id)">
                    <span class="srow__dot" :class="`srow__dot--${statusOf(sensor)}`" :title="statusTitle(sensor)" />
                    <span class="srow__name">{{ sensor.name }}</span>
                    <span class="srow__depth oe-num">{{ shortDepth(sensor) }}</span>

                    <span class="srow__sub oe-num">
                        <span>{{ sensor.organization }}</span>
                        <span class="srow__vars">{{ shortVars(sensor) }}<template v-if="isProfiler(sensor)"> · profiler</template></span>
                        <span v-if="distanceOf(sensor)">{{ distanceOf(sensor) }}</span>
                        <span class="srow__age" :title="sensor.latest_data_at ? `Last reading ${new Date(sensor.latest_data_at).toLocaleString('en-CA')}` : undefined">{{ ageOf(sensor) }}</span>
                    </span>

                    <!-- The selected sensor expands with what a name and depth can't say. -->
                    <div v-if="sensor.id === selectedSensor?.id" class="srow__detail" @click.stop>
                        <div class="srow__kv">
                            <span>Coverage</span><span class="oe-num">{{ formatDataRange(sensor) }}</span>
                            <span>Position</span><span class="oe-num">{{ coordTxt(sensor.latitude, sensor.longitude) }}</span>
                            <span>Depth</span><span>{{ depth2txt(sensor) }}</span>
                            <span>Measures</span>
                            <span>
                                <template v-for="(varKey, k) in modelVariablesOf(sensor.variables)" :key="varKey">
                                    <span v-if="k">, </span>{{ variableLabel(varKey) }}<span v-if="instrumentCount(sensor, varKey) > 1"
                                        class="text-muted" :title="`${instrumentCount(sensor, varKey)} instruments measure this here`"> ×{{ instrumentCount(sensor, varKey) }}</span>
                                </template>
                            </span>
                        </div>
                        <div class="srow__acts">
                            <UButton size="xs" variant="outline" color="neutral" icon="i-mdi-information-outline" @click="openInfoDialog(sensor)">Details</UButton>
                            <UButton size="xs" variant="outline" color="neutral" icon="i-mdi-chart-bar" @click="mainStore.setActiveBottomTab('comparison')">Compare with model</UButton>
                            <UButton v-if="selectedInstruments.length > 1" size="xs" variant="outline" color="neutral"
                                icon="i-mdi-compare-horizontal" :class="compareInstruments ? 'oe-on' : ''"
                                :aria-pressed="compareInstruments"
                                :title="`Plot each of the ${selectedInstruments.length} instruments that measure ${variableLabel(selectedVar)} here, with their differences`"
                                @click="toggleCompareInstruments">Instruments</UButton>
                        </div>
                    </div>
                </div>
            </div>
        </template>
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
import { sensorIsRealtime } from '~~/composables/useSensorStatus';
import { coordTxt, depth2txt, formatDataRange } from '~~/composables/useSensorFormat';
import SensorInfoDialog from './SensorInfoDialog.vue';
import { useVariableRegistry } from '~~/composables/useVariableRegistry';
import { trackEvent } from '~~/composables/useAnalytics';
import { sensorInstruments } from '~~/composables/useSensorTimeseries';
import { useViewState } from '~~/composables/useViewState';

const mainStore = useMainStore();
// Labels and the "is this a variable we show?" test both come from
// variable_config.yml — see useVariableRegistry for why they share a source.
const { variableLabel, variableShort, modelVariablesOf } = useVariableRegistry();

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

// Several instruments can report one variable at a site (ONC child locations),
// and that differs per variable, so the compare toggle follows the selected
// variable. It's Explore's state; the timeseries chart reads it.
const selectedVar = computed(() => mainStore.selected_variable?.var ?? '');
const selectedInstruments = computed(() => sensorInstruments(
    mainStore.sensors.find(s => s.id === selectedSensor.value?.id), selectedVar.value));
const compareInstruments = useViewState('explore')('compareInstruments', false);

function instrumentCount(sensor: Sensor, variable: string) {
    return sensorInstruments(sensor, variable).length;
}

function toggleCompareInstruments() {
    compareInstruments.value = !compareInstruments.value;
    if (compareInstruments.value) {
        mainStore.setActiveBottomTab('explore');
        mainStore.setExploreView('series');
    }
}

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
// Default order puts live sensors first; "Nearest" measures from the last
// clicked point (or the selected sensor, which becomes that point).
type SortMode = 'status' | 'distance' | 'name';
const sortMode = ref<SortMode>('status');
const sortItems = computed(() => [
    { label: 'Live first', value: 'status' },
    { label: 'Nearest', value: 'distance', disabled: !mainStore.lastClickedMapPoint },
    { label: 'A–Z', value: 'name' },
]);

function distanceKm(sensor: Sensor): number | null {
    const p = mainStore.lastClickedMapPoint;
    if (!p) return null;
    const rad = Math.PI / 180;
    const dLat = (sensor.latitude - p.lat) * rad;
    const dLng = (sensor.longitude - p.lng) * rad;
    const h = Math.sin(dLat / 2) ** 2 + Math.cos(p.lat * rad) * Math.cos(sensor.latitude * rad) * Math.sin(dLng / 2) ** 2;
    return 2 * 6371 * Math.asin(Math.sqrt(h));
}

const statusRank = (s: Sensor) => (statusOf(s) === 'live' ? 0 : statusOf(s) === 'historic' ? 1 : 2);
const sortedSensors = computed(() => {
    const list = [...filteredSensors.value];
    if (sortMode.value === 'name') return list.sort((a, b) => a.name.localeCompare(b.name));
    if (sortMode.value === 'distance' && mainStore.lastClickedMapPoint) {
        return list.sort((a, b) => (distanceKm(a) ?? Infinity) - (distanceKm(b) ?? Infinity));
    }
    return list.sort((a, b) => statusRank(a) - statusRank(b));
});

/** live: reading within REALTIME_THRESHOLD_MS; historic: active, older data only; inactive. */
function statusOf(sensor: Sensor): 'live' | 'historic' | 'inactive' {
    if (!sensor.active) return 'inactive';
    return sensorIsRealtime(sensor) ? 'live' : 'historic';
}

function statusTitle(sensor: Sensor): string {
    return { live: 'Live: data in the last 14 days', historic: 'Active, historical data only', inactive: 'Inactive' }[statusOf(sensor)];
}

function isProfiler(sensor: Sensor): boolean {
    return sensor.depth == null || sensor.depth < 0;
}

/** Depth for the narrow right-hand column; depth2txt has the long form. */
function shortDepth(sensor: Sensor): string {
    if (isProfiler(sensor)) {
        return sensor.depth_min != null && sensor.depth_max != null
            ? `${sensor.depth_min.toFixed(0)}–${sensor.depth_max.toFixed(0)} m` : 'profiler';
    }
    return depth2txt(sensor);
}

function shortVars(sensor: Sensor): string {
    return modelVariablesOf(sensor.variables).map(v => variableShort(v)).join(' ');
}

function distanceOf(sensor: Sensor): string {
    const d = distanceKm(sensor);
    if (d == null) return '';
    if (d < 0.5) return 'here';
    return d < 10 ? `${d.toFixed(1)} km` : `${Math.round(d)} km`;
}

/** Time since the last reading, compact: "3 h", "12 d", or the month for older records. */
function ageOf(sensor: Sensor): string {
    if (!sensor.latest_data_at) return '';
    const ms = Date.now() - new Date(sensor.latest_data_at).getTime();
    const h = ms / 3_600_000;
    if (h < 1) return 'now';
    if (h < 48) return `${Math.round(h)} h`;
    if (h < 24 * 60) return `${Math.round(h / 24)} d`;
    return new Date(sensor.latest_data_at).toLocaleDateString('en-CA', { year: 'numeric', month: 'short' });
}

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
.sensors {
    height: 100%;
    display: flex;
    flex-direction: column;
    min-height: 0;
}

.sensors__head {
    display: grid;
    gap: 8px;
    padding: 12px 14px 10px;
    border-bottom: 1px solid var(--ui-border);
    flex-shrink: 0;
}

.sensors__sort {
    width: auto;
    min-width: 0;
    margin-right: -8px;
    font-size: 11px;
}

.sensors__list {
    flex: 1 1 auto;
    min-height: 0;
    overflow-y: auto;
}

.srow {
    display: grid;
    grid-template-columns: 8px 1fr auto;
    gap: 2px 8px;
    align-items: center;
    padding: 7px 14px;
    border-bottom: 1px solid var(--ui-border-muted);
    cursor: pointer;
}

.srow:hover {
    background: var(--ui-bg-elevated);
}

.srow:focus-visible {
    outline: 2px solid var(--ui-primary);
    outline-offset: -2px;
}

.srow--on,
.srow--on:hover {
    background: var(--ui-bg-elevated);
    box-shadow: inset 2px 0 0 var(--ui-primary);
    padding-bottom: 10px;
}

.srow__dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
}

.srow__dot--live { background: var(--oe-live); }
.srow__dot--historic { box-shadow: inset 0 0 0 1.3px var(--oe-stale); }
.srow__dot--inactive { background: var(--ui-text-dimmed); opacity: 0.6; }

.srow__name {
    font-size: 12.5px;
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}

.srow__depth {
    font-size: 11px;
    color: var(--ui-text-muted);
}

.srow__sub {
    grid-column: 2 / 4;
    display: flex;
    gap: 8px;
    font-size: 10.5px;
    color: var(--ui-text-muted);
    min-width: 0;
}

.srow__sub > span {
    white-space: nowrap;
    flex-shrink: 0;
}

.srow__sub > .srow__vars {
    flex-shrink: 1;
    color: var(--ui-text-toned);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    min-width: 0;
}

.srow__age {
    margin-left: auto;
    white-space: nowrap;
}

.srow__detail {
    grid-column: 2 / 4;
    display: grid;
    gap: 8px;
    margin-top: 6px;
    cursor: default;
}

.srow__kv {
    display: grid;
    grid-template-columns: 64px 1fr;
    gap: 3px 8px;
    font-size: 11px;
}

.srow__kv > span:nth-child(odd) {
    color: var(--ui-text-muted);
}

.srow__acts {
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
}
</style>
