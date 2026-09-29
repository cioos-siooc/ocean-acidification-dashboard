<template>
    <!-- Sensor metadata + download links. Opened from the sensor list
         (sensorInfo.vue) and from the Comparison workspace header. -->
    <UModal v-model:open="open" :ui="{ content: 'max-w-[480px]' }">
        <template #content>
        <div class="bg-elevated rounded-lg" v-if="sensor">
            <div class="px-4 pt-4 pb-2 text-lg flex items-center">
                <UIcon name="i-mdi-circle" :style="{ color: sensorStatusColor(sensor) }" class="size-[14px] mr-2" />
                {{ sensor.name }}
            </div>
            <div class="px-4 pb-2 text-sm text-muted" v-if="sensor.organization">{{ sensor.organization }}</div>
            <div class="px-4 py-3">
                <div>
                    <div class="px-4 py-1"><div class="text-muted text-xs">Location</div>
                        <div class="text-sm">{{ coordTxt(sensor.latitude, sensor.longitude) }}</div>
                    </div>
                    <div class="px-4 py-1"><div class="text-muted text-xs">Depth</div>
                        <div class="text-sm">{{ depth2txt(sensor) }}</div>
                    </div>
                    <div class="px-4 py-1"><div class="text-muted text-xs">Data range</div>
                        <div class="text-sm">{{ formatDataRange(sensor) }}</div>
                    </div>
                    <div class="px-4 py-1"><div class="text-muted text-xs">Variables</div>
                        <!-- Sensors with nothing to download (ONC, or an ERDDAP link of an
                             unrecognized layout) fall back to a plain list. -->
                        <div class="text-sm" v-if="!downloads?.variables.length">
                            {{ variables.map(variableLabel).join(', ') }}
                        </div>
                        <div v-else>
                            <div v-for="v in downloads.variables" :key="v.canonical"
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
                            <div v-if="downloads.api === 'ONC'" class="mt-3 var-note">
                                ONC data is ordered through Oceans 3.0 (account required). Each link opens Data
                                Search with that instrument selected.
                            </div>
                        </div>
                    </div>

                </div>
            </div>
            <USeparator v-if="downloads" />
            <div class="flex items-center gap-2 px-2 py-2">
                <UButton variant="subtle" size="xs" v-if="downloads?.dataset" leading-icon="i-mdi-open-in-new" :href="downloads.dataset" target="_blank" rel="noopener">
                    Go to {{ downloads.api }}
                </UButton>
                <div class="grow" />
                <UButton variant="ghost" size="sm" @click="open = false">Close</UButton>
            </div>
        </div>
        </template>
    </UModal>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { useMainStore } from '@/stores/main';
import { sensorStatusColor } from '~~/composables/useSensorStatus';
import { sensorDownloads } from '~~/composables/useSensorDownloadLinks';
import { useVariableRegistry } from '~~/composables/useVariableRegistry';
import { coordTxt, depth2txt, formatDataRange } from '~~/composables/useSensorFormat';

type Sensor = ReturnType<typeof useMainStore>['sensors'][number];

const open = defineModel<boolean>('open', { default: false });
const props = defineProps<{ sensor: Sensor | null }>();

const { variableLabel, modelVariablesOf } = useVariableRegistry();

const variables = computed(() => modelVariablesOf(props.sensor?.variables));
// null for non-ERDDAP sensors (e.g. ONC), which have no direct download URLs to offer.
// Restricted to the same variables the dialog lists, so a sensor never offers a
// download for something the app doesn't otherwise acknowledge.
const downloads = computed(() => sensorDownloads(props.sensor, variables.value));
</script>

<style scoped>
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
