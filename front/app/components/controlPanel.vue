<template>
    <v-navigation-drawer v-model="isOpen" location="left" :width="mainStore.controlPanel_width" class="pa-2" absolute
        persistent mobile :scrim="false" style="height:100%; z-index:999; top:0;">
        <!-- <v-row>
            <v-col cols="1">
                <v-btn icon>
                     <v-icon @click="isOpen = !isOpen">mdi-chevron-left</v-icon>
                </v-btn>
            </v-col>
        </v-row> -->

        <v-expansion-panels v-model="panels" focusable inset multiple class="panel-container">
            <v-expansion-panel value="variables" class="variables-panel">
                <v-expansion-panel-title class="text-subtitle-1">Variables</v-expansion-panel-title>
                <v-expansion-panel-text>
                    <variables />
                </v-expansion-panel-text>
            </v-expansion-panel>

            <!-- SENSOR -->
            <v-expansion-panel value="sensors" class="sensors-panel">
                <v-expansion-panel-title class="text-subtitle-1">Sensors</v-expansion-panel-title>
                <v-expansion-panel-text class="ma-0 pa-0">
                    <sensorInfo />
                </v-expansion-panel-text>
            </v-expansion-panel>
        </v-expansion-panels>
    </v-navigation-drawer>
</template>


<script setup lang="ts">
import { ref, watch, onMounted, computed } from 'vue';
import { useMainStore } from '../stores/main';
const mainStore = useMainStore();

const panels = ref<Array<string>>(['variables', 'sensors']);

const isOpen = computed(() => mainStore.isControlPanelOpen);
</script>

<style scoped lang="scss">
:deep(.v-expansion-panel-text__wrapper) {
    padding: 0;
}

// Keep the Variables panel always in view: the drawer itself doesn't scroll,
// only the Sensors panel's own content area does.
:deep(.v-navigation-drawer__content) {
    display: flex;
    flex-direction: column;
    overflow: hidden;
}

:deep(.panel-container) {
    display: flex;
    flex-direction: column;
    flex-wrap: nowrap;
    flex: 1 1 auto;
    min-height: 0;
}

:deep(.variables-panel) {
    flex: 0 0 auto;
}

:deep(.sensors-panel) {
    display: flex;
    flex-direction: column;
    flex: 1 1 auto;
    min-height: 0;
}

:deep(.sensors-panel .v-expansion-panel-text) {
    flex: 1 1 auto;
    min-height: 0;
    overflow: hidden;
}

:deep(.sensors-panel .v-expansion-panel-text__wrapper) {
    height: 100%;
    overflow-y: auto;
}
</style>