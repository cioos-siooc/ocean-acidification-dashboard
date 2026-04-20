<template>
    <v-navigation-drawer v-model="isOpen" location="left" :width="mainStore.controlPanel_width" class="pa-2" absolute persistent mobile
        :scrim="false" style="height:100%; z-index:9999; top:0;">
        <!-- <v-row>
            <v-col cols="1">
                <v-btn icon>
                     <v-icon @click="isOpen = !isOpen">mdi-chevron-left</v-icon>
                </v-btn>
            </v-col>
        </v-row> -->

        <v-expansion-panels v-model="panels" focusable inset multiple>
            <v-expansion-panel value="variables">
                <v-expansion-panel-title class="text-subtitle-1">Variables</v-expansion-panel-title>
                <v-expansion-panel-text>
                    <colorBarSelect @autorange="autorange" />
                </v-expansion-panel-text>
            </v-expansion-panel>

            <!-- SENSOR -->
            <v-expansion-panel value="sensors">
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

const panels = ref<Array<string>>(['variables']);

const isOpen = computed(() => mainStore.isControlPanelOpen);
</script>

<style scoped lang="scss">
:deep(.v-expansion-panel-text__wrapper) {
    padding: 0;
}
</style>