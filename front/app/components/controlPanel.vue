<template>
    <v-navigation-drawer v-model="isOpen" location="left" width="300" class="pa-2" absolute persistent mobile
        :scrim="false" style="height:100%; z-index:9999; top:0;">
        <!-- <v-row>
            <v-col cols="1">
                <v-btn icon>
                     <v-icon @click="isOpen = !isOpen">mdi-chevron-left</v-icon>
                </v-btn>
            </v-col>
        </v-row> -->

        <v-expansion-panels focusable inset multiple>
            <v-expansion-panel>
                <v-expansion-panel-title class="text-subtitle-1">Colorbar Settings</v-expansion-panel-title>
                <v-expansion-panel-text>
                    <colorBarSelect @autorange="autorange" />
                </v-expansion-panel-text>
            </v-expansion-panel>

            <v-expansion-panel>
                <v-expansion-panel-title class="text-subtitle-1">Overlays</v-expansion-panel-title>
                <v-expansion-panel-text>
                    <overlays @toggle-vertical-profile="drawerOpen = !drawerOpen" @show-how="showHow = true" />
                </v-expansion-panel-text>
            </v-expansion-panel>

            <!-- SENSOR -->
            <v-expansion-panel >
                <v-expansion-panel-title class="text-subtitle-1">Sensors</v-expansion-panel-title>
                <v-expansion-panel-text class="ma-0 pa-0">
                    <sensorInfo />
                </v-expansion-panel-text>
            </v-expansion-panel>
        </v-expansion-panels>
    </v-navigation-drawer>
</template>


<script setup lang="ts">
import { ref, watch, onMounted } from 'vue';

const isOpen = ref(true);
</script>

<style scoped lang="scss">
:deep(.v-expansion-panel-text__wrapper) {
  padding: 0;
}
</style>