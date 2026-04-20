<template>
  <v-card class="colorbar"
    :style="{ left: (mainStore.isControlPanelOpen ? mainStore.controlPanel_width + 16 : 16) + 'px' }">
    <v-col class="pa-0">
      <!-- <v-row class="mx-1 my-1 px-0"> -->
      <v-btn icon size="20px" flat :variant="showBathymetryContours ? 'elevated' : 'text'"
        :color="showBathymetryContours ? 'primary' : undefined"
        @click="mainStore.setShowBathymetryContours(!showBathymetryContours)"
        :title="showBathymetryContours ? 'Hide bathymetry contours' : 'Show bathymetry contours'" class="ma-2">
        <IconsContour />
      </v-btn>
      <!-- </v-row> -->

      <!-- <v-row class="mx-1 my-1 px-0"> -->
      <v-btn icon size="20px" flat @click="toggleVerticalProfile" title="Vertical Profile" class="ma-2">
        <IconsProfile />
      </v-btn>
      <!-- </v-row> -->

      <v-divider></v-divider>

      <v-btn icon size="20px" flat
        :disabled="!selectedVariableName || selectedVariableName === 'bathymetry' || mainStore.autoRangeDisabled"
        @click="autorange" title="Auto-range colorbar to data range" class="ma-2">
        <IconsAutorange />
      </v-btn>

      <v-btn size="20px" disabled icon @click="showSettings = !showSettings" class="ma-2">
        <IconsConfig />
      </v-btn>

      <v-divider></v-divider>

      <!-- <v-row class="mx-1 my-1 px-0"> -->
      <v-btn icon size="20px" flat @click="showHow" title="How to use" class="ma-2">
        <IconsHelp />
      </v-btn>
      <!-- </v-row> -->
    </v-col>
  </v-card>
</template>

<script setup lang="ts">
import { computed, toRef, ref, watch } from 'vue';

import { useMainStore } from '../stores/main'
const mainStore = useMainStore();

const emit = defineEmits<{
  (e: 'toggle-vertical-profile'): void;
  (e: 'show-how'): void;
  (e: 'autorange'): void;
}>();

const showSettings = ref(false);

////////////////////////////////////// COMPUTED //////////////////////////////////////

const selectedVariableName = computed(() => mainStore.selected_variable.var);

const showBathymetryContours = computed(() => mainStore.showBathymetryContours);

////////////////////////////////////// METHODS //////////////////////////////////////


function autorange() {
  mainStore.setAutoRangeDisabled(true);
  emit('autorange');
}

function toggleVerticalProfile() {
  emit('toggle-vertical-profile');
}

const showHow = () => {
  emit('show-how');
}
</script>

<style scoped>
.colorbar {
  position: absolute;
  padding: 3px;
  width: fit-content;
  transition: left 0.3s ease;
  /* background: rgba(255, 255, 255, 0.9); */
  border-radius: 6px;
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.2);
  font-family: Inter, system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue';
  font-size: 11px;
}
</style>