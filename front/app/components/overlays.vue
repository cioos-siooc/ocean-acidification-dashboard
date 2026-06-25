<template>
  <v-card class="colorbar" width="40px">
    <v-col class="pa-0">

      <!-- TOGGLE NAVIGATION PANEL -->
      <v-btn icon size="20px" flat :variant="isControlPanelOpen ? 'elevated' : 'text'"
        @click="mainStore.toggleIsControlPanelOpen" title="Toggle Navigation Panel" class="ma-2">
        <v-icon size="16px">{{ isControlPanelOpen ? 'mdi-menu-open' : 'mdi-menu' }}</v-icon>
      </v-btn>

      <v-divider></v-divider>

      <!-- BATHYMETRY CONTOURS -->
      <v-btn icon size="20px" flat :variant="showBathymetryContours ? 'elevated' : 'text'"
        :color="showBathymetryContours ? 'primary' : undefined"
        @click="mainStore.setShowBathymetryContours(!showBathymetryContours)" title="Bathymetry Contours" class="ma-2">
        <IconsContour />
      </v-btn>

      <!-- VERTICAL PROFILE -->
      <v-btn icon size="20px" flat @click="toggleVerticalProfile" title="Vertical Profile" class="ma-2">
        <IconsProfile />
      </v-btn>

      <!-- MONTHLY CLIMATOLOGY -->
      <v-btn icon size="20px" flat title="Long-term climatology" class="ma-2" 
        @click="mainStore.setShowClimatologyDialog(true)">
         <IconsStats />
      </v-btn>

      <v-divider></v-divider>

      <!-- AUTO COLOR -->
      <v-btn icon size="20px" flat
        :disabled="!selectedVariableName || selectedVariableName === 'bathymetry' || mainStore.autoRangeDisabled"
        @click="autorange" title="Auto Color" class="ma-2">
        <IconsAutorange />
      </v-btn>

      <!-- COLOR SETTINGS -->
      <v-btn size="20px" flat icon @click="showColorbarSettings = !showColorbarSettings" title="Color Settings"
        class="ma-2">
        <IconsConfig />
      </v-btn>

      <!-- <v-divider></v-divider> -->

      <!-- <v-row class="mx-1 my-1 px-0"> -->
      <!-- <v-btn icon size="20px" flat @click="showHow" title="How to Use" class="ma-2" disabled>
        <IconsHelp />
      </v-btn> -->
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

////////////////////////////////////// COMPUTED //////////////////////////////////////

const showColorbarSettings = computed({
  get: () => mainStore.showColorbarSettings,
  set: (val: boolean) => mainStore.setShowColorbarSettings(val)
});

const selectedVariableName = computed(() => mainStore.selected_variable.var);

const showBathymetryContours = computed(() => mainStore.showBathymetryContours);

const isControlPanelOpen = computed(() => mainStore.isControlPanelOpen);

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