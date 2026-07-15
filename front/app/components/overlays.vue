<template>
  <v-card class="colorbar" :class="{ 'colorbar--expanded': isHovering }" @mouseenter="isHovering = true"
    @mouseleave="isHovering = false">
    <div class="colorbar-inner">
      <!-- TOGGLE NAVIGATION PANEL -->
        <v-btn flat :variant="isControlPanelOpen ? 'elevated' : 'text'"
          @click="mainStore.toggleIsControlPanelOpen" class="ma-1 overlay-btn">
          <v-icon size="16px">{{ isControlPanelOpen ? 'mdi-menu-open' : 'mdi-menu' }}</v-icon>
          <span class="overlay-btn__label">Toggle Navigation Panel</span>
        </v-btn>

        <v-divider></v-divider>

        <!-- BATHYMETRY CONTOURS -->
        <v-btn flat :variant="showBathymetryContours ? 'elevated' : 'text'"
          :color="showBathymetryContours ? 'primary' : undefined"
          @click="mainStore.setShowBathymetryContours(!showBathymetryContours)" class="ma-1 overlay-btn">
          <IconsContour />
          <span class="overlay-btn__label">Bathymetry Contours</span>
        </v-btn>

        <!-- CURSOR COORDINATES -->
        <v-btn flat :variant="showCursorCoords ? 'elevated' : 'text'"
          @click="mainStore.setShowCursorCoords(!showCursorCoords)" class="ma-1 overlay-btn">
          <v-icon size="16px">{{ showCursorCoords ? 'mdi-cursor-default' : 'mdi-cursor-default-outline' }}</v-icon>
          <span class="overlay-btn__label">Cursor Coordinates</span>
        </v-btn>

        <!-- VERTICAL PROFILE -->
        <v-btn flat @click="toggleVerticalProfile" class="ma-1 overlay-btn">
          <IconsProfile />
          <span class="overlay-btn__label">Vertical Profile</span>
        </v-btn>

        <v-divider></v-divider>

        <!-- AUTO COLOR -->
        <v-btn flat
          :disabled="!selectedVariableName || selectedVariableName === 'bathymetry' || mainStore.autoRangeDisabled"
          @click="autorange" class="ma-1 overlay-btn">
          <IconsAutorange />
          <span class="overlay-btn__label">Auto Color</span>
        </v-btn>

        <!-- COLOR SETTINGS -->
        <v-btn flat @click="showColorbarSettings = !showColorbarSettings" class="ma-1 overlay-btn">
          <IconsConfig />
          <span class="overlay-btn__label">Color Settings</span>
        </v-btn>

        <!-- <v-divider></v-divider> -->

        <!-- <v-btn flat @click="showHow" class="ma-1 overlay-btn" disabled>
          <IconsHelp />
          <span class="overlay-btn__label">How to Use</span>
        </v-btn> -->
    </div>
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

////////////////////////////////////// STATE //////////////////////////////////////

const isHovering = ref(false);

////////////////////////////////////// COMPUTED //////////////////////////////////////

const showColorbarSettings = computed({
  get: () => mainStore.showColorbarSettings,
  set: (val: boolean) => mainStore.setShowColorbarSettings(val)
});

const selectedVariableName = computed(() => mainStore.selected_variable.var);

const showBathymetryContours = computed(() => mainStore.showBathymetryContours);

const showCursorCoords = computed(() => mainStore.showCursorCoords);

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
  width: 40px;
  transition: width 0.25s ease;
  border-radius: 6px;
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.2);
  font-family: Inter, system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue';
  font-size: 11px;
  overflow: hidden;
}

.colorbar--expanded {
  width: 190px;
}

.colorbar-inner {
  display: flex;
  flex-direction: column;
}

.overlay-btn {
  display: flex;
  align-items: center;
  justify-content: flex-start;
  min-width: 0 !important;
  padding: 0 8px !important;
  overflow: hidden;
}

.overlay-btn__label {
  margin-left: 8px;
  white-space: nowrap;
  opacity: 0;
  max-width: 0;
  overflow: hidden;
  transition: opacity 0.15s ease, max-width 0.2s ease;
}

.colorbar--expanded .overlay-btn__label {
  opacity: 1;
  max-width: 160px;
}
</style>
