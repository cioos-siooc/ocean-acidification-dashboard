<template>
  <v-card class="colorbar">
    <v-btn icon size="x-small" flat :variant="showBathymetryContours ? 'elevated' : 'text'"
      :color="showBathymetryContours ? 'primary' : undefined"
      @click="mainStore.setShowBathymetryContours(!showBathymetryContours)"
      :title="showBathymetryContours ? 'Hide bathymetry contours' : 'Show bathymetry contours'">
      <IconsContour />
    </v-btn>

    <v-btn icon size="x-small" flat @click="toggleVerticalProfile" title="Vertical Profile">
      <IconsProfile />
    </v-btn>

  </v-card>
</template>

<script setup lang="ts">
import { computed, toRef, ref, watch } from 'vue';

import { useMainStore } from '../stores/main'
const mainStore = useMainStore();

const emit = defineEmits<{
  (e: 'toggle-vertical-profile'): void;
}>();

const showBathymetryContours = computed(() => mainStore.showBathymetryContours);

function toggleVerticalProfile() {
  emit('toggle-vertical-profile');
}
</script>

<style scoped>
.colorbar {
  padding: 6px 8px;
  /* background: rgba(255, 255, 255, 0.9); */
  border-radius: 6px;
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.2);
  font-family: Inter, system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue';
  font-size: 11px;
}
</style>