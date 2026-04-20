<template>
  <div
    v-if="visible"
    class="sensor-picker"
    :style="{ left: x + 'px', top: y + 'px' }"
  >
    <div class="sensor-picker__title">Select sensor</div>
    <v-list density="compact" class="sensor-picker__list pa-0">
      <v-list-item
        v-for="sensor in sortedSensors"
        :key="sensor.id"
        class="sensor-picker__item"
        @click="pick(sensor)"
      >
        <v-list-item-title class="text-body-2">{{ sensor.name }}</v-list-item-title>
        <v-list-item-subtitle class="text-caption">{{ sensor.depth }} m depth</v-list-item-subtitle>
      </v-list-item>
    </v-list>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import type { MultiSensorCandidate } from '../../../composables/useBuoyLayer';

const props = defineProps<{
  visible: boolean;
  x: number;
  y: number;
  sensors: MultiSensorCandidate[];
}>();

const emit = defineEmits<{
  (e: 'pick', sensor: MultiSensorCandidate): void;
  (e: 'close'): void;
}>();

const sortedSensors = computed(() =>
  [...props.sensors].sort((a, b) => a.depth - b.depth)
);

function pick(sensor: MultiSensorCandidate) {
  emit('pick', sensor);
  emit('close');
}
</script>

<style scoped>
.sensor-picker {
  position: absolute;
  z-index: 10000;
  background: #1e1e1e;
  border: 1px solid rgba(255, 255, 255, 0.15);
  border-radius: 6px;
  box-shadow: 0 4px 16px rgba(0, 0, 0, 0.5);
  min-width: 180px;
  max-width: 260px;
  transform: translate(-50%, calc(-100% - 12px));
  pointer-events: all;
}

.sensor-picker__title {
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: rgba(255, 255, 255, 0.5);
  padding: 8px 12px 4px;
}

.sensor-picker__list {
  background: transparent !important;
}

.sensor-picker__item {
  cursor: pointer;
  border-radius: 4px;
  margin: 0 4px 2px;
}

.sensor-picker__item:hover {
  background: rgba(255, 255, 255, 0.08) !important;
}
</style>
