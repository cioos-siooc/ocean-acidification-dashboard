<template>
  <v-row class="time-controls">
    <v-spacer></v-spacer>
    <v-menu v-model="datePickerOpen" :close-on-content-click="false" offset-y>
      <template #activator="{ props: menuProps }">
        <v-btn v-bind="menuProps" size="20px" icon flat :title="'Jump to date'"><v-icon size="14px">mdi-calendar</v-icon></v-btn>
      </template>
      <v-date-picker v-model="pickedDate" :allowed-dates="allowedDates" hide-header show-adjacent-months :max="maxDate"
        :min="minDate" @update:model-value="onDatePicked"></v-date-picker>
    </v-menu>

    <v-divider vertical class="mx-2" style="height: 24px"></v-divider>

    <!-- <v-btn size="x-small" icon flat :title="'Move to start'"
      @click="moveToStart"><v-icon>mdi-skip-backward</v-icon></v-btn> -->
    <v-btn size="20px" icon flat :title="'Step backward'" @click="stepBackward"><v-icon
        size="14px">mdi-skip-previous</v-icon></v-btn>
    <v-btn size="20px" icon flat :title="playing ? 'Pause' : 'Play'" @click="togglePlay">
      <v-icon size="14px" v-if="!playing">mdi-play</v-icon>
      <v-icon size="14px" v-else>mdi-pause </v-icon>
    </v-btn>
    <v-btn size="20px" icon flat :title="'Step forward'" @click="stepForward"><v-icon size="14px">mdi-skip-next</v-icon></v-btn>
    <!-- <v-btn size="x-small" icon flat :title="'Move to end'" @click="moveToEnd"><v-icon>mdi-skip-forward</v-icon></v-btn> -->

    <v-divider vertical class="mx-2" style="height: 24px"></v-divider>

    <!-- <v-btn size="x-small" icon flat :color="loop ? 'primary' : undefined" :title="'Loop: ' + (loop ? 'on' : 'off')"
      @click="loop = !loop"><v-icon>mdi-repeat</v-icon></v-btn> -->

    <v-menu offset-y>
      <template #activator="{ props }">
        <v-btn v-bind="props" size="20px" icon flat
          :title="`Speed: x${speed}`"><v-icon size="14px">mdi-speedometer</v-icon></v-btn>
      </template>
      <v-list>
        <v-list-item v-for="s in speeds" :key="s" @click="setSpeed(s)">
          <v-list-item-title>{{ s }}x</v-list-item-title>
        </v-list-item>
      </v-list>
    </v-menu>
    <v-spacer></v-spacer>
  </v-row>
</template>

<script setup lang="ts">
import { ref, computed, watch, onBeforeUnmount } from 'vue';
import moment from 'moment';

import { useMainStore } from '../stores/main'
const mainStore = useMainStore();

////////////////////////////////////  PROPS & STATE  ///////////////////////////////////

// const props = defineProps<{
//   currentDt: moment.Moment | null;
// }>();
const emit = defineEmits(['update:dt']);

const playing = ref(false);
// const loop = ref(false);
const datePickerOpen = ref(false);
const pickedDate = ref<Date | null>(null);

const speeds = [0.5, 1, 2];
const speed = ref(1);
let timer: number | null = null;
const baseInterval = 1000; // ms between steps at 1x speed

////////////////////////////////  COMPUTED  ///////////////////////////////////

const selectedVariable = computed(() => mainStore.selected_variable);

const selectedDatetime = computed(() => {
  const dt = selectedVariable.value?.dt;
  return dt ? moment.utc(dt) : null;
});

const dts = computed(() => {
  return mainStore.variables.find(v => v.name === selectedVariable.value.name)?.dts
    .map(ts => moment.utc(ts))
  // .format('YYYY-MM-DD'))
  // .reduce((set, key) => set.add(key), new Set<string>());
});

const minDate = computed(() => {
  if (!dts.value || dts.value.length === 0) return null;
  return moment.utc(Math.min(...dts.value)).format('YYYY-MM-DD');
});

const maxDate = computed(() => {
  if (!dts.value || dts.value.length === 0) return null;
  return moment.utc(Math.max(...dts.value)).format('YYYY-MM-DD');
});

const DFN = computed(() => mainStore.dfnDays);
const midDate = computed(() => mainStore.midDate);

///////////////////////////////////  WATCHERS  ///////////////////////////////////

// watch(() => props.timestamps, (nv) => {
//   // if timestamps updated and playing, ensure timer continues
//   if (playing.value) restartTimer();
// });

///////////////////////////////////  METHODS  ///////////////////////////////////

function allowedDates(date: string) {
  if (!dts.value) return false;
  const time = moment.utc(date).valueOf();
  return dts.value.some(dt => Math.abs(dt - time) < 12 * 3600 * 1000); // allow if within 12 hours of any timestamp
}

function currentIndex() {
  const cur = selectedDatetime.value ? selectedDatetime.value.valueOf() : null;
  if (cur === null) return -1;
  // find nearest index
  let best = 0;
  for (let i = 0; i < dts.value.length; i++) {
    if (Math.abs(dts.value[i] - cur) < Math.abs(dts.value[best] - cur)) best = i;
  }
  return best;
}

function getIndexForDt(dt: moment.Moment | null) {
  if (!dt) return -1;
  const target = dt.valueOf();
  // find nearest index
  let best = 0;
  for (let i = 0; i < dts.value.length; i++) {
    if (Math.abs(dts.value[i] - target) < Math.abs(dts.value[best] - target)) best = i;
  }
  return best;
}

// function emitForIndex(i: number) {
//   if (!props.timestamps || props.timestamps.length === 0) return;
//   i = Math.max(0, Math.min(props.timestamps.length - 1, i));
//   emit('update:dt', moment.utc(props.timestamps[i]));
// }

function stepForward() {
  const idx = currentIndex();
  if (idx < 0) return false
  if (dts.value[idx]?.valueOf() > midDate.value?.clone().add(DFN.value, 'days').valueOf()) {
    return false
  }
  if (idx < dts.value.length - 1) {
    const dt = moment.utc(dts.value[idx + 1]);
    mainStore.updateSelectedVariable({ dt });
    return true
  }
  else {
    return false
  }
}

function stepBackward() {
  const idx = currentIndex();
  if (idx < 0) return false
  if (dts.value[idx]?.valueOf() < midDate.value?.clone().subtract(DFN.value, 'days').valueOf()) {
    return false
  }
  if (idx > 0) {
    const dt = moment.utc(dts.value[idx - 1]);
    mainStore.updateSelectedVariable({ dt });
    return true
  }
  else {
    return false
  }
}

// function moveToStart() {
//   if (!props.timestamps || props.timestamps.length === 0) return;
//   emitForIndex(0);
// }
// function moveToEnd() {
//   if (!props.timestamps || props.timestamps.length === 0) return;
//   emitForIndex(props.timestamps.length - 1);
// }

function setSpeed(s: number) {
  speed.value = s;
  if (playing.value) restartTimer();
}

function startTimer() {
  stopTimer();
  const interval = Math.max(50, baseInterval / (speed.value || 1));
  timer = window.setInterval(() => {
    stepForward() || togglePlay();
  }, interval) as unknown as number;
}
function stopTimer() {
  if (timer != null) { clearInterval(timer); timer = null; }
}

function restartTimer() { startTimer(); }

function togglePlay() {
  playing.value = !playing.value;
  if (playing.value) startTimer(); else stopTimer();
}

function onDatePicked(date: string) {
  mainStore.setMidDate(moment.utc(date));

  // Find the closest available timestamp to the picked date and update selected variable
  const idx = getIndexForDt(moment.utc(date));
  if (idx >= 0) {
    const dt = moment.utc(dts.value[idx]);
    mainStore.updateSelectedVariable({ dt });
  }

  datePickerOpen.value = false;
}

onBeforeUnmount(() => stopTimer());
</script>

<style scoped>
.time-controls {
  display: flex;
  align-items: center;
  gap: 6px;
  margin: 0;
  padding: 0;
}
</style>
