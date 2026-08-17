<template>
  <v-row class="time-controls">
    <v-spacer></v-spacer>
    <template v-if="!hideDatePicker">
      <v-menu v-model="datePickerOpen" :close-on-content-click="false" offset-y>
        <template #activator="{ props: menuProps }">
          <v-btn v-bind="menuProps" size="20px" icon flat :title="'Jump to date'"><v-icon size="14px">mdi-calendar</v-icon></v-btn>
        </template>
        <v-card>
          <v-date-picker v-model="pickedDate" :allowed-dates="allowedDates" hide-header show-adjacent-months :max="maxDate"
            :min="minDate"></v-date-picker>
          <v-card-actions>
            <v-spacer></v-spacer>
            <v-btn text @click="cancelDatePicker">Cancel</v-btn>
            <v-btn color="primary" @click="confirmDatePicker">OK</v-btn>
          </v-card-actions>
        </v-card>
      </v-menu>

      <v-divider vertical class="mx-2" style="height: 24px"></v-divider>
    </template>

    <v-btn size="20px" icon flat :title="`Back one ${unitLabel}`" @click="stepBackward"><v-icon
        size="14px">mdi-skip-previous</v-icon></v-btn>
    <v-btn size="20px" icon flat :title="playing ? 'Pause' : 'Play'" @click="togglePlay">
      <v-icon size="14px" v-if="!playing">mdi-play</v-icon>
      <v-icon size="14px" v-else>mdi-pause </v-icon>
    </v-btn>
    <v-btn size="20px" icon flat :title="`Forward one ${unitLabel}`" @click="stepForward"><v-icon size="14px">mdi-skip-next</v-icon></v-btn>

    <v-divider vertical class="mx-2" style="height: 24px"></v-divider>

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
import { ref, computed, onBeforeUnmount } from 'vue';
import moment from 'moment';

import { useMainStore } from '../stores/main'
import { addBins, floorToBin } from '~~/composables/useTimeDepthWindow'
const mainStore = useMainStore();

////////////////////////////////////  PROPS & STATE  ///////////////////////////////////

// ExplorePanel already has its own jump-to-date control (the clickable range
// label next to the paging arrows) that also recentres its window on the
// picked date — this icon would be a second, redundant way to do the same
// thing there. Cross-Section has no such control of its own, so it keeps this
// as its only way to jump to an arbitrary date.
const props = defineProps<{ hideDatePicker?: boolean }>()

const playing = ref(false);
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
  return mainStore.variables.find(v =>  v.source === selectedVariable.value.source && v.var === selectedVariable.value.var)?.dts
    .map(ts => moment.utc(ts))
});

// Bin mode (1H/1D/1M) drives what one "step" means. Lives on the store —
// ExplorePanel's own toggle — since these controls have no window/coverage
// of their own to derive it from.
const binMode = computed(() => mainStore.exploreBinMode);
const unitLabel = computed(() => ({ hourly: 'hour', daily: 'day', monthly: 'month' }[binMode.value]));

// Only hourly is bounded by a known timestamp list — daily/monthly reach back
// far further (up to two decades) than that list covers, so the picker stays
// unrestricted in those modes rather than falsely limiting it to the hourly
// window.
const minDate = computed(() => {
  if (binMode.value !== 'hourly') return undefined;
  if (!dts.value || dts.value.length === 0) return null;
  return moment.utc(Math.min(...dts.value)).format('YYYY-MM-DD');
});

const maxDate = computed(() => {
  if (binMode.value !== 'hourly') return undefined;
  if (!dts.value || dts.value.length === 0) return null;
  return moment.utc(Math.max(...dts.value)).format('YYYY-MM-DD');
});

///////////////////////////////////  METHODS  ///////////////////////////////////

function allowedDates(date: string) {
  if (binMode.value !== 'hourly') return true;
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

// Hourly steps to the next/previous *real* model output instant (tiles are
// keyed to actual ERDDAP timestamps, not a clean top-of-hour grid) — daily/
// monthly have no such fixed list, so they just add/subtract one calendar
// bin via the same `addBins`/`floorToBin` the depth-section window uses.
// Deliberately unbounded by any chart window: stepping never recentres a
// chart (only an explicit date-picker jump does), so there is nothing here
// for a step to stay inside of.
function stepHourly(dir: 1 | -1) {
  const idx = currentIndex();
  if (idx < 0) return false
  const newIdx = idx + dir
  if (newIdx < 0 || newIdx >= dts.value.length) return false
  mainStore.updateSelectedVariable({ dt: moment.utc(dts.value[newIdx]) });
  return true
}

function stepCoarse(dir: 1 | -1) {
  const cur = selectedDatetime.value;
  if (!cur) return false
  const next = addBins(floorToBin(cur.toDate(), binMode.value), dir, binMode.value);
  // Forward-only cap at "now" — there's nothing to play into past today.
  // Backward is intentionally open-ended: daily/monthly reach back up to two
  // decades and there's no cheap coverage bound available here to clamp to.
  if (dir > 0 && next.getTime() > floorToBin(new Date(), binMode.value).getTime()) return false
  mainStore.updateSelectedVariable({ dt: moment.utc(next) });
  return true
}

function stepForward() {
  return binMode.value === 'hourly' ? stepHourly(1) : stepCoarse(1);
}

function stepBackward() {
  return binMode.value === 'hourly' ? stepHourly(-1) : stepCoarse(-1);
}

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

function confirmDatePicker() {
  if (!pickedDate.value) return;
  const picked = Array.isArray(pickedDate.value) ? pickedDate.value[0] : pickedDate.value;

  if (binMode.value === 'hourly') {
    // Find the closest available timestamp to the picked date and update selected variable
    const idx = getIndexForDt(moment.utc(picked));
    if (idx >= 0) mainStore.updateSelectedVariable({ dt: moment.utc(dts.value[idx]) });
  } else {
    // v-date-picker hands back a Date via its LOCAL Y/M/D getters — carry the
    // calendar day across via UTC construction (not moment.utc(picked)
    // directly, which would reinterpret the same instant at the browser's UTC
    // offset and drift the day), matching useTimeDepthWindow's own picker.
    const centreUTC = new Date(Date.UTC(picked.getFullYear(), picked.getMonth(), picked.getDate()));
    mainStore.updateSelectedVariable({ dt: moment.utc(floorToBin(centreUTC, binMode.value)) });
  }

  datePickerOpen.value = false;
}

function cancelDatePicker() {
  pickedDate.value = null;
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
