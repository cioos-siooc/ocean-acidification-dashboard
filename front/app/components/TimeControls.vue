<template>
  <div class="flex flex-wrap -m-3 time-controls">
    <div class="grow" />
    <template v-if="!hideDatePicker">
      <UPopover v-model:open="datePickerOpen">
  <UButton variant="solid" class="size-[20px] p-0 justify-center shrink-0" :title="'Jump to date'"><UIcon name="i-mdi-calendar" class="size-[14px]" /></UButton>
  <template #content>
    <div class="bg-elevated rounded-lg">
              <UCalendar v-model="pickedCalendarDate" :min-value="minCalendarDate" :max-value="maxCalendarDate"
                :is-date-disabled="isCalendarDateDisabled" />
              <div class="flex items-center gap-2 px-2 py-2">
                <div class="grow" />
                <UButton variant="ghost" @click="cancelDatePicker">Cancel</UButton>
                <UButton color="primary" @click="confirmDatePicker">OK</UButton>
              </div>
            </div>
  </template>
</UPopover>

      <USeparator orientation="vertical" class="mx-2" style="height: 24px" />
    </template>

    <UButton variant="solid" class="size-[20px] p-0 justify-center shrink-0" :title="`Back one ${unitLabel}`" @click="stepBackward"><UIcon name="i-mdi-skip-previous" class="size-[14px]" /></UButton>
    <UButton variant="solid" class="size-[20px] p-0 justify-center shrink-0" :title="playing ? 'Pause' : 'Play'" @click="togglePlay">
      <UIcon name="i-mdi-play" class="size-[14px]" v-if="!playing" />
      <UIcon name="i-mdi-pause" class="size-[14px]" v-else />
    </UButton>
    <UButton variant="solid" class="size-[20px] p-0 justify-center shrink-0" :title="`Forward one ${unitLabel}`" @click="stepForward"><UIcon name="i-mdi-skip-next" class="size-[14px]" /></UButton>

    <USeparator orientation="vertical" class="mx-2" style="height: 24px" />

    <UPopover>
  <UButton variant="solid" class="size-[20px] p-0 justify-center shrink-0" :title="`Speed: x${speed}`"><UIcon name="i-mdi-speedometer" class="size-[14px]" /></UButton>
  <template #content>
    <div class="py-1">
            <div v-for="s in speeds" :key="s" role="button" tabindex="0" @click="setSpeed(s)"
              class="px-3 py-1 text-sm cursor-pointer hover:bg-elevated">
              {{ s }}x
            </div>
          </div>
  </template>
</UPopover>
    <div class="grow" />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onBeforeUnmount } from 'vue';
import moment from 'moment';

import { useMainStore } from '../stores/main'
import { addBins, floorToBin } from '~~/composables/useTimeDepthWindow'
import { toCalendarDate, fromCalendarDate } from '~~/composables/useCalendarDate'
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

// UCalendar speaks CalendarDate; the rest of this component keeps working in
// Date. See composables/useCalendarDate.ts for why the boundary sits here.
const pickedCalendarDate = computed({
  get: () => toCalendarDate(pickedDate.value),
  set: (v) => { pickedDate.value = fromCalendarDate(v) },
});
const minCalendarDate = computed(() => toCalendarDate(minDate.value) ?? undefined);
const maxCalendarDate = computed(() => toCalendarDate(maxDate.value) ?? undefined);
const isCalendarDateDisabled = (d: { year: number; month: number; day: number }) =>
  !allowedDates(new Date(d.year, d.month - 1, d.day));

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
