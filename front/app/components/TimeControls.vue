<template>
  <v-row class="time-controls">
    <v-spacer></v-spacer>
    <v-btn size="x-small" icon flat :title="'Move to start'" @click="moveToStart"><v-icon>mdi-skip-backward</v-icon></v-btn>
    <v-btn size="x-small" icon flat :title="'Step backward'" @click="stepBackward"><v-icon>mdi-skip-previous</v-icon></v-btn>
    <v-btn size="x-small" icon flat :title="playing ? 'Pause' : 'Play'" @click="togglePlay"><v-icon v-if="!playing">mdi-play</v-icon><v-icon v-else>mdi-pause</v-icon></v-btn>
    <v-btn size="x-small" icon flat :title="'Step forward'" @click="stepForward"><v-icon>mdi-skip-next</v-icon></v-btn>
    <v-btn size="x-small" icon flat :title="'Move to end'" @click="moveToEnd"><v-icon>mdi-skip-forward</v-icon></v-btn>
    <v-divider vertical class="mx-2" style="height: 24px"></v-divider>

    <v-btn size="x-small" icon flat :color="loop ? 'primary' : undefined" :title="'Loop: ' + (loop ? 'on' : 'off')" @click="loop = !loop"><v-icon>mdi-repeat</v-icon></v-btn>

    <v-menu offset-y>
      <template #activator="{ props }">
        <v-btn v-bind="props" size="x-small" icon flat :title="`Speed: x${speed}`"><v-icon>mdi-speedometer</v-icon></v-btn>
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

const props = defineProps<{
  timestamps: number[]; // epoch ms sorted ascending
  currentDt: moment.Moment | null;
}>();
const emit = defineEmits(['update:dt']);

const playing = ref(false);
const loop = ref(false);
const speeds = [0.5, 1, 2];
const speed = ref(1);
let timer: number | null = null;
const baseInterval = 1000; // ms between steps at 1x speed

function currentIndex() {
  if (!props.timestamps || props.timestamps.length === 0) return -1;
  const cur = props.currentDt ? props.currentDt.valueOf() : null;
  if (cur === null) return -1;
  // find nearest index
  let best = 0;
  for (let i = 0; i < props.timestamps.length; i++) {
    if (Math.abs(props.timestamps[i] - cur) < Math.abs(props.timestamps[best] - cur)) best = i;
  }
  return best;
}

function getIndexForDt(dt: moment.Moment | null) {
  if (!dt) return -1;
  const val = dt.valueOf();
  for (let i = 0; i < props.timestamps.length; i++) {
    if (props.timestamps[i] === val) return i;
  }
  // nearest
  let best = 0;
  for (let i = 0; i < props.timestamps.length; i++) {
    if (Math.abs(props.timestamps[i] - val) < Math.abs(props.timestamps[best] - val)) best = i;
  }
  return best;
}

function emitForIndex(i: number) {
  if (!props.timestamps || props.timestamps.length === 0) return;
  i = Math.max(0, Math.min(props.timestamps.length - 1, i));
  emit('update:dt', moment.utc(props.timestamps[i]));
}

function stepForward() {
  if (!props.timestamps || props.timestamps.length === 0) return;
  const idx = currentIndex();
  if (idx < 0) {
    // no current, jump to first
    emitForIndex(0);
    return;
  }
  if (idx < props.timestamps.length - 1) emitForIndex(idx + 1);
  else if (loop.value) emitForIndex(0);
}

function stepBackward() {
  if (!props.timestamps || props.timestamps.length === 0) return;
  const idx = currentIndex();
  if (idx < 0) {
    emitForIndex(props.timestamps.length - 1);
    return;
  }
  if (idx > 0) emitForIndex(idx - 1);
  else if (loop.value) emitForIndex(props.timestamps.length - 1);
}

function moveToStart() {
  if (!props.timestamps || props.timestamps.length === 0) return;
  emitForIndex(0);
}
function moveToEnd() {
  if (!props.timestamps || props.timestamps.length === 0) return;
  emitForIndex(props.timestamps.length - 1);
}

function setSpeed(s: number) {
  speed.value = s;
  if (playing.value) restartTimer();
}

function startTimer() {
  stopTimer();
  const interval = Math.max(50, baseInterval / (speed.value || 1));
  timer = window.setInterval(() => {
    // advance one step
    const idx = currentIndex();
    if (idx < 0) { emitForIndex(0); return; }
    if (idx < props.timestamps.length - 1) emitForIndex(idx + 1);
    else {
      if (loop.value) emitForIndex(0);
      else togglePlay(); // stop
    }
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

watch(() => props.timestamps, (nv) => {
  // if timestamps updated and playing, ensure timer continues
  if (playing.value) restartTimer();
});

onBeforeUnmount(() => stopTimer());
</script>

<style scoped>
.time-controls {
  display: flex;
  align-items: center;
  gap: 6px;
  margin:0;
  padding:0;
}
</style>
