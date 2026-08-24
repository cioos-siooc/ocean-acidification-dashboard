<template>
  <div class="colorbar bg-elevated" style="max-width:200px; width:fit-content;">
    <div class="flex flex-wrap my-0 mx-2 p-0">
      <!-- Says whose depth this is. Without it the box reads as a description
           of whatever the user just clicked, and a sensor at 1257 m sitting
           beside a model level at 441.5 m looks like a contradiction rather
           than two different things. This box only ever describes the raster
           layer the map is painting. -->
      <div class="w-full m-0 p-0 layer-label" style="height:16px">
        <span>MAP LAYER &middot; {{ selectedVariable.source }}</span>
      </div>

      <div class="w-full m-0 p-0" style="height:20px">
        <span>{{ variableLabel(selectedVariable.var) }}</span>
      </div>

      <div class="w-full m-0 p-0" style="height:20px">
        <span>{{ formattedDt }}</span>
      </div>
      <div class="w-full m-0 p-0" style="height:20px">
        <span>Depth {{ selectedVariable.depth }}{{ selectedVariable.depth && !isNaN(Number(selectedVariable.depth)) ? ' m' : '' }}</span>
      </div>

      <!-- The layer is as empty as the chart at a point the model does not
           reach; saying so here stops the depth above from looking like a
           reading taken at the selected location. -->
      <div v-if="outsideDomain" class="w-full m-0 p-0 layer-empty" style="height:20px">
        <span>no model data here</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, toRef, ref, watch } from 'vue';
import moment from 'moment';

import { useMainStore } from '../stores/main'
const mainStore = useMainStore();

import { useVariableRegistry } from '~~/composables/useVariableRegistry'
const { variableLabel } = useVariableRegistry()
import { utc2pst } from '~~/composables/useUTC2PST'

////////////////////////////////////// COMPUTED //////////////////////////////////////

const showColorbarSettings = computed({
  get: () => mainStore.showColorbarSettings,
  set: (val: boolean) => mainStore.setShowColorbarSettings(val)
});

const selectedVariable = computed(() => mainStore.selected_variable);

// Reported by whichever pane last fetched for the selected point (see
// stores/main.ts's `modelDomain`). Null means "not established" — treated as
// in-domain, so the normal case is never labelled as missing data.
const outsideDomain = computed(() => mainStore.modelDomain?.inDomain === false);

// selected_variable.dt is a real model instant in hourly mode (PST display
// makes sense there), but a UTC calendar-day/month bin start in daily/monthly
// mode (see ExplorePanel.vue's onCellClick) — shifting those to PST can roll
// them onto the wrong day, so daily/monthly stay in UTC and drop the
// time-of-day component that bin doesn't actually have.
const formattedDt = computed(() => {
  const dt = selectedVariable.value.dt;
  if (!dt) return '';
  if (mainStore.exploreBinMode === 'monthly') return moment.utc(dt).format('MMM YYYY');
  if (mainStore.exploreBinMode === 'daily') return moment.utc(dt).format('ddd MMM DD, YYYY');
  return utc2pst(moment(dt));
});

////////////////////////////////////// METHODS //////////////////////////////////////

</script>

<style scoped>
.colorbar {
  position: absolute;
  padding: 3px;
  width: fit-content;
  transition: left 0.3s ease;
  border-radius: 6px;
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.2);
  /* font-family: Inter, system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue'; */
  font-family: monospace;
  font-size: 11px;
  color: #ccc;
}

.layer-label {
  font-size: 9px;
  letter-spacing: 0.06em;
  color: #888;
}

.layer-empty {
  color: rgb(251, 191, 36);
}
</style>