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

    <!-- Only set once a tile request outlasts index.vue's show delay, so fast
         tiles and smooth playback never flash it. -->
    <div v-if="mapLayerLoading" class="loading-glow" role="status" aria-label="Updating map layer">
      <div class="loading-ring" />
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

const mapLayerLoading = computed(() => mainStore.mapLayerLoading);

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

/* A bright arc chasing around the box's border: a rotating conic gradient,
   masked down to a ring so the contents stay untouched. The glow lives on the
   unmasked wrapper — a filter on the ring itself would be masked away too. */
@property --loading-angle {
  syntax: '<angle>';
  inherits: false;
  initial-value: 0deg;
}

.loading-glow {
  position: absolute;
  inset: 0;
  pointer-events: none;
  filter: drop-shadow(0 0 3px #22d3ee) drop-shadow(0 0 6px rgba(34, 211, 238, 0.6));
}

.loading-ring {
  position: absolute;
  inset: 0;
  padding: 3px;
  border-radius: 6px;
  background: conic-gradient(from var(--loading-angle), transparent 0 55%, #22d3ee 85%, #e0fbff 97%, transparent 100%);
  -webkit-mask: linear-gradient(#000 0 0) content-box, linear-gradient(#000 0 0);
  -webkit-mask-composite: xor;
  mask: linear-gradient(#000 0 0) content-box exclude, linear-gradient(#000 0 0);
  animation: map-layer-loading 1.4s linear infinite;
}

@keyframes map-layer-loading {
  to { --loading-angle: 360deg; }
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