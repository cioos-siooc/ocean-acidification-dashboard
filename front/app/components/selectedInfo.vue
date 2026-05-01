<template>
  <v-card class="colorbar" max-width="200px" width="fit-content">
    <v-row gap="0" class="my-0 mx-2 pa-0">
      <v-col cols="12" class="ma-0 pa-0" style="height:20px">
        <span>{{ var2name(selectedVariable.var) }}</span>
      </v-col>

      <!-- <v-divider class="mx-0"></v-divider> -->

      <!-- <v-col cols="12" class="ma-0 pa-0" style="height:20px">
        <span>Model</span>
      </v-col> -->
      <v-col cols="12" class="ma-0 pa-0" style="height:20px">
        <span>{{ utc2pst(moment(selectedVariable.dt)) }}</span>
      </v-col>
      <v-col cols="12" class="ma-0 pa-0" style="height:20px">
        <span>Depth {{ formatDepth(selectedVariable.depth) }} m</span>
      </v-col>
      <v-col v-if="lastClicked" cols="12" class="ma-0 pa-0" style="height:20px">
        <span>{{ lastClicked?.lat.toFixed(5) }} , {{ lastClicked?.lng.toFixed(5)
          }}</span>
      </v-col>

      <!-- <v-divider class="mx-0"></v-divider>

      <v-col cols="12" class="ma-0 pa-0" style="height:20px">
        <span>Sensor</span>
      </v-col> -->

    </v-row>
  </v-card>
</template>

<script setup lang="ts">
import { computed, toRef, ref, watch } from 'vue';
import moment from 'moment';

import { useMainStore } from '../stores/main'
const mainStore = useMainStore();

import { var2name } from '../../composables/useVar2Name'
import { utc2pst } from '../../composables/useUTC2PST'
import { formatDepth } from '../../composables/useFormatDepth'

////////////////////////////////////// COMPUTED //////////////////////////////////////

const showColorbarSettings = computed({
  get: () => mainStore.showColorbarSettings,
  set: (val: boolean) => mainStore.setShowColorbarSettings(val)
});

const selectedVariable = computed(() => mainStore.selected_variable);
const lastClicked = computed(() => mainStore.lastClickedMapPoint);

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
</style>