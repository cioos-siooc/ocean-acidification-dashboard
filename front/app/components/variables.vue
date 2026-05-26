<template>
  <v-card v-if="selectedVariableName" class="colorbar">
    <div class="label">
      <!-- VARIABLES -->
      <v-select v-model="selectedVarName" label="Field" :items="variableItems" :disabled="variableItems.length === 0"
        item-title="label" item-value="var" density="compact" hide-details variant="outlined" class="my-4"
        :menu-props="{ location: 'end', offset: 35, zIndex: 9999 }" style="width: 100%"></v-select>

      <!-- SOURCES -->
      <v-select v-model="selectedSource" :items="sourceItems" label="Source" item-title="label" item-value="source"
        :disabled="sourceItems.length === 0" density="compact" hide-details variant="outlined" class="my-4"
        :menu-props="{ location: 'end', offset: 35, zIndex: 9999 }" style="width: 100%">
        <template #prepend-inner>
          <v-btn icon size="12px" @click="showSourceInfo = !showSourceInfo" title="About this data source">
            <v-icon :color="colors.yellow.base" size="12px">mdi-information-variant</v-icon>
          </v-btn>
        </template>
      </v-select>

      <!-- DEPTHS -->
      <v-select v-if="depths && depths.length > 0" v-model="selectedDepth" :items="depths" label="Depth"
        item-title="title" item-value="value" :disabled="!depths || depths.length === 0" density="compact" hide-details
        variant="outlined" class="my-4" :menu-props="{ location: 'end', offset: 75, zIndex: 9999 }" style="width: 100%">
        <template #item="{ props, item }">
          <v-list-item v-bind="props" :title="item.title"
            :style="{ color: item.hasImage ? colors.green.lighten2 : colors.orange.lighten2 }">
          </v-list-item>
        </template>
        <template #selection="{ item }">
          <div class="colormap-selection">{{ item.title }}</div>
        </template>
        <template #append>
          <v-btn icon size="12px" @click="deeper" title="Deeper depth selection">
            <v-icon :color="colors.orange.lighten2" size="10px">mdi-arrow-down</v-icon>
          </v-btn>
          <v-btn icon size="12px" @click="shallower" title="Shallower depth selection">
            <v-icon :color="colors.green.lighten2" size="10px">mdi-arrow-up</v-icon>
          </v-btn>
        </template>
        <!-- <template #append>
          <v-btn icon size="12px" @click="shallower" title="Clear depth selection">
            <v-icon size="10px">mdi-arrow-up</v-icon>
          </v-btn>
        </template> -->
      </v-select>

      <!-- COLORBAR -->
      <!-- <ColormapBar /> -->

      <!-- <v-card-actions class="ma-0 pa-0" style="min-height:24px">
        <v-spacer></v-spacer>
        <v-btn size="x-small" icon @click="showSettings = !showSettings" class="ma-0 pa-0">
          <IconsConfig />
        </v-btn>
        <v-btn icon size="x-small" flat
          :disabled="!selectedVariableName || selectedVariableName === 'bathymetry' || mainStore.autoRangeDisabled"
          @click="autorange" title="Auto-range colorbar to data range" class="ma-0 pa-0">
          <IconsAutorange />
        </v-btn>
      </v-card-actions> -->
    </div>

  </v-card>

  <v-dialog v-model="showSourceInfo" max-width="50%">
    <v-card class="pa-5">
      <about-ssc v-if="selectedSource === 'SalishSeaCast'"></about-ssc>
      <about-liveocean v-else-if="selectedSource === 'LiveOcean'"></about-liveocean>
      <about-nonna v-else-if="selectedSource === 'NONNA'"></about-nonna>
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import { computed, toRef, ref, watch } from 'vue';
import moment from 'moment-timezone';
import { var2name } from '../../composables/useVar2Name';
import colors from 'vuetify/util/colors'

import { useMainStore } from '../stores/main'
const mainStore = useMainStore();

const emit = defineEmits<{
  (e: 'autorange'): void;
}>();

////////////////////////////////  TYPES  ///////////////////////////////////

// const props = defineProps<{
//   label?: string;
//   variables?: VarMeta[];
//   selectedVar?: string;                // selected variable (v-model:selectedVar)
//   selectedSource?: string;             // selected source (v-model:selectedSource)
//   stops?: Array<[number, string]>;    // explicit stops pass-thru
//   colormaps?: Array<{ name: string; description?: string; stops?: Array<[number, string]>; mode?: string }>; // available colormaps
//   colormap?: string;                  // selected colormap name (v-model:colormap)
//   min?: number;                       // optional v-model:min
//   max?: number;                       // optional v-model:max
// }>();


///////////////////////////////////  REF  ///////////////////////////////////

// const models = [
//   { var: 'SalishSeaCast', label: 'SalishSeaCast' }
// ];
// const selectedModel = ref(modelItems.value[0].var);
const showSettings = ref(false);
const showSourceInfo = ref(false);

// const label = props.label ?? '';
// const variables = props.variables ?? [];
// const modelValue = toRef(props, 'modelValue');

// local adjustable bounds (start at actual bounds or props if provided)
// const localMin = ref(props.min ?? varMin.value);
// const localMax = ref(props.max ?? varMax.value);

///////////////////////////////////  COMPUTED  ///////////////////////////////////

const variables = computed(() => mainStore.variables);
const variableItems = computed(() => variables.value.filter(v => v.source === selectedSource.value).map((v) => ({ var: v.var, label: var2name(v.var), colormapMin: v.colormapMin, colormapMax: v.colormapMax })));
const selectedVariable = computed(() => mainStore.selected_variable);
const sourceItems = computed(() => {
  const sources = variables.value.filter(v => v.var == selectedVariable.value.var).map(v => ({ source: v.source, label: v.source }));
  // Check if selectedSource is in the sources list, if not set it to the first source
  if (!sources.some(s => s.source === selectedVariable.value.source)) {
    const firstSource = sources[0]?.source ?? '';
    mainStore.updateSelectedVariable({ source: firstSource });
  }
  return sources;
});
const selectedSource = computed({
  get() { return selectedVariable.value.source },
  set(v: string) {
    // Find depths for the new source/variable combination and update selected variable with new source and reset depth
    const variable = variables.value.find(variable => variable.source === v && variable.var === selectedVariable.value.var);
    const depths = variable?.depths ?? [];
    
    // Find closest available depth in the new source
    let newDepth = depths.length > 0 ? depths[0]?.depth_image : null;
    let newDepthNc = depths.length > 0 ? depths[0]?.depth_nc : null;
    
    if (depths.length > 0 && selectedVariable.value.depth_nc !== null) {
      // Find depth with closest depth_nc value
      const currentDepthNc = selectedVariable.value.depth_nc;
      const closestDepth = depths.reduce((closest, current) =>
        Math.abs(current.depth_nc - currentDepthNc) < Math.abs(closest.depth_nc - currentDepthNc)
          ? current
          : closest
      );
      newDepth = closestDepth.depth_image;
      newDepthNc = closestDepth.depth_nc;
    }
    
    // Find closest available datetime in the new source
    let newDt = selectedVariable.value.dt;
    const dts = variable?.dts ?? [];
    console.log(dts, newDt);
    if (dts.length > 0 && selectedVariable.value.dt) {
      // dts are Unix timestamps in seconds; find the closest one
      const currentTimeMs = selectedVariable.value.dt.valueOf();
      const closestDtUnix = dts.reduce((closest, current) =>
        Math.abs(current * 1 - currentTimeMs) < Math.abs(closest * 1 - currentTimeMs)
          ? current
          : closest
      );
      newDt = moment.unix(closestDtUnix/1000).tz('UTC');
      console.log(`currentTimeMs: ${currentTimeMs}, closestDtUnix: ${closestDtUnix}, newDt: ${newDt}`);
    } else if (dts.length > 0) {
      // If no datetime selected yet, use the most recent available
      newDt = moment.unix(Math.max(...dts)/1000).tz('UTC');
    }
    
    mainStore.updateSelectedVariable({ source: v, depth: newDepth, depth_nc: newDepthNc, dt: newDt });
  }
});

const selectedVarName = computed({
  get() { return selectedVariable.value.var },
  set(v: string) {
    const matchingVar = variables.value.find(variable => variable.source === selectedVariable.value.source && variable.var === v);
    const colormap = matchingVar?.colormap ?? null;
    const colormapMin = matchingVar?.colormapMin ?? null;
    const colormapMax = matchingVar?.colormapMax ?? null;
    const precision = matchingVar?.precision ?? null;
    mainStore.updateSelectedVariable({ var: v, colormap, colormapMin, colormapMax, precision });
  }
});

const depths = computed(() =>
  (mainStore.variables.
    find(v => v.source === selectedVariable.value.source && v.var === selectedVariable.value.var)?.depths ?? []).
    filter(v => v.depth_image !== undefined).
    slice().sort((a, b) => {
      // surface first, bottom last, numeric by depth_nc
      const aIsBottom = a.depth_image === 'bottom';
      const bIsBottom = b.depth_image === 'bottom';
      if (aIsBottom && !bIsBottom) return 1;
      if (!aIsBottom && bIsBottom) return -1;
      return a.depth_nc - b.depth_nc;
    }).
    map(v => ({
      title: isNaN(Number(v.depth_image)) ? v.depth_image : v.depth_image + ' m',
      value: v.depth_image,
      hasImage: v.hasImage,
    }))
);

const selectedDepth = computed({
  get() { return selectedVariable.value.depth },
  set(v: string | null) {
    const raw = mainStore.variables
      .find(variable => variable.source === selectedVariable.value.source && variable.var === selectedVariable.value.var)
      ?.depths.find(d => d.depth_image === v);
    mainStore.updateSelectedVariable({ depth: v, depth_nc: raw?.depth_nc ?? null });
  }
})

const selectedVariableName = computed(() => mainStore.selected_variable.var);

///////////////////////////////////  WATCHERS  ///////////////////////////////////

//////////////////////////////////  METHODS  ///////////////////////////////////





function deeper() {
  if (selectedDepth.value === null) return;
  const currentIndex = depths.value.findIndex(d => d.value === selectedDepth.value);
  if (currentIndex < depths.value.length - 1) {
    selectedDepth.value = depths.value[currentIndex + 1]?.value ?? null;
  }
}

function shallower() {
  if (selectedDepth.value === null) return;
  const currentIndex = depths.value.findIndex(d => d.value === selectedDepth.value);
  if (currentIndex > 0) {
    selectedDepth.value = depths.value[currentIndex - 1]?.value ?? null;
  }
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

.label {
  font-weight: 600;
  margin-bottom: 6px;
}

.label-text {
  font-weight: 600;
}

.colormap-item {
  display: flex;
  align-items: center;
}

.colormap-selection {
  display: flex;
  align-items: center;
}

.mini-bar {
  display: inline-block;
}

.colormap-label {
  font-size: 11px;
}
</style>

<style lang="css">
.v-number-input__control {
  width: 14px;
}
</style>