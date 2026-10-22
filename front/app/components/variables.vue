<template>
  <div class="vars" v-if="selectedVariableName">
    <div class="vars__field">
      <label class="oe-label" for="field-select">Field</label>
      <USelectMenu id="field-select" v-model="selectedVarName" :items="variableItems" :disabled="variableItems.length === 0"
        label-key="label" value-key="var" size="sm" class="w-full">
        <template #trailing>
          <span v-if="selectedUnit" class="vars__meta">{{ selectedUnit }}</span>
          <UIcon name="i-mdi-chevron-down" class="size-[14px] text-dimmed" />
        </template>
      </USelectMenu>
    </div>

    <div class="vars__field">
      <div class="flex items-center justify-between">
        <label class="oe-label" for="source-select">Source</label>
        <button type="button" class="vars__info" title="About this data source" @click="showSourceInfo = !showSourceInfo">
          About
        </button>
      </div>
      <USelectMenu id="source-select" v-model="selectedSource" :items="sourceItems" label-key="label" value-key="source"
        :disabled="sourceItems.length === 0" size="sm" class="w-full" />
    </div>

    <!-- Depth stays a dropdown: the model has dozens of levels. The steppers
         walk one level at a time, which is how depth is usually explored. -->
    <div v-if="depths && depths.length > 0" class="vars__field">
      <div class="flex items-center justify-between">
        <label class="oe-label" for="depth-select">Depth</label>
        <span class="vars__meta oe-num" v-if="depthIndex >= 0">level {{ depthIndex + 1 }} / {{ depths.length }}</span>
      </div>
      <div class="flex items-stretch">
        <USelectMenu id="depth-select" v-model="selectedDepth" :items="depths" label-key="title" value-key="value"
          size="sm" class="grow oe-num" :ui="{ base: 'rounded-e-none' }" />
        <div class="vars__step">
          <button type="button" :disabled="depthIndex <= 0" title="Shallower level" aria-label="Shallower level" @click="shallower">
            <UIcon name="i-mdi-chevron-up" class="size-[14px]" />
          </button>
          <button type="button" :disabled="depthIndex >= depths.length - 1" title="Deeper level" aria-label="Deeper level" @click="deeper">
            <UIcon name="i-mdi-chevron-down" class="size-[14px]" />
          </button>
        </div>
      </div>
    </div>
  </div>

  <UModal v-model:open="showSourceInfo" :ui="{ content: 'max-w-[50vw]' }">
    <template #content>
      <div class="p-5">
      <about-ssc v-if="selectedSource === 'SalishSeaCast'"></about-ssc>
      <about-nonna v-else-if="selectedSource === 'NONNA'"></about-nonna>
      </div>
    </template>
  </UModal>
</template>

<script setup lang="ts">
import { computed, toRef, ref, watch } from 'vue';
import moment from 'moment-timezone';
import { useVariableRegistry } from '~~/composables/useVariableRegistry';
import { trackEvent } from '~~/composables/useAnalytics';

import { useMainStore, formatDepthLabel } from '../stores/main'
const mainStore = useMainStore();
const { variableLabel, displayUnit } = useVariableRegistry();

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
const variableItems = computed(() => variables.value.filter(v => v.source === selectedSource.value).map((v) => ({ var: v.var, label: variableLabel(v.var), colormapMin: v.colormapMin, colormapMax: v.colormapMax })));
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
    let newDepthNc: number | null = depths.length > 0 ? depths[0] : null;

    if (depths.length > 0 && selectedVariable.value.depth_nc !== null) {
      // Find depth closest to the currently selected depth value
      const currentDepthNc = selectedVariable.value.depth_nc;
      newDepthNc = depths.reduce((closest, current) =>
        Math.abs(current - currentDepthNc) < Math.abs(closest - currentDepthNc)
          ? current
          : closest
      );
    }
    const newDepth = newDepthNc !== null ? formatDepthLabel(newDepthNc) : null;
    
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
    trackEvent('variable_changed', { variable: v, source: selectedVariable.value.source });
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
    slice().sort((a, b) => {
      // surface first, bottom (-1) last
      const aIsBottom = a === -1;
      const bIsBottom = b === -1;
      if (aIsBottom && !bIsBottom) return 1;
      if (!aIsBottom && bIsBottom) return -1;
      return a - b;
    }).
    map(v => {
      const label = formatDepthLabel(v);
      return {
        title: isNaN(Number(label)) ? label : label + ' m',
        value: label,
      };
    })
);

const selectedDepth = computed({
  get() { return selectedVariable.value.depth },
  set(v: string | null) {
    const raw = mainStore.variables
      .find(variable => variable.source === selectedVariable.value.source && variable.var === selectedVariable.value.var)
      ?.depths.find(d => formatDepthLabel(d) === v);
    mainStore.updateSelectedVariable({ depth: v, depth_nc: raw ?? null });
  }
})

const selectedVariableName = computed(() => mainStore.selected_variable.var);
const selectedUnit = computed(() => selectedVariableName.value ? displayUnit(selectedVariableName.value) : '');
const depthIndex = computed(() => depths.value.findIndex(d => d.value === selectedDepth.value));

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
.vars {
  display: grid;
  gap: 12px;
  padding: 12px 14px;
}

.vars__field {
  display: grid;
  gap: 5px;
}

.vars__meta {
  font-size: 11px;
  color: var(--ui-text-muted);
}

.vars__info {
  font-size: 11px;
  color: var(--ui-text-muted);
  cursor: pointer;
}

.vars__info:hover {
  color: var(--ui-text);
}

.vars__step {
  display: grid;
  grid-template-rows: 1fr 1fr;
  border: 1px solid var(--ui-border-accented);
  border-left: 0;
  border-radius: 0 var(--ui-radius) var(--ui-radius) 0;
}

.vars__step button {
  width: 24px;
  display: grid;
  place-items: center;
  color: var(--ui-text-muted);
  cursor: pointer;
}

.vars__step button + button {
  border-top: 1px solid var(--ui-border-accented);
}

.vars__step button:hover:not(:disabled) {
  color: var(--ui-text);
  background: var(--ui-bg-elevated);
}

.vars__step button:disabled {
  color: var(--ui-text-dimmed);
  cursor: not-allowed;
}
</style>
