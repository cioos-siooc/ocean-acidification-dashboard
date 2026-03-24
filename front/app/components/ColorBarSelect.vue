<template>
  <v-card class="colorbar">
    <div class="label">
      <v-select v-model="selectedVarName" label="Field" :items="variableItems" :disabled="variableItems.length === 0"
        item-title="label" item-value="var" density="compact" hide-details variant="outlined" class="my-4"
        :menu-props="{ location: 'end' }" style="width: 100%"></v-select>

      <v-select v-model="selectedSource" :items="sourceItems" label="Source" item-title="label" item-value="source"
        :disabled="sourceItems.length === 0" density="compact" hide-details variant="outlined" class="my-4"
        :menu-props="{ location: 'end' }" style="width: 100%"></v-select>

      <div class="bar" :style="barStyle"></div>
      <div class="ticks">
        <div class="tick left">{{ colormapMin }}</div>
        <div class="tick center">{{ ((colormapMin + colormapMax) / 2).toFixed(1) }}</div>
        <div class="tick right">{{ colormapMax }}</div>
      </div>

      <v-card-actions class="ma-0 pa-0" style="min-height:24px">
        <v-spacer></v-spacer>
        <v-btn size="x-small" icon @click="showSettings = !showSettings"> <v-icon>mdi-cog</v-icon> </v-btn>
      </v-card-actions>
    </div>

    <v-row v-if="showSettings" class="ma-0 pa-0" style="place-items: center;">
      <v-select v-model="selectedColormap" label="Color map" :items="Object.entries(colormaps).map(([name, cmap]) => ({ name, raw: cmap }))" item-title="name" item-value="name"
        density="compact" hide-details variant="outlined" class="my-4" close-on-click="false"
        :menu-props="{ location: 'end' }" style="width: 100%; margin-top: 6px">
        <template #item="{ props, item }">
          <v-list-item v-bind="props" :title="undefined">
            <div class="colormap-item">
              <div class="mini-bar" :style="colormapStyle(item.raw)"></div>
              <div class="colormap-label">{{ item.raw.name }}</div>
            </div>
          </v-list-item>
        </template>
        <template #selection="{ item }">
          <div class="colormap-selection">
            <span>{{ item.raw.name }}</span>
          </div>
        </template>
      </v-select>

      <v-number-input v-model.number="colormapMin" hide-details :reverse="false" controlVariant="stacked" label="Min"
        :hideInput="false" density="compact" :inset="false" :step="0.1" inputmode="decimal"
        style="width: 30%; scale:75%"></v-number-input>

      <v-btn width="30%" size="x-small" color="grey" @click="resetToDefaults"
        :title="'Reset min/max to defaults'">Reset</v-btn>

      <v-number-input v-model.number="colormapMax" hide-details :reverse="false" controlVariant="stacked" label="Max"
        :hideInput="false" density="compact" :inset="false" :step="0.1" inputmode="decimal"
        style="width: 30%; scale:75%"></v-number-input>
    </v-row>
  </v-card>
</template>

<script setup lang="ts">
import { computed, toRef, ref, watch } from 'vue';
import { var2name } from '../../composables/useVar2Name';

import { useMainStore } from '../stores/main'
const mainStore = useMainStore();

////////////////////////////////  TYPES  ///////////////////////////////////

interface VarMeta {
  var: string;
  min?: number;
  max?: number;
  precision?: number;
  source?: string;
}

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

// const label = props.label ?? '';
// const variables = props.variables ?? [];
// const modelValue = toRef(props, 'modelValue');

// local adjustable bounds (start at actual bounds or props if provided)
// const localMin = ref(props.min ?? varMin.value);
// const localMax = ref(props.max ?? varMax.value);

///////////////////////////////////  COMPUTED  ///////////////////////////////////

const variables = computed(() => mainStore.variables);
const variableItems = computed(() => variables.value.map((v) => ({ var: v.var, label: var2name(v.var), colormapMin: v.colormapMin, colormapMax: v.colormapMax })));
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
  set(v: string) { mainStore.updateSelectedVariable({ source: v }) }
});

const selectedVarName = computed({
  get() { return selectedVariable.value.var },
  set(v: string) {
    const matchingVar = variables.value.find(variable => variable.var === v);
    const colormap = matchingVar?.colormap ?? null;
    const colormapMin = matchingVar?.colormapMin ?? null;
    const colormapMax = matchingVar?.colormapMax ?? null;
    const precision = matchingVar?.precision ?? null;
    mainStore.updateSelectedVariable({ var: v, colormap, colormapMin, colormapMax, precision });
  }
});

const colormapMin = computed({
  get() {
    return selectedVariable.value.colormapMin
  },
  set(v: number | null) { mainStore.updateSelectedVariable({ colormapMin: v }) }
});
const colormapMax = computed({
  get() { return selectedVariable.value.colormapMax },
  set(v: number | null) { mainStore.updateSelectedVariable({ colormapMax: v }) }
});

const selectedColormap = computed({
  get() { return selectedVariable.value.colormap },
  set(v: string | null) { mainStore.updateSelectedVariable({ colormap: v }) }
});

// Default variable bounds
const default_colormapMin = computed(() => variables.value.find(v => v.var === selectedVariable.value.var && v.source === selectedVariable.value.source)?.colormapMin ?? 0);
const default_colormapMax = computed(() => variables.value.find(v => v.var === selectedVariable.value.var && v.source === selectedVariable.value.source)?.colormapMax ?? 1);

const colormaps = computed(() => mainStore.colormaps);
const barStyle = computed(() => {
  const palette = colormaps.value[selectedColormap.value]?.stops
  const stops = palette?.map(s => `${s[1]} ${Math.round(s[0] * 100)}%`).join(', ');
  return {
    background: `linear-gradient(90deg, ${stops})`
  };
});


///////////////////////////////////  WATCHERS  ///////////////////////////////////

//////////////////////////////////  METHODS  ///////////////////////////////////

function resetToDefaults() {
  // Reset adjustable bounds to the variable's default bounds
  mainStore.updateSelectedVariable({
    colormapMin: default_colormapMin.value,
    colormapMax: default_colormapMax.value
  });
}

function colormapStyle(item: any) {
  if (!item || !Array.isArray(item.raw.stops)) return {};
  const raw = item.raw;
  // Normalize stops (if absolute mode, convert to normalized 0..1 using current min/max)
  const stops = (raw.mode === 'absolute')
    ? (raw.stops || []).map((s: any) => {
      const mn = colormapMin.value;
      const mx = colormapMax.value;
      const rng = mx - mn || 1.0;
      return [Math.max(0, Math.min(1, (s[0] - mn) / rng)), s[1]];
    })
    : raw.stops;
  const stopsStr = stops.map((s: any) => `${s[1]} ${Math.round(s[0] * 100)}%`).join(', ');
  return {
    background: `linear-gradient(90deg, ${stopsStr})`,
    width: '48px',
    height: '12px',
    borderRadius: '3px',
    border: '1px solid rgba(0,0,0,0.08)',
    marginRight: '8px',
    display: 'inline-block'
  };
}
</script>


<style scoped>
.colorbar {


  padding: 6px 8px;
  background: rgba(255, 255, 255, 0.9);
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

.bar {
  height: 14px;
  border-radius: 4px;
  border: 1px solid rgba(0, 0, 0, 0.08);
}

.ticks {
  display: flex;
  justify-content: space-between;
  margin-top: 6px;
}

.tick {
  color: #333;
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
  color: #222;
}
</style>

<style lang="css">
.v-number-input__control {
  width: 14px;
}
</style>