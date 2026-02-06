<template>
  <v-card class="colorbar">
    <div class="label">
      <v-select v-model="selectedModel" :items="models" label="Model" item-title="label" item-value="var"
        density="compact" hide-details variant="outlined" class="my-4"
        :menu-props="{ offsetX: true, right: true, location: 'right' }" style="width: 100%"></v-select>


      <v-select v-if="options.length" v-model="selectedVarLocal" label="Variable" :items="options" item-title="label"
        item-value="var" density="compact" hide-details variant="outlined" class="my-4"
        :menu-props="{ offsetX: true, right: true, location: 'right' }" style="width: 100%"></v-select>



      <v-select v-if="(props.colormaps && props.colormaps.length)" v-model="selectedColormapLocal" label="Color map"
        :items="props.colormaps" item-title="name" item-value="name" density="compact" hide-details variant="outlined"
        class="my-4" close-on-click="false" :menu-props="{ offsetX: true, right: true, location: 'right' }"
        style="width: 100%; margin-top: 6px">
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

      <div v-else class="label-text">{{ label }}</div>

      <!-- <v-row class="mx-0 my-2" v-if="current">
        <v-col cols="6">
          <v-number-input v-model.number="localMin" :reverse="false" controlVariant="stacked" label=""
            :hideInput="false" density="compact" :inset="false"></v-number-input>
        </v-col>
        <v-col cols="6">
          <v-number-input v-model.number="localMax" :reverse="false" controlVariant="stacked" label=""
            :hideInput="false" density="compact" :inset="false"></v-number-input>
        </v-col>
      </v-row> -->

    </div>

    <div class="bar" :style="barStyle"></div>
    <div class="ticks">
      <div class="tick left">{{ currentMin }}</div>
      <div class="tick center">{{ ((currentMin + currentMax) / 2).toFixed(1) }}</div>
      <div class="tick right">{{ currentMax }}</div>
    </div>

    <v-card-actions class="ma-0 pa-0" style="min-height:24px">
      <v-spacer></v-spacer>
      <!-- Config button -->
      <v-btn size="x-small" icon @click="showSettings = !showSettings"> <v-icon>mdi-cog</v-icon> </v-btn>
    </v-card-actions>

    <v-row v-if="showSettings" class="ma-0 pa-0" style="place-items: center;">
      <!-- <v-col cols="5" class="ma-0 pa-0"> -->
        <v-number-input v-model.number="localMin" hide-details :reverse="false" controlVariant="stacked" label="Min"
          :hideInput="false" density="compact" :inset="false" :step="0.1" inputmode="decimal"
          style="width: 30%;; scale:75%"></v-number-input>
      <!-- </v-col> -->
      <!-- <v-col cols="2" class="ma-0 pa-0"> -->
        <v-btn width="30%" size="x-small" color="grey" @click="resetToDefaults" :title="'Reset min/max to defaults'">Reset</v-btn>
      <!-- </v-col> -->
      <!-- <v-col cols="5" class="ma-0 pa-0"> -->
        <v-number-input v-model.number="localMax" hide-details :reverse="false" controlVariant="stacked" label="Max"
          :hideInput="false" density="compact" :inset="false" :step="0.1" inputmode="decimal"
          style="width: 30%; scale:75%"></v-number-input>
      <!-- </v-col> -->
    </v-row>
  </v-card>
</template>

<script setup lang="ts">
import { computed, toRef, ref, watch } from 'vue';
import { var2name } from '../../composables/useVar2Name';

interface VarMeta {
  var: string;
  min?: number;
  max?: number;
  precision?: number;
}

const props = defineProps<{
  label?: string;
  variables?: VarMeta[];
  modelValue?: string;                // selected variable (v-model)
  stops?: Array<[number, string]>;    // explicit stops pass-thru
  colormaps?: Array<{ name: string; description?: string; stops?: Array<[number, string]>; mode?: string }>; // available colormaps
  colormap?: string;                  // selected colormap name (v-model:colormap)
  min?: number;                       // optional v-model:min
  max?: number;                       // optional v-model:max
}>();

const models = [
  { var: 'SalishSeaCast', label: 'Salish Sea Cast' }
];
const selectedModel = ref(models[0].var);
const showSettings = ref(false);

const emit = defineEmits(['update:modelValue', 'update:colormap', 'update:min', 'update:max']);

const label = props.label ?? '';
const variables = props.variables ?? [];
const modelValue = toRef(props, 'modelValue');

const options = computed(() => variables.map((v) => ({ var: v.var, label: var2name(v.var), min: v.min, max: v.max })));

const selectedVarLocal = computed({
  get: () => modelValue.value ?? (options.value[0] ? options.value[0].var : ''),
  set: (v: string) => emit('update:modelValue', v),
});

const current = computed(() => variables.find((x) => x.var === selectedVarLocal.value) || null);

// actual variable bounds
const varMin = computed(() => current.value?.min ?? 0);
const varMax = computed(() => current.value?.max ?? 1);

// local adjustable bounds (start at actual bounds or props if provided)
const localMin = ref(props.min ?? varMin.value);
const localMax = ref(props.max ?? varMax.value);

// when variable or incoming props change, reset local bounds to actual bounds or prop values
watch([varMin, varMax], () => {
  // respect externally provided min/max if present, otherwise reset to var bounds
  localMin.value = (props.min !== undefined) ? props.min : varMin.value;
  localMax.value = (props.max !== undefined) ? props.max : varMax.value;
});
watch(() => props.min, (v) => {
  if (v !== undefined) localMin.value = v;
});
watch(() => props.max, (v) => {
  if (v !== undefined) localMax.value = v;
});

// enforce invariants / clamp within var bounds
watch(localMin, (v) => {
  if (v == null) return;
  if (v > localMax.value) localMin.value = localMax.value;
  if (v < varMin.value) localMin.value = varMin.value;
  if (v > varMax.value) localMin.value = varMax.value;
  // emit change to parent
  emit('update:min', localMin.value);
});
watch(localMax, (v) => {
  if (v == null) return;
  if (v < localMin.value) localMax.value = localMin.value;
  if (v < varMin.value) localMax.value = varMin.value;
  if (v > varMax.value) localMax.value = varMax.value;
  // emit change to parent
  emit('update:max', localMax.value);
});

// currentMin/currentMax are the adjustable values used elsewhere
const currentMin = computed(() => localMin.value);
const currentMax = computed(() => localMax.value);

const selectedColormapLocal = computed({
  get: () => props.colormap ?? (props.colormaps && props.colormaps.length ? props.colormaps[0].name : ''),
  set: (v: string) => emit('update:colormap', v),
});

const currentColormap = computed(() => {
  const name = selectedColormapLocal.value;
  if (!name || !props.colormaps) return null;
  return props.colormaps.find((c: any) => c.name === name) || null;
});

const palette = computed(() => {
  if (currentColormap.value && Array.isArray(currentColormap.value.stops) && currentColormap.value.stops.length > 0) {
    // if mode is 'absolute', normalize to 0..1 using currentMin/currentMax
    if (currentColormap.value.mode === 'absolute') {
      const mn = currentMin.value;
      const mx = currentMax.value;
      const rng = mx - mn || 1.0;
      return currentColormap.value.stops.map((s: any) => [(s[0] - mn) / rng, s[1]]);
    }
    return currentColormap.value.stops;
  }
  return props.stops ?? [
    [0.0, '#440154'],
    [0.25, '#00f'],
    [0.5, '#0f0'],
    [0.75, '#fde725'],
    [1.0, '#f00']
  ];
});

const barStyle = computed(() => {
  const stops = palette.value.map(s => `${s[1]} ${Math.round(s[0] * 100)}%`).join(', ');
  return {
    background: `linear-gradient(90deg, ${stops})`
  };
});

function format(v: number) {
  // if (Math.abs(v) >= 1000 || Math.abs(v) < 0.01) return v.toExponential(2);
  // return Number(v).toFixed(2);
  return v
}

function colormapStyle(item: any) {
  if (!item || !Array.isArray(item.stops)) return {};
  // Normalize stops (if absolute mode, convert to normalized 0..1 using current min/max)
  const stops = (item.mode === 'absolute')
    ? (item.stops || []).map((s: any) => {
      const mn = currentMin.value; const mx = currentMax.value; const rng = mx - mn || 1.0;
      return [Math.max(0, Math.min(1, (s[0] - mn) / rng)), s[1]];
    })
    : item.stops;
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

function resetToDefaults() {
  // Reset adjustable bounds to the variable's default bounds
  localMin.value = varMin.value;
  localMax.value = varMax.value;
}

const title = computed(() => `${label} ${currentMin.value} → ${currentMax.value}`);
</script>

<style scoped>
.colorbar {
  position: absolute;
  left: 16px;
  top: 16px;
  width: 220px;
  padding: 6px 8px;
  background: rgba(255, 255, 255, 0.9);
  border-radius: 6px;
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.2);
  z-index: 9998;
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
  width:14px;
}
</style>