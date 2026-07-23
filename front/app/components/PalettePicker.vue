<template>
  <div class="palette-picker">
    <v-text-field v-model="paletteSearch" placeholder="Search palettes…"
      hide-details prepend-inner-icon="mdi-magnify" class="mb-2" clearable />

    <div class="d-flex ga-2 mb-3">
      <v-btn size="small" :variant="reversedActive ? 'flat' : 'outlined'" :color="reversedActive ? 'primary' : undefined"
        :disabled="!reverseAvailable" @click="toggleReversed">
        Reverse
      </v-btn>
      <v-btn size="small" :variant="invertedActive ? 'flat' : 'outlined'" :color="invertedActive ? 'primary' : undefined"
        :disabled="!invertAvailable" @click="toggleInverted">
        Invert
      </v-btn>
    </div>

    <div class="swatch-grid">
      <button v-for="base in filteredBases" :key="base" type="button" class="swatch-cell"
        :class="{ 'swatch-cell--selected': base === selectedBase }" :title="base" @click="selectBase(base)">
        <div class="swatch-preview" :style="swatchStyle(base)"></div>
        <span class="swatch-name">{{ base }}</span>
      </button>
      <div v-if="filteredBases.length === 0" class="swatch-empty">No palettes match "{{ paletteSearch }}"</div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { useMainStore } from '../stores/main'
import { baseColormapName, colormapVariant, resolveColormap, resolveColormapName } from '../../composables/useColormapResolver'

const props = defineProps<{ modelValue: string | null }>()
const emit = defineEmits<{ 'update:modelValue': [string] }>()

const mainStore = useMainStore()
const colormaps = computed(() => mainStore.colormaps)

const selectedColormap = computed({
  get: () => props.modelValue,
  set: (v: string | null) => emit('update:modelValue', v ?? '')
})

const paletteSearch = ref('')

const baseNames = computed(() => {
  const set = new Set<string>()
  for (const key of Object.keys(colormaps.value)) set.add(baseColormapName(key))
  return Array.from(set).sort()
})

const filteredBases = computed(() => {
  const q = paletteSearch.value?.toLowerCase() ?? ''
  return baseNames.value.filter(b => b.includes(q))
})

const selectedBase = computed(() => baseColormapName(selectedColormap.value ?? ''))
const reversedActive = computed(() => colormapVariant(selectedColormap.value ?? '').reversed)
const invertedActive = computed(() => colormapVariant(selectedColormap.value ?? '').inverted)

const reverseAvailable = computed(() => resolveColormapName(colormaps.value, selectedBase.value, invertedActive.value, true) !== null)
const invertAvailable = computed(() => resolveColormapName(colormaps.value, selectedBase.value, true, reversedActive.value) !== null)

function toggleReversed() {
  const name = resolveColormapName(colormaps.value, selectedBase.value, invertedActive.value, !reversedActive.value)
  if (name) selectedColormap.value = name
}

function toggleInverted() {
  const name = resolveColormapName(colormaps.value, selectedBase.value, !invertedActive.value, reversedActive.value)
  if (name) selectedColormap.value = name
}

function selectBase(base: string) {
  const name = resolveColormapName(colormaps.value, base, invertedActive.value, reversedActive.value)
    ?? resolveColormapName(colormaps.value, base, false, false)
    ?? base
  selectedColormap.value = name
}

function swatchStyle(base: string) {
  const key = resolveColormapName(colormaps.value, base, false, false) ?? base
  const stops = resolveColormap(colormaps.value, key)?.stops
  if (!Array.isArray(stops)) return {}
  const str = stops.map((s: any) => `${s[1]} ${Math.round(s[0] * 100)}%`).join(', ')
  return { background: `linear-gradient(90deg, ${str})` }
}
</script>

<style scoped>
.swatch-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(76px, 1fr));
  gap: 8px;
  max-height: 180px;
  overflow-y: auto;
  padding: 2px;
}

.swatch-cell {
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding: 4px;
  border-radius: 6px;
  border: 1px solid transparent;
  background: transparent;
  cursor: pointer;
  font: inherit;
  color: inherit;
}

.swatch-cell:hover {
  background: rgba(128, 128, 128, 0.12);
}

.swatch-cell--selected {
  border-color: rgb(var(--v-theme-primary));
  background: rgba(128, 128, 128, 0.12);
}

.swatch-preview {
  height: 14px;
  border-radius: 3px;
  border: 1px solid rgba(0, 0, 0, 0.08);
}

.swatch-name {
  font-size: 10px;
  text-align: center;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.swatch-empty {
  grid-column: 1 / -1;
  font-size: 11px;
  opacity: 0.6;
  text-align: center;
  padding: 12px 0;
}
</style>
