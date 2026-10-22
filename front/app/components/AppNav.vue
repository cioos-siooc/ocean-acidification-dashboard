<template>
  <!-- Top-level views. Explore and Cross-Section are footer panes beside the
       map; Analysis and Comparison open fullscreen workspaces. All four write
       the same `activeBottomTab` the footer and workspaces read. -->
  <nav class="app-nav" aria-label="Views">
    <button
      v-for="t in tabs"
      :key="t.value"
      type="button"
      class="app-nav__item"
      :class="{ 'app-nav__item--active': mainStore.activeBottomTab === t.value }"
      :disabled="t.disabled"
      :title="t.title"
      :aria-current="mainStore.activeBottomTab === t.value ? 'page' : undefined"
      @click="mainStore.setActiveBottomTab(t.value)"
    >
      {{ t.label }}
    </button>

    <!-- Analysis needs its source (model or sensor) picked before it opens,
         since the fullscreen workspace has no rail of its own. -->
    <UDropdownMenu :items="analysisItems" :content="{ align: 'start' }">
      <button
        type="button"
        class="app-nav__item"
        :class="{ 'app-nav__item--active': mainStore.activeBottomTab === 'analysis' }"
      >
        Analysis
        <UIcon name="i-mdi-chevron-down" class="size-[14px] opacity-60" />
      </button>
    </UDropdownMenu>

    <button
      type="button"
      class="app-nav__item"
      :class="{ 'app-nav__item--active': mainStore.activeBottomTab === 'comparison' }"
      :disabled="!hasSensor"
      :title="hasSensor ? 'Compare the model against the sensor' : 'Select a sensor first'"
      @click="mainStore.setActiveBottomTab('comparison')"
    >
      Comparison
    </button>
  </nav>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { DropdownMenuItem } from '@nuxt/ui'
import { useMainStore } from '../stores/main'

const mainStore = useMainStore()

const hasSensor = computed(() => !!mainStore.selectedSensor?.id)

const tabs = computed(() => [
  { value: 'explore' as const, label: 'Explore', disabled: false, title: 'The clicked point over time' },
  { value: 'crossSection' as const, label: 'Cross-section', disabled: false, title: 'Depth along a line you draw' },
])

const analysisItems = computed<DropdownMenuItem[]>(() => [
  {
    label: 'Model',
    onSelect: () => { mainStore.setAnalysisSource('model'); mainStore.setActiveBottomTab('analysis') },
  },
  {
    label: hasSensor.value ? 'Sensor' : 'Sensor (select a sensor first)',
    disabled: !hasSensor.value,
    onSelect: () => { mainStore.setAnalysisSource('sensor'); mainStore.setActiveBottomTab('analysis') },
  },
])
</script>

<style scoped>
.app-nav {
  display: flex;
  align-items: stretch;
  height: 100%;
  gap: 2px;
}

.app-nav__item {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 0 10px;
  font-size: 12.5px;
  color: var(--ui-text-muted);
  cursor: pointer;
}

.app-nav__item:hover:not(:disabled) {
  color: var(--ui-text);
}

.app-nav__item:disabled {
  /* Readable, not "broken": the title says why it's unavailable. */
  color: var(--ui-text-dimmed);
  cursor: not-allowed;
}

.app-nav__item--active {
  color: var(--ui-text);
  box-shadow: inset 0 -2px 0 var(--ui-primary);
}

.app-nav__item:focus-visible {
  outline: 2px solid var(--ui-primary);
  outline-offset: -2px;
}
</style>
