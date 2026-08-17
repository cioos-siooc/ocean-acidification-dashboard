<template>
  <div class="chart-context-bar">
    <template v-for="(item, i) in items" :key="item.label">
      <span class="ctx-item">
        <span class="ctx-label">{{ item.label }}</span>
        <span class="ctx-value">{{ item.value }}</span>
      </span>
      <span v-if="i < items.length - 1" class="ctx-sep">&middot;</span>
    </template>
  </div>
</template>

<script setup lang="ts">
/**
 * Static readout of what a chart is actually plotting — field, depth,
 * datetime range, coordinate(s) — as opposed to selectedInfo.vue's map-corner
 * box, which only ever describes the map's current raster layer. The two can
 * legitimately disagree: a depth-time heatmap plots every depth against one
 * map depth layer, and Cross-Section plots a whole line against one map point
 * — each pane passes only the items that apply to what it's showing.
 */
defineProps<{ items: { label: string; value: string }[] }>()
</script>

<style scoped>
.chart-context-bar {
  display: flex; align-items: center; flex-wrap: wrap; gap: 10px;
  margin-bottom: 6px; padding: 4px 10px; min-height: 26px;
  background: rgba(255,255,255,0.03); border-radius: 4px; font-size: 11.5px;
}
.ctx-item { display: flex; align-items: baseline; gap: 5px; }
.ctx-label { color: rgba(255,255,255,0.35); font-size: 9.5px; text-transform: uppercase; letter-spacing: 0.04em; }
.ctx-value { color: rgba(255,255,255,0.75); font-variant-numeric: tabular-nums; }
.ctx-sep { color: rgba(255,255,255,0.2); }
</style>
