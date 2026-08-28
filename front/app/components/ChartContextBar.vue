<template>
  <div class="chart-context-bar">
    <template v-for="(item, i) in items" :key="item.label">
      <span class="ctx-item" :class="`ctx-item--${item.tone ?? 'normal'}`" :title="item.title">
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
 *
 * Depth in particular is never shown unlabelled here. A chart can carry a
 * model depth and a sensor depth at once (a deep-ocean mooring at 1257 m
 * overlaid on a model level at 442 m), and one bare "Depth 442 m" beside a
 * line drawn from the other source reads as a contradiction — so callers pass
 * one item per source and let `tone` mark the one that has nothing to draw.
 */
export type ContextItem = {
  label: string
  value: string
  /** 'muted' = present but empty; 'warn' = the reason the chart looks wrong. */
  tone?: 'normal' | 'muted' | 'warn'
  /** Native tooltip for the long-form explanation the bar has no room for. */
  title?: string
}

defineProps<{ items: ContextItem[] }>()
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
.ctx-item--muted .ctx-value { color: rgba(255,255,255,0.4); font-style: italic; }
.ctx-item--warn .ctx-label { color: rgba(251,191,36,0.65); }
.ctx-item--warn .ctx-value { color: rgb(251,191,36); }
.ctx-sep { color: rgba(255,255,255,0.2); }
</style>
