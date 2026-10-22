<template>
  <button
    type="button"
    class="card"
    :class="{ 'card--on': active }"
    :aria-pressed="active"
    :title="`Show ${label} on the map and chart`"
    @click="$emit('select')"
  >
    <span class="card__name"><b>{{ short }}</b><span v-if="unit">{{ unit }}</span></span>
    <span class="card__vals oe-num">
      <span class="card__model" :title="`Model at ${modelDepthLabel}`">{{ modelText }}</span>
      <template v-if="sensorSelected">
        <span v-if="hasSensor" class="card__sensor" :class="{ 'card__sensor--stale': sensorStale }" :title="sensorTitle">{{ sensorText }}</span>
        <span v-else class="card__none" title="The selected sensor doesn't measure this">—</span>
      </template>
    </span>
    <div ref="chartEl" class="card__plot" />
  </button>
</template>

<script setup lang="ts">
import { ref, watch, onMounted, onBeforeUnmount } from 'vue'
import * as echarts from 'echarts'
import { chart as chartPalette } from '@/config/palette'

/**
 * One variable in the point snapshot: latest model value over the sensor's
 * latest, each in its series colour, beside a tiny 72 h plot of both on one
 * shared scale (so a gap between the lines is a real difference).
 */
const props = defineProps<{
  short: string
  label: string
  unit: string
  active: boolean
  modelText: string
  modelDepthLabel: string
  /** Whether a sensor is selected at all; without one there's no second line. */
  sensorSelected: boolean
  hasSensor: boolean
  sensorText: string
  sensorTitle: string
  sensorStale: boolean
  /** [isoTime, displayValue] pairs. */
  model: [string, number | null][]
  sensor: [string, number | null][]
}>()

defineEmits<{ select: [] }>()

const chartEl = ref<HTMLDivElement | null>(null)
let chart: echarts.ECharts | null = null

function render() {
  if (!chart) return
  chart.setOption({
    animation: false,
    grid: { left: 1, right: 4, top: 3, bottom: 3 },
    xAxis: { type: 'time', show: false },
    yAxis: { type: 'value', show: false, scale: true },
    tooltip: { show: false },
    series: [
      { type: 'line', data: props.sensor, showSymbol: false, silent: true, lineStyle: { width: 1.1, color: chartPalette.sensor }, connectNulls: false,
        endLabel: { show: false } },
      { type: 'line', data: props.model, showSymbol: false, silent: true, lineStyle: { width: 1.2, color: chartPalette.model }, z: 3 },
    ],
  }, true)
}

onMounted(() => {
  if (!chartEl.value) return
  chart = echarts.init(chartEl.value, undefined, { renderer: 'svg' })
  render()
})

watch(() => [props.model, props.sensor], render)

onBeforeUnmount(() => { chart?.dispose(); chart = null })

defineExpose({ resize: () => chart?.resize() })
</script>

<style scoped>
.card {
  display: grid;
  grid-template-columns: 52px 1fr;
  grid-template-rows: auto 1fr;
  gap: 0 6px;
  min-width: 0;
  padding: 4px 6px;
  text-align: left;
  background: var(--ui-bg-muted);
  border: 1px solid var(--ui-border);
  cursor: pointer;
}

.card:hover {
  background: var(--ui-bg-elevated);
}

.card:focus-visible {
  outline: 2px solid var(--ui-primary);
  outline-offset: -2px;
}

.card--on,
.card--on:hover {
  background: #1b1913;
  border-color: #5a4a2a;
  box-shadow: inset 2px 0 0 var(--ui-primary);
}

.card__name {
  grid-column: 1 / 3;
  display: flex;
  justify-content: space-between;
  gap: 4px;
  font-size: 10.5px;
  color: var(--ui-text-muted);
  white-space: nowrap;
}

.card__name b {
  font-weight: 500;
  color: var(--ui-text);
}

.card__vals {
  display: grid;
  align-content: center;
  font-size: 12px;
  line-height: 1.25;
  white-space: nowrap;
}

.card__model { color: var(--oe-model); }
.card__sensor { color: var(--oe-sensor); }
.card__sensor--stale { opacity: 0.55; }
.card__none { color: var(--ui-text-dimmed); }

.card__plot {
  min-width: 0;
  height: 28px;
  align-self: center;
}
</style>
