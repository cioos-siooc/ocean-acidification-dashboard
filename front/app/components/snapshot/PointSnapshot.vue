<template>
  <!-- Every variable at the clicked point at once, model and sensor, so
       comparing latest pH and temperature doesn't mean switching fields.
       Clicking a card switches the field for the map and the chart. -->
  <aside class="snap" :class="{ 'snap--loading': loading }" aria-label="Point snapshot">
    <div class="snap__head oe-num">
      <span class="snap__title">Point · {{ hours }} h to map time</span>
      <span class="snap__key"><i style="background: var(--oe-model)" />Model {{ modelDepthLabel }}</span>
      <span v-if="sensorName" class="snap__key"><i style="background: var(--oe-sensor)" />{{ sensorName }} {{ sensorDepthLabel }}</span>
    </div>

    <div v-if="error" class="snap__note">{{ error }}</div>
    <div v-else-if="outsideDomain" class="snap__note">Outside the model domain: sensor values only.</div>

    <div class="snap__cards">
      <SnapshotCard
        v-for="c in cards"
        :key="c.varId"
        v-bind="c.props"
        @select="select(c.varId)"
      />
    </div>
  </aside>
</template>

<script setup lang="ts">
import { computed, ref, watch, onBeforeUnmount } from 'vue'
import moment from 'moment'
import axios from 'axios'
import { useMainStore, formatDepthLabel } from '../../stores/main'
import { fetchPointSnapshot, type PointSnapshot } from '~~/composables/usePointSnapshot'
import { useVariableRegistry } from '~~/composables/useVariableRegistry'
import { depth2txt } from '~~/composables/useSensorFormat'
import { trackEvent } from '~~/composables/useAnalytics'
import SnapshotCard from './SnapshotCard.vue'

const mainStore = useMainStore()
const { variableLabel, variableShort, displayUnit, toDisplayValue, formatDisplayValue } = useVariableRegistry()

const hours = 72

const data = ref<PointSnapshot | null>(null)
const loading = ref(false)
const error = ref<string | null>(null)

const point = computed(() => mainStore.lastClickedMapPoint)
const selectedSensor = computed(() => mainStore.selectedSensor?.id
  ? mainStore.sensors.find(s => s.id === mainStore.selectedSensor!.id) ?? null
  : null)

// What the request depends on. `dt` moves on every playback step; the debounce
// below and the request cache keep that cheap.
const params = computed(() => {
  const sv = mainStore.selected_variable
  if (!point.value || sv.depth_nc == null || !sv.dt) return null
  return {
    lat: point.value.lat,
    lon: point.value.lng,
    depth: sv.depth_nc,
    to: moment.utc(sv.dt).format('YYYY-MM-DDTHH:mm:ss'),
    hours,
    sensorId: selectedSensor.value?.id ?? null,
    sensorDepth: selectedSensor.value?.depth ?? null,
  }
})

let timer: ReturnType<typeof setTimeout> | null = null
let controller: AbortController | null = null

async function load() {
  const p = params.value
  if (!p) { data.value = null; return }
  controller?.abort()
  controller = new AbortController()
  loading.value = true
  try {
    data.value = await fetchPointSnapshot(p, controller.signal)
    error.value = null
  } catch (e: unknown) {
    if (axios.isCancel(e)) return
    error.value = "Couldn't load the snapshot for this point."
  } finally {
    loading.value = false
  }
}

watch(params, () => {
  if (timer) clearTimeout(timer)
  timer = setTimeout(load, 400)
}, { immediate: true, deep: true })

onBeforeUnmount(() => { if (timer) clearTimeout(timer); controller?.abort() })

const outsideDomain = computed(() => !!data.value?.modelError)

const modelDepthLabel = computed(() => {
  const d = data.value?.model?.depth ?? mainStore.selected_variable.depth_nc
  return d == null ? '' : `${formatDepthLabel(d)} m`
})

const sensorName = computed(() => selectedSensor.value?.name ?? '')
const sensorDepthLabel = computed(() => {
  const s = selectedSensor.value
  if (!s) return ''
  // A profiler is read at the model's level; say so rather than "variable depth".
  if (s.depth == null || s.depth < 0) return data.value?.sensor?.depth != null ? `${formatDepthLabel(data.value.sensor.depth)} m` : 'profiler'
  return depth2txt(s)
})

// Fixed order: the API's, which follows variable_config.yml.
const varIds = computed(() => {
  const fromModel = data.value?.model ? Object.keys(data.value.model.values) : []
  if (fromModel.length) return fromModel
  const fromConfig = mainStore.variables.filter(v => v.source === mainStore.selected_variable.source).map(v => v.var)
  return fromConfig
})

function lastNonNull(values: (number | null)[]): number | null {
  for (let i = values.length - 1; i >= 0; i--) if (values[i] != null) return values[i]!
  return null
}

const windowStartMs = computed(() => data.value ? moment.utc(data.value.to).subtract(hours, 'hours').valueOf() : 0)

const cards = computed(() => varIds.value.map(varId => {
  const model = data.value?.model
  const modelVals = model?.values[varId] ?? []
  const modelSeries = (model?.time ?? []).map((t, i) => [t, toDisplayValue(varId, modelVals[i] ?? null)] as [string, number | null])
  const modelLatest = toDisplayValue(varId, lastNonNull(modelVals))

  const sv = data.value?.sensor?.variables[varId]
  const sensorSeries = (sv?.time ?? []).map((t, i) => [t, toDisplayValue(varId, sv!.value[i] ?? null)] as [string, number | null])
  const last = sv?.last ?? null
  const lastMs = last ? moment.utc(last.time).valueOf() : null
  const stale = lastMs != null && lastMs < windowStartMs.value

  return {
    varId,
    props: {
      short: variableShort(varId),
      label: variableLabel(varId),
      unit: displayUnit(varId),
      active: mainStore.selected_variable.var === varId,
      modelText: formatDisplayValue(varId, modelLatest),
      modelDepthLabel: modelDepthLabel.value,
      sensorSelected: !!selectedSensor.value,
      hasSensor: !!sv,
      sensorText: last ? formatDisplayValue(varId, toDisplayValue(varId, last.value)) : '—',
      sensorStale: stale,
      sensorTitle: last
        ? `${sensorName.value}, ${stale ? 'last reading' : 'latest'} ${moment.utc(last.time).local().format('D MMM YYYY HH:mm')}`
        : `${sensorName.value}: no readings`,
      model: modelSeries,
      sensor: sensorSeries,
    },
  }
}))

function select(varId: string) {
  if (varId === mainStore.selected_variable.var) return
  trackEvent('variable_changed', { variable: varId, source: mainStore.selected_variable.source })
  mainStore.selectVariable(varId)
}
</script>

<style scoped>
.snap {
  display: grid;
  grid-template-rows: auto auto 1fr;
  gap: 6px;
  min-height: 0;
  padding: 6px 0 4px 10px;
  border-left: 1px solid var(--ui-border);
  transition: opacity 0.2s;
}

.snap--loading {
  opacity: 0.7;
}

.snap__head {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 2px 10px;
  font-size: 10.5px;
  color: var(--ui-text-muted);
}

.snap__title {
  color: var(--ui-text);
}

.snap__key {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  white-space: nowrap;
}

.snap__key i {
  width: 10px;
  height: 2px;
}

.snap__note {
  font-size: 11px;
  color: var(--ui-text-muted);
}

.snap__cards {
  display: grid;
  grid-template-columns: 1fr 1fr;
  grid-auto-rows: minmax(44px, 1fr);
  gap: 4px;
  min-height: 0;
  overflow-y: auto;
}
</style>
