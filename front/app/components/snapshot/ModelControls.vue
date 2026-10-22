<template>
  <!-- The map's model and depth. The field itself is picked from the snapshot
       cards below, so there's no Field dropdown here. -->
  <div class="mc">
    <div class="mc__field">
      <div class="mc__lab">
        <label class="oe-label" for="source-select">Model</label>
        <button type="button" class="mc__link" title="About this model" @click="showSourceInfo = true">About</button>
      </div>
      <USelectMenu id="source-select" v-model="selectedSource" :items="sourceItems" label-key="label" value-key="source"
        :disabled="sourceItems.length === 0" :search-input="false" size="xs" class="w-full" />
    </div>

    <!-- Depth stays a dropdown: the model has dozens of levels. The steppers
         walk one level at a time, which is how depth is usually explored. -->
    <div v-if="depths.length > 0" class="mc__field">
      <div class="mc__lab">
        <label class="oe-label" for="depth-select">Depth</label>
        <span v-if="depthIndex >= 0" class="mc__meta oe-num">{{ depthIndex + 1 }} / {{ depths.length }}</span>
      </div>
      <div class="flex items-stretch">
        <USelectMenu id="depth-select" v-model="selectedDepth" :items="depths" label-key="title" value-key="value"
          size="xs" class="grow min-w-0 oe-num" :ui="{ base: 'rounded-e-none' }" />
        <button type="button" class="mc__step" :disabled="depthIndex <= 0" title="Shallower level" aria-label="Shallower level" @click="step(-1)">
          <UIcon name="i-mdi-chevron-up" class="size-[14px]" />
        </button>
        <button type="button" class="mc__step" :disabled="depthIndex >= depths.length - 1" title="Deeper level" aria-label="Deeper level" @click="step(1)">
          <UIcon name="i-mdi-chevron-down" class="size-[14px]" />
        </button>
      </div>
    </div>
  </div>

  <UModal v-model:open="showSourceInfo" :ui="{ content: 'max-w-[50vw]' }">
    <template #content>
      <div class="p-5">
        <about-ssc v-if="selectedSource === 'SalishSeaCast'" />
        <about-nonna v-else-if="selectedSource === 'NONNA'" />
      </div>
    </template>
  </UModal>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import moment from 'moment-timezone'
import { useMainStore, formatDepthLabel } from '../../stores/main'

const mainStore = useMainStore()
const showSourceInfo = ref(false)

const variables = computed(() => mainStore.variables)
const selectedVariable = computed(() => mainStore.selected_variable)

// Models that carry the current field. With one model this is a one-item menu,
// kept so a second (e.g. LiveOcean) only has to appear in /variables.
const sourceItems = computed(() => {
  const sources = variables.value.filter(v => v.var === selectedVariable.value.var).map(v => ({ source: v.source, label: v.source }))
  if (sources.length && !sources.some(s => s.source === selectedVariable.value.source)) {
    mainStore.updateSelectedVariable({ source: sources[0]!.source })
  }
  return sources
})

const selectedSource = computed({
  get() { return selectedVariable.value.source },
  set(v: string) {
    const variable = variables.value.find(x => x.source === v && x.var === selectedVariable.value.var)
    const depths = variable?.depths ?? []

    // Keep the depth and instant as close as the new model allows.
    let newDepthNc: number | null = depths[0] ?? null
    const curDepth = selectedVariable.value.depth_nc
    if (depths.length && curDepth !== null) {
      newDepthNc = depths.reduce((a, b) => Math.abs(b - curDepth) < Math.abs(a - curDepth) ? b : a)
    }

    let newDt = selectedVariable.value.dt
    const dts = variable?.dts ?? []
    if (dts.length && selectedVariable.value.dt) {
      const cur = selectedVariable.value.dt.valueOf()
      const closest = dts.reduce((a, b) => Math.abs(b - cur) < Math.abs(a - cur) ? b : a)
      newDt = moment.unix(closest / 1000).tz('UTC')
    } else if (dts.length) {
      newDt = moment.unix(Math.max(...dts) / 1000).tz('UTC')
    }

    mainStore.updateSelectedVariable({
      source: v,
      depth: newDepthNc !== null ? formatDepthLabel(newDepthNc) : null,
      depth_nc: newDepthNc,
      dt: newDt,
    })
  },
})

const rawDepths = computed(() =>
  variables.value.find(v => v.source === selectedVariable.value.source && v.var === selectedVariable.value.var)?.depths ?? [])

// Surface first, bottom (-1) last.
const depths = computed(() => rawDepths.value.slice()
  .sort((a, b) => (a === -1 ? 1 : 0) - (b === -1 ? 1 : 0) || a - b)
  .map(d => {
    const label = formatDepthLabel(d)
    return { title: isNaN(Number(label)) ? label : `${label} m`, value: label }
  }))

const selectedDepth = computed({
  get() { return selectedVariable.value.depth },
  set(v: string | null) {
    const raw = rawDepths.value.find(d => formatDepthLabel(d) === v)
    mainStore.updateSelectedVariable({ depth: v, depth_nc: raw ?? null })
  },
})

const depthIndex = computed(() => depths.value.findIndex(d => d.value === selectedDepth.value))

function step(dir: 1 | -1) {
  const next = depths.value[depthIndex.value + dir]
  if (depthIndex.value >= 0 && next) selectedDepth.value = next.value
}
</script>

<style scoped>
.mc {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
  gap: 8px;
}

.mc__field {
  display: grid;
  gap: 3px;
  min-width: 0;
}

.mc__lab {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
}

.mc__meta,
.mc__link {
  font-size: 10.5px;
  color: var(--ui-text-muted);
}

.mc__link {
  cursor: pointer;
}

.mc__link:hover {
  color: var(--ui-text);
}

.mc__step {
  width: 22px;
  display: grid;
  place-items: center;
  border: 1px solid var(--ui-border-accented);
  border-left: 0;
  color: var(--ui-text-muted);
  cursor: pointer;
}

.mc__step:last-child {
  border-radius: 0 var(--ui-radius) var(--ui-radius) 0;
}

.mc__step:hover:not(:disabled) {
  color: var(--ui-text);
  background: var(--ui-bg-elevated);
}

.mc__step:disabled {
  color: var(--ui-text-dimmed);
  cursor: not-allowed;
}
</style>
