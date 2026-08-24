<template>
  <!-- One control per view, not per chart or table: a tab like Extreme Events shows
       a chart and two tables that are all one query's output, and three separate
       download icons would suggest three unrelated things to download. -->
  <UDropdownMenu v-if="datasets.length > 1" :items="items" :content="{ align: 'end' }">
    <UButton
      :icon="icon"
      :size="size"
      :variant="variant"
      color="neutral"
      :disabled="!hasRows"
      :title="hasRows ? 'Download data (CSV)' : 'Nothing to download yet'"
      :aria-label="label ?? 'Download data as CSV'"
    >
      {{ label }}
    </UButton>
  </UDropdownMenu>

  <UButton
    v-else
    :icon="icon"
    :size="size"
    :variant="variant"
    color="neutral"
    :disabled="!hasRows"
    :title="hasRows ? `Download ${datasets[0]?.label ?? 'data'} (CSV)` : 'Nothing to download yet'"
    :aria-label="label ?? 'Download data as CSV'"
    @click="run(datasets[0])"
  >
    {{ label }}
  </UButton>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { downloadCsv, useCsvExport, type CsvDataset } from '~~/composables/useCsvExport'

const props = withDefaults(defineProps<{
  datasets: CsvDataset[]
  /** Text beside the icon; icon-only when omitted. */
  label?: string
  size?: 'xs' | 'sm' | 'md' | 'lg' | 'xl'
  variant?: 'ghost' | 'soft' | 'subtle' | 'outline' | 'solid'
}>(), {
  label: undefined,
  size: 'sm',
  variant: 'ghost',
})

const icon = 'i-mdi-download'

// The context (point, depth, variable, query window) comes from whichever host
// provided the registry, so callers never thread it through by hand.
const csv = useCsvExport()

const hasRows = computed(() => props.datasets.some(d => d.rows.length > 0))

function run(dataset?: CsvDataset) {
  if (!dataset?.rows.length) return
  downloadCsv(dataset, csv?.context.value ?? null)
}

// Empty datasets stay listed but disabled, so the menu keeps a stable shape as
// data loads instead of items appearing and shifting under the pointer.
const items = computed(() => props.datasets.map(d => ({
  label: d.label,
  icon: 'i-mdi-file-delimited-outline',
  description: d.rows.length ? `${d.rows.length} row${d.rows.length === 1 ? '' : 's'}` : 'No data',
  disabled: !d.rows.length,
  onSelect: () => run(d),
})))
</script>
