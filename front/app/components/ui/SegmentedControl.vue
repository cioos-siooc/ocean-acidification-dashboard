<template>
  <div
    ref="root"
    role="radiogroup"
    :aria-label="ariaLabel"
    class="inline-flex isolate"
    :class="[block && 'flex w-full', wrap && 'flex-wrap gap-1']"
  >
    <UButton
      v-for="(item, i) in items"
      :key="String(item.value)"
      role="radio"
      :aria-checked="item.value === modelValue"
      :tabindex="item.value === modelValue ? 0 : -1"
      :disabled="disabled || item.disabled"
      :title="item.title ?? item.label"
      :icon="item.icon"
      :size="size"
      :color="item.value === modelValue ? color : 'neutral'"
      :variant="item.value === modelValue ? 'solid' : unselectedVariant"
      :class="[
        'justify-center',
        block && 'flex-1',
        itemClass,
        // Joined-bar styling only makes sense on a single row: collapse the
        // shared border and square off the inner corners so it reads as one
        // control. When wrapping, rows would end up with half-rounded ends, so
        // segments stay individually rounded and separated by the container gap.
        !wrap && i > 0 && '-ms-px',
        !wrap && items.length > 1 && i === 0 && 'rounded-e-none',
        !wrap && items.length > 1 && i === items.length - 1 && 'rounded-s-none',
        !wrap && i > 0 && i < items.length - 1 && 'rounded-none',
      ]"
      @click="select(item)"
      @keydown="onKeydown($event, i)"
    >
      {{ item.label }}
    </UButton>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'

export interface SegmentedItem {
  value: string | number
  label?: string
  /** Iconify name, e.g. 'i-mdi-map-marker'. */
  icon?: string
  /** Tooltip; falls back to `label`. */
  title?: string
  disabled?: boolean
}

const props = withDefaults(defineProps<{
  modelValue: string | number
  items: SegmentedItem[]
  size?: 'xs' | 'sm' | 'md' | 'lg' | 'xl'
  color?: 'primary' | 'secondary' | 'success' | 'info' | 'warning' | 'error' | 'neutral'
  /** Look of the *unselected* segments. 'tonal' mirrors Vuetify's tonal toggle. */
  variant?: 'tonal' | 'outline'
  disabled?: boolean
  /** Stretch to fill the row, segments sharing width equally. */
  block?: boolean
  wrap?: boolean
  /** Extra classes for each segment — e.g. 'px-1' to squeeze many into one row. */
  itemClass?: string
  ariaLabel?: string
}>(), {
  size: 'sm',
  color: 'primary',
  variant: 'tonal',
  disabled: false,
  block: false,
  wrap: false,
  itemClass: undefined,
  ariaLabel: undefined,
})

const emit = defineEmits<{ 'update:modelValue': [string | number] }>()

const root = ref<HTMLElement | null>(null)
const unselectedVariant = computed(() => (props.variant === 'outline' ? 'outline' : 'subtle'))

function select(item: SegmentedItem) {
  // Mandatory by design (mirrors v-btn-toggle's `mandatory`): re-clicking the
  // active segment is a no-op rather than clearing the selection.
  if (item.disabled || props.disabled || item.value === props.modelValue) return
  emit('update:modelValue', item.value)
}

/** Arrow/Home/End move the selection, per the radiogroup keyboard contract. */
function onKeydown(e: KeyboardEvent, index: number) {
  const keys = ['ArrowRight', 'ArrowDown', 'ArrowLeft', 'ArrowUp', 'Home', 'End']
  if (!keys.includes(e.key)) return
  const enabled = props.items.map((it, i) => ({ it, i })).filter(({ it }) => !it.disabled)
  if (!enabled.length) return
  e.preventDefault()

  const pos = enabled.findIndex(({ i }) => i === index)
  let next: number
  if (e.key === 'Home') next = 0
  else if (e.key === 'End') next = enabled.length - 1
  else if (e.key === 'ArrowRight' || e.key === 'ArrowDown') next = (pos + 1) % enabled.length
  else next = (pos - 1 + enabled.length) % enabled.length

  const target = enabled[next]
  emit('update:modelValue', target.it.value)
  // Roving tabindex: keep focus on whichever segment is now selected.
  root.value?.querySelectorAll<HTMLElement>('[role="radio"]')[target.i]?.focus()
}
</script>
