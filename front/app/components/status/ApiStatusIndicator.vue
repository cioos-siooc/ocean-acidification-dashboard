<template>
  <UTooltip :text="tooltip">
    <UButton
      v-if="status === 'degraded' || status === 'down'"
      :color="status === 'down' ? 'error' : 'warning'"
      variant="subtle"
      size="xs"
      icon="i-mdi-cloud-off-outline"
      :label="status === 'down' ? 'Server offline' : 'Database unavailable'"
      :loading="checking"
      @click="recheckNow"
    />
    <span v-else class="flex items-center gap-1.5 px-2 text-xs text-muted" role="status">
      <span class="size-2 rounded-full" :class="status === 'ok' ? 'bg-success' : 'bg-neutral-500'" />
      {{ status === 'ok' ? 'Online' : 'Connecting…' }}
    </span>
  </UTooltip>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import moment from 'moment'
import { useApiStatus } from '~~/composables/useApiStatus'

const { status, lastCheckedAt, recheck } = useApiStatus()
const checking = ref(false)

const tooltip = computed(() => {
  const at = lastCheckedAt.value ? ` · checked ${moment(lastCheckedAt.value).format('HH:mm')}` : ''
  switch (status.value) {
    case 'ok': return `Data server online${at}`
    case 'degraded': return `The data server is up but its database isn't responding${at}. Click to retry.`
    case 'down': return `Can't reach the data server${at}. Click to retry.`
    default: return 'Checking the data server…'
  }
})

async function recheckNow() {
  checking.value = true
  try { await recheck() } finally { checking.value = false }
}
</script>
