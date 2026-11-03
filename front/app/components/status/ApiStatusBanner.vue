<template>
  <UAlert
    v-if="failing && !dismissed"
    :color="status === 'down' ? 'error' : 'warning'"
    variant="subtle"
    icon="i-mdi-cloud-off-outline"
    :title="status === 'down' ? 'The data server is offline' : 'The data server’s database is unavailable'"
    description="Maps and charts can’t load data until it’s back. We’re checking automatically and will let you know when it recovers."
    orientation="horizontal"
    :actions="[{ label: 'Retry now', color: 'neutral', variant: 'outline', loading: checking, onClick: recheckNow }]"
    close
    class="rounded-none shrink-0"
    @update:open="dismissed = true"
  />
  <UAlert
    v-else-if="recovered && status === 'ok'"
    color="success"
    variant="subtle"
    icon="i-mdi-cloud-check-outline"
    title="The data server is back"
    description="Reload the page to load the data that failed earlier."
    orientation="horizontal"
    :actions="[{ label: 'Reload', color: 'neutral', variant: 'outline', onClick: reload }]"
    close
    class="rounded-none shrink-0"
    @update:open="recovered = false"
  />
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useApiStatus } from '~~/composables/useApiStatus'

const { status, recovered, recheck } = useApiStatus()
const failing = computed(() => status.value === 'degraded' || status.value === 'down')
const checking = ref(false)

// A dismissed outage banner stays hidden until the next outage.
const dismissed = ref(false)
watch(status, (s) => { if (s === 'ok') dismissed.value = false })

async function recheckNow() {
  checking.value = true
  try { await recheck() } finally { checking.value = false }
}

function reload() {
  window.location.reload()
}
</script>
