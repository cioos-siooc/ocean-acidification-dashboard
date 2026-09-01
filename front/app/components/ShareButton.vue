<template>
  <UPopover v-model:open="open" :content="{ side: 'bottom', align: 'end' }">
    <UButton
      variant="ghost"
      icon="i-mdi-share-variant"
      class="shrink-0"
      title="Share this view"
      :aria-label="label ?? 'Share this view'"
      @click="build"
    >{{ label }}</UButton>

    <template #content>
      <div class="p-3 w-[420px] flex flex-col gap-2">
        <div class="text-sm font-medium">Share this view</div>
        <div class="flex items-center gap-2">
          <UInput
            ref="inputEl"
            v-model="url"
            readonly
            class="grow"
            :ui="{ base: 'font-mono text-xs' }"
            @focus="selectAll"
          />
          <UButton
            :icon="copied ? 'i-mdi-check' : 'i-mdi-content-copy'"
            :color="copied ? 'success' : 'primary'"
            :disabled="!url"
            :title="copied ? 'Copied' : 'Copy link'"
            @click="copy"
          />
        </div>
        <div class="text-xs text-muted">
          Opens the dashboard exactly as you have it now — variable, depth, time and colour
          range, the map's position, your clicked point or drawn line, the selected sensor,
          the open panel and its settings, and your sensor filters.
        </div>
      </div>
    </template>
  </UPopover>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'
import { useMainStore } from '../stores/main'
import { buildShareUrl } from '~~/composables/useShareState'
import { trackEvent } from '~~/composables/useAnalytics'

/**
 * The link is a *snapshot*: it is built when the popover opens, not kept in
 * sync with the app afterwards. The address bar is never rewritten during
 * normal use — a URL that changes on every pan and click adds history churn
 * and makes ordinary interaction feel consequential — so the hash only ever
 * appears on a link someone was given.
 */

/** Text beside the icon; icon-only when omitted (the fullscreen workspaces,
 *  where this sits next to an icon-only DownloadButton). */
defineProps<{ label?: string }>()

const mainStore = useMainStore()
const open = ref(false)
const url = ref('')
const copied = ref(false)
const inputEl = ref<any>(null)

async function build() {
  copied.value = false
  try {
    url.value = await buildShareUrl()
    trackEvent('share_link_created', {
      tab: mainStore.activeBottomTab,
      variable: mainStore.selected_variable.var,
      depth: mainStore.selected_variable.depth_nc,
      has_sensor: !!mainStore.selectedSensor?.id,
      has_cross_section: !!mainStore.crossSectionLine?.length,
    })
  } catch (e) {
    console.error('Failed to build share link:', e)
    url.value = ''
    mainStore.pushSnack({ color: 'error', text: 'Could not build a share link for this view.' })
  }
}

function selectAll(e: FocusEvent) {
  (e.target as HTMLInputElement)?.select()
}

async function copy() {
  if (!url.value) return
  try {
    await navigator.clipboard.writeText(url.value)
    copied.value = true
    mainStore.pushSnack({ color: 'success', text: 'Share link copied to the clipboard.' })
  } catch {
    // Clipboard access is refused outside a secure context (plain-HTTP dev
    // hosts included), which is exactly where this gets tested — fall back to
    // selecting the text so Ctrl+C still works.
    const el = inputEl.value?.$el?.querySelector('input') as HTMLInputElement | undefined
    el?.select()
    mainStore.pushSnack({ color: 'warning', text: 'Copy blocked by the browser — the link is selected, press Ctrl+C.' })
  }
}

// Reopening should never show a stale "Copied" tick from last time.
watch(open, (isOpen) => { if (!isOpen) copied.value = false })
</script>
