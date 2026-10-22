<template>
  <UApp>
  <div class="h-screen flex flex-col overflow-hidden bg-default text-default">
    <header class="app-header">
      <NuxtLink to="/" class="app-brand">
        <iconsOceanECOLogo :size="22" :loop="false" style="display:block;" />
        <span class="app-brand__name">OceanECO</span>
        <span class="app-brand__ver oe-num">v{{ versionLabel }}</span>
      </NuxtLink>

      <AppNav v-if="route.path === '/'" />

      <div class="grow" />

      <UButton variant="ghost" color="neutral" size="xs" href="https://docs.google.com/forms/d/e/1FAIpQLSdGiIclM5wvIbPReZydsXKiRBXbZsQVEdoQPlA0EruKIoNJkg/viewform?usp=dialog" target="_blank" rel="noopener noreferrer">
        Feedback
      </UButton>

      <ShareButton label="Share" />

      <!-- Hidden for v1.0; the /caseStudy pages still exist. -->
      <!-- <NuxtLink to="/caseStudy" target="_blank">
        <UButton variant="ghost">Case Studies</UButton>
      </NuxtLink> -->

      <UButton variant="ghost" color="neutral" size="xs" to="/about" target="_blank">About</UButton>

      <a href="https://cioospacific.ca/" target="_blank" rel="noopener noreferrer" class="partner-link" title="CIOOS Pacific">
        <img src="/cioos_pacific.png" alt="CIOOS Pacific" class="partner-logo" />
      </a>
    </header>

    <NuxtRouteAnnouncer />
    <NuxtPage v-if="!isMobile" />

    <MobileBlocker />
  </div>
  </UApp>
</template>

<script setup lang="ts">
import { computed, onBeforeMount, onMounted, onBeforeUnmount, ref } from 'vue'
import moment from 'moment'
import axios from 'axios'

import { useRuntimeConfig } from '#app'
import { useMainStore, formatDepthLabel } from './stores/main'
import { readShareHash, decodeShareState, applyShareState, applySharedVariable } from '~~/composables/useShareState'
import ShareButton from './components/ShareButton.vue'
import AppNav from './components/AppNav.vue'
const mainStore = useMainStore();
const route = useRoute();
const config = useRuntimeConfig();
// NUXT_PUBLIC_* env values are JSON-parsed at runtime, so "1.0" arrives as the number 1.
const versionLabel = computed(() => {
  const v = String(config.public.version ?? '');
  return /^\d+$/.test(v) ? `${v}.0` : v;
});
const apiBaseUrl = config.public.apiBaseUrl;

// A shared link's payload is decoded before anything is fetched, so
// `getVariables()` below can honour it instead of the cold-start default.
// `shareRestorePending` is flipped synchronously (the decode itself is async —
// it goes through DecompressionStream) so index.vue, which mounts while the
// decode is still in flight, knows to hold its bootstrap map click.
onBeforeMount(async () => {
  const payload = readShareHash();
  if (payload) {
    mainStore.shareRestorePending = true;
    const shared = await decodeShareState(payload);
    if (shared) {
      mainStore.setPendingShare(shared);
      applyShareState(shared);
    } else {
      mainStore.shareRestorePending = false;
      mainStore.pushSnack({ color: 'warning', text: "This share link couldn't be read — showing the default view." });
    }
  }
  getColormaps();
  // Awaited so the restore flag is only released once the shared variable has
  // been resolved against `/variables` — index.vue's bootstrap map click waits
  // on that flag (see `maybeInitClick`).
  await getVariables();
  mainStore.clearPendingShare();
});

// Block phones / small tablets: this map- and chart-heavy dashboard needs a large
// screen. Keep the initial value `false` so SSR/first client render match (avoids a
// hydration mismatch); the real decision happens on the client after mount. The
// MobileBlocker overlay hides everything via CSS, and unmounting NuxtPage below stops
// the heavy MapboxGL page from initializing on unsupported devices.
const isMobile = ref(false);
// Must stay in sync with MobileBlocker.vue's media query.
const MOBILE_QUERY = '(max-width: 900px), (pointer: coarse) and (max-width: 1024px)';
let mql: MediaQueryList | null = null;
const updateIsMobile = () => { if (mql) isMobile.value = mql.matches; };

onMounted(() => {
  mql = window.matchMedia(MOBILE_QUERY);
  updateIsMobile();
  mql.addEventListener('change', updateIsMobile);
});

onBeforeUnmount(() => {
  mql?.removeEventListener('change', updateIsMobile);
});

async function getVariables() {
  try {
    const r = await axios.get(`${apiBaseUrl}/variables`);
    const data = r.data;
    console.log('data: ', data);

    // Convert datetimes to epoch ms numbers (plain numbers avoid deep Vue proxy overhead)
    data.forEach((v: any) => {
      v.dts = v.dts?.map((dtstr: string) => moment.utc(dtstr).valueOf());
    });

    mainStore.setVariables(data);

    // A shared link picks the variable; its selection is validated field by
    // field against the list just fetched, so a retired variable or a depth
    // level that no longer exists degrades to the nearest sensible thing
    // rather than to a blank map.
    const shared = mainStore.pendingShare;
    if (shared) {
      const resolved = applySharedVariable(shared, data);
      // `dt` is the one field with no usable fallback — without an instant
      // there is nothing to render — so a payload that can't resolve one drops
      // through to the cold-start default below.
      if (resolved && resolved.patch.var && resolved.patch.dt) {
        mainStore.updateSelectedVariable(resolved.patch);
        for (const issue of resolved.issues) {
          mainStore.pushSnack({ color: 'warning', text: `Shared view: ${issue}.` });
        }
        return;
      }
      if (resolved?.issues.length) {
        for (const issue of resolved.issues) {
          mainStore.pushSnack({ color: 'warning', text: `Shared view: ${issue} — falling back to the default.` });
        }
      }
    }

    if (data.length > 0) {
      const varId = 'temperature';
      const varMeta = data.find((v: any) => v.var === varId);
      const source = varMeta?.source ?? '';
      const dts = varMeta?.dts ?? [];
      const precision = varMeta?.precision || 0.1;
      const depthNc = (varMeta?.depths && varMeta.depths.length > 0) ? varMeta.depths[0] : 0.5;
      const depth = formatDepthLabel(depthNc);
      const colormap = varMeta?.colormap ?? null;
      const colormapMin = varMeta?.colormapMin ?? null;
      const colormapMax = varMeta?.colormapMax ?? null;
      if (dts.length > 0) {
        mainStore.updateSelectedVariable({
          var: varId,
          source: source,
          dt: moment.utc(dts[dts.length - 1]),
          depth: depth,
          depth_nc: depthNc,
          precision: precision,
          colormap: colormap,
          colormapMin: colormapMin,
          colormapMax: colormapMax
        });
      }
    }


  } catch (e) {
    console.error('Failed to fetch variables:', e);
  }
}


async function getColormaps() {
  try {
    const r = await axios.get(`${apiBaseUrl}/colormaps`);
    const list = r.data;
    const map: Record<string, any> = {};
    for (const c of list) map[c.name] = c;
    // colormaps.value = map;
    mainStore.setColormaps(map);
    return map;
  } catch (e) {
    console.error('Failed to fetch colormaps:', e);
    mainStore.setColormaps({});
    return {};
  }
}
</script>

<style scoped>
/* Fonts and base type live in assets/css/main.css with the other tokens. */
.app-header {
  display: flex;
  align-items: center;
  gap: 12px;
  height: 36px;
  padding: 0 12px 0 14px;
  flex-shrink: 0;
  background: var(--ui-bg-muted);
  border-bottom: 1px solid var(--ui-border);
}

.app-brand {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-right: 12px;
}

.app-brand__name {
  font-weight: 600;
  font-size: 13.5px;
  letter-spacing: 0.02em;
}

.app-brand__ver {
  font-size: 11px;
  color: var(--ui-text-muted);
}

.partner-link {
  display: flex;
  align-items: center;
  margin-left: 8px;
  padding-left: 12px;
  border-left: 1px solid var(--ui-border);
}

/* The partner mark sits quietly in the corner rather than centre stage. */
.partner-logo {
  height: 20px;
  opacity: 0.7;
  transition: opacity 0.2s;
}

.partner-link:hover .partner-logo {
  opacity: 1;
}
</style>