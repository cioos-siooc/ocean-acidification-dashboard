<template>
  <UApp>
  <div class="h-screen flex flex-col overflow-hidden bg-default text-default">
    <header class="flex items-center gap-3 px-5 h-12 shrink-0 bg-elevated border-b border-default">
      <div class="flex items-center" style="flex-shrink:0;">
        <iconsOceanECOLogo :size="40" :loop="false" style="display:block;" />
      </div>

      <div class="flex items-center min-w-0">
        <span class="">OceanECO</span>
        <span class="text-xs font-medium mx-2" style="font-family: monospace">v{{ config.public.version }}</span>
        <UTooltip text="This dashboard is in active development. Data, features, and layout may change without notice."
          :content="{ side: 'bottom' }" :ui="{ content: 'max-w-[280px]' }">
          <UBadge size="sm" color="warning" variant="solid">BETA</UBadge>
        </UTooltip>
      </div>

      <a href="https://cioospacific.ca/" target="_blank" rel="noopener noreferrer" class="logo-link"
        style="position: absolute; left: 50%; transform: translateX(-50%);">
        <img src="/cioos_pacific.png" alt="OA Logo" class="logo-icon" />
      </a>

      <!-- Optional: Add menu items here -->
      <!-- FEEDBACK FORM -->
      <UButton variant="ghost" href="https://docs.google.com/forms/d/e/1FAIpQLSdGiIclM5wvIbPReZydsXKiRBXbZsQVEdoQPlA0EruKIoNJkg/viewform?usp=dialog" target="_blank" rel="noopener noreferrer">
        Feedback
      </UButton>

      <ShareButton label="Share" />

      <NuxtLink to="/caseStudy" target="_blank">
        <UButton variant="ghost">Case Studies</UButton>
      </NuxtLink>

      <NuxtLink to="/about" target="_blank">
        <UButton variant="ghost">About</UButton>
      </NuxtLink>

    </header>

    <NuxtRouteAnnouncer />
    <NuxtPage v-if="!isMobile" />

    <MobileBlocker />
  </div>
  </UApp>
</template>

<script setup lang="ts">
import { onBeforeMount, onMounted, onBeforeUnmount, ref } from 'vue'
import moment from 'moment'
import axios from 'axios'

import { useRuntimeConfig } from '#app'
import { useMainStore, formatDepthLabel } from './stores/main'
import { readShareHash, decodeShareState, applyShareState, applySharedVariable } from '~~/composables/useShareState'
import ShareButton from './components/ShareButton.vue'
const mainStore = useMainStore();
const config = useRuntimeConfig();
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

<style>
/* Inter is self-hosted by @nuxt/fonts (pulled in by @nuxt/ui) — it picks the
   family up from this declaration and the --font-sans theme token, so there is
   no external stylesheet request. The old rule here also enumerated a long list
   of .v-application/.text-* Vuetify selectors that no longer exist. */
html,
body {
  font-family: 'Inter', ui-sans-serif, system-ui, sans-serif;
}

/* Preserve monospace for code and specific monospace classes */
code,
pre,
kbd,
samp,
.text-monospace {
  font-family: monospace;
}
</style>

<style scoped>
.logo-link {
  display: flex;
  align-items: center;
  text-decoration: none;
  cursor: pointer;
}

.logo-icon {
  height: 32px;
  /* width: 40px; */
  margin-right: 16px;
  border-radius: 4px;
}

.logo-icon:hover {
  opacity: 0.8;
  transition: opacity 0.2s;
}
</style>