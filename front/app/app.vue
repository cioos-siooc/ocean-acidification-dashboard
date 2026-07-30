<template>
  <v-app theme="dark">
    <v-app-bar class="px-5" app>
      <div class="d-flex align-center" style="flex-shrink:0;">
        <iconsOceanECOLogo :size="40" :loop="false" style="display:block;" />
      </div>

      <v-app-bar-title>
        <span class="">OceanECO</span>
        <span class="text-label-medium mx-2" style="font-family: monospace">v{{ config.public.version }}</span>
        <v-tooltip location="bottom" max-width="280">
          <template #activator="{ props }">
            <v-chip v-bind="props" size="small" color="warning" variant="flat" label>BETA</v-chip>
          </template>
          <span>This dashboard is in active development. Data, features, and layout may change without
            notice.</span>
        </v-tooltip>
      </v-app-bar-title>

      <a href="https://cioospacific.ca/" target="_blank" rel="noopener noreferrer" class="logo-link"
        style="position: absolute; left: 50%; transform: translateX(-50%);">
        <img src="/cioos_pacific.png" alt="OA Logo" class="logo-icon" />
      </a>

      <!-- Optional: Add menu items here -->
      <!-- FEEDBACK FORM -->
      <v-btn text href="https://docs.google.com/forms/d/e/1FAIpQLSdGiIclM5wvIbPReZydsXKiRBXbZsQVEdoQPlA0EruKIoNJkg/viewform?usp=dialog" target="_blank" rel="noopener noreferrer">
        Feedback
      </v-btn>

      <NuxtLink to="/about" target="_blank">
        <v-btn text>About</v-btn>
      </NuxtLink>

    </v-app-bar>

    <NuxtRouteAnnouncer />
    <NuxtPage />
  </v-app>
</template>

<script setup lang="ts">
import { onBeforeMount, ref } from 'vue'
import moment from 'moment'
import axios from 'axios'

import { useRuntimeConfig } from '#app'
import { useMainStore, formatDepthLabel } from './stores/main'
const mainStore = useMainStore();
const config = useRuntimeConfig();
const apiBaseUrl = config.public.apiBaseUrl;

onBeforeMount(() => {
  getVariables();
  getColormaps();
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
/* Global font application for Vuetify and general elements */
html,
body,
.v-application,
.v-application .text-body-1,
.v-application .text-body-2,
.v-application .text-h1,
.v-application .text-h2,
.v-application .text-h3,
.v-application .text-h4,
.v-application .text-h5,
.v-application .text-h6,
.v-application .text-subtitle-1,
.v-application .text-subtitle-2,
.v-application .text-button,
.v-application .text-caption,
.v-application .text-overline {
  font-family: 'Inter', sans-serif !important;
}

/* Preserve monospace for code and specific monospace classes */
code,
pre,
kbd,
samp,
.text-monospace {
  font-family: monospace !important;
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