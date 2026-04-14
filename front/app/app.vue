<template>
  <v-app theme="dark">
    <v-app-bar density="compact" class="px-5" app>
      <NuxtLink to="/" class="logo-link">
        <img src="/OA_logo.png" alt="OA Logo" class="logo-icon" />
      </NuxtLink>
      <v-app-bar-title>
        <span class="">OAH</span>
        <span class="text-label-medium mx-2" style="font-family: monospace">v{{ config.public.version }}</span>
      </v-app-bar-title>
      <v-spacer></v-spacer>
      <!-- Optional: Add menu items here -->
      <NuxtLink to="/modeleval" target="_blank">
        <v-btn density="compact" text>Model Evaluation</v-btn>
      </NuxtLink>
      <NuxtLink to="/about" target="_blank">
        <v-btn density="compact" text>About</v-btn>
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
import { useMainStore } from './stores/main'
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
      const depth = (varMeta?.depths && varMeta.depths.length > 0) ? varMeta.depths[0].depth : 0.5;
      const colormap = varMeta?.colormap ?? null;
      const colormapMin = varMeta?.colormapMin ?? null;
      const colormapMax = varMeta?.colormapMax ?? null;
      if (dts.length > 0) {
        mainStore.updateSelectedVariable({
          var: varId,
          source: source,
          dt: moment.utc(dts[dts.length - 1]),
          depth: depth,
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
.logo-link {
  display: flex;
  align-items: center;
  text-decoration: none;
  cursor: pointer;
}

.logo-icon {
  height: 40px;
  width: 40px;
  margin-right: 16px;
  border-radius: 4px;
}

.logo-icon:hover {
  opacity: 0.8;
  transition: opacity 0.2s;
}
</style>