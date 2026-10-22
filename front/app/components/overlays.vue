<template>
  <!-- Map controls, grouped: one row per group, related buttons side by side.
       Each button's name is in its tooltip; the row caption says what the
       group is for. -->
  <div class="tb">
    <div class="tb__row">
      <span class="tb__cap">Panel</span>
      <UTooltip text="Show or hide the left panel" :content="{ side: 'right' }">
        <button type="button" class="tb__btn" :class="{ 'tb__btn--on': isControlPanelOpen }"
          aria-label="Left panel" :aria-pressed="isControlPanelOpen" @click="mainStore.toggleIsControlPanelOpen">
          <UIcon name="i-mdi-dock-left" class="size-[15px]" />
        </button>
      </UTooltip>
    </div>

    <div class="tb__row">
      <span class="tb__cap">Layers</span>
      <UTooltip v-for="b in layerButtons" :key="b.label" :text="b.label" :content="{ side: 'bottom' }">
        <button type="button" class="tb__btn" :class="{ 'tb__btn--on': b.on }" :aria-label="b.label" :aria-pressed="b.on" @click="b.toggle">
          <component :is="b.component" v-if="b.component" />
          <UIcon v-else :name="b.icon!" class="size-[15px]" />
        </button>
      </UTooltip>
    </div>

    <div class="tb__row">
      <span class="tb__cap">Readout</span>
      <UTooltip v-for="b in readoutButtons" :key="b.label" :text="b.label" :content="{ side: 'bottom' }">
        <button type="button" class="tb__btn" :class="{ 'tb__btn--on': b.on }" :aria-label="b.label" :aria-pressed="b.on" @click="b.toggle">
          <component :is="b.component" v-if="b.component" />
          <UIcon v-else :name="b.icon!" class="size-[15px]" />
        </button>
      </UTooltip>
    </div>

    <div class="tb__row">
      <span class="tb__cap">Colour</span>
      <UTooltip text="Fit the colour range to the visible data" :content="{ side: 'bottom' }">
        <button type="button" class="tb__btn" aria-label="Auto colour range" :disabled="autoRangeUnavailable" @click="autorange">
          <IconsAutorange />
        </button>
      </UTooltip>
      <UTooltip text="Colour settings" :content="{ side: 'bottom' }">
        <button type="button" class="tb__btn" :class="{ 'tb__btn--on': showColorbarSettings }" aria-label="Colour settings"
          :aria-pressed="showColorbarSettings" @click="showColorbarSettings = !showColorbarSettings">
          <UIcon name="i-mdi-tune-variant" class="size-[15px]" />
        </button>
      </UTooltip>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, resolveComponent, type Component } from "vue";

import { useMainStore } from "../stores/main";

const mainStore = useMainStore();

const emit = defineEmits<{
  (e: "show-how"): void;
  (e: "autorange"): void;
}>();

////////////////////////////////////// COMPUTED //////////////////////////////////////

const showColorbarSettings = computed({
  get: () => mainStore.showColorbarSettings,
  set: (val: boolean) => mainStore.setShowColorbarSettings(val),
});

const selectedVariableName = computed(() => mainStore.selected_variable.var);

const showModelLayer = computed(() => mainStore.showModelLayer);

const showSensorLayer = computed(() => mainStore.showSensorLayer);

const showBathymetryContours = computed(() => mainStore.showBathymetryContours);

const showMapLabels = computed(() => mainStore.showMapLabels);

const showCursorCoords = computed(() => mainStore.showCursorCoords);

const showCursorValue = computed(() => mainStore.showCursorValue);

const isControlPanelOpen = computed(() => mainStore.isControlPanelOpen);

const autoRangeUnavailable = computed(() =>
  !selectedVariableName.value || selectedVariableName.value === "bathymetry" || mainStore.autoRangeDisabled
);

type ToolButton = { label: string; on: boolean; toggle: () => void; icon?: string; component?: Component | string };

// Icons components are auto-imported globally (components/icons/*).
const contourIcon = resolveComponent("IconsContour");
const profileIcon = resolveComponent("IconsProfile");

const layerButtons = computed<ToolButton[]>(() => [
  { label: "Model layer", icon: "i-mdi-layers", on: showModelLayer.value, toggle: () => mainStore.setShowModelLayer(!showModelLayer.value) },
  { label: "Sensors", icon: "i-mdi-map-marker-multiple", on: showSensorLayer.value, toggle: () => mainStore.setShowSensorLayer(!showSensorLayer.value) },
  { label: "Bathymetry contours", component: contourIcon, on: showBathymetryContours.value, toggle: () => mainStore.setShowBathymetryContours(!showBathymetryContours.value) },
  { label: "Map labels", icon: "i-mdi-format-text", on: showMapLabels.value, toggle: () => mainStore.setShowMapLabels(!showMapLabels.value) },
]);

const readoutButtons = computed<ToolButton[]>(() => [
  { label: "Cursor coordinates", icon: "i-mdi-crosshairs-gps", on: showCursorCoords.value, toggle: () => mainStore.setShowCursorCoords(!showCursorCoords.value) },
  { label: "Cursor value", icon: "i-mdi-eyedropper", on: showCursorValue.value, toggle: () => mainStore.setShowCursorValue(!showCursorValue.value) },
  { label: "Vertical profile", component: profileIcon, on: mainStore.isVerticalProfileOpen, toggle: () => mainStore.toggleIsVerticalProfileOpen() },
]);

////////////////////////////////////// METHODS //////////////////////////////////////

function autorange() {
  mainStore.setAutoRangeDisabled(true);
  emit("autorange");
}

const showHow = () => {
  emit("show-how");
};
</script>

<style scoped>
.tb {
  display: grid;
  gap: 4px;
  justify-items: start;
}

.tb__row {
  display: flex;
  align-items: stretch;
  background: rgba(19, 22, 26, 0.94);
  border: 1px solid var(--ui-border);
}

.tb__cap {
  width: 58px;
  padding-left: 7px;
  display: flex;
  align-items: center;
  font-size: 9.5px;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--ui-text-muted);
}

.tb__btn {
  width: 30px;
  height: 28px;
  display: grid;
  place-items: center;
  border-left: 1px solid var(--ui-border);
  color: var(--ui-text-muted);
  cursor: pointer;
}

/* Custom SVG icons (contour, profile, autorange) size themselves. */
.tb__btn :deep(svg) {
  width: 15px;
  height: 15px;
}

.tb__btn:hover:not(:disabled) {
  color: var(--ui-text);
  background: var(--ui-bg-elevated);
}

.tb__btn--on {
  color: var(--ui-primary);
  background: var(--ui-bg-elevated);
  box-shadow: inset 0 -2px 0 var(--ui-primary);
}

.tb__btn:disabled {
  color: var(--ui-text-dimmed);
  cursor: not-allowed;
}

.tb__btn:focus-visible {
  outline: 2px solid var(--ui-primary);
  outline-offset: -2px;
}
</style>
