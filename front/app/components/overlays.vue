<template>
  <div
    class="bg-elevated rounded-lg colorbar"
    :class="{ 'colorbar--expanded': isHovering }"
    @mouseenter="isHovering = true"
    @mouseleave="isHovering = false"
  >
    <div class="colorbar-inner">
      <!-- TOGGLE NAVIGATION PANEL -->
      <UButton
        class="m-1 overlay-btn"
        :variant="isControlPanelOpen ? 'solid' : 'ghost'"
        @click="mainStore.toggleIsControlPanelOpen"
      >
        <UIcon
          :name="isControlPanelOpen ? 'i-mdi-menu-open' : 'i-mdi-menu'"
          class="size-[16px]"
        />
        <span class="overlay-btn__label">Toggle Left Panel</span>
      </UButton>

      <USeparator />

      <!-- BATHYMETRY CONTOURS -->
      <UButton
        class="m-1 overlay-btn"
        :variant="showBathymetryContours ? 'solid' : 'ghost'"
        :color="showBathymetryContours ? 'primary' : ''"
        @click="mainStore.setShowBathymetryContours(!showBathymetryContours)"
      >
        <IconsContour />
        <span class="overlay-btn__label">Bathymetry Contours</span>
      </UButton>

      <!-- MAP LABELS (water names + place labels) -->
      <UButton
        class="m-1 overlay-btn"
        :variant="showMapLabels ? 'solid' : 'ghost'"
        :color="showMapLabels ? 'primary' : ''"
        @click="mainStore.setShowMapLabels(!showMapLabels)"
      >
        <UIcon
          :name="showMapLabels ? 'i-mdi-tag-text' : 'i-mdi-tag-text-outline'"
          class="size-[16px]"
        />
        <span class="overlay-btn__label">Map Labels</span>
      </UButton>

      <!-- CURSOR COORDINATES -->
      <UButton
        class="m-1 overlay-btn"
        :variant="showCursorCoords ? 'solid' : 'ghost'"
        :color="showCursorCoords ? 'primary' : ''"
        @click="mainStore.setShowCursorCoords(!showCursorCoords)"
      >
        <UIcon
          :name="
            showCursorCoords ? 'i-mdi-cursor-default' : 'i-mdi-cursor-default-outline'
          "
          class="size-[16px]"
        />
        <span class="overlay-btn__label">Cursor Coordinates</span>
      </UButton>

      <!-- VERTICAL PROFILE -->
      <UButton
        class="m-1 overlay-btn"
        :variant="mainStore.isVerticalProfileOpen ? 'solid' : 'ghost'"
        :color="mainStore.isVerticalProfileOpen ? 'primary' : ''"
        @click="mainStore.toggleIsVerticalProfileOpen"
      >
        <IconsProfile />
        <span class="overlay-btn__label">Vertical Profile</span>
      </UButton>

      <USeparator />

      <!-- AUTO COLOR -->
      <UButton
        variant="solid"
        class="m-1 overlay-btn"
        color=""
        :disabled="
          !selectedVariableName ||
          selectedVariableName === 'bathymetry' ||
          mainStore.autoRangeDisabled
        "
        @click="autorange"
      >
        <IconsAutorange />
        <span class="overlay-btn__label">Auto Color</span>
      </UButton>

      <!-- COLOR SETTINGS -->
      <UButton
        variant="solid"
        color=""
        class="m-1 overlay-btn"
        @click="showColorbarSettings = !showColorbarSettings"
      >
        <UIcon name="i-mdi-palette" class="size-[16px]" />
        <span class="overlay-btn__label">Color Settings</span>
      </UButton>

      <!-- <USeparator /> -->

      <!-- <UButton variant="solid" class="m-1 overlay-btn" disabled @click="showHow">
          <IconsHelp />
          <span class="overlay-btn__label">How to Use</span>
        </UButton> -->
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, toRef, ref, watch } from "vue";

import { useMainStore } from "../stores/main";

const mainStore = useMainStore();

const emit = defineEmits<{
  (e: "show-how"): void;
  (e: "autorange"): void;
}>();

////////////////////////////////////// STATE //////////////////////////////////////

const isHovering = ref(false);

////////////////////////////////////// COMPUTED //////////////////////////////////////

const showColorbarSettings = computed({
  get: () => mainStore.showColorbarSettings,
  set: (val: boolean) => mainStore.setShowColorbarSettings(val),
});

const selectedVariableName = computed(() => mainStore.selected_variable.var);

const showBathymetryContours = computed(() => mainStore.showBathymetryContours);

const showMapLabels = computed(() => mainStore.showMapLabels);

const showCursorCoords = computed(() => mainStore.showCursorCoords);

const isControlPanelOpen = computed(() => mainStore.isControlPanelOpen);

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
.colorbar {
  padding: 3px;
  width: 40px;
  transition: width 0.25s ease;
  border-radius: 6px;
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.2);
  font-family: Inter, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue";
  font-size: 11px;
  overflow: hidden;
}

.colorbar--expanded {
  width: 190px;
}

.colorbar-inner {
  display: flex;
  flex-direction: column;
}

.overlay-btn {
  display: flex;
  align-items: center;
  justify-content: flex-start;
  min-width: 0 !important;
  padding: 0 8px !important;
  overflow: hidden;
}

/* The rail is deliberately narrower than its buttons' natural width (the label
   is clipped until hover expands it). Vuetify's button kept its icon at a fixed
   size regardless; Nuxt UI's is a plain flex row, so without this the icon is
   the thing that gets squeezed — to 0px wide. */
.overlay-btn > :not(.overlay-btn__label) {
  flex-shrink: 0;
}

.overlay-btn__label {
  margin-left: 8px;
  white-space: nowrap;
  opacity: 0;
  max-width: 0;
  overflow: hidden;
  transition: opacity 0.15s ease, max-width 0.2s ease;
}

.colorbar--expanded .overlay-btn__label {
  opacity: 1;
  max-width: 160px;
}
</style>
