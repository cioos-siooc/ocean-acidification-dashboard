<template>
    <aside v-show="isOpen" class="control-panel absolute left-0 top-0 h-full bg-muted border-r border-default"
        :style="{ width: mainStore.controlPanel_width + 'px', zIndex: 999 }">
        <div class="variables-block border-b border-default">
            <variables />
        </div>

        <div class="sensors-block">
            <sensorInfo />
        </div>
    </aside>
</template>


<script setup lang="ts">
import { computed } from 'vue';
import { useMainStore } from '../stores/main';
const mainStore = useMainStore();

const isOpen = computed(() => mainStore.isControlPanelOpen);
</script>

<style scoped lang="scss">
// Keep the Variables block always in view: the drawer itself doesn't scroll,
// only the Sensors block's own content area does.
// (These rules used to sit on Vuetify's internal .v-navigation-drawer__content.)
.control-panel {
    display: flex;
    flex-direction: column;
    overflow: hidden;
}

.variables-block {
    flex: 0 0 auto;
}

.sensors-block {
    flex: 1 1 auto;
    min-height: 0;
    // sensorInfo owns its own scrolling so its search/filter row can stay pinned.
    overflow: hidden;
}
</style>