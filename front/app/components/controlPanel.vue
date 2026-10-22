<template>
    <aside v-show="isOpen" class="control-panel absolute left-0 top-0 h-full bg-muted border-r border-default"
        :style="{ width: mainStore.controlPanel_width + 'px', zIndex: 999 }">
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
// The panel is the sensor list alone (model controls live in the footer's
// snapshot); the drawer doesn't scroll, sensorInfo's list does.
.control-panel {
    display: flex;
    flex-direction: column;
    overflow: hidden;
}

.sensors-block {
    flex: 1 1 auto;
    min-height: 0;
    // sensorInfo owns its own scrolling so its search/filter row can stay pinned.
    overflow: hidden;
}
</style>