<template>
    <aside v-show="isOpen" class="control-panel p-2 absolute left-0 top-0 h-full bg-default border-r border-default"
        :style="{ width: mainStore.controlPanel_width + 'px', zIndex: 999 }">
        <!-- <div class="flex flex-wrap -m-3">
            <div class="p-3 w-1/12">
                <UButton variant="ghost" class="shrink-0">
                     <UIcon name="i-mdi-chevron-left" />
                </UButton>
            </div>
        </div> -->

        <div class="variables-block">
            <variables />
        </div>

        <USeparator class="my-2" />

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
    overflow-y: auto;
}
</style>