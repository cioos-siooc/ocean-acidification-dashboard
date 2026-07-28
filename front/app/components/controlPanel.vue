<template>
    <v-navigation-drawer v-model="isOpen" location="left" :width="mainStore.controlPanel_width" class="pa-2" absolute
        persistent mobile :scrim="false" style="height:100%; z-index:999; top:0; ">
        <!-- <v-row>
            <v-col cols="1">
                <v-btn icon>
                     <v-icon @click="isOpen = !isOpen">mdi-chevron-left</v-icon>
                </v-btn>
            </v-col>
        </v-row> -->

        <div class="variables-block">
            <variables />
        </div>

        <v-divider class="my-2" />

        <div class="sensors-block">
            <sensorInfo />
        </div>
    </v-navigation-drawer>
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
:deep(.v-navigation-drawer__content) {
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