<template>
    <div class="layers-control">
        <v-menu v-model:opened="menu" location="left-start" :close-on-content-click="false">
            <template #activator="{ props }">
                <v-btn v-bind="props" icon elevation="2" aria-label="Layers">
                    <v-icon>mdi-layers</v-icon>
                </v-btn>
            </template>

            <div class="pa-2 d-flex flex-column menu-content">
                <v-btn v-for="variable in variables" :key="variable.var" size="small" @click="clickIcon(variable.var)"
                    class="mb-1 noCap" aria-label="Toggle layer 1">
                    {{ var2name(variable.var) }}
                </v-btn>
            </div>
        </v-menu>
    </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useMainStore } from '../stores/main'

import { var2name } from '../../composables/useVar2Name'

///////////////////////////////////  SETUP  ///////////////////////////////////

const mainStore = useMainStore()

const menu = ref(false)
const emit = defineEmits(['toggleLayer'])

///////////////////////////////////  COMPUTED  ///////////////////////////////////
const variables = computed(() => mainStore.variables)

///////////////////////////////////  METHODS  ////////////////////////////////////
function clickIcon(varId: string) {
    emit('toggleLayer', varId)
    menu.value = false
}
</script>


<style scoped>
.layers-control {
    position: absolute;
    top: 8px;
    left: 8px;
    z-index: 1100;
}

.menu-content {
    margin-right: 12px;
    /* keep a small gap so menu doesn't overlap the activator */
}

.noCap {
    text-transform: none;
}
</style>
