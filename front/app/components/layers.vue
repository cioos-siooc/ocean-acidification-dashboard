<template>
    <div class="layers-control">
        <UPopover v-model:open="menu" arrow :content="{ side: 'left-start' }">
  <UButton variant="ghost" class="shrink-0" aria-label="Layers">
                      <UIcon name="i-mdi-layers" />
                  </UButton>
  <template #content>
    <div class="p-2 flex flex-col menu-content">
                    <UButton size="sm" class="mb-1 noCap" v-for="variable in variables" :key="variable.var" @click="clickIcon(variable.var)" aria-label="Toggle layer 1">
                        {{ variableLabel(variable.var) }}
                    </UButton>
                </div>
  </template>
</UPopover>
    </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useMainStore } from '../stores/main'

import { useVariableRegistry } from '~~/composables/useVariableRegistry'

///////////////////////////////////  SETUP  ///////////////////////////////////

const mainStore = useMainStore()
const { variableLabel } = useVariableRegistry()

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
