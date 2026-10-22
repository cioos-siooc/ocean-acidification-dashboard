<template>
    <div style="display: flex; align-items: flex-end; gap: 8px;">
        <div style="flex: 1; min-width: 0;">
            <div class="bar" :style="barStyle"></div>
            <!-- Rounded ticks placed at their true position on the ramp, rather
                 than the raw min/mid/max of the colour range (0.01 · 12.51 · 25.00). -->
            <div class="ticks oe-num">
                <span v-for="t in ticks" :key="t.value" class="tick" :style="{ left: t.pct + '%' }">{{ t.label }}</span>
            </div>
        </div>
        <div style="flex: 0 0 auto; padding-bottom: 2px;">
            <UPopover v-if="unitOptions.length > 1" arrow :content="{ side: 'top' }">
  <button type="button" class="unit-label unit-label--clickable">
                          {{ unit }}<UIcon name="i-mdi-menu-down" class="size-[10px] ml-1" />
                      </button>
  <template #content>
    <div class="py-1">
                        <div v-for="u in unitOptions" :key="u" role="button" tabindex="0"
                            class="px-3 py-1 text-sm cursor-pointer hover:bg-elevated"
                            :class="u === unit ? 'text-primary font-medium' : ''"
                            @click="mainStore.setUnitPreference(selectedVariable.var, u)">
                            {{ u }}
                        </div>
                    </div>
  </template>
</UPopover>
            <span v-else class="unit-label">{{ unit }}</span>
        </div>
    </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { useMainStore } from '../stores/main';
import { resolveColormap } from '~~/composables/useColormapResolver';
import { useVariableRegistry } from '~~/composables/useVariableRegistry';

const mainStore = useMainStore();
const { displayUnit, toDisplayValue, variableUnit, variableAltUnits } = useVariableRegistry();

const selectedVariable = computed(() => mainStore.selected_variable);

const unit = computed(() => displayUnit(selectedVariable.value.var));

// Canonical unit + whatever alternates variable_config.yml defines (empty
// for the other 6 variables) — clicking the unit label opens this list
// directly on the legend, rather than the toggle living behind Color Settings.
const unitOptions = computed(() => [
    variableUnit(selectedVariable.value.var),
    ...variableAltUnits(selectedVariable.value.var).map(u => u.unit),
]);

const colormapMin = computed(() => selectedVariable.value.colormapMin);
const colormapMax = computed(() => selectedVariable.value.colormapMax);
const displayMin = computed(() => toDisplayValue(selectedVariable.value.var, colormapMin.value));
const displayMax = computed(() => toDisplayValue(selectedVariable.value.var, colormapMax.value));
// "Nice" ticks (1, 2, 2.5 or 5 × 10^n steps) over the colour range, in the
// display unit, positioned by where they fall on the ramp.
const ticks = computed(() => {
    const lo = displayMin.value, hi = displayMax.value;
    if (lo == null || hi == null || !(hi > lo)) return [];
    const raw = (hi - lo) / 5;
    const mag = 10 ** Math.floor(Math.log10(raw));
    const step = [1, 2, 2.5, 5, 10].map(f => f * mag).find(st => st >= raw) ?? raw;
    const decimals = Math.max(0, -Math.floor(Math.log10(step) + 1e-9) + (step / mag === 2.5 ? 1 : 0));
    const out: { value: number; label: string; pct: number }[] = [];
    for (let v = Math.ceil(lo / step) * step; v <= hi + step * 1e-6; v += step) {
        const pct = (v - lo) / (hi - lo) * 100;
        if (pct < 2 || pct > 98) continue;
        out.push({ value: v, label: v.toFixed(decimals), pct });
    }
    return out;
});


const selectedColormap = computed(() => selectedVariable.value.colormap);
const colormaps = computed(() => mainStore.colormaps);

const barStyle = computed(() => {
    const palette = resolveColormap(colormaps.value, selectedColormap.value)?.stops;
    const stops = palette?.map(s => `${s[1]} ${Math.round(s[0] * 100)}%`).join(', ');
    return {
        background: `linear-gradient(90deg, ${stops})`,
    };
});
</script>

<style scoped>
.bar {
    height: 8px;
}

.ticks {
    position: relative;
    height: 14px;
    margin-top: 3px;
}

.tick {
    position: absolute;
    transform: translateX(-50%);
    font-size: 10.5px;
    color: var(--ui-text-muted);
    white-space: nowrap;
}

.unit-label {
    display: inline-flex;
    align-items: center;
    font-size: 11px;
    color: var(--ui-text-muted);
}

.unit-label--clickable {
    background: none;
    border: none;
    padding: 0;
    cursor: pointer;
}

.unit-label--clickable:hover {
    color: var(--ui-text);
}
</style>
