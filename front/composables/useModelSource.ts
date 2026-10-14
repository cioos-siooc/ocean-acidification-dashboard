// The model sources `/variables` offers (SalishSeaCast, LiveOcean) and what
// each one can do — read from the per-variable entries, which all carry their
// source's metadata (shared/variable_config.yml's `sources:`).
//
// LiveOcean is a 3-day forecast with an archive starting mid-2025, so it can't
// back a climatology or a multi-year trend; `hasCapability()` is the one place
// components ask, instead of comparing source names.

import { computed } from 'vue';
import { useMainStore, type VariableMeta } from '@/stores/main';

export type SourceMeta = {
    source: string;
    label: string;
    forecast: boolean;
    forecastFrom: number | null;
    runDate: string | null;
    stepHours: number;
    capabilities: { climatology?: boolean; trend?: boolean };
    /** First available instant, epoch ms. */
    recordStart: number | null;
};

function toMeta(v: VariableMeta): SourceMeta {
    return {
        source: v.source,
        label: v.sourceLabel ?? v.source,
        forecast: !!v.forecast,
        forecastFrom: v.forecastFrom ?? null,
        runDate: v.runDate ?? null,
        stepHours: v.stepHours ?? 1,
        capabilities: v.capabilities ?? { climatology: true, trend: true },
        recordStart: v.dts?.length ? v.dts[0]! : null,
    };
}

export function useModelSource() {
    const mainStore = useMainStore();

    const sources = computed<SourceMeta[]>(() => {
        const seen = new Map<string, SourceMeta>();
        for (const v of mainStore.variables) {
            if (v?.source && !seen.has(v.source)) seen.set(v.source, toMeta(v));
        }
        return [...seen.values()];
    });

    function sourceMeta(source: string | null | undefined): SourceMeta | null {
        return sources.value.find(s => s.source === source) ?? null;
    }

    const current = computed(() => sourceMeta(mainStore.selected_variable.source));

    function sourceLabel(source?: string | null): string {
        const s = source ?? mainStore.selected_variable.source;
        return sourceMeta(s)?.label ?? s ?? '';
    }

    /** Unknown sources (and SalishSeaCast) default to capable. */
    function hasCapability(capability: 'climatology' | 'trend', source?: string | null): boolean {
        const meta = sourceMeta(source ?? mainStore.selected_variable.source);
        return meta?.capabilities?.[capability] ?? true;
    }

    /** True when `t` (epoch ms) falls in the selected source's forecast span. */
    function isForecast(t: number | null | undefined, source?: string | null): boolean {
        const meta = sourceMeta(source ?? mainStore.selected_variable.source);
        return !!(meta?.forecast && meta.forecastFrom != null && t != null && t >= meta.forecastFrom);
    }

    /**
     * Calendar years the source's daily series spans, for the Analysis
     * workspace's year range. SalishSeaCast's daily table reaches back to 2007
     * (the `/variables` instants only cover its recent hourly window, so its
     * start can't be read from them); other sources start at their first instant.
     */
    function modelYearRange(source?: string | null): [number, number] {
        const meta = sourceMeta(source ?? mainStore.selected_variable.source);
        const thisYear = new Date().getUTCFullYear();
        if (!meta || meta.source === 'SalishSeaCast' || meta.recordStart == null) return [2007, Math.max(2026, thisYear)];
        return [new Date(meta.recordStart).getUTCFullYear(), thisYear];
    }

    return { sources, current, sourceMeta, sourceLabel, hasCapability, isForecast, modelYearRange };
}
