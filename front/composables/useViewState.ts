import { computed, type WritableComputedRef } from 'vue'
import { useMainStore } from '@/stores/main'

/**
 * Store-backed replacement for a component's local `ref` when the value is a
 * *setting the user chose* rather than derived or transient data.
 *
 * The analysis and comparison tabs each carry a handful of these — a threshold,
 * a baseline window, a chosen secondary variable, a chart's zoom extent — and a
 * shared link that restored only the open tab dropped every one of them, so the
 * recipient landed on the right tab showing the default view. Rather than
 * promote each into its own store field (dozens of fields nothing else reads),
 * they go into one namespaced bag on the store that share capture takes
 * wholesale.
 *
 * ```ts
 * const field = useViewState('analysis.extremes')
 * const windowDays = field('windowDays', 5)   // use exactly like ref(5)
 * ```
 *
 * The returned ref is writable, so `v-model` works unchanged. Values must stay
 * JSON-serialisable — they travel inside the share link.
 */
export function useViewState(scope: string) {
    const store = useMainStore()

    return function field<T>(name: string, initial: T): WritableComputedRef<T> {
        return computed<T>({
            get() {
                const v = store.viewState[scope]?.[name]
                return (v === undefined ? initial : v) as T
            },
            set(value) {
                store.setViewStateField(scope, name, value)
            },
        })
    }
}

/**
 * Reads a plain (non-reactive) value out of the same bag — for the imperative
 * corners that are not part of the render, e.g. an ECharts zoom extent applied
 * inside `setOption` and written back from a `datazoom` handler.
 */
export function readViewState<T>(scope: string, name: string, fallback: T): T {
    const store = useMainStore()
    const v = store.viewState[scope]?.[name]
    return (v === undefined ? fallback : v) as T
}

export function writeViewState(scope: string, name: string, value: unknown): void {
    useMainStore().setViewStateField(scope, name, value)
}

/**
 * Wires an ECharts instance's zoom extent into the bag: applies whatever is
 * stored (so a restored view opens zoomed the way the sender left it) and
 * records every subsequent user zoom/pan.
 *
 * Returns the extent to spread into the option's `dataZoom` entries — ECharts
 * resets `start`/`end` on a `setOption(..., true)` replace, so the value has to
 * be re-supplied on every render, not just on init.
 */
export interface ZoomExtent { start: number, end: number }

export function useChartZoom(scope: string, name = 'zoom') {
    function current(): ZoomExtent {
        return readViewState<ZoomExtent>(scope, name, { start: 0, end: 100 })
    }
    /** Call once, right after `echarts.init`. */
    function track(chart: { on: (e: string, cb: () => void) => void, getOption: () => any }): void {
        chart.on('datazoom', () => {
            const dz = (chart.getOption().dataZoom as any[]) || []
            const z = dz[0]
            if (z && typeof z.start === 'number' && typeof z.end === 'number') {
                writeViewState(scope, name, { start: z.start, end: z.end })
            }
        })
    }
    return { current, track }
}
