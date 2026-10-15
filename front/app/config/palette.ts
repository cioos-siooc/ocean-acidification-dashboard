/**
 * The handful of Material palette values this app pulled from
 * `vuetify/util/colors`, inlined so the dependency could be dropped.
 *
 * These are chart/marker colours (ECharts series, map icons) rather than UI
 * chrome — the chrome now comes from Nuxt UI's theme tokens (see
 * `app/app.config.ts`). Values are Vuetify's own, copied verbatim, so nothing
 * shifted in the migration.
 */
export const material = {
    red:    { lighten3: '#ef9a9a', lighten4: '#ffcdd2' },
    green:  { lighten2: '#81c784', lighten3: '#a5d6a7', lighten4: '#c8e6c9' },
    orange: { lighten2: '#ffb74d' },
    yellow: { base: '#ffeb3b', accent2: '#ffff00' },
    blue:   { darken2: '#1976d2' },
    amber:  { lighten2: '#ffd54f' },
    cyan:   { lighten2: '#4dd0e1' },
    purple: { lighten3: '#ce93d8' },
} as const

export default material
