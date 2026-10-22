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

/**
 * Chart colours for the v2 "Instrument" theme. Mirrors the CSS tokens in
 * assets/css/main.css (ECharts draws on canvas, so it can't read CSS vars).
 * Model is the accent; sensor data is always blue, so the two sources read the
 * same in every chart, the snapshot and the sensor list.
 */
export const chart = {
    text: '#a7afb7',      // axis labels
    muted: '#7d868f',     // secondary labels, climatology, "Now"
    axis: '#3a4048',      // axis lines
    grid: '#1e2227',      // split lines
    tooltipBg: 'rgba(19,22,26,0.96)',
    tooltipBorder: '#3a4048',
    model: '#e2a336',
    sensor: '#5fb3d6',
    clim: '#7d868f',
    night: 'rgba(255,255,255,0.025)',
    // One per instrument when a sensor's instruments are compared; the first
    // matches the single merged sensor line.
    instruments: ['#5fb3d6', '#9ad08a', '#c9a0dc', '#e07a7a'],
    fontSans: "'IBM Plex Sans', system-ui, sans-serif",
    fontMono: "'IBM Plex Mono', ui-monospace, monospace",
} as const

export default material
