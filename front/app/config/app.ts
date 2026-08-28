/**
 * App-wide domain constants.
 *
 * These describe fixed properties of *what this app is* — a Salish Sea /
 * BC-coast dashboard — as opposed to per-environment settings, which belong
 * in `runtimeConfig` (see `nuxt.config.ts`: API base URL, Mapbox token,
 * PostHog keys). Rule of thumb: if it changes between dev and prod it is
 * runtime config; if changing it would mean the app covers a different
 * place, it belongs here.
 */

/**
 * The single timezone the UI displays in.
 *
 * Everything in ClickHouse is stored UTC; everything the user reads is local
 * BC time. This constant marks that boundary — conversion happens at the
 * chart-render and API-query edges (see `TimeseriesChart.vue` and
 * `useSunCalc.ts`), never in between. Date *pickers* deliberately do not use
 * it: a calendar day is a Y/M/D triple with no instant attached.
 */
export const APP_TIMEZONE = 'America/Vancouver'

/** Initial map extent — the SalishSeaCast model domain, [[W, S], [E, N]]. */
export const MAP_BOUNDS: [[number, number], [number, number]] = [
  [-126.4, 46.85],
  [-121.3, 51.1],
]

/** Zoom floor/ceiling: below 5 the domain is a speck, above 14 the model grid is coarser than the view. */
export const MAP_MIN_ZOOM = 5
export const MAP_MAX_ZOOM = 14

/** Mapbox style for the basemap. */
export const MAP_STYLE = 'mapbox://styles/taimazb/cmkfvuotu00mw01svbld48v7y?optimize=true&fresh=true'
