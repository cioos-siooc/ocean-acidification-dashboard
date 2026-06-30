# Advanced Analysis Mode for the Analytics tab

## Context

The "Analysis Builder" tab (`front/app/components/analytics.vue`) currently shows one
variable's historical stats (overlay/annual-summary chart + threshold table) for a
clicked map point, confined to a fixed 440px footer panel. The user wants an
"Advanced Mode" with 5 additional statistical/extreme-event analyses, opened via a
button on the existing tab as a fullscreen overlay (basic mode stays as-is,
unmodified, behind it). Approved features (sensor-vs-model validation explicitly
excluded, to be designed later):

1. Extreme/compound event detection (marine-heatwave-style methodology)
2. Compound stress (two variables crossing thresholds simultaneously)
3. Trend significance (Mann-Kendall + Theil-Sen)
4. Climatology anomaly view
5. Cross-variable correlation

All five are computable client-side from the existing `POST /analysis/timeseries`
endpoint (`api/modules/ocean_analysis.py`) — it returns raw daily `{time, value}`
data already, single-variable per request, no backend changes needed. Comparing
multiple variables (compound stress, correlation) means firing parallel requests
(the endpoint has no semaphore/rate limit on it).

## Fullscreen mechanism: `v-dialog fullscreen`, not footer resizing

Considered resizing the footer panel itself (`index.vue`'s `footerHeight` ref drives
`calc(100% - footerHeight)` for the map), but that requires hiding/restoring several
absolutely-positioned map overlays (`controlPanel`, `SelectedVariableDrawer`,
`Overlays`, query-mode toggle, `ColormapBar` — all z-index 999–9999 children of
`mapContainer`, which don't get clipped when their parent's height collapses to 0)
and an explicit Mapbox `.resize()` call on every transition. **Decided against this** —
use a Vuetify `v-dialog` with the `fullscreen` prop instead, which teleports to
`<body>` and overlays the entire viewport independent of the page's layout, exactly
like the existing `EchartsLineDialog.vue` pattern (`v-dialog v-model="..."` +
`v-card`, just with `fullscreen` added). This needs **zero changes to `index.vue`**
— map, footer, and overlays are untouched; the dialog simply sits on top.

## File plan

New subdirectory `front/app/components/analysis/` (sibling components allowed per
user — existing `front/app/components/` is otherwise flat, but this feature has
enough new files to warrant grouping):

- `AdvancedAnalysisDialog.vue` — `v-dialog v-model="modelValue" fullscreen` wrapper
  (mirrors `EchartsLineDialog.vue`'s `modelValue`/`update:modelValue` contract) +
  `v-card` with a close button + houses the shared header (point/variable/depth
  read-only display from `useMainStore()`, season picker) + `v-tabs` switching
  between the 5 feature panels below.
- `ExtremeEvents.vue`
- `CompoundStress.vue`
- `Trend.vue`
- `Climatology.vue`
- `Correlation.vue`

New composables (`front/composables/`):
- `useAnalysisFetch.ts` — extracts `analytics.vue`'s `fetchRegionTimeseries` into a
  reusable `fetchAnalysisSeries({ variable, stat, depth, point|polygon, yearRange }, signal)`
  that POSTs to `${apiBaseUrl}/analysis/timeseries`. This also fixes the existing bug
  at `analytics.vue:664` where the URL is hardcoded instead of using the already-computed
  (but currently unused) `apiBaseUrl`. Each call site owns its own `AbortController` —
  do **not** share one controller across concurrent panels (the current single-controller
  pattern in `analytics.vue` assumes only one in-flight request ever exists, which breaks
  once Compound Stress / Correlation fetch multiple series concurrently).
- `useAnalysisStatistics.ts` — pure functions, moved out of `analytics.vue` (which will
  import from here instead of redefining them) plus new ones:
  - Moved: `percentileOf`, `linearRegression`, `groupByYear`, `filterBySeason`,
    `computeBoxplotData`, `yearColor`
  - New: `mannKendallTest(values: number[])`, `theilSenSlope(pts)`,
    `computeClimatologyBaseline(series, windowDays)`, `pearsonCorrelation(xs, ys)`
  - Also move the shared `availableVariables` (id/name pairs) list here so both
    `analytics.vue` and the new picker UIs use one source (note: `mainStore.variables`
    lacks display names, so this hardcoded list — not the store — stays the source of
    truth, matching today's pattern).

**Modified:**
- `front/app/components/analytics.vue` — add an "Advanced Mode" button in the sidebar
  header (next to the existing reset icon) that opens
  `<AdvancedAnalysisDialog v-model="advancedOpen" />`. The existing 3-column basic
  layout is untouched. Replace inline math + fetch with the new composables (also
  fixes the URL bug as part of the same touch).

`index.vue` is **not modified** under this approach.

## AdvancedAnalysisDialog.vue — container design

- Reads point/variable/depth directly from `useMainStore()` (read-only display —
  changing them happens via the map/picker outside this panel, same as basic mode).
- Owns `selectedSeason` (shared across all 5 tabs — lifted up since extremes/trend/
  climatology all need the same season filter; copy the existing season-picker UI
  from `analytics.vue`).
- Owns a `v-tabs` strip switching `activeAdvTab` between the 5 features.
- Fetches the **primary series once**, on dialog open (watch `modelValue`, fetch on
  first becoming `true`, cache thereafter), re-fetched on point/variable/depth
  change (not on season change — season filtering is client-side via `filterBySeason`).
  Passes the resolved array down as a prop to children — simpler typing than providing
  the composable instance.
- A small in-memory cache (`Map<string, Promise<SeriesPoint[]>>` keyed by
  `JSON.stringify({variable,depth,lat,lng,mode})`), shared across `CompoundStress` and
  `Correlation` so picking the same 2nd variable in both tabs doesn't double-fetch.

## Feature designs

### 1. Extreme/compound event detection (`ExtremeEvents.vue`)

Marine-heatwave-style methodology (Hobday et al. 2016), adapted for both tails
(low-extreme for pH/omega/oxygen, high-extreme for temperature):

- `computeClimatologyBaseline(series, windowDays=5)` in the stats composable: maps
  each point to a **leap-year-agnostic day-of-year (1–365)** via a fixed non-leap
  cumulative-days table, **dropping Feb 29 from the pooling** entirely (so every
  year contributes consistently and Feb 29 doesn't get an under-sampled bucket —
  this is the opposite concern from `analytics.vue`'s `OVERLAY_REF_YEAR` trick,
  which solves display positioning, not statistical pooling — call this out as a
  code comment since a future maintainer could conflate the two). For each of the
  365 buckets, pool all values within ±`windowDays` (circular wrap Dec31→Jan1)
  across all years, compute `mean`, `std`, `p10`, `p90`. Memoize per
  `(variable, depth, point, windowDays)` — it's an O(365 × n) pass, cheap once but
  wasteful if re-run per UI tick.
- Detection: flag each day against `p90` (direction `'above'`) or `p10` (`'below'`)
  — direction defaults by variable (`below` for ph_total/omega_arag/omega_cal/
  dissolved_oxygen, `above` for temperature; user-overridable toggle, reuse the
  existing Above/Below `v-btn-toggle` pattern). Group calendar-consecutive flagged
  days into runs, merge runs separated by ≤`maxGapDays` (default 2), keep merged
  events with full calendar duration ≥ `minDurationDays` (default 5). Event duration
  = `(endTime - startTime)/86400000 + 1` (full calendar span, including any
  sub-threshold gap days absorbed by a merge — not just the count of flagged days).
  Feb-29 data points reuse Feb-28's baseline bucket when classifying (documented
  deliberate choice). `windowDays`/`minDurationDays`/`maxGapDays` are user-configurable
  refs, defaulted to Hobday's canonical 5/5/2.
- Per event: `startTime`, `endTime`, `durationDays`, `peakValue`, `peakAnomaly`
  (vs. baseline mean), `meanIntensity`. Per-year summary: event count, total
  event-days, mean/max intensity (an event spanning a year boundary is attributed
  to its start year — simplest, matches Hobday's own tracker convention).
- UI: timeline chart (value line + shaded threshold band/markArea per event,
  reusing the dark ECharts theme) + events table sorted by intensity/duration +
  per-year summary bar chart.

### 2. Compound stress (`CompoundStress.vue`)

- Owns a 2nd-variable `v-select` (from the shared `availableVariables` list) + its
  own threshold value/direction `v-btn-toggle` (copy verbatim from
  `analytics.vue:105-114`). Receives `primarySeries`/`primaryLoading` as props for
  variable #1 (avoids re-fetching the already-loaded primary); fetches variable #2
  itself via `useAnalysisFetch`.
- Inner-join both series by date (`Map<time, value>` per series, intersect keys),
  evaluate both threshold conditions per shared day, count per-year days where
  **both** hold plus longest streak (generalize the existing single-variable streak
  logic already in `analytics.vue`'s `yearlyStats`).
- UI: dual-line chart (both variables, shaded compound-stress regions) + per-year
  table (days / streak), same visual language as the basic tab's stats table.

### 3. Trend significance (`Trend.vue`)

- Read-only consumer of `series`/`season` props — no fetching of its own.
- Computed on **season-filtered annual means** (n ≈ 19, not raw daily ~6935 points
  — keeps Mann-Kendall/Theil-Sen O(n²) trivial and avoids autocorrelation issues of
  testing on daily data).
- `mannKendallTest(values)`: pairwise sign-sum `S`, tie-corrected variance
  `Var(S) = [n(n-1)(2n+5) - Σ_t t(t-1)(2t+5)] / 18`, continuity-corrected
  `Z = (S∓1)/√Var(S)`, two-sided `p = 2(1 - Φ(|Z|))` via the Abramowitz & Stegun
  26.2.17 normal-CDF approximation, `trend` label at α=0.05.
- `theilSenSlope(pts)`: median of all pairwise slopes `(y_j-y_i)/(x_j-x_i)`,
  intercept = median of `(y_i - slope·x_i)`. Use calendar year as `x` (meaningful
  "units/year" label), consistent with the Mann-Kendall direction.
- UI: annual-mean line + Theil-Sen trend line overlay + stat callouts (slope, p,
  significance label, S/Z detail).

### 4. Climatology anomaly (`Climatology.vue`)

- Reuses the same `computeClimatologyBaseline` as Extreme Events (cache shared via
  `AdvancedAnalysisDialog.vue` so it's computed once per variable/point/depth, not
  twice).
- Plots each year's daily values as `value - baseline.mean[doy]` rather than
  absolute value, using the existing year-color gradient (`yearColor`) from the
  overlay chart for visual consistency. Optional ± baseline std band.

### 5. Cross-variable correlation (`Correlation.vue`)

- Owns a multi-select (`v-select multiple`, capped at 4 picks, from
  `availableVariables`), pre-seeded with the primary variable. Fetches each
  *additional* picked variable in parallel via `Promise.allSettled` (mirror
  `TimeseriesChart.vue`'s pattern, lines 146-167) — but surface a visible per-variable
  error chip if one fails (current `TimeseriesChart` pattern only `console.warn`s,
  not sufficient here since a missing variable invalidates a whole matrix row/column).
- Join: build `Map<time, value>` per series (O(n) each), intersect keys starting
  from the smallest map, compute Pearson `r` for every pair from the joined arrays.
  Do this in a memoized function gated on the variable selection changing, not on
  every render.
- UI: N×N heatmap (reuse `HeatmapChart.vue`'s visualMap/heatmap option-builder
  shape) with a **fixed diverging colormap** centered at 0 and `visualMap.min/max`
  pinned to `[-1, 1]` (not `dataMin`/`dataMax` — correlation is bounded and the
  existing sequential per-variable colormap in `HeatmapChart.vue` is semantically
  wrong here). Clicking a cell shows a scatter detail (that pair's joined values,
  colored by year) with a trend line overlay.

## Verification

- `cd front && npm run lint` and `npm run build` (catches type errors across the
  new composables/components).
- Manual run via dev server (`npm run dev`): click a map point, open the Analysis
  Builder tab, click "Advanced Mode" — confirm the fullscreen dialog opens over
  everything and closes cleanly back to the basic tab. Exercise each of the 5 tabs:
  Extreme Events (toggle direction/season, confirm events list is sane), Compound
  Stress (pick a 2nd variable + threshold), Trend (confirm p-value/slope render),
  Climatology (confirm anomaly chart centers near 0), Correlation (pick 3-4
  variables, confirm matrix renders and a cell click shows the scatter detail).
