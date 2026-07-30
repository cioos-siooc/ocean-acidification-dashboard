---
name: run
description: Launch and drive the OceanECO frontend (Nuxt/Vuetify/MapboxGL + FastAPI) for this docker-compose project to visually verify UI changes.
---

# Running this app

This is a docker-compose project (see repo root `CLAUDE.md` for the full
service table). For frontend verification you only need the `front`
service.

## Dev server

The dev stack is usually already running:

```bash
docker ps --format '{{.Names}}\t{{.Ports}}'   # look for *-front-1 on 0.0.0.0:9010
```

If not, start it (must include `--env-file .env.dev` or ports fall back to
3000/4000/5432 instead of the documented 9010/9011/9012 — see
`feedback-docker-compose-env-file` memory):

```bash
docker compose -f docker-compose.dev.yml --env-file .env.dev up
```

Smoke check: `curl -sf http://localhost:9010 >/dev/null`.

## Drive it

**`chromium-cli` is NOT installed in this environment.** Don't spend a turn
checking `which chromium-cli` — it isn't there. Instead this repo has a
working Playwright-based driver already wired up:

```bash
source .claude/skills/run/scripts/resolve-playwright.sh   # resolves NODE_PATH for a cached playwright install
node .claude/skills/run/scripts/example-drive.cjs          # nav, dismiss dialog, expand Sensors panel, screenshot
```

`resolve-playwright.sh` finds the Playwright package cached under
`~/.npm/_npx/<hash>/node_modules/playwright` (left behind by a prior `npx
playwright ...` run) and exports `NODE_PATH` so a plain `node script.js` can
`require('playwright')` without a project-local install. Chromium/Firefox
binaries are already cached at `~/.cache/ms-playwright`. If that cache is
ever missing, the script falls back to `npx --yes playwright install
chromium` (needs network).

Write one-off driver scripts as plain Node + `require('playwright')` (see
`scripts/example-drive.cjs` as a template) — `chromium()`, `newPage()`,
`goto()`, `locator(...).click()`, `screenshot()`. No REPL/CLI tool needed.

### Gotchas

- **First-run dialog blocks everything.** `BetaDisclaimerDialog.vue` opens a
  modal (`v-overlay__scrim`) on first load that intercepts all clicks until
  dismissed. Scope the close button to the dialog:
  `page.getByRole('dialog').getByRole('button', { name: 'Close', exact: true })`.
  A bare `getByRole('button', { name: 'Close' })` also matches the Nuxt
  devtools panel's close button and throws a Playwright strict-mode
  violation (2 matches).
- **Sensors panel starts collapsed.** `controlPanel.vue` initializes
  `panels = ref(['variables'])` — click the "Sensors"
  `v-expansion-panel-title` text to expand it before looking for
  `.v-list-item` sensor rows.
- **Sensor markers are canvas-rendered, not DOM.** They're a MapboxGL GL
  symbol layer queried via `map.queryRenderedFeatures` on click
  (`useStationsInteraction.ts`), not `mapboxgl.Marker` DOM elements — you
  can't `page.click()` a CSS selector for a map pin. To exercise the
  map-click → `mainStore.selectedSensor` path without replicating Mapbox's
  Web Mercator pixel projection, it's usually sufficient to trigger the same
  store mutation via the sensor list instead (`sensorInfo.vue`'s
  `selectSensor()` and the map's `clickSensor()` in `index.vue` both call
  `mainStore.selectSensor(id, depth)` — same reactive state, same
  downstream effects).
- **Testing scroll-into-view / off-screen behavior:** Playwright's
  `.click()` auto-scrolls the target into view *before* clicking, which
  masks whether the app's own `scrollIntoView` logic is what did the work.
  To isolate app-driven scrolling, dispatch the click without Playwright's
  actionability scroll:
  `await locator.evaluate(el => el.dispatchEvent(new MouseEvent('click', {bubbles: true, cancelable: true, view: window})))`.
  The sensors list's scroll container is Vuetify's own
  `.v-navigation-drawer__content` (built-in `overflow-y:auto`), not a
  custom class.
- Ignore `ERR_CERT_DATE_INVALID` / sensor-timeseries 500s in `console
  --errors` output during local dev — the frontend's `NUXT_PUBLIC_API_BASE_URL`
  points at a remote staging API (`oa-api2.cioospacificlabs.ca`) by default
  in `.env.dev`, whose cert/backend issues are unrelated to local frontend
  changes.
