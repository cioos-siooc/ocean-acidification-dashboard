// Capture the 6 core OceanECO features as high-DPI panels for a print poster.
//
// Runs against the local dev frontend (latest v0.3.1 UI) but reroutes its API
// calls to the remote API, which has real data (local ClickHouse is empty).
// Non-invasive: the running dev container is never modified.
const { chromium } = require('playwright');

const APP = 'http://localhost:9010';
const LOCAL_API = 'http://localhost:9011';
const REMOTE_API = 'https://oa-api2.cioospacificlabs.ca';
const OUT = process.env.SHOT_DIR || '.';
const SENSOR = 'Point Wells'; // ORCA profiler: 0-94.75 m, data through 2026-08-03

const log = (...a) => console.log(...a);

async function shot(target, name, opts = {}) {
  await target.screenshot({ path: `${OUT}/panel-${name}.png`, ...opts });
  log(`  ✓ panel-${name}.png`);
}

(async () => {
  const browser = await chromium.launch({ args: ['--no-sandbox', '--use-gl=angle', '--enable-unsafe-swiftshader'] });
  const ctx = await browser.newContext({
    viewport: { width: 1680, height: 1050 },
    deviceScaleFactor: 2,
    ignoreHTTPSErrors: true,
  });

  const stats = { ok: 0, fail: 0 };
  await ctx.route(`${LOCAL_API}/**`, async (route) => {
    const req = route.request();
    const target = req.url().replace(LOCAL_API, REMOTE_API);
    try {
      const r = await ctx.request.fetch(target, {
        method: req.method(),
        headers: { ...req.headers(), host: new URL(REMOTE_API).host },
        data: req.postDataBuffer() || undefined,
        timeout: 90000,
      });
      stats.ok++;
      await route.fulfill({
        response: r,
        headers: { ...r.headers(), 'access-control-allow-origin': '*', 'access-control-allow-headers': '*', 'access-control-allow-methods': '*' },
      });
    } catch (e) {
      stats.fail++;
      await route.abort();
    }
  });

  const page = await ctx.newPage();
  await page.goto(APP, { waitUntil: 'domcontentloaded', timeout: 120000 });

  // --- Dismiss the first-run beta dialog (it blocks all clicks behind a scrim).
  const dlg = page.getByRole('dialog').filter({ hasText: 'Beta Version Notice' });
  await dlg.waitFor({ state: 'visible', timeout: 30000 }).catch(() => log('  (no beta dialog)'));
  if (await dlg.isVisible().catch(() => false)) {
    await dlg.getByRole('button', { name: 'Close', exact: true }).click({ timeout: 15000 });
    await dlg.waitFor({ state: 'hidden', timeout: 20000 });
    log('  beta dialog dismissed');
  }
  await page.locator('.v-overlay__scrim').waitFor({ state: 'hidden', timeout: 15000 }).catch(() => {});

  const mapEl = page.locator('.mapboxgl-map');
  await mapEl.waitFor({ state: 'visible', timeout: 30000 });
  await page.waitForTimeout(12000); // model raster tiles + sensor symbol layer

  const footer = page.locator('.footer-resizable');
  const EARLY = process.env.SKIP_EARLY !== '1';

  if (EARLY) {
    // ============ A. Hero map: model raster + sensor stations ============
    log('A: map');
    const box = await mapEl.boundingBox();
    await page.mouse.move(box.x + box.width * 0.34, box.y + box.height * 0.45);
    await page.mouse.wheel(0, -260); // tighten onto the Salish Sea model domain
    await page.waitForTimeout(9000);
    await shot(mapEl, 'a-map');

    // ============ B. Timeseries chart (model + climatology + day/night) ============
    log('B: timeseries');
    await page.waitForTimeout(3000);
    await shot(footer, 'b-timeseries');

    // ============ C. Model Analysis builder ============
    log('C: model analysis');
    await page.getByRole('button', { name: /Model Analysis/ }).click();
    await page.waitForTimeout(14000); // fetches full historical series
    await shot(footer, 'c-analysis');
    await page.getByRole('button', { name: /^Timeseries/ }).click();
    await page.waitForTimeout(1500);
  }

  // ============ D. Sensor list + metadata ============
  log('D: sensor list');
  const sensorsPanel = page.getByText('Sensors', { exact: true });
  if (await sensorsPanel.isVisible().catch(() => false)) {
    await sensorsPanel.click();
    await page.waitForTimeout(2500);
  }
  const search = page.getByRole('textbox', { name: 'Search sensors' });
  await search.fill(SENSOR);
  await page.waitForTimeout(2500);
  const row = page.locator('.v-list-item').filter({ hasText: SENSOR }).first();
  await row.waitFor({ state: 'visible', timeout: 20000 });
  await shot(page, 'd-sensorlist', { clip: { x: 0, y: 0, width: 372, height: 1050 } });

  // Per-sensor metadata dialog (info icon on the list row)
  const info = row.locator('button:has(.mdi-information-variant), .mdi-information-variant').first();
  if (await info.isVisible().catch(() => false)) {
    await info.click();
    await page.waitForTimeout(3500);
    const meta = page.getByRole('dialog').last();
    if (await meta.isVisible().catch(() => false)) {
      await shot(meta, 'd2-sensormeta');
      await page.keyboard.press('Escape');
      await page.waitForTimeout(2000);
    }
  } else log('  (no info icon)');

  // Select the profiler sensor -> unlocks Comparison / Sensor Analysis tabs
  await row.click();
  await page.waitForTimeout(10000);

  // ============ E. Model vs Sensor comparison ============
  log('E: comparison');
  const cmpTab = page.getByRole('button', { name: /Comparison/ });
  await cmpTab.waitFor({ state: 'visible', timeout: 25000 });
  await cmpTab.click();
  await page.waitForTimeout(16000);
  await shot(footer, 'e-comparison');

  // ============ F. Depth profile (Hovmoller) in the advanced dialog ============
  log('F: depth profile');
  const fs = page.locator('button:has(.mdi-fullscreen)').first();
  if (await fs.isVisible().catch(() => false)) {
    await fs.click();
    await page.waitForTimeout(4000);
    const depthTab = page.getByRole('tab', { name: /Depth Profile/ });
    if (await depthTab.isVisible({ timeout: 15000 }).catch(() => false)) {
      await depthTab.click();
      await page.waitForTimeout(22000); // profile grid is a heavy custom series
      const adv = page.getByRole('dialog').last();
      await shot(adv, 'f-depthprofile');
    } else {
      log('  !! Depth Profile tab not found');
      await shot(page, 'f-depthprofile-DEBUG');
    }
  } else {
    log('  !! fullscreen button not found');
    await shot(page, 'f-depthprofile-DEBUG');
  }

  log(`reroute ok=${stats.ok} fail=${stats.fail}`);
  await browser.close();
})().catch((e) => { console.error('ERROR', e.message); process.exit(1); });
