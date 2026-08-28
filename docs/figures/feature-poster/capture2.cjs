// Second pass: better sensor choices for the comparison + depth-profile panels.
//   Folger Pinnacle (25 m, 15.3 y overlap) -> dense model/sensor comparison + scatter
//   JF2C Mooring    (11-260 m profiler)    -> depth profile that fills the depth axis
const { chromium } = require('playwright');

const APP = 'http://localhost:9010';
const LOCAL_API = 'http://localhost:9011';
const REMOTE_API = 'https://oa-api2.cioospacificlabs.ca';
const OUT = process.env.SHOT_DIR || '.';

const log = (...a) => console.log(...a);

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
      await route.fulfill({ response: r, headers: { ...r.headers(), 'access-control-allow-origin': '*', 'access-control-allow-headers': '*', 'access-control-allow-methods': '*' } });
    } catch { stats.fail++; await route.abort(); }
  });

  const page = await ctx.newPage();
  await page.goto(APP, { waitUntil: 'domcontentloaded', timeout: 120000 });

  const dlg = page.getByRole('dialog').filter({ hasText: 'Beta Version Notice' });
  await dlg.waitFor({ state: 'visible', timeout: 30000 }).catch(() => {});
  if (await dlg.isVisible().catch(() => false)) {
    await dlg.getByRole('button', { name: 'Close', exact: true }).click({ timeout: 15000 });
    await dlg.waitFor({ state: 'hidden', timeout: 20000 });
  }
  await page.locator('.v-overlay__scrim').waitFor({ state: 'hidden', timeout: 15000 }).catch(() => {});
  await page.locator('.mapboxgl-map').waitFor({ state: 'visible', timeout: 30000 });
  await page.waitForTimeout(9000);

  const footer = page.locator('.footer-resizable');

  // Expand the Sensors expansion panel (collapsed by default)
  const sp = page.getByText('Sensors', { exact: true });
  if (await sp.isVisible().catch(() => false)) { await sp.click(); await page.waitForTimeout(2500); }
  const search = page.getByRole('textbox', { name: 'Search sensors' });

  async function pickSensor(name) {
    await search.fill('');
    await page.waitForTimeout(800);
    await search.fill(name);
    await page.waitForTimeout(2800);
    const row = page.locator('.v-list-item').filter({ hasText: name }).first();
    await row.waitFor({ state: 'visible', timeout: 25000 });
    await row.click();
    await page.waitForTimeout(11000);
    log(`  selected: ${name}`);
  }

  async function openComparison() {
    const t = page.getByRole('button', { name: /Comparison/ });
    await t.waitFor({ state: 'visible', timeout: 25000 });
    await t.click();
    await page.waitForTimeout(17000);
  }

  async function openAdvanced() {
    const fs = page.locator('button:has(.mdi-fullscreen)').first();
    await fs.waitFor({ state: 'visible', timeout: 20000 });
    await fs.click();
    await page.waitForTimeout(5000);
  }

  async function grabTab(tabName, file, waitMs) {
    const t = page.getByRole('tab', { name: tabName });
    if (!(await t.isVisible({ timeout: 12000 }).catch(() => false))) { log(`  !! tab missing: ${tabName}`); return false; }
    await t.click();
    await page.waitForTimeout(waitMs);
    await page.getByRole('dialog').last().screenshot({ path: `${OUT}/panel-${file}.png` });
    log(`  ✓ panel-${file}.png`);
    return true;
  }

  // ---- Folger Pinnacle: long dense overlap ----
  log('E2: Folger Pinnacle comparison');
  await pickSensor('Folger Pinnacle');
  await openComparison();
  await footer.screenshot({ path: `${OUT}/panel-e2-comparison.png` });
  log('  ✓ panel-e2-comparison.png');

  await openAdvanced();
  await grabTab(/Scatter/, 'g-scatter', 16000);
  await grabTab(/Seasonal Cycle/, 'h-seasonal', 14000);
  await page.keyboard.press('Escape');
  await page.waitForTimeout(3000);

  // ---- JF2C Mooring: deep profiler ----
  log('F2: JF2C depth profile');
  await pickSensor('JF2C Mooring');
  await openComparison();
  await openAdvanced();
  await grabTab(/Depth Profile/, 'f2-depthprofile', 24000);

  log(`reroute ok=${stats.ok} fail=${stats.fail}`);
  await browser.close();
})().catch((e) => { console.error('ERROR', e.message); process.exit(1); });
