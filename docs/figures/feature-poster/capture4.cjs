// Final pass: left control panel (unfiltered sensor list), the per-sensor
// metadata card cropped to the card itself, and the variable picker open.
const { chromium } = require('playwright');

const APP = 'http://localhost:9010';
const LOCAL_API = 'http://localhost:9011';
const REMOTE_API = 'https://oa-api2.cioospacificlabs.ca';
const OUT = process.env.SHOT_DIR || '.';
const log = (...a) => console.log(...a);

(async () => {
  const browser = await chromium.launch({ args: ['--no-sandbox', '--use-gl=angle', '--enable-unsafe-swiftshader'] });
  const ctx = await browser.newContext({ viewport: { width: 1680, height: 1180 }, deviceScaleFactor: 3, ignoreHTTPSErrors: true });

  const stats = { ok: 0, fail: 0 };
  await ctx.route(`${LOCAL_API}/**`, async (route) => {
    const req = route.request();
    const target = req.url().replace(LOCAL_API, REMOTE_API);
    try {
      const r = await ctx.request.fetch(target, {
        method: req.method(), headers: { ...req.headers(), host: new URL(REMOTE_API).host },
        data: req.postDataBuffer() || undefined, timeout: 90000,
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

  // ---- I. Variable picker open (shows the 8 model variables incl. pH / omega) ----
  log('I: variable picker');
  await page.getByText('Temperature', { exact: true }).first().click();
  await page.waitForTimeout(2500);
  const menu = page.locator('.v-overlay .v-list').first();
  if (await menu.isVisible().catch(() => false)) {
    await menu.screenshot({ path: `${OUT}/panel-i-variables.png` });
    log('  ✓ panel-i-variables.png');
  } else log('  !! variable menu not visible');
  await page.keyboard.press('Escape');
  await page.waitForTimeout(1500);

  // ---- D3. Control panel with the full (unfiltered) sensor list ----
  log('D3: control panel');
  const sp = page.getByText('Sensors', { exact: true });
  if (await sp.isVisible().catch(() => false)) { await sp.click(); await page.waitForTimeout(3500); }
  await page.waitForSelector('.v-list-item', { timeout: 30000 });
  await page.waitForTimeout(2500);
  await page.screenshot({ path: `${OUT}/panel-d3-controlpanel.png`, clip: { x: 0, y: 0, width: 340, height: 700 } });
  log('  ✓ panel-d3-controlpanel.png');

  // ---- D4. Metadata card, cropped to the card ----
  log('D4: metadata card');
  const row = page.locator('.v-list-item').first();
  const info = row.locator('.mdi-information-variant').first();
  if (await info.isVisible().catch(() => false)) {
    await info.click();
    await page.waitForTimeout(4000);
    const card = page.locator('.v-overlay .v-card').last();
    if (await card.isVisible().catch(() => false)) {
      await card.screenshot({ path: `${OUT}/panel-d4-sensormeta.png` });
      log('  ✓ panel-d4-sensormeta.png');
    } else log('  !! card not visible');
  } else log('  !! no info icon');

  log(`reroute ok=${stats.ok} fail=${stats.fail}`);
  await browser.close();
})().catch((e) => { console.error('ERROR', e.message); process.exit(1); });
