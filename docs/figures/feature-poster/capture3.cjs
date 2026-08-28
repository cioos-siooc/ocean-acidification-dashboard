// Third pass: comparison + scatter for in-domain fixed-depth sensors
// (verified via /extractTimeseries that SalishSeaCast covers their coords).
const { chromium } = require('playwright');

const APP = 'http://localhost:9010';
const LOCAL_API = 'http://localhost:9011';
const REMOTE_API = 'https://oa-api2.cioospacificlabs.ca';
const OUT = process.env.SHOT_DIR || '.';
const TARGETS = [
  { q: 'Campbell River Underwater', slug: 'campbell' },
  { q: '5mbss', slug: '5mbss' },
];

const log = (...a) => console.log(...a);

(async () => {
  const browser = await chromium.launch({ args: ['--no-sandbox', '--use-gl=angle', '--enable-unsafe-swiftshader'] });
  const ctx = await browser.newContext({ viewport: { width: 1680, height: 1050 }, deviceScaleFactor: 2, ignoreHTTPSErrors: true });

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

  const footer = page.locator('.footer-resizable');
  const sp = page.getByText('Sensors', { exact: true });
  if (await sp.isVisible().catch(() => false)) { await sp.click(); await page.waitForTimeout(2500); }
  const search = page.getByRole('textbox', { name: 'Search sensors' });

  for (const t of TARGETS) {
    log(`--- ${t.q}`);
    await search.fill(''); await page.waitForTimeout(800);
    await search.fill(t.q); await page.waitForTimeout(2800);
    const row = page.locator('.v-list-item').filter({ hasText: new RegExp(t.q, 'i') }).first();
    if (!(await row.isVisible({ timeout: 20000 }).catch(() => false))) { log('  !! not in list'); continue; }
    await row.click();
    await page.waitForTimeout(11000);

    const cmp = page.getByRole('button', { name: /Comparison/ });
    if (!(await cmp.isVisible({ timeout: 25000 }).catch(() => false))) { log('  !! no Comparison tab'); continue; }
    await cmp.click();
    await page.waitForTimeout(18000);
    await footer.screenshot({ path: `${OUT}/cmp-${t.slug}.png` });
    log(`  ✓ cmp-${t.slug}.png`);

    const fs = page.locator('button:has(.mdi-fullscreen)').first();
    if (await fs.isVisible().catch(() => false)) {
      await fs.click();
      await page.waitForTimeout(5000);
      const st = page.getByRole('tab', { name: /Scatter/ });
      if (await st.isVisible({ timeout: 12000 }).catch(() => false)) {
        await st.click();
        await page.waitForTimeout(16000);
        await page.getByRole('dialog').last().screenshot({ path: `${OUT}/scatter-${t.slug}.png` });
        log(`  ✓ scatter-${t.slug}.png`);
      }
      await page.keyboard.press('Escape');
      await page.waitForTimeout(3000);
    }
  }

  log(`reroute ok=${stats.ok} fail=${stats.fail}`);
  await browser.close();
})().catch((e) => { console.error('ERROR', e.message); process.exit(1); });
