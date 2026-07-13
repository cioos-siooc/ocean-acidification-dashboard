// Representative smoke-drive: load the app, dismiss the first-run dialog,
// expand the Sensors panel, screenshot. Adapt for the interaction under test.
//
// Run:
//   source .claude/skills/run/scripts/resolve-playwright.sh
//   node .claude/skills/run/scripts/example-drive.cjs
const { chromium } = require('playwright');

const BASE_URL = process.env.APP_URL || 'http://localhost:9010';
const SHOT_DIR = process.env.SHOT_DIR || '.';

(async () => {
  const browser = await chromium.launch({ args: ['--no-sandbox'] });
  const page = await browser.newPage({ viewport: { width: 1400, height: 900 } });
  const consoleErrors = [];
  page.on('console', (msg) => { if (msg.type() === 'error') consoleErrors.push(msg.text()); });
  page.on('pageerror', (err) => consoleErrors.push('pageerror: ' + err.message));

  await page.goto(BASE_URL, { waitUntil: 'networkidle', timeout: 60000 });

  // First-run "Beta Version Notice" dialog blocks the whole UI behind a
  // scrim. Scope to the dialog — a bare getByRole('button', {name:'Close'})
  // also matches the Nuxt devtools panel's close button and throws a
  // strict-mode violation.
  const betaClose = page.getByRole('dialog').getByRole('button', { name: 'Close', exact: true });
  if (await betaClose.isVisible({ timeout: 5000 }).catch(() => false)) {
    await betaClose.click();
  }

  // The "Sensors" expansion panel in the left control panel is collapsed by
  // default (controlPanel.vue: `panels = ref(['variables'])`).
  await page.getByText('Sensors', { exact: true }).click();
  await page.waitForSelector('.v-list-item', { timeout: 30000 });

  await page.screenshot({ path: `${SHOT_DIR}/drive.png` });
  console.log('console errors:', consoleErrors.length ? consoleErrors : 'none');

  await browser.close();
})().catch((err) => {
  console.error('ERROR', err);
  process.exit(1);
});
