// Render poster.html to a print-ready PDF (A3 landscape) and a 300 DPI PNG.
const { chromium } = require('playwright');
const path = require('path');

const DIR = process.env.SHOT_DIR || __dirname;
const SRC = 'file://' + path.join(DIR, 'poster.html');

// A3 landscape: 420 x 297 mm. CSS px at 96 dpi -> 1587.4 x 1122.5
const CSS_W = 420 / 25.4 * 96;
const CSS_H = 297 / 25.4 * 96;
const DPI = 300;
const SCALE = DPI / 96; // 3.125 -> 4961 x 3508 px

(async () => {
  const browser = await chromium.launch({ args: ['--no-sandbox'] });
  const page = await browser.newPage({
    viewport: { width: Math.round(CSS_W), height: Math.round(CSS_H) },
    deviceScaleFactor: SCALE,
  });
  await page.goto(SRC, { waitUntil: 'load', timeout: 60000 });
  await page.waitForTimeout(2500);

  const fit = await page.evaluate(() => document.body.getAttribute('data-fit'));
  console.log('fit:', fit);

  await page.screenshot({ path: path.join(DIR, 'OceanECO-features.png'), clip: { x: 0, y: 0, width: CSS_W, height: CSS_H } });
  console.log('✓ OceanECO-features.png (300 dpi)');

  await page.pdf({
    path: path.join(DIR, 'OceanECO-features.pdf'),
    width: '420mm', height: '297mm',
    printBackground: true, margin: { top: '0', right: '0', bottom: '0', left: '0' },
    pageRanges: '1',
  });
  console.log('✓ OceanECO-features.pdf (A3 landscape)');

  await browser.close();
})().catch((e) => { console.error('ERROR', e.message); process.exit(1); });
