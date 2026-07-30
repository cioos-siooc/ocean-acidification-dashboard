<template>
  <canvas ref="canvasRef" :width="size" :height="size" :style="{ width: size + 'px', height: size + 'px' }"></canvas>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount, watch } from 'vue';

const props = defineProps({
  size: { type: Number, default: 248 },
  loop: { type: Boolean, default: true }
});

// Phase (fraction of one loop cycle, see T below) at which the reveal is fully
// drawn and at full opacity but hasn't started the loop's fade-out yet — the
// frame frozen on when loop=false, so the animation settles into a complete,
// fully-visible icon instead of stopping mid-reveal or mid-fade.
const FREEZE_PHASE = 0.75;

const canvasRef = ref(null);
let rafId = null;

// Extracted configuration from your class Component
const STOPS = [[0.0,[21,19,107]],[0.18,[74,28,140]],[0.4,[150,40,140]],[0.62,[221,72,104]],[0.82,[247,128,52]],[1.0,[251,210,80]]];
const MASK = [
  [0,1,1,0,0,0,0],
  [0,1,1,1,1,0,0],
  [0,0,1,1,1,1,0],
  [0,0,0,1,1,1,0],
];
const MCOL = 4;
const MROW = 2;

// Extracted helper functions
const clamp = (x, lo = 0, hi = 1) => Math.max(lo, Math.min(hi, x));
const ease = (x) => 1 - Math.pow(1 - clamp(x), 3);
const magma = (t) => {
  t = clamp(t);
  for (let i = 0; i < STOPS.length - 1; i++) {
    if (t <= STOPS[i + 1][0]) {
      const A = STOPS[i], B = STOPS[i + 1], f = (t - A[0]) / ((B[0] - A[0]) || 1);
      return [0, 1, 2].map(j => Math.round(A[1][j] + (B[1][j] - A[1][j]) * f));
    }
  }
  return STOPS[STOPS.length - 1][1];
};
const rgba = (c, a) => `rgba(${c[0]},${c[1]},${c[2]},${a})`;
const field = (c, r) => {
  const d = Math.hypot((c - 3.4) / 3.6, (r - 2.0) / 2.3);
  return clamp(0.95 - d * 0.62 + 0.05 * Math.sin(c * 1.7 + r * 2.0));
};

function startAnimation() {
  const cv = canvasRef.value;
  if (!cv) return;
  if (rafId) { cancelAnimationFrame(rafId); rafId = null; }

  const ctx = cv.getContext('2d');
  const css = props.size;
  const dpr = Math.min(window.devicePixelRatio || 1, 2);

  cv.width = css * dpr;
  cv.height = css * dpr;
  const mountTime = performance.now();
  const T = 5400;

  const drawTile = (now) => {
    const S = css;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, S, S);

    const el = now - mountTime;
    const rawPhase = el / T;
    const phase = props.loop ? rawPhase % 1 : Math.min(rawPhase, FREEZE_PHASE);
    const t = now / 1000;

    // loop envelope
    const A = Math.max(0, Math.min(ease(phase / 0.05), 1 - ease((phase - 0.9) / 0.1)));
    if (A <= 0.001) return;

    const m = S * 0.05, side = S - 2 * m, x0 = m, y0 = m, cr = S * 0.16;
    ctx.save();
    ctx.beginPath(); ctx.roundRect(x0, y0, side, side, cr); ctx.clip();
    ctx.globalAlpha = A; ctx.fillStyle = '#0a0c14'; ctx.fillRect(0, 0, S, S);

    // ---- map field grid ----
    const cols = 7, rows = 4;
    const gx0 = x0 + side * 0.085, gy0 = y0 + side * 0.075, gw = side * 0.83, gh = side * 0.42;
    const cw = gw / cols, ch = gh / rows, pad = cw * 0.16;
    const bloom = clamp(phase / 0.22);
    for (let r = 0; r < rows; r++) for (let c = 0; c < cols; c++) {
      if (!MASK[r][c]) continue;
      const ordN = Math.hypot(c - MCOL, r - MROW) / 4.6;
      const cp = ease((bloom - ordN * 0.55) / 0.45);
      if (cp <= 0) continue;
      const col = magma(field(c, r));
      const shim = 0.82 + 0.18 * Math.sin(t * 1.4 + c * 0.9 + r * 1.1);
      const cx = gx0 + c * cw + cw / 2, cy = gy0 + r * ch + ch / 2;
      const sc = 0.55 + 0.45 * cp;
      ctx.save();
      ctx.globalAlpha = A * cp * shim;
      ctx.translate(cx, cy); ctx.scale(sc, sc);
      ctx.fillStyle = rgba(col, 1);
      ctx.beginPath(); ctx.roundRect(-(cw - pad) / 2, -(ch - pad) / 2, cw - pad, ch - pad, (cw - pad) * 0.26); ctx.fill();
      ctx.restore();
    }

    // ---- marker (clicked point) ----
    const mcx = gx0 + MCOL * cw + cw / 2, mcy = gy0 + MROW * ch + ch / 2;
    const mp = ease((phase - 0.1) / 0.08);
    const plotTop = y0 + side * 0.525;
    if (mp > 0) {
      const pulse = 0.5 + 0.5 * Math.sin(t * 3.0);
      const rb = S * 0.026;
      ctx.globalAlpha = A * mp * (0.55 * (1 - pulse));
      ctx.strokeStyle = 'rgba(251,210,120,1)'; ctx.lineWidth = S * 0.008;
      ctx.beginPath(); ctx.arc(mcx, mcy, rb * (1.4 + 1.5 * pulse), 0, 7); ctx.stroke();
      ctx.globalAlpha = A * mp;
      ctx.strokeStyle = 'rgba(255,236,200,0.95)'; ctx.lineWidth = S * 0.009;
      ctx.beginPath(); ctx.arc(mcx, mcy, rb, 0, 7); ctx.stroke();
      ctx.fillStyle = 'rgba(255,245,225,1)';
      ctx.beginPath(); ctx.arc(mcx, mcy, rb * 0.42, 0, 7); ctx.fill();
    }

    // ---- connector: point -> plot ----
    const conP = ease((phase - 0.22) / 0.08);
    if (conP > 0) {
      ctx.globalAlpha = A * conP * 0.6;
      ctx.strokeStyle = 'rgba(247,170,90,0.85)'; ctx.lineWidth = Math.max(1, S * 0.005);
      ctx.setLineDash([S * 0.018, S * 0.018]);
      const y1 = mcy + S * 0.03;
      ctx.beginPath(); ctx.moveTo(mcx, y1); ctx.lineTo(mcx, y1 + (plotTop - y1) * conP); ctx.stroke();
      ctx.setLineDash([]);
    }
    ctx.restore();

    // ---- timeseries plot (own clip: bounded left/right like the card, but open at the bottom so the dip can spill past the card edge) ----
    ctx.save();
    ctx.beginPath(); ctx.rect(x0, y0, side, side * 2); ctx.clip();
    const pX0 = gx0, pW = gw, pBase = y0 + side * 0.83, pPeakTop = y0 + side * 0.5, pH = pBase - pPeakTop;
    ctx.globalAlpha = A * 0.45; ctx.strokeStyle = 'rgba(255,255,255,0.07)'; ctx.lineWidth = 1;
    [0.0, 0.5, 1.0].forEach(g => { const yy = pPeakTop + pH * g; ctx.beginPath(); ctx.moveTo(pX0, yy); ctx.lineTo(pX0 + pW, yy); ctx.stroke(); });

    const drawP = ease((phase - 0.3) / 0.27);
    const done = clamp((phase - 0.58) / 0.1);
    if (drawP > 0) {
      const N = 110, mid = 0.5, sig = 0.2;
      const pts = [];
      for (let i = 0; i <= N; i++) {
        const u = i / N;
        let yv = Math.exp(-Math.pow(u - mid, 2) / (2 * sig * sig));
        yv += 0.05 * Math.sin(u * 30 + 1.3) * Math.exp(-Math.pow((u - mid) / 0.45, 2));
        const X = pX0 + pW * u;
        let Y = pBase - yv * pH * 0.92;
        Y += Math.sin(u * 7 + t * 1.4) * pH * 0.03 * done * Math.exp(-Math.pow((u - mid) / 0.5, 2));
        Y = pPeakTop + pBase - Y; // flip wrt x-axis: mirror vertically within the plot bounds
        pts.push([X, Y]);
      }
      const head = Math.max(1, Math.round(N * drawP));
      // area fill
      ctx.globalAlpha = A;
      ctx.beginPath(); ctx.moveTo(pts[0][0], pBase);
      for (let i = 0; i <= head; i++) ctx.lineTo(pts[i][0], pts[i][1]);
      ctx.lineTo(pts[head][0], pBase); ctx.closePath();
      const ag = ctx.createLinearGradient(0, pPeakTop, 0, pBase);
      ag.addColorStop(0, 'rgba(247,128,52,0.20)'); ag.addColorStop(1, 'rgba(247,128,52,0)');
      ctx.fillStyle = ag; ctx.fill();
      // stroke
      const grad = ctx.createLinearGradient(pX0, 0, pX0 + pW, 0);
      [0, 0.25, 0.5, 0.75, 1].forEach(s => { const cc = magma(0.16 + s * 0.8); grad.addColorStop(s, rgba(cc, 1)); });
      ctx.strokeStyle = grad; ctx.lineWidth = Math.max(1.6, S * 0.013); ctx.lineJoin = 'round'; ctx.lineCap = 'round';
      ctx.shadowColor = 'rgba(247,128,52,0.5)'; ctx.shadowBlur = S * 0.045;
      ctx.beginPath(); ctx.moveTo(pts[0][0], pts[0][1]);
      for (let i = 1; i <= head; i++) ctx.lineTo(pts[i][0], pts[i][1]);
      ctx.stroke(); ctx.shadowBlur = 0;
      // head dot
      const hp = pts[head], col = magma(0.16 + (head / N) * 0.8);
      const dr = S * 0.016 * (done > 0 ? 1 + 0.28 * Math.sin(t * 3) : 1);
      ctx.globalAlpha = A * 0.3; ctx.fillStyle = rgba(col, 1);
      ctx.beginPath(); ctx.arc(hp[0], hp[1], dr * 2.4, 0, 7); ctx.fill();
      ctx.globalAlpha = A; ctx.fillStyle = rgba(col, 1);
      ctx.beginPath(); ctx.arc(hp[0], hp[1], dr, 0, 7); ctx.fill();
    }

    ctx.restore();
    // tile border
    ctx.save();
    ctx.globalAlpha = A * 0.8; ctx.strokeStyle = 'rgba(255,255,255,0.08)'; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.roundRect(x0, y0, side, side, cr); ctx.stroke();
    ctx.restore();
  };

  const _loop = (now) => {
    drawTile(now);
    // Once the reveal has fully settled, stop requesting frames — the last
    // drawn frame remains on the canvas as a static image.
    if (!props.loop && (now - mountTime) / T >= FREEZE_PHASE) {
      rafId = null;
      return;
    }
    rafId = requestAnimationFrame(_loop);
  };

  rafId = requestAnimationFrame(_loop);
}

onMounted(startAnimation);
watch(() => props.size, startAnimation);
watch(() => props.loop, startAnimation);

onBeforeUnmount(() => {
  if (rafId) {
    cancelAnimationFrame(rafId);
  }
});
</script>
