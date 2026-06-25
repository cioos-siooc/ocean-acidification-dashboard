<template>
  <div style="width:500px;height:480px;background:#0b0c11;border:1px solid #1b1d25;border-radius:16px;box-shadow:0 16px 44px rgba(0,0,0,0.45);display:flex;flex-direction:column;align-items:center;justify-content:center;gap:30px;">
    <canvas ref="canvasRef" width="248" height="248" style="width:248px;height:248px;"></canvas>
    <div style="display:flex;flex-direction:column;align-items:center;gap:11px;">
      <div ref="wordRef" style="opacity:0;font-family:'Space Grotesk',sans-serif;font-size:40px;letter-spacing:-0.8px;line-height:1;">
        <span style="color:#e9ebf2;font-weight:400;">Ocean</span><span style="font-weight:600;background:linear-gradient(90deg,#a72d8a,#e85362,#f98e3a,#fbd24a);-webkit-background-clip:text;background-clip:text;color:transparent;">ECO</span>
      </div>
      <div ref="subRef" style="opacity:0;font-family:'IBM Plex Mono',monospace;font-size:9.5px;letter-spacing:3px;color:#5a6172;text-transform:uppercase;">Ocean Environmental Conditions Monitoring</div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount } from 'vue';

const canvasRef = ref(null);
const wordRef = ref(null);
const subRef = ref(null);
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

onMounted(() => {
  const cv = canvasRef.value;
  if (!cv) return;
  const ctx = cv.getContext('2d');
  const css = 248; // Extracted from your hardcoded sizing
  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  
  cv.width = css * dpr; 
  cv.height = css * dpr;
  const mountTime = performance.now();
  const T = 5400;

  const reveal = (now) => {
    const el = now - mountTime;
    const wp = ease((el - 350) / 950);
    if (wordRef.value) { 
      wordRef.value.style.opacity = wp; 
      wordRef.value.style.transform = `translateY(${(1 - wp) * 12}px)`; 
    }
    if (subRef.value) {
      subRef.value.style.opacity = clamp((el - 950) / 700) * 0.9;
    }
  };

  const drawTile = (now) => {
    const S = css;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, S, S);

    const el = now - mountTime;
    const phase = (el % T) / T;
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
    const plotTop = y0 + side * 0.595;
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

    // ---- timeseries plot ----
    const pX0 = gx0, pW = gw, pBase = y0 + side * 0.9, pPeakTop = y0 + side * 0.64, pH = pBase - pPeakTop;
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
    reveal(now);
    rafId = requestAnimationFrame(_loop);
  };

  rafId = requestAnimationFrame(_loop);
});

onBeforeUnmount(() => {
  if (rafId) {
    cancelAnimationFrame(rafId);
  }
});
</script>