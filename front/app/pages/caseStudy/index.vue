<template>
  <v-main class="cs-index">
    <header class="masthead">
      <div class="wrap">
        <span class="eyebrow">OceanECO &middot; Field Notes</span>
        <h1>Case Studies</h1>
        <p class="dek">Real events, traced entirely inside OceanECO &mdash; one query at a time, against the model's own hindcast and the region's real sensors.</p>
      </div>
    </header>

    <div class="wrap">
      <NuxtLink v-for="c in cases" :key="c.to" :to="c.to" class="case-card-link">
        <v-card class="case-card" elevation="0">
          <span class="tag mono">{{ c.tag }}</span>
          <h2>{{ c.title }}</h2>
          <p>{{ c.dek }}</p>
          <div class="stats mono">
            <span v-for="s in c.stats" :key="s">{{ s }}</span>
          </div>
          <span class="read-link">Read the walkthrough &rarr;</span>
        </v-card>
      </NuxtLink>
    </div>
  </v-main>
</template>

<script setup lang="ts">
useHead({
  title: 'Case Studies — OceanECO',
  meta: [
    { name: 'description', content: 'Real ocean events traced through OceanECO\'s own tools, one case study at a time.' },
  ],
})

const cases = [
  {
    to: '/caseStudy/2021-heat-dome',
    tag: 'JUN–AUG 2021',
    title: 'When the Heat Dome Reached the Salish Sea',
    dek: 'A historic marine heatwave, traced from the surface spike down to a real mooring 223 metres below — and how far it actually reached.',
    stats: ['24.5°C peak', '33-day event', 'R 0.922 model–sensor'],
  },
]
</script>

<style scoped>
@font-face { font-family: 'Fraunces CS'; font-weight: 600; font-style: normal; src: url('/fonts/case-study/fraunces-600.ttf') format('truetype'); font-display: swap; }
@font-face { font-family: 'Fraunces CS'; font-weight: 700; font-style: normal; src: url('/fonts/case-study/fraunces-700.ttf') format('truetype'); font-display: swap; }
@font-face { font-family: 'Public Sans CS'; font-weight: 400; font-style: normal; src: url('/fonts/case-study/pubsans-400.ttf') format('truetype'); font-display: swap; }
@font-face { font-family: 'Public Sans CS'; font-weight: 500; font-style: normal; src: url('/fonts/case-study/pubsans-500.ttf') format('truetype'); font-display: swap; }
@font-face { font-family: 'Public Sans CS'; font-weight: 600; font-style: normal; src: url('/fonts/case-study/pubsans-600.ttf') format('truetype'); font-display: swap; }
@font-face { font-family: 'IBM Plex Mono CS'; font-weight: 500; font-style: normal; src: url('/fonts/case-study/plexmono-500.ttf') format('truetype'); font-display: swap; }

.cs-index{
  --paper: #f1f6f5;
  --paper-raised: #ffffff;
  --ink: #0d2126;
  --ink-soft: #3f5b60;
  --ink-faint: #6d8a8a;
  --deep: #0f3a46;
  --tide: #157983;
  --heat: #c94f1f;
  --mist: #d2ddda;
  --mist-2: #e7eeec;
}
@media (prefers-color-scheme: dark){
  .cs-index{
    --paper: #0b1a1d;
    --paper-raised: #102428;
    --ink: #e9f2f1;
    --ink-soft: #a3bcba;
    --ink-faint: #6f8d8b;
    --deep: #cfeae9;
    --tide: #5cc4c9;
    --heat: #ff8f57;
    --mist: #1f393e;
    --mist-2: #142a2e;
  }
}

.cs-index{
  background: var(--paper);
  color: var(--ink);
  font-family: 'Public Sans CS', -apple-system, 'Segoe UI', sans-serif;
  min-height: 100%;
}

.cs-index h1, .cs-index h2{
  font-family: 'Fraunces CS', Georgia, serif;
  font-weight: 600;
  color: var(--deep);
  text-wrap: balance;
  margin: 0;
}

.mono{ font-family: 'IBM Plex Mono CS', ui-monospace, monospace; font-variant-numeric: tabular-nums; }
.eyebrow{
  font-family: 'IBM Plex Mono CS', monospace;
  font-size: 12.5px;
  letter-spacing: .11em;
  text-transform: uppercase;
  color: var(--tide);
  font-weight: 500;
}

.wrap{ max-width: 760px; margin: 0 auto; padding: 0 24px; }

header.masthead{
  padding: 72px 0 44px;
  border-bottom: 1px solid var(--mist);
  margin-bottom: 40px;
}
header.masthead h1{
  font-size: clamp(32px, 5vw, 46px);
  line-height: 1.1;
  margin: 14px 0 16px;
}
header.masthead .dek{
  font-size: 18px;
  color: var(--ink-soft);
  max-width: 56ch;
  margin: 0;
}

.case-card-link{ text-decoration: none; display: block; margin-bottom: 40px; }
.case-card{
  background: var(--paper-raised) !important;
  border: 1px solid var(--mist) !important;
  border-radius: 6px !important;
  box-shadow: none !important;
  padding: 28px 30px 26px;
  transition: border-color .15s ease, transform .15s ease;
}
.case-card-link:hover .case-card{
  border-color: var(--tide) !important;
  transform: translateY(-1px);
}
.case-card .tag{
  font-size: 12px;
  letter-spacing: .08em;
  color: var(--tide);
  display: block;
  margin-bottom: 10px;
}
.case-card h2{
  font-size: 25px;
  line-height: 1.2;
  margin-bottom: 10px;
}
.case-card p{
  color: var(--ink-soft);
  font-size: 15.5px;
  line-height: 1.55;
  margin: 0 0 16px;
}
.case-card .stats{
  display: flex;
  gap: 16px;
  flex-wrap: wrap;
  font-size: 12.5px;
  color: var(--heat);
  margin-bottom: 16px;
}
.case-card .read-link{
  font-size: 14px;
  color: var(--tide);
  font-weight: 500;
}
</style>
