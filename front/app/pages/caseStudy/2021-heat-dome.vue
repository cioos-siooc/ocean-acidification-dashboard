<template>
  <v-main class="case-study">
    <div class="wrap col back-row">
      <NuxtLink to="/" class="back-link">&larr; Back to the map</NuxtLink>
    </div>

    <header class="masthead">
      <div class="wrap col">
        <span class="eyebrow">OceanECO &middot; Field Notes</span>
        <h1>When the Heat Dome Reached the Salish&nbsp;Sea</h1>
        <p class="dek">In late June 2021, a historic heat dome pushed Pacific Northwest air temperatures past 40&nbsp;&deg;C. This is what it did to the water underneath &mdash; traced entirely inside OceanECO, point by point, from the hindcast model and a real mooring's own sensor record.</p>
        <div class="meta mono">
          <span>49.2000&deg;N, &minus;123.5000&deg;W</span>
          <span class="dot">&middot;</span>
          <span>Central Strait of Georgia</span>
          <span class="dot">&middot;</span>
          <span>Jun&ndash;Aug 2021</span>
        </div>

        <div class="stat-strip">
          <v-card v-for="s in stats" :key="s.label" class="stat" elevation="0">
            <span class="num">{{ s.num }}</span>
            <span class="label">{{ s.label }}</span>
          </v-card>
        </div>
      </div>
    </header>

    <div class="wrap">

      <section>
        <div class="col">
          <v-chip class="tag" variant="outlined" size="small" label>01 &mdash; Explore &middot; Timeseries</v-chip>
          <h2>The spike at the surface</h2>
          <p class="lede">Two weeks tell the whole story. Pick a point in the central Strait of Georgia, set the depth to the surface, and watch the daily model line climb through the top of its own climatology.</p>
          <p>OceanECO always plots a variable against its own history &mdash; a shaded band running from the historical minimum to maximum for that day of year, with the long&#8209;term mean dashed through the middle. On June 12, 2021, surface temperature at this point was an unremarkable 14.5&nbsp;&deg;C. Seventeen days later it was <span class="stat-inline">20.9&nbsp;&deg;C</span>. A week after that, <span class="stat-inline">22.1&nbsp;&deg;C</span>. The model line doesn't just rise &mdash; for most of July it sits above the shaded &ldquo;Max&rdquo; band entirely, which is the chart's plain-language way of saying: this hadn't happened before in the record it's comparing against.</p>
        </div>
        <figure class="shot">
          <v-card class="frame" elevation="0">
            <img src="/case-studies/2021-heat-dome/timeseries.webp" alt="OceanECO Timeseries pane showing daily surface temperature at 49.2N -123.5W for the year ending June 2021, with the model line rising sharply above the shaded historical max band in late June and peaking above 22 degrees C.">
          </v-card>
          <figcaption><span class="fig-no mono">FIG&nbsp;1</span><span class="fig-text">Explore &rarr; Timeseries, Temperature at 0.5&nbsp;m, daily bins, window centred on Jun&nbsp;29,&nbsp;2021. The orange &ldquo;MAP&rdquo; marker sits at the day the raster layer is painting; the model line pushes well past the blue &ldquo;Max&rdquo; band through most of July.</span></figcaption>
        </figure>
      </section>

      <section>
        <div class="col">
          <v-chip class="tag" variant="outlined" size="small" label>02 &mdash; Analysis &middot; Extreme Events</v-chip>
          <h2>Thirty-three days above threshold</h2>
          <p class="lede">The same point's full record, run through a marine-heatwave detector, without telling it where to look.</p>
          <p>OceanECO's Extreme Events tool flags any run of days crossing the 90th percentile of a day-of-year climatological baseline &mdash; the standard marine&#8209;heatwave definition &mdash; then merges nearby runs and discards anything shorter than five days. Pointed at this location's full 2007&ndash;2026 daily record with no hints about 2021, it independently surfaces a run from <span class="stat-inline">July&nbsp;5 to August&nbsp;6, 2021</span>: 33 days, peaking at 24.5&nbsp;&deg;C, 4.5&nbsp;&deg;C above baseline. The per&#8209;year tally on the right puts 2021 at 4 events and 48 heatwave-days total &mdash; a lot, but not even the single worst year at this point: 2022 edges it out with a higher peak anomaly (+4.89&nbsp;&deg;C), a reminder that the tool is reporting what the record actually contains rather than confirming a story picked in advance.</p>
        </div>
        <figure class="shot">
          <v-card class="frame" elevation="0">
            <img src="/case-studies/2021-heat-dome/extreme-events.png" alt="OceanECO Analysis Extreme Events tab showing nineteen years of daily temperature with detected heatwave events highlighted as vertical bands, and an events table listing a 33-day event from July 5 to August 6 2021 peaking at 24.5 degrees C.">
          </v-card>
          <figcaption><span class="fig-no mono">FIG&nbsp;2</span><span class="fig-text">Analysis (Model) &rarr; Extreme Events, 90th&#8209;percentile threshold, 5&#8209;day minimum duration, 2&#8209;day merge gap &mdash; the built-in marine-heatwave methodology. Third row of the events table is the 2021 heat dome.</span></figcaption>
        </figure>
      </section>

      <section>
        <div class="col">
          <v-chip class="tag" variant="outlined" size="small" label>03 &mdash; Explore &middot; Model depth</v-chip>
          <h2>How deep did it go</h2>
          <p class="lede">The Hovm&ouml;ller view answers a question the surface chart can't: did the rest of the water column feel this at all?</p>
          <p>Switching to Model depth trades the single-depth line for the whole water column at this point across a year &mdash; depth down, time across, colour for the variable. The temperature panel makes the answer obvious at a glance: the bright band is a thin skin, confined to roughly the top 20&nbsp;metres, sitting on top of water that stays cold and essentially unmoved year&#8209;round. Click June 29 and the companion depth profile on the right draws the same story as a single curve: about 21&nbsp;&deg;C at the surface, collapsing to 9&nbsp;&deg;C by 100&nbsp;metres &mdash; strong, heatwave&#8209;driven stratification, not a water&#8209;column&#8209;wide warming event.</p>
          <p>Swap the field to Omega Aragonite and the picture changes character rather than just colour. The saturation state is visibly patchier and noisier than temperature &mdash; carbonate chemistry doesn't track heat in lockstep &mdash; and below about 20&nbsp;metres the water sits undersaturated (&Omega;&nbsp;&lt;&nbsp;1, corrosive to aragonite shells) for most of the year regardless of what the surface is doing. The heat dome was a surface event; the Salish Sea's deeper acidification problem is a standing one.</p>
        </div>
        <div class="shot-pair">
          <figure class="shot">
            <v-card class="frame" elevation="0">
              <img src="/case-studies/2021-heat-dome/depth-temperature.webp" alt="OceanECO Model depth Hovmoller heatmap of temperature across the full water column for one year, showing a bright warm band confined to the top 20 metres around late June 2021, with a stratified vertical profile shown alongside.">
            </v-card>
            <figcaption><span class="fig-no mono">FIG&nbsp;3</span><span class="fig-text">Temperature, full column, daily bins. Warm band confined near-surface; profile drawer shows the Jun&nbsp;29 cast.</span></figcaption>
          </figure>
          <figure class="shot">
            <v-card class="frame" elevation="0">
              <img src="/case-studies/2021-heat-dome/depth-omega.webp" alt="OceanECO Model depth Hovmoller heatmap of omega aragonite across the full water column for one year, showing a noisier, patchier signal than temperature and chronic undersaturation below about 20 metres.">
            </v-card>
            <figcaption><span class="fig-no mono">FIG&nbsp;4</span><span class="fig-text">Omega Aragonite, same point and window. Noisier signal; chronic undersaturation below ~20&nbsp;m year&#8209;round.</span></figcaption>
          </figure>
        </div>
      </section>

      <section>
        <div class="col">
          <v-chip class="tag" variant="outlined" size="small" label>04 &mdash; Analysis &middot; Compound Stress</v-chip>
          <h2>Warm and corrosive, at once</h2>
          <p class="lede">A single threshold rarely tells the whole ecological story. Compound Stress asks for two at a time.</p>
          <p>Set a joint condition &mdash; Temperature above 19&nbsp;&deg;C <em>and</em> Omega Aragonite below 1 on the same day &mdash; and OceanECO counts, per year, how often both held at once. In 2021 that's a comparatively modest <span class="stat-inline">4&nbsp;days</span>, in a longest streak of 2. 2022 is the standout year on this particular pairing, with 18 days and a 6&#8209;day streak &mdash; a useful check against assuming the year with the biggest heat anomaly must also be the year of worst compound stress. It usually isn't the same year twice.</p>
        </div>
        <figure class="shot">
          <v-card class="frame" elevation="0">
            <img src="/case-studies/2021-heat-dome/compound-stress.png" alt="OceanECO Analysis Compound Stress tab configured for Temperature above 19 degrees C and Omega Aragonite below 1, with a per-year table of days both conditions held simultaneously.">
          </v-card>
          <figcaption><span class="fig-no mono">FIG&nbsp;5</span><span class="fig-text">Analysis (Model) &rarr; Compound Stress: Temperature &gt; 19&nbsp;&deg;C vs. Omega Aragonite &lt; 1, per-year days and longest streak.</span></figcaption>
        </figure>
      </section>

      <section>
        <div class="col">
          <v-chip class="tag" variant="outlined" size="small" label>05 &mdash; Analysis &middot; Climatology Anomaly</v-chip>
          <h2>The long view</h2>
          <p class="lede">Zoomed out to nineteen years, 2021 stops looking singular and starts looking like a data point in a widening spread.</p>
          <p>Climatology Anomaly re-plots every year's daily values as a deviation from the pooled day-of-year mean, all overlaid on one synthetic calendar. What jumps out isn't just 2021's summer excursions past +3&nbsp;&deg;C &mdash; it's how many other years, especially from the mid-2010s onward, reach into the same territory. The heat dome was extreme, but the chart it shows up on is one this location has been filling in with warmer and more erratic summers for a while.</p>
        </div>
        <figure class="shot">
          <v-card class="frame" elevation="0">
            <img src="/case-studies/2021-heat-dome/climatology-anomaly.png" alt="OceanECO Analysis Climatology Anomaly tab showing every year from 2007 to 2026 overlaid as a deviation from the pooled day-of-year mean, a dense tangle of coloured lines with several years reaching well above zero in summer.">
          </v-card>
          <figcaption><span class="fig-no mono">FIG&nbsp;6</span><span class="fig-text">Analysis (Model) &rarr; Climatology Anomaly, all 20 years overlaid, full season. Colour runs cool&#8209;to&#8209;warm from 2007 (blue) to 2026 (red).</span></figcaption>
        </figure>
      </section>

      <section style="margin-bottom: 0;">
        <div class="col">
          <v-chip class="tag" variant="outlined" size="small" label>06 &mdash; Comparison &middot; Timeseries</v-chip>
          <h2>Does the water agree with the model?</h2>
          <p class="lede">A hindcast is only as useful as its agreement with something that was actually moored in the water. So we asked one.</p>
          <p>ONC's <strong>Boundary Pass Mooring</strong> sat at 223&nbsp;metres depth from October 2012 to August 2021, logging temperature the entire time &mdash; including straight through the 2021 heat dome. OceanECO's Comparison workspace lines its record up against the model at the matched depth: <span class="stat-inline">bias &minus;0.017&nbsp;&deg;C</span>, <span class="stat-inline">RMSE 0.284&nbsp;&deg;C</span>, <span class="stat-inline">Pearson r&nbsp;0.922</span> across 2,675 matched days. Model and sensor track each other closely for the whole record &mdash; and both stay flat between roughly 7 and 11&nbsp;&deg;C straight through the summer of 2021. At 223&nbsp;metres, an instrument that was actually there confirms what the model already said: the heat dome never reached it.</p>
        </div>
        <figure class="shot">
          <v-card class="frame" elevation="0">
            <img src="/case-studies/2021-heat-dome/comparison.png" alt="OceanECO Comparison workspace Timeseries tab overlaying model and sensor temperature at Boundary Pass Mooring, 226 metres depth, from 2012 to 2021, with the two lines tracking closely and staying between roughly 7 and 11 degrees C throughout, including summer 2021.">
          </v-card>
          <figcaption><span class="fig-no mono">FIG&nbsp;7</span><span class="fig-text">Comparison &rarr; Timeseries, Boundary Pass Mooring vs. model, 226.3&nbsp;m, full overlapping record. Right-hand panel: matched-day summary statistics.</span></figcaption>
        </figure>
      </section>

    </div>

    <footer>
      <div class="wrap col">
        <h3>How this was made</h3>
        <p>Every figure above came from clicking through the live app &mdash; picking a point, a depth, a variable, a date &mdash; nothing here was hand-plotted. The model source is SalishSeaCast, a curvilinear hindcast of the Salish Sea; daily aggregates cover back to 2007, which is what every chart here runs on, since OceanECO's hourly ingestion only started tracking live data from the start of this year. Carbonate-chemistry fields (pH, &Omega; aragonite, &Omega; calcite) are derived via PyCO2SYS from the model's temperature, salinity, alkalinity and DIC.</p>
        <p>Try it yourself: the same six tools &mdash; Timeseries, Model depth, Extreme Events, Compound Stress, Climatology Anomaly, and Comparison against any of the region's real profilers &mdash; work at any point in the Salish Sea, for any window in the record.</p>
        <div class="signoff mono">
          <span>OceanECO &middot; CIOOS Pacific</span>
          <span>Case study &middot; 2021 Pacific NW heat dome</span>
        </div>
      </div>
    </footer>
  </v-main>
</template>

<script setup lang="ts">
useHead({
  title: 'When the Heat Dome Reached the Salish Sea — OceanECO Case Study',
  meta: [
    {
      name: 'description',
      content: 'A verified walkthrough of the 2021 Pacific Northwest heat dome through OceanECO\'s Timeseries, Depth, Analysis, and Comparison tools.',
    },
  ],
})

const stats = [
  { num: '24.5°C', label: 'Peak surface temperature' },
  { num: '+4.5°C', label: 'Above the day-of-year baseline' },
  { num: '33 d', label: 'Longest heatwave, Jul 5–Aug 6' },
  { num: 'R 0.922', label: 'Model–sensor agreement at 226 m' },
]
</script>

<style scoped>
@font-face { font-family: 'Fraunces CS'; font-weight: 500; font-style: normal; src: url('/fonts/case-study/fraunces-500.ttf') format('truetype'); font-display: swap; }
@font-face { font-family: 'Fraunces CS'; font-weight: 600; font-style: normal; src: url('/fonts/case-study/fraunces-600.ttf') format('truetype'); font-display: swap; }
@font-face { font-family: 'Fraunces CS'; font-weight: 700; font-style: normal; src: url('/fonts/case-study/fraunces-700.ttf') format('truetype'); font-display: swap; }
@font-face { font-family: 'Public Sans CS'; font-weight: 400; font-style: normal; src: url('/fonts/case-study/pubsans-400.ttf') format('truetype'); font-display: swap; }
@font-face { font-family: 'Public Sans CS'; font-weight: 500; font-style: normal; src: url('/fonts/case-study/pubsans-500.ttf') format('truetype'); font-display: swap; }
@font-face { font-family: 'Public Sans CS'; font-weight: 600; font-style: normal; src: url('/fonts/case-study/pubsans-600.ttf') format('truetype'); font-display: swap; }
@font-face { font-family: 'Public Sans CS'; font-weight: 700; font-style: normal; src: url('/fonts/case-study/pubsans-700.ttf') format('truetype'); font-display: swap; }
@font-face { font-family: 'IBM Plex Mono CS'; font-weight: 400; font-style: normal; src: url('/fonts/case-study/plexmono-400.ttf') format('truetype'); font-display: swap; }
@font-face { font-family: 'IBM Plex Mono CS'; font-weight: 500; font-style: normal; src: url('/fonts/case-study/plexmono-500.ttf') format('truetype'); font-display: swap; }

/* ---------- tokens (scoped to this page — :root/body can't be targeted from
   scoped styles, since Vue never stamps the scoping attribute on them) ---------- */
.case-study{
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
  --shadow: 220 30% 20%;
}
@media (prefers-color-scheme: dark){
  .case-study{
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
    --shadow: 200 40% 2%;
  }
}

.case-study, .case-study *{ box-sizing: border-box; }
.case-study ::selection{ background: var(--heat); color: #fff; }

.case-study{
  background: var(--paper);
  color: var(--ink);
  font-family: 'Public Sans CS', -apple-system, 'Segoe UI', sans-serif;
  font-size: 17px;
  line-height: 1.65;
  -webkit-font-smoothing: antialiased;
}

.case-study :deep(a){ color: var(--tide); text-decoration-thickness: 1.5px; text-underline-offset: 2px; }

.case-study h1, .case-study h2, .case-study h3{
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

/* ---------- layout shell ---------- */
.wrap{ max-width: 1160px; margin: 0 auto; padding: 0 24px; }
.col{ max-width: 700px; margin: 0 auto; }
.col p{ margin: 0 0 1.1em; color: var(--ink); }
.col p.lede{ font-size: 19px; color: var(--ink-soft); }

.back-row{ padding-top: 20px; max-width: 780px; }
.back-link{
  font-family: 'IBM Plex Mono CS', monospace;
  font-size: 12.5px;
  color: var(--ink-faint) !important;
  text-decoration: none !important;
}
.back-link:hover{ color: var(--tide) !important; }

header.masthead{
  padding: 40px 0 48px;
  border-bottom: 1px solid var(--mist);
  margin-bottom: 56px;
}
header.masthead .col{ max-width: 780px; }
header.masthead h1{
  font-size: clamp(34px, 5.4vw, 54px);
  line-height: 1.08;
  margin: 14px 0 20px;
}
header.masthead .dek{
  font-size: 19.5px;
  color: var(--ink-soft);
  max-width: 60ch;
  margin: 0 0 22px;
}
header.masthead .meta{
  font-size: 13px;
  color: var(--ink-faint);
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
  align-items: center;
}
header.masthead .meta .dot{ opacity: .6; }

/* ---------- stat strip ---------- */
.stat-strip{
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 1px;
  background: var(--mist);
  border: 1px solid var(--mist);
  border-radius: 3px;
  overflow: hidden;
  margin: 34px 0 0;
}
.stat-strip .stat{
  background: var(--paper-raised) !important;
  border-radius: 0 !important;
  box-shadow: none !important;
  padding: 20px 18px 18px;
}
.stat :deep(.num){
  font-family: 'IBM Plex Mono CS', monospace;
  font-weight: 500;
  font-size: 27px;
  color: var(--heat);
  display: block;
  line-height: 1.1;
}
.stat :deep(.label){
  font-size: 12.5px;
  color: var(--ink-soft);
  margin-top: 6px;
  display: block;
  line-height: 1.4;
}

/* ---------- article sections ---------- */
section{ margin: 0 0 76px; }
.tag{
  font-family: 'IBM Plex Mono CS', monospace !important;
  font-size: 12px !important;
  letter-spacing: .08em;
  color: var(--tide) !important;
  background: var(--mist-2) !important;
  border-color: var(--mist) !important;
  margin-bottom: 14px;
}
section h2{
  font-size: clamp(25px, 3.2vw, 32px);
  line-height: 1.18;
  margin-bottom: 16px;
}
section .col > p:last-of-type{ margin-bottom: 0; }

.stat-inline{
  font-family: 'IBM Plex Mono CS', monospace;
  font-weight: 500;
  color: var(--heat);
}

/* ---------- figures ---------- */
figure.shot{
  max-width: 1080px;
  margin: 30px auto 8px;
}
figure.shot .frame{
  border: 1px solid var(--mist) !important;
  border-radius: 5px !important;
  overflow: hidden;
  background: var(--paper-raised) !important;
  box-shadow: 0 1px 2px hsl(var(--shadow) / .08), 0 12px 32px -18px hsl(var(--shadow) / .35) !important;
}
figure.shot img{
  display: block;
  width: 100%;
  height: auto;
}
figure.shot figcaption{
  padding: 12px 4px 0;
  display: flex;
  gap: 12px;
  align-items: baseline;
}
figure.shot figcaption .fig-no{
  font-family: 'IBM Plex Mono CS', monospace;
  font-size: 12px;
  color: var(--ink-faint);
  flex-shrink: 0;
}
figure.shot figcaption .fig-text{
  font-size: 14.5px;
  color: var(--ink-soft);
  line-height: 1.5;
}
.shot-pair{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 20px;
  max-width: 1080px;
  margin: 30px auto 8px;
}
.shot-pair figure.shot{ margin: 0; max-width: none; }
@media (max-width: 860px){
  .shot-pair{ grid-template-columns: 1fr; }
  .stat-strip{ grid-template-columns: repeat(2, 1fr); }
}

/* ---------- closing ---------- */
footer{
  background: var(--mist-2);
  border-top: 1px solid var(--mist);
  padding: 52px 0 60px;
  margin-top: 8px;
}
footer .col{ max-width: 700px; }
footer h3{ font-size: 21px; margin-bottom: 12px; }
footer p{ color: var(--ink-soft); font-size: 15.5px; margin: 0 0 1em; }
footer p:last-child{ margin-bottom: 0; }
footer .signoff{
  margin-top: 28px;
  padding-top: 22px;
  border-top: 1px solid var(--mist);
  font-size: 13px;
  color: var(--ink-faint);
  display: flex;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 10px;
}

@media (max-width: 640px){
  header.masthead{ padding: 32px 0 36px; }
  section{ margin-bottom: 56px; }
}
</style>
