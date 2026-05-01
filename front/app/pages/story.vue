<template>
  <v-main class="story-page">

    <!-- ══════════════════════════════════════════════════════
         SECTION 1 – THE VANISHING DINNER
    ══════════════════════════════════════════════════════ -->
    <section class="s1-hero" ref="heroSection">
      <div class="s1-plate-wrap">
        <!-- Animated seafood plate -->
        <div class="animated-plate">
          <div class="plate-dish"></div>
          <div
            v-for="item in plateItems"
            :key="item.id"
            class="plate-item"
            :class="item.state"
            :style="{ left: item.x + '%', top: item.y + '%', fontSize: item.size + 'rem' }"
          >{{ item.emoji }}</div>
        </div>
        <!-- Fade-to-grey overlay that grows after 3 s -->
        <div class="s1-desaturate-overlay" :style="{ opacity: desatOpacity }"></div>
      </div>

      <div class="s1-content">
        <p class="s1-eyebrow">THE UNSEEN COASTAL CRISIS</p>
        <h1 class="s1-title">THE VANISHING DINNER</h1>
        <p class="s1-subtitle">
          The invisible foundation of our coast is crumbling.<br />
          The future of your favourite seafood harvest is being rewritten,<br />
          starting on your plate.
        </p>
        <div class="s1-body">
          <p>
            The vibrant wilderness of the West Coast isn't just about sunsets and salmon runs—it is
            also defined by the iconic harvests that sustain our communities, economies, and dinner
            tables.
          </p>
          <p>
            A platter of local oysters, wild-caught salmon, or Dungeness crab: these are symbols of
            our coastal identity. But beneath the surface, two invisible chemical threats are
            attacking the foundation of their survival. This is not a future problem; it is
            reshaping our marine environment right now.
          </p>
          <p class="s1-teaser">To save the harvest, we must see the threat. Meet the <strong>"OAH Twins."</strong></p>
        </div>
        <div class="s1-scroll-hint">
          <v-icon icon="mdi-chevron-down" size="32" class="bounce" />
        </div>
      </div>
    </section>

    <!-- ══════════════════════════════════════════════════════
         SECTION 2 – MEET THE OAH TWINS
    ══════════════════════════════════════════════════════ -->
    <section class="s2-twins" ref="twinsSection">
      <div class="s2-ocean-bg">
        <!-- animated CO2 bubbles -->
        <div
          v-for="b in co2Bubbles"
          :key="b.id"
          class="co2-bubble"
          :style="{ left: b.x + '%', animationDelay: b.delay + 's', animationDuration: b.dur + 's' }"
        >
          CO₂
        </div>
        <!-- algae bloom layer (Hypoxia) -->
        <div class="algae-bloom" :class="{ visible: showHypoxia }"></div>
        <!-- dead zone layer -->
        <div class="dead-zone" :class="{ visible: showHypoxia }">
          <span class="dead-zone-label">☠ Dead Zone</span>
        </div>
      </div>

      <div class="s2-content">
        <div class="s2-header">
          <h2 class="section-eyebrow">SECTION 2</h2>
          <h2 class="section-title">MEET THE OAH TWINS:<br />ACIDIFICATION &amp; HYPOXIA</h2>
          <p class="section-subtitle">
            Meet the two powerful forces reshaping our marine wilderness.<br />
            They are invisible, distinct-but-related, and devastating.
          </p>
        </div>

        <!-- TYPOGRAPHIC OA block -->
        <div class="typo-block oa-typo" :class="{ revealed: showOA }">
          <div class="typo-stat">⅓</div>
          <div class="typo-stat-label">of all carbon emissions<br>absorbed by our oceans</div>
          <div class="typo-giant">OCEAN</div>
          <div class="typo-accent">ACIDIFICATION</div>
          <div class="ph-bar" style="max-width:460px;margin:1.5rem 0">
            <span class="ph-label">pH 8.2 Neutral</span>
            <div class="ph-track">
              <div class="ph-fill" :style="{ width: phFillWidth + '%' }"></div>
            </div>
            <span class="ph-label acidic">pH 7.9 Acidic</span>
          </div>
          <p class="typo-body">
            The ocean has absorbed a third of global carbon emissions, fundamentally changing its
            chemical balance. This pH drop steals the carbonate that shells and skeletons are built
            from — leaving oysters, crabs, and foundational plankton fractured and struggling
            to survive.
          </p>
        </div>

        <!-- TYPOGRAPHIC HYPOXIA block -->
        <div class="typo-block hypoxia-typo" :class="{ revealed: showHypoxia }">
          <div class="typo-stat teal">40%</div>
          <div class="typo-stat-label teal">of world's ocean volume<br>suffers oxygen decline since 1960</div>
          <div class="typo-giant teal">HYPOXIA</div>
          <div class="typo-accent teal-accent">THE SUFFOCATOR</div>
          <div class="nutrient-cycle" style="margin:1.5rem 0">
            <span>💧 Runoff</span><span>🌿 Bloom</span><span>🦠 Bacteria</span><span>🐟 ☠</span>
          </div>
          <p class="typo-body">
            Excess nutrients fuel unnatural algae blooms. When they die, bacteria decompose the
            matter — consuming all dissolved oxygen and creating vast seafloor dead zones where
            marine life must flee or suffocate.
          </p>
        </div>

      </div>
    </section>

    <!-- ══════════════════════════════════════════════════════
         SECTION 3 – EXPLODING OCEAN LAYERS
    ══════════════════════════════════════════════════════ -->
    <section class="s3-layers" ref="layersSection">
      <div class="s3-sticky-container">
        <div class="s3-canvas-area">
          <!-- Solid block phase -->
          <div class="solid-block" :class="{ hidden: layersExploded }">
            <div
              v-for="(layer, i) in oceanLayers"
              :key="'solid-' + i"
              class="solid-stripe"
              :style="{ background: layer.gradient, opacity: 0.85 + i * 0.01 }"
            ></div>
          </div>

          <!-- Exploded layers phase -->
          <div class="exploded-layers" :class="{ visible: layersExploded }">
            <div
              v-for="(layer, i) in oceanLayers"
              :key="'layer-' + i"
              class="ocean-layer"
              :style="{
                background: layer.gradient,
                transform: layersExploded
                  ? `translateY(${(i - oceanLayers.length / 2) * layerGap}px)`
                  : 'translateY(0)',
                transitionDelay: i * 0.06 + 's',
              }"
            >
              <div class="layer-label-left">{{ layer.depth }}</div>
              <div class="layer-label-right" :class="layer.labelClass">{{ layer.label }}</div>
            </div>
          </div>

          <!-- Annotation panels that fade in after explosion -->
          <transition name="fade">
            <div v-if="layersExploded" class="layer-annotations">
              <div class="annotation annotation-oa">
                <span class="annotation-dot oa-dot"></span>
                <p><strong>Ocean Acidification</strong> is most intense near the surface — where CO₂ enters the water.</p>
              </div>
              <div class="annotation annotation-hypoxia">
                <span class="annotation-dot hypoxia-dot"></span>
                <p><strong>Hypoxia</strong> accumulates near the seafloor — where dead organic matter sinks and bacteria consume oxygen.</p>
              </div>
            </div>
          </transition>
        </div>

        <!-- Text panel -->
        <div class="s3-text-panel">
          <h2 class="section-eyebrow">SECTION 3</h2>
          <h2 class="section-title">VOYAGE INTO THE DEEP:<br />THE HIDDEN BATTLE FOR OAH</h2>
          <p class="section-subtitle">The complexity of ocean monitoring means we can't just measure the surface.</p>

          <transition name="slide-up" mode="out-in">
            <div v-if="!layersExploded" key="pre">
              <p class="s3-callout">📡 "Measuring the surface is not enough."</p>
              <p>
                The ocean is not a uniform block of water. It is a vibrant, stratified wilderness of
                layers, structured by depth, temperature, and salinity. Every fjord, inlet, and deep
                channel behaves differently.
              </p>
            </div>
            <div v-else key="post">
              <p class="s3-callout">💡 The layers reveal where the battle is hidden.</p>
              <p>
                The OAH Twins use this structure to their advantage, hiding their devastating effects
                in specific depth zones. You cannot measure the multi-layer health of a sensitive
                fjord by only checking the surface pH.
              </p>
              <p>
                Standard monitoring — like floating a sensor on a buoy — often misses critical
                changes happening just a few meters down. This invisible, multi-layer stratification
                is exactly why our coast requires advanced, cohesive monitoring at every depth, all
                at once.
              </p>
            </div>
          </transition>
        </div>
      </div>
    </section>

    <!-- ══════════════════════════════════════════════════════
         SECTION 4 – WHY OUR COAST IS ON THE FRONT LINE
    ══════════════════════════════════════════════════════ -->
    <section class="s4-coast" ref="coastSection">
      <div class="s4-map-area">
        <!-- Pseudo-3D perspective coast -->
        <div class="coast-3d-wrapper">
          <div class="coast-map-plane" :class="{ active: upwellingActive }">
            <!-- Sky strip -->
            <div class="c3d-sky">
              <span v-for="t in 10" :key="t" class="c3d-tree"
                :style="{ left: (t*10-4)+'%', fontSize: (0.8+(t%3)*0.25)+'rem' }">🌲</span>
            </div>
            <!-- Ocean surface band -->
            <div class="c3d-surface">Surface — CO₂ Entry</div>
            <!-- Mid water -->
            <div class="c3d-mid">Mid-Water Column</div>
            <!-- Deep layer -->
            <div class="c3d-deep">Deep Water — Upwelling Origin</div>
            <!-- Multiple upwelling arrows -->
            <div
              v-for="u in upwellingArrows"
              :key="u.id"
              class="c3d-upwelling"
              :class="{ animating: upwellingActive }"
              :style="{ left: u.x+'%', animationDelay: u.delay+'s' }"
            >
              <div class="c3d-shaft"></div>
              <div class="c3d-head">▲</div>
            </div>
            <!-- Coast SVG silhouette -->
            <svg class="coast-svg" viewBox="0 0 400 120" preserveAspectRatio="none">
              <path
                d="M0,40 C30,35 50,50 80,38 C110,26 130,55 160,42 C190,29 210,60 240,45
                   C270,30 290,52 320,38 C350,24 370,48 400,35 L400,0 L0,0 Z"
                fill="rgba(8,40,80,0.85)" />
              <path
                d="M0,60 C20,52 45,65 70,55 C100,44 125,68 155,57 C185,46 205,72 235,60
                   C265,48 285,65 315,54 C345,43 375,60 400,52 L400,120 L0,120 Z"
                fill="rgba(1,13,30,0.6)" />
            </svg>
            <!-- Coastal spots -->
            <div
              v-for="spot in coastalSpots"
              :key="spot.id"
              class="c3d-spot"
              :class="{ glowing: upwellingActive }"
              :style="{ left: spot.x+'%', top: spot.y+'%' }"
            >
              {{ spot.icon }}
              <span class="c3d-spot-label">{{ spot.label }}</span>
            </div>
            <!-- Upwelling label -->
            <div class="c3d-upwell-label" :class="{ visible: upwellingActive }">
              ↑ Deep Upwelling — acidic &amp; low-O₂
            </div>
          </div>
        </div>
      </div>

      <div class="s4-text">
        <h2 class="section-eyebrow">SECTION 4</h2>
        <h2 class="section-title">WHY OUR COAST IS<br />ON THE FRONT LINE</h2>
        <p class="section-subtitle">A naturally harsh environment meets a global magnifier.</p>
        <p>
          The stunning complexity of our coast — with its endless fjords, islands, and deep channels
          — makes it particularly vulnerable to Ocean Acidification and Hypoxia. We are on the front
          line of these invisible threats.
        </p>
        <p>
          It begins with a natural process called <strong>Summer Upwelling</strong>. Powerful winds
          pull deep, cold ocean water up to the surface. This deep water is naturally acidic and
          naturally low in oxygen. When upwelling occurs, these already harsh waters are funnelled
          directly into our complex coastal inlets, trapping the acidity and low oxygen exactly where
          our essential fisheries and shellfish farms operate.
        </p>
        <p>
          Global climate change acts as a magnifying glass, making these natural upwelling events
          earlier, longer, and more severe — pushing naturally challenging conditions past a critical
          tipping point for marine life.
        </p>
      </div>
    </section>

    <!-- ══════════════════════════════════════════════════════
         SECTION 5 – DECODING THE WILDERNESS
    ══════════════════════════════════════════════════════ -->
    <section class="s5-data" ref="dataSection">
      <div class="s5-pipeline">
        <!-- Phase 1: Sensors -->
        <div class="pipeline-phase" :class="{ active: dataPhase >= 1 }">
          <div class="phase-icon">
            <div class="buoy">🛟</div>
            <div class="probe">📡</div>
          </div>
          <div class="phase-flow" :class="{ flowing: dataPhase >= 2 }">
            <span v-for="d in 5" :key="d" class="data-dot" :style="{ animationDelay: d * 0.15 + 's' }"></span>
          </div>
          <p class="phase-label">Sparse sensor network<br /><em>Surface buoys &amp; deep probes</em></p>
        </div>

        <!-- Phase 2: Model -->
        <div class="pipeline-phase" :class="{ active: dataPhase >= 2 }">
          <div class="phase-icon model-brain">
            <div class="brain-glow">🧠</div>
            <div class="math-symbols">
              <span>∂T</span><span>∇²</span><span>Σ</span><span>∫</span>
            </div>
          </div>
          <div class="phase-flow" :class="{ flowing: dataPhase >= 3 }">
            <span v-for="d in 5" :key="d" class="data-dot" :style="{ animationDelay: d * 0.15 + 's' }"></span>
          </div>
          <p class="phase-label">Computational model<br /><em>Learns, predicts, fills gaps</em></p>
        </div>

        <!-- Phase 3: Grid -->
        <div class="pipeline-phase" :class="{ active: dataPhase >= 3 }">
          <div class="phase-icon grid-viz">
            <div
              v-for="r in 5"
              :key="r"
              class="grid-row"
            >
              <div
                v-for="c in 8"
                :key="c"
                class="grid-cell"
                :style="{ background: gridCellColor(r, c), opacity: dataPhase >= 3 ? 1 : 0 }"
              ></div>
            </div>
          </div>
          <p class="phase-label">Full-ocean 3D grid<br /><em>Every depth, every location</em></p>
        </div>
      </div>

      <div class="s5-text">
        <h2 class="section-eyebrow">SECTION 5</h2>
        <h2 class="section-title">DECODING THE WILDERNESS:<br />THE IMPORTANCE OF SCIENTIFIC VISIBILITY</h2>
        <p class="section-subtitle">We are transforming invisible data into actionable foresight.</p>
        <p>
          If we cannot see the crisis, we cannot prepare for it. To secure our coastal future, we
          are utilising advanced science to decode these invisible patterns across thousands of miles
          of complex shoreline.
        </p>
        <p>
          It is impossible to place sensors in every cubic metre of ocean. That is why advanced
          computational modelling is essential. By feeding real-time and historical sensor data into
          sophisticated mathematical models, scientists can "see" where the data ends. They simulate
          and predict the health of every ocean layer in 3D.
        </p>
        <p>
          These predictive models fill the vast data gaps, giving us cohesive visibility. It's about
          more than just monitoring; it's about providing essential foresight — allowing coastal
          communities, scientists, and sustainable growers to move from reactive survival to proactive
          adaptation.
        </p>
      </div>
    </section>

    <!-- ══════════════════════════════════════════════════════
         SECTION 6 – CALL TO ACTION
    ══════════════════════════════════════════════════════ -->
    <section class="s6-cta" ref="ctaSection">
      <div class="s6-header">
        <h2 class="section-eyebrow">SECTION 6</h2>
        <h2 class="section-title">YOUR ROLE IN THE OCEAN'S FUTURE</h2>
        <p class="section-subtitle">
          How you can help build a resilient coastline.<br />
          The unseen crisis requires collective action.
        </p>
      </div>

      <div class="cta-cards">
        <div class="cta-card" :class="{ visible: ctaVisible }">
          <div class="cta-card-icon">💧</div>
          <h3>Protect Our Waters</h3>
          <h4>Stop Hypoxia at the Source</h4>
          <p>
            Prevent Hypoxia in your community by limiting runoff. Reduce fertilizer use in your
            garden, manage urban runoff, and advocate for sustainable agricultural practices and
            effective municipal wastewater management. Clean inputs equal a breathable, resilient
            seafloor.
          </p>
        </div>

        <div class="cta-card" :class="{ visible: ctaVisible }" style="transition-delay: 0.15s">
          <div class="cta-card-icon">🌱</div>
          <h3>Fight Acidification</h3>
          <h4>Transition Away from Fossil Fuels</h4>
          <p>
            Lower your overall carbon footprint by supporting local, clean energy transitions.
            Acidification is driven by CO₂ absorption. Reducing carbon emissions in your individual
            and community life is the most fundamental way to rebalance pH and protect shell-builders
            from further corrosive shifts.
          </p>
        </div>

        <div class="cta-card" :class="{ visible: ctaVisible }" style="transition-delay: 0.3s">
          <div class="cta-card-icon">🦪</div>
          <h3>Support Local</h3>
          <h4>Sustainable Adaptors</h4>
          <p>
            Strengthen the coastal economy and ecosystem by supporting sustainable local adaptors.
            Buy seafood — especially oysters, mussels, and salmon — from sustainable local growers
            and fisheries who are actively adapting and monitoring their waters. Your choice
            reinforces sustainable practices and resilient livelihoods.
          </p>
        </div>
      </div>

      <div class="s6-closing">
        <blockquote>
          "Data is clarity. Action is hope. By understanding the invisible, we can secure the vibrant
          future of our coast."
        </blockquote>
      </div>
    </section>

  </v-main>
</template>

<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount, computed } from 'vue'

// ── Section 1 ──────────────────────────────────────────────────────────────
const heroLoaded = ref(false)
const desatOpacity = ref(0)
let desatTimer: ReturnType<typeof setTimeout> | null = null
let plateTimer: ReturnType<typeof setInterval> | null = null

type PlateState = 'healthy' | 'fading' | 'corrupted' | 'gone'
interface PlateItem { id: number; emoji: string; x: number; y: number; size: number; state: PlateState }

// Position items in a circle around the plate
const rawPlateItems: Omit<PlateItem, 'state'>[] = [
  { id: 0, emoji: '🦪', x: 50, y: 18, size: 2.2 },
  { id: 1, emoji: '🦀', x: 72, y: 30, size: 2.4 },
  { id: 2, emoji: '🐟', x: 78, y: 52, size: 2.0 },
  { id: 3, emoji: '🦐', x: 65, y: 70, size: 1.8 },
  { id: 4, emoji: '🦞', x: 42, y: 75, size: 2.2 },
  { id: 5, emoji: '🐚', x: 22, y: 65, size: 1.7 },
  { id: 6, emoji: '🦑', x: 18, y: 45, size: 2.0 },
  { id: 7, emoji: '🐠', x: 28, y: 26, size: 1.9 },
]

const plateItems = ref<PlateItem[]>(rawPlateItems.map(i => ({ ...i, state: 'healthy' })))

function startPlateAnimation() {
  let idx = 0
  const advance = () => {
    const item = plateItems.value[idx]
    if (!item) return
    if (item.state === 'healthy') {
      item.state = 'fading'
      setTimeout(() => {
        const i1 = plateItems.value[idx]
        if (i1) i1.state = 'corrupted'
        setTimeout(() => {
          const i2 = plateItems.value[idx]
          if (i2) i2.state = 'gone'
          idx = (idx + 1) % plateItems.value.length
          // If full cycle done, reset all and restart
          if (idx === 0) {
            setTimeout(() => {
              plateItems.value.forEach(i => { i.state = 'healthy' })
            }, 1000)
          }
        }, 800)
      }, 600)
    }
  }
  plateTimer = setInterval(advance, 1400)
}

// ── Section 2 ──────────────────────────────────────────────────────────────
const showOA      = ref(false)
const showHypoxia = ref(false)
const phFillWidth = ref(10)

const co2Bubbles = Array.from({ length: 12 }, (_, i) => ({
  id: i,
  x:     5 + Math.random() * 90,
  delay: Math.random() * 4,
  dur:   4 + Math.random() * 3,
}))

// ── Section 3 ──────────────────────────────────────────────────────────────
const layersExploded = ref(false)
const layerGap = 52  // px between exploded layers

const oceanLayers = [
  { depth: '0 m',     label: 'Surface – CO₂ entry zone',     gradient: 'linear-gradient(90deg,#0d4f8c,#1a7bbf)', labelClass: 'oa-label'      },
  { depth: '10 m',    label: 'Shallow photic zone',            gradient: 'linear-gradient(90deg,#0d4f8c,#1565c0)', labelClass: ''              },
  { depth: '30 m',    label: 'Thermocline begins',             gradient: 'linear-gradient(90deg,#0a3f70,#1255a8)', labelClass: ''              },
  { depth: '50 m',    label: 'Mid-water column',               gradient: 'linear-gradient(90deg,#083260,#0d468f)', labelClass: ''              },
  { depth: '100 m',   label: 'Cold, dense water',              gradient: 'linear-gradient(90deg,#06264d,#0a3870)', labelClass: ''              },
  { depth: '150 m',   label: 'Low-light zone',                 gradient: 'linear-gradient(90deg,#041c3a,#082d5c)', labelClass: ''              },
  { depth: '200 m',   label: 'Oxygen-poor layer',              gradient: 'linear-gradient(90deg,#021329,#062246)', labelClass: 'hypoxia-label' },
  { depth: '300 m',   label: 'Near-bottom accumulation',       gradient: 'linear-gradient(90deg,#010b19,#041530)', labelClass: 'hypoxia-label' },
  { depth: 'Seafloor',label: '☠ Dead Zone — Hypoxia maximum', gradient: 'linear-gradient(90deg,#020608,#040e1e)', labelClass: 'hypoxia-label' },
]

// ── Section 4 ──────────────────────────────────────────────────────────────
const upwellingActive = ref(false)

const coastalSpots = [
  { id: 1, x: 18, y: 38, icon: '🦪', label: 'Shellfish Farm' },
  { id: 2, x: 50, y: 30, icon: '🐟', label: 'Salmon Route'   },
  { id: 3, x: 78, y: 42, icon: '🦀', label: 'Crab Ground'    },
]

const upwellingArrows = [
  { id: 1, x: 25, delay: 0   },
  { id: 2, x: 50, delay: 0.5 },
  { id: 3, x: 72, delay: 1.0 },
]

// ── Section 5 ──────────────────────────────────────────────────────────────
const dataPhase = ref(0)

function gridCellColor(r: number, c: number): string {
  const val = (Math.sin(r * 1.2 + c * 0.8) + 1) / 2
  const hue = 200 + val * 60
  const lit  = 30 + val * 40
  return `hsl(${hue}, 80%, ${lit}%)`
}

// ── Section 6 ──────────────────────────────────────────────────────────────
const ctaVisible = ref(false)

// ── Intersection Observer ──────────────────────────────────────────────────
const twinsSection  = ref<HTMLElement | null>(null)
const layersSection = ref<HTMLElement | null>(null)
const coastSection  = ref<HTMLElement | null>(null)
const dataSection   = ref<HTMLElement | null>(null)
const ctaSection    = ref<HTMLElement | null>(null)

let observer: IntersectionObserver | null = null
let scrollHandler: (() => void) | null = null

onMounted(() => {
  startPlateAnimation()

  // Start hero desaturation after 3 s
  desatTimer = setTimeout(() => {
    const start = Date.now()
    const tick = () => {
      const elapsed = Date.now() - start
      desatOpacity.value = Math.min(elapsed / 3000, 0.55)
      if (elapsed < 3000) requestAnimationFrame(tick)
    }
    requestAnimationFrame(tick)
  }, 3000)

  // IntersectionObserver for section reveals
  observer = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        if (!entry.isIntersecting) continue
        const el = entry.target
        if (el === twinsSection.value) {
          setTimeout(() => { showOA.value = true }, 200)
          setTimeout(() => {
            phFillWidth.value = 100
            showHypoxia.value = true
          }, 900)
        }
        if (el === coastSection.value) {
          setTimeout(() => { upwellingActive.value = true }, 400)
        }
        if (el === dataSection.value) {
          setTimeout(() => { dataPhase.value = 1 }, 300)
          setTimeout(() => { dataPhase.value = 2 }, 1000)
          setTimeout(() => { dataPhase.value = 3 }, 1800)
        }
        if (el === ctaSection.value) {
          setTimeout(() => { ctaVisible.value = true }, 200)
        }
      }
    },
    { threshold: 0.25 }
  )

  ;[twinsSection, coastSection, dataSection, ctaSection].forEach(r => {
    if (r.value) observer!.observe(r.value)
  })

  // Scroll handler for Section 3 layer explosion (scroll-based)
  scrollHandler = () => {
    if (!layersSection.value) return
    const rect = layersSection.value.getBoundingClientRect()
    const progress = 1 - (rect.top + rect.height * 0.35) / window.innerHeight
    layersExploded.value = progress > 0.3
  }
  window.addEventListener('scroll', scrollHandler, { passive: true })
  scrollHandler()
})

onBeforeUnmount(() => {
  if (desatTimer) clearTimeout(desatTimer)
  if (plateTimer) clearInterval(plateTimer)
  observer?.disconnect()
  if (scrollHandler) window.removeEventListener('scroll', scrollHandler)
})
</script>

<style scoped>
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Space+Grotesk:wght@400;600;700&display=swap');

/* ═══════════════════════════ BASE ════════════════════════════ */
.story-page {
  background: #050d1a;
  color: #e8f0fe;
  font-family: 'Georgia', serif;
  overflow-x: hidden;
}

section {
  position: relative;
  width: 100%;
}

.section-eyebrow {
  font-size: 0.7rem;
  letter-spacing: 0.25em;
  color: #4fc3f7;
  text-transform: uppercase;
  margin-bottom: 0.5rem;
  font-family: 'Space Grotesk', monospace;
}

.section-title {
  font-size: clamp(1.8rem, 4vw, 3.2rem);
  font-weight: 700;
  line-height: 1.1;
  margin-bottom: 1rem;
  color: #fff;
  font-family: 'Bebas Neue', cursive;
  letter-spacing: 0.05em;
}

.section-subtitle {
  font-size: 1.05rem;
  color: #90caf9;
  margin-bottom: 2rem;
  line-height: 1.6;
}

/* ═══════════════════════════ SECTION 1 ════════════════════════════ */
.s1-hero {
  min-height: 100vh;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  position: relative;
  overflow: hidden;
  background: radial-gradient(ellipse at 50% 40%, #0d2a4a 0%, #020812 70%);
}

.s1-plate-wrap {
  position: absolute;
  inset: 0;
  z-index: 0;
  display: flex;
  align-items: center;
  justify-content: center;
}

.animated-plate {
  position: relative;
  width: min(55vw, 480px);
  height: min(55vw, 480px);
}

.plate-dish {
  position: absolute;
  inset: 0;
  border-radius: 50%;
  background: radial-gradient(ellipse at 40% 35%, #2a1a0a 0%, #100a04 60%, #070503 100%);
  border: 3px solid rgba(255,200,80,0.15);
  box-shadow:
    0 0 80px rgba(0,0,0,0.8),
    inset 0 0 60px rgba(0,0,0,0.6);
}

.plate-item {
  position: absolute;
  transform: translate(-50%, -50%);
  transition: filter 0.6s ease, opacity 0.6s ease, transform 0.6s ease;
  cursor: default;
  user-select: none;
  filter: drop-shadow(0 2px 4px rgba(0,0,0,0.5));
}
.plate-item.fading {
  filter: grayscale(0.6) drop-shadow(0 2px 4px rgba(0,0,0,0.5));
  opacity: 0.55;
}
.plate-item.corrupted {
  filter: grayscale(1) sepia(0.4) drop-shadow(0 0 6px rgba(200,50,50,0.4));
  opacity: 0.35;
  transform: translate(-50%, -50%) scale(0.8) rotate(-8deg);
}
.plate-item.gone {
  opacity: 0;
  transform: translate(-50%, -50%) scale(0.2) rotate(-20deg);
}

.s1-desaturate-overlay {
  position: absolute;
  inset: 0;
  background: rgba(10, 20, 40, 1);
  pointer-events: none;
  transition: opacity 0.1s linear;
}

.s1-content {
  position: relative;
  z-index: 1;
  max-width: 860px;
  margin: 0 auto;
  padding: 8rem 2rem 4rem;
  text-align: center;
}

.s1-eyebrow {
  font-size: 0.7rem;
  letter-spacing: 0.3em;
  color: #4fc3f7;
  text-transform: uppercase;
  margin-bottom: 1rem;
  font-family: 'Space Grotesk', monospace;
}

.s1-title {
  font-size: clamp(3rem, 8vw, 6.5rem);
  font-family: 'Bebas Neue', cursive;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: #fff;
  line-height: 1;
  margin-bottom: 1.5rem;
  text-shadow: 0 4px 30px rgba(0,0,0,0.8);
}

.s1-subtitle {
  font-size: clamp(1rem, 2vw, 1.3rem);
  color: #b3e5fc;
  line-height: 1.7;
  margin-bottom: 2rem;
}

.s1-body {
  font-size: 1rem;
  color: #cfd8e3;
  line-height: 1.85;
  max-width: 680px;
  margin: 0 auto 2rem;
}
.s1-body p { margin-bottom: 1rem; }
.s1-teaser { color: #4fc3f7; font-style: italic; font-size: 1.05rem; }

.s1-scroll-hint {
  margin-top: 2rem;
  color: #4fc3f7;
  animation: fadeIn 1s 2s both;
}

.bounce {
  animation: bounce 1.8s infinite;
}
@keyframes bounce {
  0%, 100% { transform: translateY(0); }
  50%       { transform: translateY(8px); }
}
@keyframes fadeIn {
  from { opacity: 0; }
  to   { opacity: 1; }
}

/* ═══════════════════════════ SECTION 2 – TYPOGRAPHIC ════════════════════════════ */
.typo-block {
  opacity: 0;
  transform: translateY(50px);
  transition: opacity 0.9s ease, transform 0.9s ease;
  padding: 4rem 0;
  border-bottom: 1px solid rgba(255,255,255,0.06);
}
.typo-block.revealed {
  opacity: 1;
  transform: translateY(0);
}

.typo-stat {
  font-family: 'Bebas Neue', cursive;
  font-size: clamp(4rem, 12vw, 9rem);
  color: #ff8f00;
  line-height: 1;
  text-shadow: 0 0 60px rgba(255,143,0,0.4);
}
.typo-stat.teal { color: #26c6da; text-shadow: 0 0 60px rgba(38,198,218,0.4); }

.typo-stat-label {
  font-family: 'Space Grotesk', sans-serif;
  font-size: 0.85rem;
  color: #90a4ae;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  margin-bottom: 1.5rem;
  line-height: 1.5;
}
.typo-stat-label.teal { color: #80deea; }

.typo-giant {
  font-family: 'Bebas Neue', cursive;
  font-size: clamp(4.5rem, 14vw, 11rem);
  color: #fff;
  line-height: 0.9;
  letter-spacing: 0.04em;
}
.typo-giant.teal { color: #26c6da; text-shadow: 0 0 40px rgba(38,198,218,0.3); }

.typo-accent {
  font-family: 'Bebas Neue', cursive;
  font-size: clamp(2.5rem, 7vw, 5.5rem);
  color: #ff6d00;
  letter-spacing: 0.12em;
  margin-bottom: 1.5rem;
  text-shadow: 0 0 40px rgba(255,109,0,0.35);
}
.typo-accent.teal-accent {
  color: #00bcd4;
  text-shadow: 0 0 40px rgba(0,188,212,0.35);
}

.typo-body {
  color: #b0bec5;
  font-size: 1rem;
  line-height: 1.85;
  max-width: 600px;
}

/* ═══════════════════════════ SECTION 2 – OCEAN BG ════════════════════════════ */
.s2-twins {
  min-height: 100vh;
  padding: 6rem 2rem;
  position: relative;
  background: linear-gradient(180deg, #050d1a 0%, #061a35 60%, #030d1a 100%);
  overflow: hidden;
}

.s2-ocean-bg {
  position: absolute;
  inset: 0;
  pointer-events: none;
  overflow: hidden;
}

.co2-bubble {
  position: absolute;
  top: -40px;
  font-size: 0.65rem;
  font-weight: 700;
  color: #ff8f00;
  background: rgba(255,111,0,0.18);
  border: 1px solid #ff8f00;
  border-radius: 50%;
  padding: 6px 8px;
  animation: sinkBubble linear infinite;
}
@keyframes sinkBubble {
  0%   { top: -40px; opacity: 0; }
  10%  { opacity: 1; }
  90%  { opacity: 0.7; }
  100% { top: 110%; opacity: 0; }
}

.algae-bloom {
  position: absolute;
  top: 0; left: 0; right: 0;
  height: 120px;
  background: radial-gradient(ellipse at 50% 0%, rgba(30,120,30,0.45) 0%, transparent 70%);
  transform: scaleY(0);
  transform-origin: top;
  transition: transform 1.5s ease 0.5s;
}
.algae-bloom.visible { transform: scaleY(1); }

.dead-zone {
  position: absolute;
  bottom: 0; left: 0; right: 0;
  height: 100px;
  background: linear-gradient(0deg, rgba(20,5,5,0.85) 0%, transparent 100%);
  display: flex;
  align-items: flex-end;
  justify-content: center;
  padding-bottom: 1rem;
  opacity: 0;
  transition: opacity 1.5s ease 1.2s;
}
.dead-zone.visible { opacity: 1; }
.dead-zone-label { color: #ff5252; font-size: 0.85rem; letter-spacing: 0.1em; }

.s2-content {
  position: relative;
  z-index: 1;
  max-width: 1200px;
  margin: 0 auto;
}

.s2-header { text-align: center; margin-bottom: 4rem; }

.s2-cards { display: none; }

.twin-card {
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 16px;
  padding: 2rem;
  opacity: 0;
  transform: translateY(40px);
  transition: opacity 0.8s ease, transform 0.8s ease;
}
.twin-card.revealed {
  opacity: 1;
  transform: translateY(0);
}

.oa-card   { border-color: rgba(255,143,0,0.3); }
.oa-card:hover { box-shadow: 0 0 30px rgba(255,143,0,0.15); }
.hypoxia-card  { border-color: rgba(76,175,80,0.3); transition-delay: 0.3s !important; }
.hypoxia-card:hover { box-shadow: 0 0 30px rgba(76,175,80,0.15); }

.twin-card h3 {
  font-size: 1.3rem;
  font-weight: 700;
  color: #fff;
  margin-bottom: 0.35rem;
}
.twin-tagline { font-size: 0.95rem; color: #90caf9; margin-bottom: 1rem; }
.twin-card p  { color: #b0bec5; font-size: 0.95rem; line-height: 1.8; }

.twin-card-icon {
  margin-bottom: 1.5rem;
  min-height: 60px;
  display: flex;
  align-items: center;
}

.shell-compare { display: flex; align-items: center; gap: 0.75rem; font-size: 1.8rem; }
.shell.dissolving { filter: grayscale(0.8) sepia(0.6); opacity: 0.6; }
.arrow-compare { color: #ff8f00; font-size: 1.4rem; }

.ph-bar {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 1.5rem;
  font-size: 0.7rem;
}
.ph-label { white-space: nowrap; color: #4fc3f7; min-width: 80px; }
.ph-label.acidic { color: #ef5350; text-align: right; }
.ph-track {
  flex: 1;
  height: 8px;
  background: linear-gradient(90deg, #1565c0, #ef5350);
  border-radius: 4px;
  overflow: hidden;
  position: relative;
}
.ph-fill {
  position: absolute;
  top: 0; left: 0; bottom: 0;
  background: rgba(255,255,255,0.35);
  transition: width 2s ease;
  transform: scaleX(-1);
}

.nutrient-cycle {
  display: flex;
  gap: 0.6rem;
  align-items: center;
  font-size: 0.8rem;
  color: #a5d6a7;
  flex-wrap: wrap;
}
.nutrient-cycle span { background: rgba(255,255,255,0.07); border-radius: 20px; padding: 3px 8px; }

/* ═══════════════════════════ SECTION 3 ════════════════════════════ */
.s3-layers {
  min-height: 200vh;
  position: relative;
  background: #020912;
}

.s3-sticky-container {
  position: sticky;
  top: 0;
  height: 100vh;
  display: grid;
  grid-template-columns: 1fr 1fr;
  align-items: center;
  padding: 2rem 4rem;
  gap: 3rem;
  overflow: hidden;
}

.s3-canvas-area {
  position: relative;
  height: 500px;
  display: flex;
  align-items: center;
  justify-content: center;
}

.solid-block {
  width: 280px;
  height: 440px;
  overflow: hidden;
  border-radius: 6px;
  box-shadow: 0 0 60px rgba(0,100,200,0.4);
  transition: opacity 0.6s ease;
}
.solid-block.hidden { opacity: 0; pointer-events: none; }

.solid-stripe {
  height: 11.1%;
  width: 100%;
}

.exploded-layers {
  position: absolute;
  inset: 0;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  opacity: 0;
  pointer-events: none;
  transition: opacity 0.5s ease;
}
.exploded-layers.visible { opacity: 1; pointer-events: all; }

.ocean-layer {
  width: 280px;
  height: 32px;
  border-radius: 4px;
  position: relative;
  display: flex;
  align-items: center;
  transition: transform 0.9s cubic-bezier(0.34,1.56,0.64,1), opacity 0.7s ease;
  box-shadow: 0 2px 12px rgba(0,0,0,0.5);
  margin: 1px 0;
}

.layer-label-left {
  position: absolute;
  left: -56px;
  font-size: 0.65rem;
  color: #78909c;
  white-space: nowrap;
  font-family: monospace;
}
.layer-label-right {
  position: absolute;
  right: -10px;
  left: 8px;
  font-size: 0.65rem;
  color: #90caf9;
  white-space: nowrap;
  text-align: right;
  padding-right: 4px;
}
.oa-label     { color: #ff8f00 !important; }
.hypoxia-label{ color: #ef5350 !important; }

.layer-annotations {
  position: absolute;
  right: -20px;
  top: 50%;
  transform: translateY(-50%);
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
}

.annotation {
  display: flex;
  align-items: flex-start;
  gap: 0.5rem;
  max-width: 200px;
}
.annotation p { font-size: 0.75rem; color: #cfd8e3; line-height: 1.5; }
.annotation-dot {
  width: 10px; height: 10px;
  border-radius: 50%;
  margin-top: 3px;
  flex-shrink: 0;
}
.oa-dot      { background: #ff8f00; box-shadow: 0 0 8px #ff8f00; }
.hypoxia-dot { background: #ef5350; box-shadow: 0 0 8px #ef5350; }

.s3-callout {
  font-size: 1.05rem;
  color: #4fc3f7;
  font-style: italic;
  margin-bottom: 1rem;
  padding: 0.75rem 1rem;
  border-left: 3px solid #4fc3f7;
  background: rgba(79,195,247,0.07);
  border-radius: 0 8px 8px 0;
}

.s3-text-panel { padding: 2rem 0; }
.s3-text-panel p { color: #b0bec5; line-height: 1.85; margin-bottom: 1rem; font-size: 0.97rem; }

/* transitions */
.slide-up-enter-active, .slide-up-leave-active { transition: all 0.5s ease; }
.slide-up-enter-from { opacity: 0; transform: translateY(20px); }
.slide-up-leave-to   { opacity: 0; transform: translateY(-20px); }
.fade-enter-active, .fade-leave-active { transition: opacity 0.7s ease; }
.fade-enter-from, .fade-leave-to       { opacity: 0; }

/* ═══════════════════════════ SECTION 4 – 3D COAST ════════════════════════════ */
.coast-3d-wrapper {
  perspective: 1000px;
  width: 100%;
}

.coast-map-plane {
  transform: rotateX(42deg) rotateZ(-5deg);
  transform-style: preserve-3d;
  transform-origin: center center;
  transition: transform 1s ease;
  background: linear-gradient(180deg, #071e3d 0%, #0d3b6e 35%, #042040 65%, #010d1e 100%);
  border-radius: 12px;
  width: 100%;
  min-height: 340px;
  position: relative;
  overflow: hidden;
  box-shadow:
    0 30px 60px rgba(0,0,0,0.7),
    0 0 0 1px rgba(79,195,247,0.12);
}
.coast-map-plane.active {
  transform: rotateX(38deg) rotateZ(-4deg);
}

.c3d-sky {
  background: linear-gradient(180deg, #0a1628 0%, #0d2a4a 100%);
  height: 60px;
  position: relative;
  overflow: hidden;
  border-bottom: 1px solid rgba(255,255,255,0.06);
}
.c3d-tree {
  position: absolute;
  bottom: 0;
  line-height: 1;
}

.c3d-surface {
  height: 60px;
  background: rgba(13, 59, 110, 0.6);
  border-bottom: 1px solid rgba(79,195,247,0.2);
  display: flex;
  align-items: center;
  padding-left: 1rem;
  font-family: 'Space Grotesk', monospace;
  font-size: 0.6rem;
  letter-spacing: 0.2em;
  color: rgba(79,195,247,0.7);
  text-transform: uppercase;
}

.c3d-mid {
  height: 70px;
  background: rgba(4, 32, 64, 0.7);
  border-bottom: 1px solid rgba(255,255,255,0.05);
  display: flex;
  align-items: center;
  padding-left: 1rem;
  font-family: 'Space Grotesk', monospace;
  font-size: 0.6rem;
  letter-spacing: 0.15em;
  color: rgba(255,255,255,0.2);
  text-transform: uppercase;
}

.c3d-deep {
  height: 80px;
  background: rgba(1, 9, 22, 0.85);
  display: flex;
  align-items: center;
  padding-left: 1rem;
  font-family: 'Space Grotesk', monospace;
  font-size: 0.6rem;
  letter-spacing: 0.15em;
  color: rgba(239,83,80,0.5);
  text-transform: uppercase;
}

.coast-svg {
  position: absolute;
  top: 55px;
  left: 0;
  width: 100%;
  height: 130px;
  pointer-events: none;
}

.c3d-upwelling {
  position: absolute;
  bottom: 10px;
  display: flex;
  flex-direction: column;
  align-items: center;
  opacity: 0.2;
  transition: opacity 0.8s ease;
  pointer-events: none;
}
.c3d-upwelling.animating {
  opacity: 1;
  animation: c3dUpwell 2.2s ease-in-out infinite;
}
@keyframes c3dUpwell {
  0%, 100% { transform: translateY(0); }
  50%       { transform: translateY(-12px); }
}
.c3d-shaft {
  width: 3px;
  height: 130px;
  background: linear-gradient(0deg, #ef5350 0%, #ff8f00 50%, #4fc3f7 100%);
  border-radius: 2px;
  box-shadow: 0 0 8px rgba(79,195,247,0.5);
}
.c3d-head {
  order: -1;
  font-size: 1.2rem;
  color: #4fc3f7;
  text-shadow: 0 0 8px #4fc3f7;
  margin-bottom: 2px;
}

.c3d-spot {
  position: absolute;
  font-size: 1.5rem;
  transform: translate(-50%, -50%);
  display: flex;
  flex-direction: column;
  align-items: center;
  transition: filter 0.5s ease;
}
.c3d-spot.glowing {
  filter: drop-shadow(0 0 8px #4fc3f7);
  animation: c3dSpotPulse 2s ease-in-out infinite;
}
@keyframes c3dSpotPulse {
  0%, 100% { transform: translate(-50%, -50%) scale(1); }
  50%       { transform: translate(-50%, -50%) scale(1.25); }
}
.c3d-spot-label {
  font-family: 'Space Grotesk', sans-serif;
  font-size: 0.5rem;
  color: #4fc3f7;
  white-space: nowrap;
  background: rgba(0,0,0,0.7);
  padding: 1px 4px;
  border-radius: 3px;
  margin-top: 2px;
}

.c3d-upwell-label {
  position: absolute;
  bottom: 8px;
  right: 12px;
  font-family: 'Space Grotesk', sans-serif;
  font-size: 0.6rem;
  color: #ef9a9a;
  letter-spacing: 0.1em;
  opacity: 0;
  transition: opacity 0.8s ease 0.6s;
  text-transform: uppercase;
}
.c3d-upwell-label.visible { opacity: 1; }

/* ═══════════════════════════ SECTION 4 ════════════════════════════ */
.s4-coast {
  min-height: 100vh;
  display: grid;
  grid-template-columns: 1fr 1fr;
  align-items: center;
  gap: 3rem;
  padding: 6rem 4rem;
  background: linear-gradient(180deg, #020912 0%, #041628 100%);
}

.s4-map-area { position: relative; }
.bathy-scene {
  width: 100%;
  min-height: 400px;
  border-radius: 16px;
  overflow: hidden;
  border: 1px solid rgba(255,255,255,0.08);
  position: relative;
}

.bathy-sky {
  height: 80px;
  background: linear-gradient(180deg, #0a1628 0%, #1a3a5c 100%);
}

.bathy-forest {
  position: absolute;
  top: 40px; left: 0; right: 0;
  height: 60px;
  display: flex;
  align-items: flex-end;
}
.tree {
  position: absolute;
  font-size: 1.2rem;
  line-height: 1;
}

.bathy-water {
  background: linear-gradient(180deg, #0d3b6e 0%, #042040 40%, #010d1e 100%);
  height: 320px;
  position: relative;
  overflow: hidden;
}

.depth-band {
  position: absolute;
  left: 0; right: 0;
  height: 33.33%;
  display: flex;
  align-items: center;
  padding-left: 1rem;
  font-size: 0.65rem;
  letter-spacing: 0.15em;
  color: rgba(255,255,255,0.25);
  border-top: 1px solid rgba(255,255,255,0.06);
  font-family: monospace;
}
.db1 { top: 0%; }
.db2 { top: 33.33%; }
.db3 { top: 66.66%; }

.upwelling-arrow {
  position: absolute;
  left: 15%;
  bottom: 10px;
  display: flex;
  flex-direction: column;
  align-items: center;
  opacity: 0.3;
  transition: opacity 0.8s ease, transform 0.8s ease;
}
.upwelling-arrow.animating {
  opacity: 1;
  animation: upwellPulse 2s ease-in-out infinite;
}
@keyframes upwellPulse {
  0%, 100% { transform: translateY(0); }
  50%       { transform: translateY(-8px); }
}

.up-shaft {
  width: 4px;
  height: 160px;
  background: linear-gradient(0deg, #ef5350, #ff8f00, #4fc3f7);
  border-radius: 2px;
}
.up-head {
  font-size: 1.6rem;
  color: #4fc3f7;
  margin-bottom: 4px;
  order: -1;
}
.up-label {
  font-size: 0.6rem;
  color: #ef9a9a;
  text-align: center;
  line-height: 1.4;
  max-width: 80px;
  margin-top: 4px;
}

.coastal-spot {
  position: absolute;
  font-size: 1.4rem;
  cursor: default;
  transition: filter 0.5s ease, transform 0.5s ease;
  display: flex;
  flex-direction: column;
  align-items: center;
}
.coastal-spot.glowing {
  filter: drop-shadow(0 0 8px #4fc3f7);
  animation: spotPulse 2s ease-in-out infinite;
}
@keyframes spotPulse {
  0%, 100% { transform: scale(1); }
  50%       { transform: scale(1.2); }
}
.spot-label {
  font-size: 0.55rem;
  color: #4fc3f7;
  white-space: nowrap;
  font-family: monospace;
  background: rgba(0,0,0,0.6);
  padding: 1px 4px;
  border-radius: 3px;
  margin-top: 2px;
}

.s4-text p { color: #b0bec5; line-height: 1.85; margin-bottom: 1rem; font-size: 0.97rem; }

/* ═══════════════════════════ SECTION 5 ════════════════════════════ */
.s5-data {
  min-height: 100vh;
  padding: 6rem 4rem;
  background: linear-gradient(180deg, #041628 0%, #050d1a 100%);
  display: flex;
  flex-direction: column;
  gap: 4rem;
}

.s5-pipeline {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 0;
  flex-wrap: wrap;
  gap: 1rem;
}

.pipeline-phase {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 1rem;
  opacity: 0.25;
  transition: opacity 0.8s ease;
  position: relative;
}
.pipeline-phase.active { opacity: 1; }

.phase-icon {
  width: 120px;
  height: 100px;
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(79,195,247,0.25);
  border-radius: 12px;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  font-size: 2rem;
  gap: 0.25rem;
  position: relative;
  overflow: hidden;
}

.model-brain {
  position: relative;
}
.brain-glow { font-size: 2.2rem; position: relative; z-index: 1; }
.math-symbols {
  display: flex;
  gap: 0.3rem;
  font-size: 0.65rem;
  color: #4fc3f7;
  font-family: monospace;
  position: relative;
  z-index: 1;
}

.pipeline-phase.active .phase-icon { box-shadow: 0 0 20px rgba(79,195,247,0.3); }

.phase-flow {
  display: flex;
  gap: 4px;
  height: 20px;
  align-items: center;
}
.data-dot {
  width: 6px; height: 6px;
  border-radius: 50%;
  background: #4fc3f7;
  opacity: 0;
}
.phase-flow.flowing .data-dot {
  animation: flowDot 1s ease-in-out infinite;
}
@keyframes flowDot {
  0%    { opacity: 0; transform: translateX(-10px); }
  50%   { opacity: 1; }
  100%  { opacity: 0; transform: translateX(10px); }
}

.phase-label {
  font-size: 0.75rem;
  text-align: center;
  color: #90caf9;
  line-height: 1.5;
  font-family: 'Space Grotesk', sans-serif;
}
.phase-label em { color: #546e7a; }

.grid-viz { padding: 6px; gap: 2px; }
.grid-row { display: flex; gap: 2px; }
.grid-cell {
  width: 14px; height: 14px;
  border-radius: 2px;
  transition: background 0.5s ease, opacity 0.5s ease;
}

.buoy { font-size: 1.8rem; }
.probe { font-size: 1.2rem; }

.s5-text { max-width: 720px; margin: 0 auto; text-align: center; }
.s5-text p { color: #b0bec5; line-height: 1.85; margin-bottom: 1rem; font-size: 0.97rem; }

/* ═══════════════════════════ SECTION 6 ════════════════════════════ */
.s6-cta {
  padding: 6rem 4rem;
  background: linear-gradient(180deg, #050d1a 0%, #03090f 100%);
}

.s6-header { text-align: center; margin-bottom: 4rem; }

.cta-cards {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
  gap: 2rem;
  max-width: 1100px;
  margin: 0 auto 4rem;
}

.cta-card {
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.1);
  border-radius: 16px;
  padding: 2.5rem 2rem;
  opacity: 0;
  transform: translateY(30px);
  transition: opacity 0.7s ease, transform 0.7s ease, box-shadow 0.3s ease;
}
.cta-card.visible {
  opacity: 1;
  transform: translateY(0);
}
.cta-card:hover { box-shadow: 0 8px 40px rgba(79,195,247,0.15); border-color: rgba(79,195,247,0.3); }

.cta-card-icon { font-size: 2.5rem; margin-bottom: 1.2rem; }
.cta-card h3 { font-size: 1.2rem; font-weight: 700; color: #fff; margin-bottom: 0.25rem; }
.cta-card h4 { font-size: 0.85rem; color: #4fc3f7; margin-bottom: 1rem; font-weight: 500; }
.cta-card p  { color: #90a4ae; font-size: 0.92rem; line-height: 1.8; }

.s6-closing {
  text-align: center;
  padding: 3rem 2rem;
  max-width: 700px;
  margin: 0 auto;
}
.s6-closing blockquote {
  font-size: clamp(1.1rem, 2.5vw, 1.5rem);
  font-style: italic;
  color: #4fc3f7;
  border: none;
  padding: 2rem;
  position: relative;
  line-height: 1.7;
}
.s6-closing blockquote::before {
  content: '"';
  font-size: 5rem;
  color: rgba(79,195,247,0.15);
  position: absolute;
  top: -0.5rem;
  left: 0;
  line-height: 1;
}

/* ═══════════════════════════ RESPONSIVE ════════════════════════════ */
@media (max-width: 900px) {
  .s3-sticky-container,
  .s4-coast {
    grid-template-columns: 1fr;
    padding: 3rem 1.5rem;
  }
  .s3-canvas-area { height: 360px; }
  .s3-sticky-container { height: auto; position: relative; }
  .s5-data { padding: 4rem 1.5rem; }
  .s6-cta  { padding: 4rem 1.5rem; }
  .s5-pipeline { flex-direction: column; }
  .layer-annotations { display: none; }
}
</style>
