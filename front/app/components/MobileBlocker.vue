<template>
    <div class="mobile-blocker">
        <div class="mobile-blocker__card p-8 text-center">
            <iconsOceanECOLogo :size="72" :loop="false" class="mb-6" style="display:inline-block;" />

            <h1 class="text-2xl font-bold mb-3">Best viewed on desktop</h1>

            <p class="text-sm mb-2" style="opacity: 0.85;">
                OceanECO is a data-intensive mapping and analysis dashboard built for larger screens.
                It isn't available on phones or small tablets yet.
            </p>

            <p class="text-xs" style="opacity: 0.7;">
                Please open this page on a desktop or laptop browser to continue.
            </p>
        </div>
    </div>
</template>

<script setup lang="ts">
// Purely presentational. Visibility is driven by the CSS media query below so it
// works instantly on first paint (SSR + pre-hydration) with no flash of the app.
</script>

<style scoped>
.mobile-blocker {
    display: none;
}

/* Coarse-pointer touch devices at phone/small-tablet widths, or any narrow viewport. */
@media (max-width: 900px),
       (pointer: coarse) and (max-width: 1024px) {
    .mobile-blocker {
        display: flex;
        position: fixed;
        inset: 0;
        z-index: 99999;
        align-items: center;
        justify-content: center;
        background: linear-gradient(160deg, #0d1b2a 0%, #1b263b 100%);
        color: #ffffff;
    }
}

.mobile-blocker__card {
    max-width: 420px;
}
</style>
