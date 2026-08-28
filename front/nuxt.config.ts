
// https://nuxt.com/docs/api/configuration/nuxt-config
export default defineNuxtConfig({
  compatibilityDate: '2025-07-15',
  devtools: { enabled: true },
  app: {
    head: {
      link: [
        { rel: 'icon', type: 'image/png', href: '/OA_logo.png' }
      ]
    }
  },
  css: ['~/assets/css/main.css'],
  // Inter is self-hosted by @nuxt/fonts (registered by @nuxt/ui) instead of
  // render-blocking on fonts.googleapis.com. Weights match what the old
  // stylesheet requested.
  fonts: {
    families: [
      { name: 'Inter', provider: 'google', weights: [300, 400, 500, 600, 700] },
      // index.vue's monospace UI labels — this was a raw @import of a Google
      // Fonts stylesheet inside a <style> block, which self-hosting never saw.
      { name: 'Roboto Mono', provider: 'google', weights: [400, 500, 700] },
    ],
  },
  nitro: {
    externals: {
      inline: ['pinia'],
    },
  },
  modules: [
    '@nuxt/eslint',
    '@nuxt/ui',
    ['@pinia/nuxt'],
  ],
  // The app has always been dark-only; @nuxt/ui pulls in @nuxtjs/color-mode,
  // which follows the system preference unless pinned. Flip this to 'system'
  // (and verify both themes) if light mode is ever wanted.
  colorMode: {
    preference: 'dark',
    fallback: 'dark',
  },
  runtimeConfig: {
    public: {
      mapboxToken: process.env.NUXT_PUBLIC_MAPBOX_TOKEN || '',
      apiBaseUrl: process.env.NUXT_PUBLIC_API_BASE_URL || '',
      version: process.env.NUXT_PUBLIC_VERSION || '',
      posthogKey: process.env.NUXT_PUBLIC_POSTHOG_KEY || '',
      posthogHost: process.env.NUXT_PUBLIC_POSTHOG_HOST || 'https://us.i.posthog.com',
    },
  },
})