import vuetify, { transformAssetUrls } from 'vite-plugin-vuetify'

// https://nuxt.com/docs/api/configuration/nuxt-config
export default defineNuxtConfig({
  compatibilityDate: '2025-07-15',
  devtools: { enabled: true },
  app: {
    head: {
      script: [
        {
          async: true,
          src: 'https://analytics.oa.cioospacificlabs.ca/script.js',
          'data-website-id': 'cc6fb6f2-ce6e-4d01-9a4a-96a1efc0d801' // Replace with the ID from Umami
        }
      ],
      link: [
        { rel: 'icon', type: 'image/png', href: '/OA_logo.png' },
        { rel: 'preconnect', href: 'https://fonts.googleapis.com' },
        { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: '' },
        { rel: 'stylesheet', href: 'https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap' }
      ]
    }
  },
  build: {
    transpile: ['vuetify'],
  },
  nitro: {
    externals: {
      inline: ['pinia'],
    },
  },
  modules: [
    '@nuxt/eslint',
    ['@pinia/nuxt'],
    (_options, nuxt) => {
      nuxt.hooks.hook('vite:extendConfig', (config) => {
        // @ts-expect-error
        config.plugins.push(vuetify({ autoImport: { labs: true } }))
      })
    },
  ],
  vite: {
    vue: {
      template: {
        transformAssetUrls,
      },
    },
  },
  runtimeConfig: {
    public: {
      mapboxToken: process.env.NUXT_PUBLIC_MAPBOX_TOKEN || '',
      apiBaseUrl: process.env.NUXT_PUBLIC_API_BASE_URL || '',
      version: process.env.NUXT_PUBLIC_VERSION || '',
    },
  },
})