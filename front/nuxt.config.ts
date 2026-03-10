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
        { rel: 'icon', type: 'image/png', href: '/OA_logo.png' }
      ]
    }
  },
  build: {
    transpile: ['vuetify'],
  },
  modules: [
    '@nuxt/eslint',
    ['@pinia/nuxt'],
    (_options, nuxt) => {
      nuxt.hooks.hook('vite:extendConfig', (config) => {
        // @ts-expect-error
        config.plugins.push(vuetify({ autoImport: true }))
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
      mapboxToken: '',
      apiBaseUrl: '',
    },
  },
})