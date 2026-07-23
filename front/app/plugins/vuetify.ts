import '@mdi/font/css/materialdesignicons.css'
import 'vuetify/styles'
import { createVuetify } from 'vuetify'

export default defineNuxtPlugin((nuxtApp) => {
  const vuetify = createVuetify({
    ssr: true,
    defaultTheme: 'dark',
    defaults: {
      global: {
        density: 'compact',
        hideDetails: 'auto',
      },
      VTextField: {
        variant: 'outlined',
      },
      VSelect: {
        variant: 'outlined',
      },
      VAutocomplete: {
        variant: 'outlined',
      },
      VCombobox: {
        variant: 'outlined',
      },
    },
  })
  nuxtApp.vueApp.use(vuetify)
})
