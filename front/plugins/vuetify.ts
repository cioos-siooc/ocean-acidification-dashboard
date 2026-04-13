import { createVuetify } from 'vuetify'
import { VVideo } from 'vuetify/labs'


export default defineNuxtPlugin((nuxtApp) => {
  const vuetify = createVuetify({
    ssr: true,
    theme: {
      defaultTheme: 'dark',
    },
    components: {
      VVideo,
    },
  })
  nuxtApp.vueApp.use(vuetify)
})
