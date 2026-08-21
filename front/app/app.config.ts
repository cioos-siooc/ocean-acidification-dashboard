export default defineAppConfig({
  ui: {
    // Neutral blue primary — closest match to the stock Vuetify palette this app
    // ran on before the migration, so the swap stays like-for-like rather than a
    // redesign. Change this one line (or point it at a custom palette defined in
    // assets/css/main.css) to rebrand the whole UI.
    colors: {
      primary: 'blue',
      neutral: 'slate',
    },
  },
})
