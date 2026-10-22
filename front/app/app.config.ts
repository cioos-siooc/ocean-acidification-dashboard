export default defineAppConfig({
  ui: {
    // Both palettes are defined in assets/css/main.css (the v2 "Instrument"
    // tokens): one ochre accent and a cool graphite neutral.
    colors: {
      primary: 'ochre',
      neutral: 'graphite',
    },
    // One accent: buttons are neutral unless a caller asks for `primary`
    // (the main action of a dialog, say), so ochre keeps meaning "selected".
    button: {
      defaultVariants: {
        color: 'neutral',
      },
    },
  },
})
