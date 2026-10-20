export default defineNuxtPlugin(() => {
  const config = useRuntimeConfig()
  if (!config.public.umamiWebsiteId) return // no-op without a website ID, mirrors backend behavior

  // Umami's tracker records pageviews itself; custom events go through
  // useAnalytics.ts's trackEvent(). The API forwards each visitor's IP and
  // User-Agent with its own events, so Umami puts both in the same session
  // without any correlation header.
  const script = document.createElement('script')
  script.defer = true
  script.src = `${config.public.umamiHost}/script.js`
  script.dataset.websiteId = config.public.umamiWebsiteId
  // Share links carry the whole view in `#s=…`; keep that out of pageview URLs.
  script.dataset.excludeHash = 'true'
  document.head.appendChild(script)
})
