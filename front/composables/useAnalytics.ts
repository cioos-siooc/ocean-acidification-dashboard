declare global {
  interface Window {
    umami?: { track: (event: string, data?: Record<string, unknown>) => void }
  }
}

/** Fire a custom Umami event. No-ops during SSR, without a website ID, and
 *  before the tracker script (injected by umami.client.ts) has loaded. */
export function trackEvent(event: string, properties?: Record<string, unknown>) {
  if (import.meta.client) window.umami?.track(event, properties)
}
