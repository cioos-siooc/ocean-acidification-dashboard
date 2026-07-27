import posthog from 'posthog-js'

/** Fire a custom PostHog event. No-ops during SSR since posthog-js is only
 *  initialized in the client-only plugin (posthog.client.ts). */
export function trackEvent(event: string, properties?: Record<string, unknown>) {
  if (import.meta.client) posthog.capture(event, properties)
}
