import posthog from 'posthog-js'
import axios from 'axios'

export default defineNuxtPlugin(() => {
  const config = useRuntimeConfig()
  if (!config.public.posthogKey) return // no-op without a key, mirrors backend behavior

  posthog.init(config.public.posthogKey, {
    api_host: config.public.posthogHost,
    autocapture: false,
    disable_session_recording: true,
    capture_pageview: true,
  })

  // No axios.create() exists anywhere in this codebase — every composable shares the
  // module-level axios default instance, so one assignment here reaches every API call
  // (useSensorTimeseries.ts, useAnalysisFetch.ts, useDepthProfileFetch.ts, etc.) with zero
  // per-composable changes. This lets the API correlate frontend/backend events under one
  // PostHog identity instead of falling back to IP-based attribution.
  axios.defaults.headers.common['X-PostHog-Distinct-Id'] = posthog.get_distinct_id()
})
