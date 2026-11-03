import { ref, readonly } from 'vue';
import axios from 'axios';
import { useRuntimeConfig } from '#app';

// Polls the API's GET /health so the UI can say *why* nothing is loading.
//   ok       — API answered and ClickHouse is reachable
//   degraded — API answered 503 {status: 'degraded'}: up, but its database isn't
//   down     — no usable answer (network error, timeout, proxy 5xx, CORS-less error page)
export type ApiStatus = 'checking' | 'ok' | 'degraded' | 'down';

const POLL_OK_MS = 60_000;
const POLL_FAILING_MS = 15_000;
// One failed check is retried quickly before it's reported, so a single dropped
// request doesn't flash an outage banner.
const CONFIRM_RETRY_MS = 3_000;
const REQUEST_TIMEOUT_MS = 8_000;

// Module-level: one poller shared by every consumer.
const status = ref<ApiStatus>('checking');
const lastCheckedAt = ref<number | null>(null);
// Set when the API comes back after being reported failing this session —
// data fetched once at startup (variables, colormaps) needs a reload.
const recovered = ref(false);

let failures = 0;
let timer: ReturnType<typeof setTimeout> | null = null;
let inFlight: Promise<void> | null = null;
let consumers = 0;
let baseUrl = '';

function onWake() {
    if (document.visibilityState === 'visible') void check();
}

async function probe(): Promise<ApiStatus> {
    try {
        await axios.get(`${baseUrl}/health`, { timeout: REQUEST_TIMEOUT_MS });
        return 'ok';
    } catch (e) {
        const res = axios.isAxiosError(e) ? e.response : undefined;
        return res?.status === 503 && res.data?.status === 'degraded' ? 'degraded' : 'down';
    }
}

function schedule(ms: number) {
    if (timer) clearTimeout(timer);
    timer = consumers > 0 ? setTimeout(() => { void check(); }, ms) : null;
}

function check(): Promise<void> {
    if (inFlight) return inFlight;
    inFlight = (async () => {
        const result = await probe();
        lastCheckedAt.value = Date.now();
        if (result === 'ok') {
            if (status.value === 'degraded' || status.value === 'down') recovered.value = true;
            failures = 0;
            status.value = 'ok';
            schedule(POLL_OK_MS);
            return;
        }
        failures += 1;
        if (failures < 2) {
            schedule(CONFIRM_RETRY_MS);
            return;
        }
        recovered.value = false;
        status.value = result;
        schedule(POLL_FAILING_MS);
    })().finally(() => { inFlight = null; });
    return inFlight;
}

export function useApiStatus() {
    baseUrl = useRuntimeConfig().public.apiBaseUrl as string;

    // Client-only; call from onMounted / pair with stop() in onBeforeUnmount.
    function start() {
        consumers += 1;
        if (consumers > 1) return;
        window.addEventListener('online', onWake);
        document.addEventListener('visibilitychange', onWake);
        void check();
    }

    function stop() {
        consumers = Math.max(0, consumers - 1);
        if (consumers > 0) return;
        window.removeEventListener('online', onWake);
        document.removeEventListener('visibilitychange', onWake);
        if (timer) clearTimeout(timer);
        timer = null;
    }

    return {
        status: readonly(status),
        lastCheckedAt: readonly(lastCheckedAt),
        recovered,
        recheck: check,
        start,
        stop,
    };
}
