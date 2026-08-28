/**
 * Generic in-memory TTL + dedupe cache for async fetchers.
 *
 * Caches the in-flight Promise (not the resolved value), so concurrent calls
 * with the same key share one request instead of firing duplicates. Failed
 * requests are evicted immediately so errors are never cached. There is no
 * invalidation hook from the backend when its own cache is cleared (e.g. on
 * an SSC sync) — TTL is the only staleness bound, mirroring
 * `api/modules/response_cache.py`'s reasoning on the backend.
 */
export function createRequestCache<T>(ttlMs: number) {
    const store = new Map<string, { expires: number; promise: Promise<T> }>();

    function fetch(key: string, fn: () => Promise<T>): Promise<T> {
        const now = Date.now();
        const entry = store.get(key);
        if (entry && entry.expires > now) return entry.promise;

        const promise = fn().catch((err) => {
            store.delete(key);
            throw err;
        });
        store.set(key, { expires: now + ttlMs, promise });
        return promise;
    }

    function clear() {
        store.clear();
    }

    return { fetch, clear };
}
