import { shallowRef } from 'vue'

// Reads model values back out of the map's own raster image. The /png tiles are
// value-packed, not colour-coded (nc2tile.write_png_packed: 24-bit
// q = (value - base) / precision across R,G,B, alpha 0 = no data, lossless WebP);
// the colours come from the layer's `raster-color`. So the image already on
// screen is a full-domain grid we can sample under the cursor, no API call.

type ProbeGrid = {
    url: string
    width: number
    height: number
    values: Float32Array // canonical units, NaN = land / no data
    bounds: [number, number, number, number] // lonmin, latmin, lonmax, latmax
}

const mercX = (lon: number) => (lon + 180) / 360
const mercY = (lat: number) => {
    const s = Math.sin((lat * Math.PI) / 180)
    return 0.5 - Math.log((1 + s) / (1 - s)) / (4 * Math.PI)
}

async function decode(url: string, precision: number, base: number, bounds: ProbeGrid['bounds']): Promise<ProbeGrid> {
    const res = await fetch(url)
    if (!res.ok) throw new Error(`${res.status} ${url}`)
    // Raw bytes: any colour management or alpha premultiplication corrupts the packed integers.
    const bmp = await createImageBitmap(await res.blob(), { colorSpaceConversion: 'none', premultiplyAlpha: 'none' })
    const { width, height } = bmp
    const canvas = typeof OffscreenCanvas !== 'undefined'
        ? new OffscreenCanvas(width, height)
        : Object.assign(document.createElement('canvas'), { width, height })
    const ctx = canvas.getContext('2d', { willReadFrequently: true }) as CanvasRenderingContext2D
    ctx.drawImage(bmp, 0, 0)
    bmp.close()
    const { data } = ctx.getImageData(0, 0, width, height)
    const values = new Float32Array(width * height)
    for (let i = 0, p = 0; i < values.length; i++, p += 4) {
        values[i] = data[p + 3] ? ((data[p]! << 16) | (data[p + 1]! << 8) | data[p + 2]!) * precision + base : NaN
    }
    return { url, width, height, values, bounds }
}

export function useRasterProbe() {
    const grid = shallowRef<ProbeGrid | null>(null)
    let pending: string | null = null

    /** Decode the image at `url` unless it's already loaded or loading. Stale results are dropped. */
    async function load(url: string, precision: number, bounds: ProbeGrid['bounds'], base = 0) {
        if (grid.value?.url === url || pending === url) return
        pending = url
        try {
            const g = await decode(url, precision, base, bounds)
            if (pending === url) grid.value = g
        } catch (e) {
            if (pending === url) grid.value = null
            console.warn('raster probe: decode failed', e)
        } finally {
            if (pending === url) pending = null
        }
    }

    function clear() {
        pending = null
        grid.value = null
    }

    /** Nearest-pixel value (canonical units) at a point, or null outside the image / on land. */
    function sample(lon: number, lat: number): number | null {
        const g = grid.value
        if (!g) return null
        const [w, s, e, n] = g.bounds
        // The image source stretches linearly between its corners in Web Mercator.
        const fx = ((mercX(lon) - mercX(w)) / (mercX(e) - mercX(w))) * g.width
        const fy = ((mercY(lat) - mercY(n)) / (mercY(s) - mercY(n))) * g.height
        const x = Math.floor(fx)
        const y = Math.floor(fy)
        if (x < 0 || y < 0 || x >= g.width || y >= g.height) return null
        const v = g.values[y * g.width + x]!
        return Number.isNaN(v) ? null : v
    }

    return { grid, load, clear, sample }
}
