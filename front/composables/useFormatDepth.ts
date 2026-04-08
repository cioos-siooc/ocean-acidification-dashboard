export function formatDepth(d: number) {
    if (d === -1) { return 'bottom' }
    return `${Number(d).toFixed(1)}`
}