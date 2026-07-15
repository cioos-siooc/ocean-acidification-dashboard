// shared/colormaps.json only stores each palette's base and "_i" (inverted
// lightness) forms. The "_r" (reversed) and "_i_r"/"_r_i" forms are derived
// here on demand — reversing a palette is just flipping its color order
// while keeping stop positions ascending, so storing those rows would be
// pure duplication.
type ColormapEntry = { name: string; stops: [number, string][]; [key: string]: any };
type ColormapMap = Record<string, ColormapEntry>;

export function baseColormapName(name: string): string {
    return name.replace(/_(?:i_r|r_i|i|r)$/, '');
}

export function colormapVariant(name: string): { inverted: boolean; reversed: boolean } {
    if (/_(?:i_r|r_i)$/.test(name)) return { inverted: true, reversed: true };
    if (name.endsWith('_i')) return { inverted: true, reversed: false };
    if (name.endsWith('_r')) return { inverted: false, reversed: true };
    return { inverted: false, reversed: false };
}

function reverseStops(stops: [number, string][]): [number, string][] {
    const n = stops.length;
    return stops.map((s, i) => [s[0], stops[n - 1 - i][1]]);
}

/** Resolves a colormap by name, deriving reversed variants that aren't stored directly. */
export function resolveColormap(colormaps: ColormapMap, name: string | null | undefined): ColormapEntry | null {
    if (!name) return null;
    if (colormaps[name]) return colormaps[name];

    const { inverted, reversed } = colormapVariant(name);
    if (!reversed) return null;

    const storedName = baseColormapName(name) + (inverted ? '_i' : '');
    const stored = colormaps[storedName];
    if (!stored || !Array.isArray(stored.stops)) return null;

    return { ...stored, name, stops: reverseStops(stored.stops) };
}

/** Builds the colormap name for a base + reverse/invert toggle state, or null if not available. */
export function resolveColormapName(colormaps: ColormapMap, base: string, inverted: boolean, reversed: boolean): string | null {
    const storedName = base + (inverted ? '_i' : '');
    if (!colormaps[storedName]) return null;
    return reversed ? `${storedName}_r` : storedName;
}
