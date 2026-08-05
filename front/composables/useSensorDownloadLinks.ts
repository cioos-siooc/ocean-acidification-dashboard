// Builds per-variable data-access links for a sensor from its `source` metadata.
//
// ERDDAP (`{"api": "ERDDAP", "link": "https://…/griddap/orca4_L3_depthgridded_025"}`)
// gives direct NetCDF/CSV downloads. They're offered one variable at a time
// rather than whole-dataset: a request for every variable over a full
// multi-year record runs into ERDDAP's per-request size limits (and would be
// ~1 GB for the ORCA grids). The two ERDDAP layouts need different queries:
//   - griddap  `?varName` — no dimension subset, so ERDDAP returns the variable
//     over its full grid whatever its dimensionality.
//   - tabledap `?time,varName` — the axis columns have to be requested
//     explicitly or they don't come back at all.
//
// ONC (`{"api": "ONC", "link": "https://data.oceannetworks.ca/DataSearch?location=FGPPN"}`)
// has no equivalent direct-download URL — Oceans 3.0 is a cart/order flow that
// needs an account. The best we can do is deep-link each variable to its
// instrument: `?location=X&deviceCategory=Y` opens Data Search with that
// instrument selected and its "Select This Data Source" button ready.

const ONC_DATA_SEARCH = 'https://data.oceannetworks.ca/DataSearch';

export interface SensorSourceLike {
    depth: number;
    // `{canonical: {name, unit, conversion_factor}}` as stored in CH `sensors.variables`.
    variables: Record<string, any>;
    // ONC only: `{locationCode, codes: [{deviceCategoryCode, sensorCategoryCodes}]}`.
    device_config?: Record<string, any> | null;
    source?: { api?: string; link?: string } | null;
}

export interface SensorVariableDownload {
    /** Canonical variable key, e.g. `temperature`. */
    canonical: string;
    /** The variable's name at the source — ERDDAP variable or ONC sensorCategoryCode. */
    sourceName: string;
    /** ERDDAP only — direct downloads. */
    nc?: string;
    csv?: string;
    /** ONC only — deep link to the variable's instrument in Oceans 3.0. */
    page?: string;
}

export interface SensorDownloads {
    api: 'ERDDAP' | 'ONC';
    /** The source's own landing page: ERDDAP's data-access form, or ONC Data Search. */
    dataset: string;
    variables: SensorVariableDownload[];
}

/** Strip a trailing slash and any response-format suffix already on the link. */
function normalizeBase(link: string): string {
    return link.trim()
        .replace(/\/+$/, '')
        .replace(/\.(html|nc|csv|json|htmlTable|graph|das|dds)$/i, '');
}

/**
 * The variables to offer, as `[canonical, sourceName]` pairs.
 *
 * `only` is the caller's list of variables the app displays (see
 * `useVariableRegistry`), and it drives both the selection and the ordering so
 * the download rows line up with the dialog's variable list. Without it this
 * falls back to every mapped variable minus the `time`/`depth` axis entries,
 * which describe columns rather than data.
 */
function dataVariables(
    variables: Record<string, { name?: string }>,
    only?: string[],
): Array<[string, string]> {
    const keys = only ?? Object.keys(variables).filter(k => k !== 'time' && k !== 'depth');
    return keys
        .filter(canonical => !!variables[canonical]?.name)
        .map(canonical => [canonical, variables[canonical]!.name as string]);
}

function erddapDownloads(sensor: SensorSourceLike, link: string, only?: string[]): SensorDownloads {
    const base = normalizeBase(link);
    const isGriddap = base.includes('/griddap/');
    const isTabledap = base.includes('/tabledap/');
    if (!isGriddap && !isTabledap) return { api: 'ERDDAP', dataset: `${base}.html`, variables: [] };

    const variables = (sensor.variables ?? {}) as Record<string, { name?: string }>;
    // Axis column names, overridable through the `time`/`depth` entries — same
    // convention `sensors/erddap_to_ch.py` follows when building its query.
    const timeCol = variables.time?.name || 'time';
    const depthCol = variables.depth?.name || 'depth';
    // A depth column only exists on variable-depth (profiler) datasets, and
    // asking for a column the dataset doesn't have is an ERDDAP error. On
    // griddap, depth is a dimension and comes back with the variable anyway.
    const variableDepth = sensor.depth == null || sensor.depth < 0;
    const axisCols = isTabledap ? [timeCol, ...(variableDepth ? [depthCol] : [])] : [];

    const vars = dataVariables(variables, only);
    const downloads = vars.map(([canonical, sourceName]) => {
        const query = [...axisCols, sourceName].join(',');
        return {
            canonical,
            sourceName,
            nc: `${base}.nc?${query}`,
            csv: `${base}.csv?${query}`,
        };
    });

    // Hakai's ERDDAP bounces *any* query-less tabledap URL — the data access
    // form included — back to its dataset index, so the form link carries the
    // full column list. It also pre-selects this sensor's variables there.
    const allCols = [...axisCols, ...vars.map(([, name]) => name)].join(',');
    const dataset = isTabledap && allCols ? `${base}.html?${allCols}` : `${base}.html`;

    return { api: 'ERDDAP', dataset, variables: downloads };
}

function oncDownloads(sensor: SensorSourceLike, link: string, only?: string[]): SensorDownloads {
    const config = sensor.device_config ?? {};
    const locationCode: string | undefined = config.locationCode;
    const dataset = link || (locationCode ? `${ONC_DATA_SEARCH}?location=${encodeURIComponent(locationCode)}` : '');
    if (!locationCode) return { api: 'ONC', dataset, variables: [] };

    // `codes` maps instruments to the sensors they carry, so invert it to find
    // the instrument that reports a given variable. `sensorCategoryCodes` is a
    // comma-separated list in the ONC catalogue's own format.
    const instrumentBySensorCode = new Map<string, string>();
    for (const entry of (config.codes ?? []) as Array<Record<string, string>>) {
        const deviceCategory = entry?.deviceCategoryCode;
        if (!deviceCategory) continue;
        for (const code of String(entry.sensorCategoryCodes ?? '').split(',')) {
            const trimmed = code.trim();
            if (trimmed) instrumentBySensorCode.set(trimmed, deviceCategory);
        }
    }

    const variables = dataVariables((sensor.variables ?? {}) as Record<string, { name?: string }>, only)
        .map(([canonical, sourceName]) => {
            const deviceCategory = instrumentBySensorCode.get(sourceName);
            // Every variable stays listed even without an instrument match — the
            // row is the sensor's variable list first and a link second. Such a
            // variable just gets no link; the station link covers it.
            if (!deviceCategory) return { canonical, sourceName };
            const query = `location=${encodeURIComponent(locationCode)}&deviceCategory=${encodeURIComponent(deviceCategory)}`;
            return { canonical, sourceName, page: `${ONC_DATA_SEARCH}?${query}` };
        });

    return { api: 'ONC', dataset, variables };
}

export function sensorDownloads(
    sensor: SensorSourceLike | null | undefined,
    only?: string[],
): SensorDownloads | null {
    const link = sensor?.source?.link;
    if (!sensor) return null;
    if (sensor.source?.api === 'ERDDAP') return link ? erddapDownloads(sensor, link, only) : null;
    if (sensor.source?.api === 'ONC') return oncDownloads(sensor, link ?? '', only);
    return null;
}
