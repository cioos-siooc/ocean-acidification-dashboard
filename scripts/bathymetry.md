# Bathymetry tiling

Two independent pipelines turn source elevation data into what the frontend renders. Both
read GeoTIFF as their common input format, so any new data source just needs to be converted
to GeoTIFF first (see below).

## 1. Raster depth tiles (`cnvMaster_RGBcoded.py`)

Produces the WebP XYZ tile pyramid served by the API at `GET /raster_tiles/{z}/{x}/{y}.webp`
(`RASTER_TILES_ROOT`, default `/opt/data/bathymetry/NONNA/raster_tiles`) and rendered by
`addBathymetryTilesLayer()` in `front/app/pages/index.vue`.

- Reads each GeoTIFF directly with `rasterio` (no whole-array load — it warps one tile window
  at a time, so multi-GB source files are fine).
- Packs each pixel's depth into the tile's RGB channels as a 24-bit fixed-point integer
  (`(depth - MIN_ORG) / STEP`, clamped to `[1, MAX_IDX]`); alpha 0 = nodata/land (transparent).
- `MIN_ORG = -3000`, `STEP = 1` in the script header — covers depths down to -3000 m at 1 m
  precision. Adjust these if a new source has deeper points or needs coarser/finer precision;
  values `>= 0` (land) are always masked transparent regardless of `MIN_ORG`.
- `MIN_ZOOM = MAX_ZOOM = 14` — single zoom level today. Bump the range if the frontend needs
  more zooms.

Run:

```bash
cd scripts
.venv/bin/python cnvMaster_RGBcoded.py --tiff-dir <dir_with_geotiffs> --pattern "<glob>.tiff"
```

- `--pattern` defaults to `NONNA*.tiff` (the original NONNA source naming). Pass a different
  glob for other sources.
- Output goes to `./raster_tiles/{z}/{x}/{y}.webp` relative to cwd (`OUTPUT_DIR` in the script).
  Deploy by copying/syncing that tree to wherever `RASTER_TILES_ROOT` points in the target
  environment.
- Tiles are additive across input files: if a tile already exists from a previous file, new
  data only fills in the still-transparent pixels — so multiple overlapping/adjacent source
  files can be processed into the same output tree without clobbering each other.

## 2. Vector depth contours (`bathymetry.sh`)

Separate pipeline, unrelated to the raster tiles above — generates 10 m interval depth contour
lines as vector tiles, served at `GET /vector/{z}/{x}/{y}.pbf` (`VECTOR_ROOT`, default
`/opt/data/bathymetry/NONNA/tiles`) and toggled by the "Bathymetry Contours" button in
`overlays.vue`.

**Standalone — no local GDAL/tippecanoe install required.** Every step runs inside its upstream
Docker image (`ghcr.io/osgeo/gdal:ubuntu-full-latest` for `gdal_contour`, `metacollin/tippecanoe`
for `tippecanoe`), with the input tiff's directory bind-mounted into the container and output
written back next to the input, owned by the invoking user (not root).

Takes a single GeoTIFF path (not a directory of many small tiles like NONNA's) — a good fit for
consolidated single-file sources like the west coast DEM (see below).

Run:

```bash
/path/to/scripts/bathymetry.sh <path/to/source.tiff>
```

Requires Docker only (able to pull `ghcr.io/osgeo/gdal:ubuntu-full-latest` and
`metacollin/tippecanoe`). Output, written next to the input tiff:

| File | Purpose |
|---|---|
| `<name>.geojson` | raw 10 m contours (both above and below 0) |
| `contour_<name>.geojson` | filtered to `ELEV <= 0` (underwater only) |
| `tiles/` | the vector tile pyramid (zoom 0–14, layer `nonna`) — copy/sync this to wherever `VECTOR_ROOT` points |

- The underwater-only filter is a separate `ogr2ogr -where "ELEV <= 0"` pass, not a tippecanoe
  `-j`/feature-filter. tippecanoe does support filtering features during tiling, but its filter
  argument must be a JSON object/hash (not a bare Mapbox GL Style expression array) and the exact
  schema wasn't confirmed against the `metacollin/tippecanoe` image — it errored with `filter is
  not a hash` when tried. `ogr2ogr -where` is the proven, well-documented alternative.
- Not tied to NONNA's filename convention, despite the `nonna` tippecanoe layer name left over
  from that source. Works unmodified for any GeoTIFF.
- GDAL's GeoJSON reader caps individual object reads at 200MB by default
  (`OGR_GEOJSON_MAX_OBJ_SIZE`) — too small for a large consolidated DEM's contour file (the west
  coast DEM's came out well over 1GB). The script sets `OGR_GEOJSON_MAX_OBJ_SIZE=0` (unlimited)
  on every GDAL container invocation to avoid `GeoJSON object too complex/large` read errors.
- Idempotent: already-generated `<name>.geojson` and `contour_<name>.geojson` are skipped — safe
  to re-run after an interruption without redoing finished steps. The final `tippecanoe` step
  always overwrites `tiles/` from scratch (tippecanoe has no incremental mode), so processing a
  second source tiff into the same directory replaces rather than merges the previous tile set —
  run it once against a single consolidated tiff, or merge sources at the GeoTIFF/GeoJSON stage
  first if you need multiple inputs combined into one tile set.

## Converting a non-GeoTIFF source (e.g. an Esri File Geodatabase raster)

Some bathymetry/DEM sources don't ship as GeoTIFF — e.g. a `.gdb` (Esri File Geodatabase)
containing a raster dataset. `rasterio`'s bundled GDAL build (used by `cnvMaster_RGBcoded.py`)
does **not** include the OpenFileGDB raster driver, so it can't open `.gdb` rasters directly —
confirmed by testing against `scripts/.venv`'s rasterio (1.5.0 / GDAL 3.12.1), which fails both
on the bare `.gdb` path and on the `OpenFileGDB:"...":<layer>` subdataset syntax.

The fix is a one-time conversion using a full GDAL build (the same
`ghcr.io/osgeo/gdal:ubuntu-full-latest` image `bathymetry.sh` already uses), which *does*
include OpenFileGDB raster support.

**Inspect the source first** to find the raster layer name and check its value range (needed to
sanity-check `MIN_ORG`/`STEP` above):

```bash
docker pull ghcr.io/osgeo/gdal:ubuntu-full-latest   # once

docker run --rm -v "<path_to>.gdb:/data/src.gdb:ro" \
  ghcr.io/osgeo/gdal:ubuntu-full-latest \
  gdalinfo /data/src.gdb
# lists SUBDATASET_N_NAME=OpenFileGDB:"/data/src.gdb":<LAYER_NAME> entries

docker run --rm -v "<path_to>.gdb:/data/src.gdb:ro" \
  ghcr.io/osgeo/gdal:ubuntu-full-latest \
  gdalinfo -stats 'OpenFileGDB:"/data/src.gdb":<LAYER_NAME>'
# shows CRS, resolution, and min/max band values
```

**Convert the chosen raster layer to GeoTIFF:**

```bash
docker run --rm \
  -v "<path_to>.gdb:/data/src.gdb:ro" \
  -v "<output_dir>:/out" \
  ghcr.io/osgeo/gdal:ubuntu-full-latest \
  gdal_translate \
    -of GTiff \
    -co COMPRESS=DEFLATE \
    -co PREDICTOR=3 \
    -co TILED=YES \
    -co BIGTIFF=YES \
    'OpenFileGDB:"/data/src.gdb":<LAYER_NAME>' \
    /out/<output_name>.tiff
```

- `PREDICTOR=3` is the floating-point DEFLATE predictor — significantly better compression
  ratio for Float32 elevation bands than the default.
- `BIGTIFF=YES` is required whenever the uncompressed raster exceeds ~4 GB (check
  `width * height * 4 bytes` for a Float32 band).
- No reprojection needed here — `cnvMaster_RGBcoded.py` reprojects per-tile from whatever CRS
  the source GeoTIFF reports (it doesn't need to be pre-warped to Web Mercator).

Then run `cnvMaster_RGBcoded.py` as in section 1, pointing `--tiff-dir`/`--pattern` at the
converted file(s).

### Example: `canada_west_coast_DEM_original.gdb`

Contains one raster dataset, `WEST_COAST_DEM` (86117×94543 px, Float32, 10 m, NAD83/BC Albers
EPSG:3005, -2767 m to +2465 m — combined land+sea DEM) plus `WEST_COAST_DEM_RLF` (an 8-bit
hillshade rendering of the same extent, not needed for depth tiles).
