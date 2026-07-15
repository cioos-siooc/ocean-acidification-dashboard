#!/usr/bin/env bash
set -euo pipefail

# Standalone bathymetry contour + vector-tile pipeline.
#
# Turns a single source GeoTIFF into a vector tile pyramid of underwater
# depth contours (10m intervals), served by the API at
# GET /vector/{z}/{x}/{y}.pbf and toggled by the "Bathymetry Contours"
# button in the frontend (overlays.vue).
#
# No local GDAL/tippecanoe install required — every step runs inside its
# upstream Docker image, with the input tiff's directory bind-mounted at
# /data. Output files are written next to the input tiff, owned by the
# invoking user (not root).
#
# Usage:
#   /path/to/bathymetry.sh <path/to/source.tiff>
#
# Works on any GeoTIFF source, not just the original NONNA files (despite
# leftover "nonna" naming in the tippecanoe layer name below). Safe to
# re-run: already-generated files are skipped.
#
# Images:
#   ghcr.io/osgeo/gdal:ubuntu-full-latest  - gdal_contour, ogr2ogr
#   metacollin/tippecanoe                   - tippecanoe
#
# (tippecanoe has a -j/--feature-filter flag that can filter features during
# tiling, which would let it apply the ELEV <= 0 underwater-only filter
# directly and skip the ogr2ogr pass below — but its filter argument must be
# a JSON object/hash, not a bare expression array, and the exact schema
# wasn't confirmed against this image, so filtering stays a separate,
# well-established ogr2ogr -where step instead.)
#
# Output (written next to the input tiff):
#   <name>.geojson           - raw 10m contours (both above and below 0),
#                               in the source tiff's original CRS
#   contour_<name>.geojson   - filtered to ELEV <= 0 (underwater only) and
#                               reprojected to WGS84 (EPSG:4326)
#   tiles/                    - vector tile pyramid (zoom 0-14, layer "nonna")

if [ $# -ne 1 ]; then
    echo "Usage: $0 <path/to/source.tiff>" >&2
    exit 1
fi

TIFF_PATH="$1"
if [ ! -f "$TIFF_PATH" ]; then
    echo "File not found: $TIFF_PATH" >&2
    exit 1
fi

GDAL_IMAGE="ghcr.io/osgeo/gdal:ubuntu-full-latest"
TIPPECANOE_IMAGE="metacollin/tippecanoe"
DATA_DIR="$(cd "$(dirname "$TIFF_PATH")" && pwd)"
TIFF_FILE="$(basename "$TIFF_PATH")"
BASE_NAME="${TIFF_FILE%.*}"
GEOJSON="${BASE_NAME}.geojson"
CONTOUR_GEOJSON="contour_${BASE_NAME}.geojson"

gdal() {
    # OGR_GEOJSON_MAX_OBJ_SIZE=0 disables GDAL's default 200MB-per-object cap on GeoJSON
    # reads — the merged contour file for a large DEM can be well over that.
    docker run --rm --user "$(id -u):$(id -g)" -e OGR_GEOJSON_MAX_OBJ_SIZE=0 \
        -v "$DATA_DIR:/data" -w /data "$GDAL_IMAGE" "$@"
}

tippecanoe() {
    # --entrypoint override makes this work regardless of the image's default
    # entrypoint, since we only want to run the tippecanoe binary with args.
    docker run --rm --user "$(id -u):$(id -g)" -v "$DATA_DIR:/data" -w /data \
        --entrypoint tippecanoe "$TIPPECANOE_IMAGE" "$@"
}

# generate contours with 10m interval, using ELEV as the attribute name, and output as geojson
if [ -f "$DATA_DIR/$GEOJSON" ]; then
    echo "Output file $GEOJSON already exists, skipping contour generation"
else
    gdal gdal_contour -b 1 -a ELEV -i 10.0 -f "GEOJSON" "$TIFF_FILE" "$GEOJSON"
fi

# filter to only include points with elevation <= 0 (underwater), reproject to WGS84 lon/lat
# (GeoJSON/tippecanoe expect EPSG:4326 — gdal_contour writes output in the source tiff's CRS,
# e.g. NAD83/BC Albers EPSG:3005 for the west coast DEM, which tippecanoe would otherwise
# misread as lon/lat and clip almost everything as out of range), and save as
# contour_<name>.geojson
if [ -f "$DATA_DIR/$CONTOUR_GEOJSON" ]; then
    echo "Output file $CONTOUR_GEOJSON already exists, skipping filter step"
else
    gdal ogr2ogr -f GEOJSON -t_srs EPSG:4326 -where "ELEV <= 0" "$CONTOUR_GEOJSON" "$GEOJSON"
fi

if [ ! -s "$DATA_DIR/$CONTOUR_GEOJSON" ]; then
    echo "$CONTOUR_GEOJSON is empty — no underwater (ELEV <= 0) contours found in $TIFF_FILE" >&2
    exit 1
fi

# Generate vector tiles with tippecanoe, using contour_<name>.geojson as input, and output to
# tiles directory, with layer name "nonna", and zoom levels 0-14.
tippecanoe --force --layer=nonna -pC --minimum-zoom=0 --maximum-zoom=14 "$CONTOUR_GEOJSON" -e tiles
