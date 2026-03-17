# Using gdal docker image to generate bathymetry contours from geotiff files.
# Docker image: ghcr.io/osgeo/gdal:ubuntu-full-latest


# generate contours with 10m interval, using ELEV as the attribute name, and output as geojson
for f in *tiff; do
    output=$(basename $f .tiff).geojson
    if [ -f $output ]; then
        echo "Output file $output already exists, skipping $f"
        continue
    fi
    gdal_contour -b 1 -a ELEV -i 10.0 -f "GEOJSON" $f $output
done


# filter to only include points with elevation <= 0, and save as contour_$f.geojson
# then remove the original geojson file to save space.
for f in NONNA*geojson; do
    ogr2ogr -f GEOJSON -where "ELEV <= 0" contour_$f $f
done

# Remove empty contour files
for f in contour_*.geojson; do
    if [ ! -s $f ]; then
        echo "Removing empty contour file $f"
        rm $f
    fi
done


# Merge all contour files into a single geojson file
ogrmerge.py -single -f GeoJSON -o merged_contours.geojson contour_*.geojson


# Generate vector tiles with tippecanoe, using merged_contours.geojson as input, and output to tiles directory, with layer name "nonna", and zoom levels 0-12.
# Use docker image metacollin/tippecanoe
tippecanoe --layer=nonna -pC --minimum-zoom=0 --maximum-zoom=14 contour_*.geojson -e tiles
