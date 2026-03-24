import numpy as np
import rasterio
from rasterio.warp import reproject, Resampling
from rasterio.transform import from_bounds
from rasterio.crs import CRS
import multiprocessing
import os
import cv2
import math
import glob
import argparse

# pip3 install rasterio opencv-python-headless numpy


###################################################################
###########################  CONFIG  ##############################

TIFF_PATTERN = 'NONNA*.tiff'   # glob pattern for input GeoTIFF files
OUTPUT_DIR   = 'raster_tiles'   # all files write here (flat mosaic tileset)
MIN_ZOOM  = 14
MAX_ZOOM  = 14
MIN_ORG   = -3000        # minimum encodable value
STEP      = 1            # value precision / color-table step
TILE_SIZE = 512          # tile size in pixels


###################################################################
###########################  CONSTANTS  ###########################

R            = 6378137           # Web Mercator Earth radius (m)
MAX_TILE_LAT = 85.0511287798066
WEB_MERCATOR = CRS.from_epsg(3857)
MAX_IDX      = math.ceil(-MIN_ORG / STEP)   # number of encodable depth levels


###################################################################
###########################  COLOR TABLE  #########################

def _build_color_table():
    """Pre-build a BGRA lookup table (OpenCV order).

    Index 0  →  [0,0,0,0]  transparent / nodata
    Index k  →  BGRA encoding of integer (k-1)

    Encoding: value_index = (depth - MIN_ORG) / STEP
    Decoding: depth = (stored_int + 1) * STEP + MIN_ORG
    """
    size   = MAX_IDX + 2
    colors = np.zeros((size, 4), dtype=np.uint8)
    for k in range(1, size):
        i = k - 1
        r = i // (256 * 256)
        g = (i - r * 256 * 256) // 256
        b = i - 256 * (256 * r + g)
        colors[k] = [b, g, r, 255]
    return colors

ALL_COLORS = _build_color_table()


###################################################################
###########################  HELPERS  #############################

def _lon_to_mx(lon): return R * np.radians(lon)
def _lat_to_my(lat): return R * np.log(np.tan(np.pi / 4 + np.radians(lat) / 2))


def _tile_mercator_bounds(z, tx, ty):
    """Return (mx_min, my_min, mx_max, my_max) in Web Mercator for XYZ tile."""
    n       = 2 ** z
    lon_min = -180.0 + 360.0 * tx       / n
    lon_max = -180.0 + 360.0 * (tx + 1) / n
    lat_max = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * ty       / n))))
    lat_min = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (ty + 1) / n))))
    return _lon_to_mx(lon_min), _lat_to_my(lat_min), _lon_to_mx(lon_max), _lat_to_my(lat_max)


def _tile_range_for_extent(z, mx_min, my_min, mx_max, my_max):
    """Return (tx_min, ty_min, tx_max, ty_max) of XYZ tiles covering a Mercator bbox."""
    n    = 2 ** z
    half = R * math.pi
    tx_min = max(0,     int(math.floor((mx_min + half) / (2 * half) * n)))
    tx_max = min(n - 1, int(math.floor((mx_max + half) / (2 * half) * n)))
    ty_min = max(0,     int(math.floor((half - my_max) / (2 * half) * n)))
    ty_max = min(n - 1, int(math.floor((half - my_min) / (2 * half) * n)))
    return tx_min, ty_min, tx_max, ty_max


###################################################################
###########################  TILE WORKER  #########################

# Filepath set once per worker process (avoids re-opening on every tile call)
_filepath = None
_src_crs  = None
_src_transform = None
_nodata   = None

def _init_worker(filepath):
    global _filepath, _src_crs, _src_transform, _nodata
    _filepath = filepath
    with rasterio.open(filepath) as src:
        _src_crs       = src.crs
        _src_transform = src.transform
        _nodata        = src.nodata


def render_tile(z, tx, ty):
    """Warp source file directly into tile bounds using rasterio (fast C path)."""
    if (tx != 2577 or ty != 5639):
        return  # TEMP: render only one tile for testing
    
    mx_min, my_min, mx_max, my_max = _tile_mercator_bounds(z, tx, ty)

    dst_transform = from_bounds(mx_min, my_min, mx_max, my_max, TILE_SIZE, TILE_SIZE)
    vals = np.full((TILE_SIZE, TILE_SIZE), np.nan, dtype=np.float32)

    with rasterio.open(_filepath) as src:
        reproject(
            source        = rasterio.band(src, 1),
            destination   = vals,
            src_transform = _src_transform,
            src_crs       = _src_crs,
            dst_transform = dst_transform,
            dst_crs       = WEB_MERCATOR,
            resampling    = Resampling.bilinear,
            src_nodata    = _nodata,
            dst_nodata    = np.nan,
        )

    vals[vals < MIN_ORG] = np.nan
    vals[vals >= 0] = np.nan  # Mark land/above-sea-level as no data (transparent) instead of forcing to 0

    has_data = ~np.isnan(vals)
    if not has_data.any():
        return

    idx = np.where(
        has_data,
        np.clip(((vals - MIN_ORG) / STEP).astype(int) + 1, 1, len(ALL_COLORS) - 1),
        0   # transparent for nodata
    )

    img = ALL_COLORS[idx].astype(np.uint8)

    tile_dir  = os.path.join(OUTPUT_DIR, str(z), str(tx))
    os.makedirs(tile_dir, exist_ok=True)
    tile_path = os.path.join(tile_dir, f'{ty}.webp')

    # If the tile already exists (written by an adjacent file), composite:
    # existing non-transparent pixels win; fill the rest with new data.
    if os.path.exists(tile_path):
        existing = cv2.imread(tile_path, cv2.IMREAD_UNCHANGED)
        if existing is not None and existing.shape == img.shape:
            mask = existing[:, :, 3] > 0
            img[mask] = existing[mask]

    cv2.imwrite(tile_path, img)
    # plot_tile(img)


###################################################################
###########################  DATA LOADER  #########################

def _get_mercator_bounds(filepath):
    """Return (mx_min, my_min, mx_max, my_max) of the file in Web Mercator,
    and print basic info. Returns None if file has no usable data."""
    with rasterio.open(filepath) as src:
        from rasterio.warp import transform_bounds
        bounds = transform_bounds(src.crs, WEB_MERCATOR, *src.bounds)
        nodata = src.nodata
        print(f'  CRS={src.crs.to_epsg()}  nodata={nodata}  '
              f'size={src.width}x{src.height}  bounds={[round(b) for b in bounds]}')
    return bounds   # (mx_min, my_min, mx_max, my_max)


###################################################################
###########################  MAIN  ################################

def main(tiff_dir='.'):
    pattern = os.path.join(tiff_dir, TIFF_PATTERN)
    tiff_files = sorted(glob.glob(pattern))
    if not tiff_files:
        print(f'No GeoTIFF files found matching: {pattern}')
        return

    print(f'Found {len(tiff_files)} GeoTIFF file(s)')

    for filepath in tiff_files:
        print(f'\n=== {filepath} ===')

        mx_min, my_min, mx_max, my_max = _get_mercator_bounds(filepath)

        # Collect ALL tile args across ALL zoom levels up front
        tile_args = []
        for zoom in range(MIN_ZOOM, MAX_ZOOM + 1):
            tx_min, ty_min, tx_max, ty_max = _tile_range_for_extent(
                zoom, mx_min, my_min, mx_max, my_max
            )
            for tx in range(tx_min, tx_max + 1):
                for ty in range(ty_min, ty_max + 1):
                    tile_args.append((zoom, tx, ty))
            print(f'  Zoom {zoom}: {(tx_max-tx_min+1)*(ty_max-ty_min+1)} tiles')

        print(f'  Total tiles: {len(tile_args)}')

        # One Pool for all zoom levels — workers keep the file open
        with multiprocessing.Pool(
            initializer = _init_worker,
            initargs    = (filepath,),
        ) as pool:
            pool.starmap(render_tile, tile_args)


def plot_tile(img):
    import matplotlib.pyplot as plt
    plt.imshow(img)
    plt.title('Rendered Tile (BGRA)')
    plt.axis('off')
    plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert GeoTIFF files to Web Mercator raster tiles')
    parser.add_argument(
        '--tiff-dir',
        default='.',
        help='Directory containing TIFF files (default: current directory)'
    )
    args = parser.parse_args()
    main(tiff_dir=args.tiff_dir)



###################################################################
def decode_image_to_values(image_path):
    """Decode a raster tile image back to depth values using the inverse of the encoding scheme.
    """
    import matplotlib.pyplot as plt
    
    img = cv2.imread(image_path, cv2.IMREAD_UNCHANGED)
    if img is None:
        return None

    # Decode the packed 24-bit integer values
    vals = img[:, :, 2].astype(np.float32) * 65536 + img[:, :, 1].astype(np.float32) * 256 + img[:, :, 0].astype(np.float32)
    # vals = vals / 255.0
    print(vals)

    # Convert back to physical values
    base = -3000
    vals = vals + base

    plt.imshow(vals, cmap='viridis')
    plt.colorbar(label='Depth (m)')
    plt.title('Decoded Depth Values from Tile')
    plt.show()