# A python script to create a webp 512x512 file with color gradient from (0,0,0,255) to (0,0,255,255) from left to right, and save it to disk. This is used for testing the raster tile rendering in the frontend.
import numpy as np
import cv2

def create_test_tile(filename: str):
    # Create a 512x512 image with a horizontal gradient from black to blue
    width, height = 512, 512
    tile = np.zeros((height, width, 4), dtype=np.uint8)
    
    for x in range(width):
        v = 5.85*x
        r = 0
        g = v//256
        b = v - g*256
        
        tile[:, x] = [b, g, r, 255]  # BGRA format
    
    # Save the image as WebP
    cv2.imwrite(filename, tile)

if __name__ == "__main__":
    create_test_tile("test_tile.webp")