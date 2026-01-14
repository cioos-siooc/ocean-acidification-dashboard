from PIL import Image
import numpy as np
from process.nc2tile import write_png_packed


def test_write_png_packed(tmp_path):
    # small float array 2x2 with values 0.0, 0.1, 0.2, 0.3 and precision 0.1
    arr = np.array([[0.0, 0.1], [0.2, 0.3]], dtype=float)
    alpha = np.full(arr.shape, 255, dtype=np.uint8)
    out = tmp_path / 'p.png'
    write_png_packed(arr, alpha, str(out), precision=0.1, base=0.0)

    img = Image.open(str(out))
    rgba = np.array(img)
    # verify packed values: quant = value/0.1 -> [0,1,2,3]
    # packed into RGB: high byte is 0, mid byte is 0, low byte is quant
    # check pixel (0,0)
    r, g, b, a = rgba[0,0]
    assert (r, g, b, a) == (0, 0, 0, 255)
    r, g, b, a = rgba[0,1]
    assert (r, g, b, a) == (0, 0, 1, 255)
    r, g, b, a = rgba[1,0]
    assert (r, g, b, a) == (0, 0, 2, 255)
    r, g, b, a = rgba[1,1]
    assert (r, g, b, a) == (0, 0, 3, 255)
