import numpy as np
from process import nc2tile


def test_cap_to_range_basic():
    arr = np.array([[1.0, np.nan, 10.0, -5.0]])
    out = nc2tile.cap_to_range(arr, 0.0, 5.0)
    assert out.shape == arr.shape
    # NaN preserved
    assert np.isnan(out[0, 1])
    # values above capped
    assert out[0, 2] == 5.0
    # values below capped
    assert out[0, 3] == 0.0


def test_cap_to_range_partial_none():
    arr = np.array([[1.0, 2.0, 3.0]])
    out = nc2tile.cap_to_range(arr, None, 2.1)
    assert np.allclose(out, np.array([[1.0, 2.0, 2.0]]), atol=1e-8)
    out2 = nc2tile.cap_to_range(arr, 1.5, None)
    assert np.allclose(out2, np.array([[1.5, 2.0, 3.0]]), atol=1e-8)
