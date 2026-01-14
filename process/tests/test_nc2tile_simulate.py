import tempfile
import numpy as np
import xarray as xr
import nc2tile


class FakeTransformer:
    @staticmethod
    def from_crs(a, b, always_xy=True):
        class T:
            @staticmethod
            def transform(xs, ys):
                # If xs/ys are single valued (min==max), expand to a small bbox so target grid has non-zero extent
                try:
                    x0 = float(xs[0])
                    y0 = float(ys[0])
                    return (np.array([x0 - 0.01, x0 + 0.01]), np.array([y0 - 0.01, y0 + 0.01]))
                except Exception:
                    return xs, ys
        return T()


def test_process_variable_simulate(monkeypatch, tmp_path):
    # Create tiny dataset with two times and a 1x1 grid
    times = np.array(['2026-01-05T00:30:00', '2026-01-05T01:30:00'], dtype='datetime64[s]')
    data = np.zeros((2, 1, 1), dtype=float)
    ds = xr.Dataset({'var1': (('time', 'y', 'x'), data)}, coords={'time': times, 'y': [0], 'x': [0]})
    fn = tmp_path / 'ds.nc'
    ds.to_netcdf(fn)

    # Monkeypatch grid loader and Transformer
    monkeypatch.setattr(nc2tile, 'get_grid_from_db', lambda table='grid': (np.array([[-123.0]]), np.array([[49.0]])))
    monkeypatch.setattr(nc2tile, 'Transformer', FakeTransformer)

    processed = nc2tile.process_variable(str(fn), 'var1', workers=1, verbose=False, simulate=True)
    assert isinstance(processed, list)
    assert '2026-01-05T00:30:00' in processed
    assert '2026-01-05T01:30:00' in processed
