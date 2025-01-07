import numpy as np
import zarr
import numcodecs

print(zarr.__version__)  # 3.0.0rc2

z = zarr.create_array(
    "zarrs/tests/data/zarr_python_compat/fletcher32.zarr",
    shape=(100, 100),
    chunks=(50, 50),
    dtype=np.uint16,
    zarr_format=2,
    fill_value=0,
    overwrite=True,
    compressors=[numcodecs.Fletcher32()],
)
z[:] = np.arange(100 * 100).reshape(100, 100)
