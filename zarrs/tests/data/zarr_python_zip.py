import numpy as np
import zarr
print(zarr.__version__) # zarr-3.0.0a4.dev25+ge9f808b4

store = zarr.store.ZipStore('zarrs/tests/data/zarr_python_compat/zarr.zip', mode='w')
root = zarr.group(store=store, zarr_version=3)
z = root.create_array(shape=(100, 100), chunks=(10, 10), name="foo", dtype=np.uint8) # fill_value=42: Broken
z[:] = 42
store.close()
