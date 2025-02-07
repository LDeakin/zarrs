#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "zarr==2.18.4",
# ]
# ///

import zarr

path_out = "tests/data/zarr_python_compat/str_v2_fv_0.zarr"
array = zarr.open(
    path_out,
    dtype=str,
    shape=(5,),
    chunks=(2,),
    compressor=None,
    mode='w',
)
array[0] = "a"
array[1] = "bb"
array[2] = ""
print(array.info)
print(array[:])
assert (array[:] == ["a", "bb", "", "0", "0"]).all()

path_out = "tests/data/zarr_python_compat/str_v2_fv_null.zarr"
array = zarr.open(
    path_out,
    dtype=str,
    shape=(5,),
    chunks=(2,),
    fill_value = None,
    compressor=None,
    mode='w',
)
array[0] = "a"
array[1] = "bb"
array[2] = ""
print(array.info)
print(array[:])
assert (array[:] == ["a", "bb", "", "", None]).all() # Yikes