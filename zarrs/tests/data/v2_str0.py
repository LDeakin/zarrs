#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "zarr==3.0.1",
# ]
# ///

import zarr

path_out = "tests/data/zarr_python_compat/str_v2_fv_0.zarr"
array = zarr.create_array(
    path_out,
    dtype=str,
    shape=(5,),
    chunks=(2,),
    filters=zarr.codecs.vlen_utf8.VLenUTF8(),
    compressors=[None],
    fill_value=0,
    zarr_format=2,
    overwrite=True,
)
array[:3] = ["a", "bb", ""]
print(array.info)
# assert (array[:] == ["a", "bb", "", "", ""]).all() # FAILURE

path_out = "tests/data/zarr_python_compat/str_v2_fv_null.zarr"
array = zarr.create_array(
    path_out,
    dtype=str,
    shape=(5,),
    chunks=(2,),
    filters=zarr.codecs.vlen_utf8.VLenUTF8(),
    compressors=[None],
    fill_value=None,
    zarr_format=2,
    overwrite=True,
)
array[:3] = ["a", "bb", ""]
print(array.info)
print(array[:])
assert (array[:] == ["a", "bb", "", "", ""]).all()