#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "zarr==3.0.1",
#     "pandas==2.2.3"
# ]
# ///

import zarr
import pandas as pd

df = pd.read_csv("tests/data/cities.csv", header=None)
cities = df[0]

path_out = "tests/data/zarr_python_compat/cities_v2.zarr"
array = zarr.create_array(
    path_out,
    dtype=str,
    shape=(len(cities),),
    chunks=(1000,),
    filters=zarr.codecs.vlen_utf8.VLenUTF8(),
    compressors=[None],
    # fill_value="",
    zarr_format=2,
    overwrite=True,
)
array[:] = cities.values
print(array.info)
