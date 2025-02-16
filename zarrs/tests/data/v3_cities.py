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

path_out = "tests/data/zarr_python_compat/cities_v3.zarr"
array = zarr.create_array(
    path_out,
    dtype=str,
    shape=(len(cities),),
    chunks=(1000,),
    compressors=[],
    zarr_format=3,
    overwrite=True,
)
array[:] = cities.values
print(array.info)

array_v2 = zarr.open(
    "tests/data/zarr_python_compat/cities_v2.zarr",
    dtype=str,
    shape=(len(cities),),
    chunks=(1000,),
)

assert (array[:] == array_v2[:]).all()

# for i in range(48):
#     v2 = open(f'tests/data/v2/cities.zarr/{i}', 'rb').read()
#     v3 = open(f'tests/data/v3/cities.zarr/c/{i}', 'rb').read()
#     assert v2 == v3

print("V2 and V3 chunks are identical!")
