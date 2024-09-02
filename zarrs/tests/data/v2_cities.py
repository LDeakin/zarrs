import zarr # v2
import pandas as pd

print(zarr.__version__)

df = pd.read_csv("tests/data/cities.csv", header=None)
cities = df[0]

path_out = 'tests/data/v2/cities.zarr'
array = zarr.open(path_out, mode='w', dtype=str, shape=(len(cities),), chunks=(1000,), compressor = None, fill_value='')
array[:] = cities.values
print(array.info)

for i in range(48):
    v2 = open(f'tests/data/v2/cities.zarr/{i}', 'rb').read()
    v3 = open(f'tests/data/v3/cities.zarr/c/{i}', 'rb').read()
    assert v2 == v3

print("V2 and V3 chunks are identical!")
