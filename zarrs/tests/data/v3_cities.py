import zarr
import pandas as pd

print(zarr.__version__) # This was generate with zarr==2.18

df = pd.read_csv("tests/data/cities.csv", header=None)
cities = df[0]

path_out = 'tests/data/zarr_python_compat/cities_v3.zarr'
array = zarr.open(path_out, mode='w', dtype=str, shape=(len(cities),), chunks=(1000,))
array[:] = cities.values
print(array.info)

array_v2 = zarr.open('tests/data/zarr_python_compat/cities_v2.zarr', dtype=str, shape=(len(cities),), chunks=(1000,))

assert((array[:] == array_v2[:]).all())

# for i in range(48):
#     v2 = open(f'tests/data/v2/cities.zarr/{i}', 'rb').read()
#     v3 = open(f'tests/data/v3/cities.zarr/c/{i}', 'rb').read()
#     assert v2 == v3

print("V2 and V3 chunks are identical!")
