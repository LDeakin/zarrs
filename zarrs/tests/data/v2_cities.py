import zarr
import pandas as pd

print(zarr.__version__) # This was generated with zarr==2.18

df = pd.read_csv("tests/data/cities.csv", header=None)
cities = df[0]

path_out = 'tests/data/zarr_python_compat/cities_v2.zarr'
array = zarr.open(path_out, mode='w', dtype=str, shape=(len(cities),), chunks=(1000,), compressor = None, fill_value='')
array[:] = cities.values
print(array.info)
