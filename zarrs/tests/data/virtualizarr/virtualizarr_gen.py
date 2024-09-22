#!/usr/bin/env python3

import virtualizarr
import xarray as xr
import numpy as np
import os
import shutil
from pathlib import Path

# fill value wrong (manually corrected)
# codecs wrong (manually corrected)
# chunk_key_encoding is wrong (manually corrected)
#   given that chunks are encoded as 0.0, storage should not need to understand the chunk key encoding

# codec metadata wrong with compression in V3 zarr.json (not spec compliant with name/configuration)

# Associated test is zarrs/tests/array_chunk_manifest.rs

assert Path.cwd().stem == "virtualizarr"

path_nc = "virtualizarr.nc"
path_zarr = "virtualizarr.zarr"

np.random.seed(seed=0)

ds = xr.Dataset(
    {
        "data": (("x", "y"), np.arange(4*8).reshape(4, 8)),
    },
    coords={
        "x": (("x"), np.arange(4)),
        "y": (("y"), np.arange(8)),
    }
)
print(ds)

ds.to_netcdf(
    path_nc,
    engine="h5netcdf",
    encoding={
        "data": {
            "chunksizes": (2, 8),
            # "zlib": True,
            # "complevel": 9,
        }
    },
)

vds = virtualizarr.open_virtual_dataset(path_nc)

marr = vds['data'].data
print(marr)
print(marr.manifest)
print(marr.zarray)
# manifest = marr.manifest
# print(manifest.dict())


if os.path.exists(path_zarr):
    shutil.rmtree(path_zarr)
vds.virtualize.to_zarr(path_zarr)
