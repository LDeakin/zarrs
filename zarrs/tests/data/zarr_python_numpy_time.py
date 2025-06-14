#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "zarr@git+https://github.com/d-v-b/zarr-python.git@feat/fixed-length-strings",
# ]
# ///

import numpy as np
import zarr
print(zarr.__version__)

for unit in ["Y", "M", "W", "D", "h", "m", "s", "ms", "us", "ns"]:
    for scale_factor in ["", "10"]:
        if scale_factor == "10" and unit not in ["ms", "us"]:
            continue
        z = zarr.create_array(
            f"zarrs/tests/data/zarr_python_compat/datetime64[{scale_factor}{unit}].zarr",
            shape=(6),
            chunks=(5),
            dtype=f'datetime64[{scale_factor}{unit}]',
            zarr_format=3,
            fill_value=np.datetime64('NaT'),
            overwrite=True,
        )
        z[:] = np.array([
            np.datetime64(0, 'Y'), # 1970, epoch
            np.datetime64('nat'),
            np.datetime64('2005-02-03'), 
            np.datetime64('2005-02-03T04:05'),
            np.datetime64('2005-02-03T04:05:06'),
            np.datetime64('nat'),
        ])

