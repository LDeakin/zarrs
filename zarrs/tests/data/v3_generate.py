import zarr # 3.0.0
import numpy as np
from numcodecs.zarr3 import BZ2, ZFPY, PCodec # 0.14.2.dev22 with https://github.com/zarr-developers/numcodecs/pull/685

compressor_blosc = zarr.codecs.BloscCodec(cname="zstd", clevel=1, shuffle=zarr.codecs.BloscShuffle.bitshuffle)
compressor_gzip = zarr.codecs.GzipCodec(level=9)
compressor_bz2 = BZ2(level=9)
serializer_zfpy = ZFPY(mode = 4, tolerance=0.01) # fixed accuracy
serializer_pcodec = PCodec(level = 8, mode_spec="auto")
compressor_zstd = zarr.codecs.ZstdCodec(level=5, checksum=False)

data = np.array(
    [
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        [10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        [20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
        [30, 31, 32, 33, 34, 35, 36, 37, 38, 39],
        [40, 41, 42, 43, 44, 45, 46, 47, 48, 49],
        [50, 51, 52, 53, 54, 55, 56, 57, 58, 59],
        [60, 61, 62, 63, 64, 65, 66, 67, 68, 69],
        [70, 71, 72, 73, 74, 75, 76, 77, 78, 79],
        [80, 81, 82, 83, 84, 85, 86, 87, 88, 89],
        [90, 91, 92, 93, 94, 95, 96, 97, 98, 99],
    ]
)

for compressor_name, compressor in [
    ("none", None),
    ("blosc", compressor_blosc),
    ("gzip", compressor_gzip),
    ("bz2", compressor_bz2),
    ("zstd", compressor_zstd),
]:
    array = zarr.create_array(
        f"tests/data/v3_zarr_python/array_{compressor_name}.zarr",
        overwrite=True,
        zarr_format=3,
        shape=[10, 10],
        chunks=[5, 5],
        dtype=np.float32,
        fill_value=0.0,
        compressors=[compressor] if compressor else None,
    )
    array[...] = np.array(data)
    array.attrs["key"] = "value"


for serializer_name, serializer in [
    ("zfpy", serializer_zfpy),
    ("pcodec", serializer_pcodec),
]:
    array = zarr.create_array(
        f"tests/data/v3_zarr_python/array_{serializer_name}.zarr",
        overwrite=True,
        zarr_format=3,
        shape=[10, 10],
        chunks=[5, 5],
        dtype=np.float32,
        fill_value=0.0,
        serializer=serializer if serializer else None,
        compressors=[],
    )
    array[...] = np.array(data)
    array.attrs["key"] = "value"