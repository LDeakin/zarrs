import zarr
import numpy as np
from numcodecs import Blosc, GZip, BZ2, ZFPY, PCodec, Zstd

compressor_blosc = Blosc(cname="zstd", clevel=1, shuffle=Blosc.BITSHUFFLE)
compressor_gzip = GZip(level=9)
compressor_bz2 = BZ2(level=9)
serializer_zfpy = ZFPY(mode = 4, tolerance=0.01) # fixed accuracy
serializer_pcodec = PCodec(level = 8, mode_spec="auto")
compressor_zstd = Zstd(level=5, checksum=False)

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

for order in ["C", "F"]:
    for compressor_name, compressor in [
        ("none", None),
        ("blosc", compressor_blosc),
        ("gzip", compressor_gzip),
        ("bz2", compressor_bz2),
        ("zstd", compressor_zstd),
    ]:
        if order == "F" and compressor is not None and compressor_name != "blosc":
            continue

        array = zarr.create_array(
            f"tests/data/v2/array_{compressor_name}_{order}.zarr",
            overwrite=True,
            zarr_format=2,
            shape=[10, 10],
            chunks=[5, 5],
            dtype=np.float32,
            fill_value=0.0,
            compressors=[compressor] if compressor else None,
            order=order,
        )
        array[...] = np.array(data)
        array.attrs["key"] = "value"


    for serializer_name, serializer in [
        ("zfpy", serializer_zfpy),
        ("pcodec", serializer_pcodec),
    ]:
        if order == "F" and serializer is not None and serializer_name != "blosc":
            continue

        array = zarr.create_array(
            f"tests/data/v2/array_{serializer_name}_{order}.zarr",
            overwrite=True,
            zarr_format=2,
            shape=[10, 10],
            chunks=[5, 5],
            dtype=np.float32,
            fill_value=0.0,
            compressors=[serializer] if serializer else None,
            order=order,
        )
        array[...] = np.array(data)
        array.attrs["key"] = "value"
