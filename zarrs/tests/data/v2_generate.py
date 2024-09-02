import zarr
import numpy as np
from numcodecs import Blosc, GZip, BZ2, ZFPY, PCodec, Zstd

compressor_blosc = Blosc(cname="zstd", clevel=1, shuffle=Blosc.BITSHUFFLE)
compressor_gzip = GZip(level=9)
compressor_bz2 = BZ2(level=9)
compressor_zfpy = ZFPY(mode = 4, tolerance=0.01) # fixed accuracy
compressor_pcodec = PCodec(level = 8, mode_spec="auto")
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
        ("blosc", compressor_blosc),
        ("gzip", compressor_gzip),
        ("bz2", compressor_bz2),
        ("zfpy", compressor_zfpy),
        ("pcodec", compressor_pcodec),
        ("zstd", compressor_zstd),
    ]:
        if order == "F" and compressor_name != "blosc":
            continue

        store = zarr.DirectoryStore(f"tests/data/v2/array_{compressor_name}_{order}.zarr")
        try:
            store.clear()
        except FileNotFoundError:
            pass
        array = zarr.creation.create(
            shape=[10, 10],
            chunks=[5, 5],
            dtype=np.float32,
            compressor=compressor,
            order=order,
            store=store,
        )
        array[...] = np.array(data)
        array.attrs["key"] = "value"
