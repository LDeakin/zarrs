| Codec Type     | Codec                                                             | ZEP                                                                 | Zarrs                    | Feature Flag* |
| -------------- | ----------------------------------------------------------------- | ------------------------------------------------------------------- | ------------------------ | ------------- |
| Array to Array | [transpose](crate::array::codec::array_to_array::transpose)       | [ZEP0001](https://zarr.dev/zeps/accepted/ZEP0001.html)              | Full support             | **transpose** |
|                | [bitround](crate::array::codec::array_to_array::bitround)         |                                                                     | Experimental<sup>†</sup> | bitround      |
| Array to Bytes | [bytes](crate::array::codec::array_to_bytes::bytes)               | [ZEP0001](https://zarr.dev/zeps/accepted/ZEP0001.html)              | Full support             |               |
|                | [sharding_indexed](crate::array::codec::array_to_bytes::sharding) | [ZEP0002](https://zarr.dev/zeps/accepted/ZEP0002.html)              | Full support             | **sharding**  |
|                | [zfp](crate::array::codec::array_to_bytes::zfp)                   |                                                                     | Experimental<sup>†</sup> | zfp           |
|                | [pcodec](crate::array::codec::array_to_bytes::pcodec)             |                                                                     | Experimental<sup>†</sup> | pcodec        |
| Bytes to Bytes | [blosc](crate::array::codec::bytes_to_bytes::blosc)               | [ZEP0001](https://zarr.dev/zeps/accepted/ZEP0001.html)              | Full support             | **blosc**     |
|                | [gzip](crate::array::codec::bytes_to_bytes::gzip)                 | [ZEP0001](https://zarr.dev/zeps/accepted/ZEP0001.html)              | Full support             | **gzip**      |
|                | [crc32c](crate::array::codec::bytes_to_bytes::crc32c)             | [ZEP0002](https://zarr.dev/zeps/accepted/ZEP0002.html)              | Full support             | **crc32c**    |
|                | [zstd](crate::array::codec::bytes_to_bytes::zstd)                 | [GitHub PR](https://github.com/zarr-developers/zarr-specs/pull/256) | Full support             | zstd          |
|                | [bz2](crate::array::codec::bytes_to_bytes::bz2)                   |                                                                     | Experimental<sup>†</sup> | bz2           |

<sup>\* Bolded feature flags are part of the default set of features.</sup>
<br>
<sup>† Experimental codecs are recommended for evaluation only.</sup>

The `"name"` of of experimental codecs in array metadata links the codec documentation in this crate.

| Experimental Codec | Name / URI                                        |
| ------------------ | ------------------------------------------------- |
| `bitround`         | <https://codec.zarrs.dev/array_to_array/bitround> |
| `zfp`              | <https://codec.zarrs.dev/array_to_bytes/zfp>      |
| `pcodec`           | <https://codec.zarrs.dev/array_to_bytes/pcodec>   |
| `bz2`              | <https://codec.zarrs.dev/bytes_to_bytes/bz2>      |
