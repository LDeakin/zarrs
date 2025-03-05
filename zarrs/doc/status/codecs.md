| Codec Type     | Default codec `name`               | Feature Flag* |
| -------------- | ---------------------------------- | ------------- |
| Array to Array | [`transpose`]                      | **transpose** |
|                | [`zarrs.bitround`] (experimental)  | bitround      |
| Array to Bytes | [`bytes`]                          |               |
|                | [`vlen-array`]                     |               |
|                | [`vlen-bytes`]                     |               |
|                | [`vlen-utf8`]                      |               |
|                | [`sharding_indexed`]               | **sharding**  |
|                | [`numcodecs.pcodec`]               | pcodec        |
|                | [`numcodecs.zfpy`]                 | zfp           |
|                | [`zarrs.vlen`] (experimental)      |               |
|                | [`zarrs.vlen_v2`] (experimental)   |               |
|                | [`zarrs.zfp`] (experimental)       | zfp           |
| Bytes to Bytes | [`blosc`]                          | **blosc**     |
|                | [`crc32c`]                         | **crc32c**    |
|                | [`gzip`]                           | **gzip**      |
|                | [`zstd`]                           | **zstd**      |
|                | [`numcodecs.bz2`]                  | bz2           |
|                | [`numcodecs.fletcher32`]           | fletcher32    |
|                | [`zarrs.gdeflate`] (experimental)  | gdeflate      |

<sup>\* Bolded feature flags are part of the default set of features.</sup>

[ZEP0001]: https://zarr.dev/zeps/accepted/ZEP0001.html
[ZEP0002]: https://zarr.dev/zeps/accepted/ZEP0001.html
[zarr-specs #256]: https://github.com/zarr-developers/zarr-specs/pull/256

[`transpose`]: crate::array::codec::array_to_array::transpose
[`zarrs.bitround`]: crate::array::codec::array_to_array::bitround

[`bytes`]: crate::array::codec::array_to_bytes::bytes
[`vlen-array`]: crate::array::codec::array_to_bytes::vlen_array
[`vlen-bytes`]: crate::array::codec::array_to_bytes::vlen_bytes
[`vlen-utf8`]: crate::array::codec::array_to_bytes::vlen_utf8
[`sharding_indexed`]: crate::array::codec::array_to_bytes::sharding
[`numcodecs.pcodec`]: crate::array::codec::array_to_bytes::pcodec
[`numcodecs.zfpy`]: crate::array::codec::array_to_bytes::zfpy
[`zarrs.vlen`]: crate::array::codec::array_to_bytes::vlen
[`zarrs.vlen_v2`]: crate::array::codec::array_to_bytes::vlen_v2
[`zarrs.zfp`]: crate::array::codec::array_to_bytes::zfp

[`blosc`]: crate::array::codec::bytes_to_bytes::blosc
[`crc32c`]: crate::array::codec::bytes_to_bytes::crc32c
[`gzip`]: crate::array::codec::bytes_to_bytes::gzip
[`zstd`]: crate::array::codec::bytes_to_bytes::zstd
[`numcodecs.bz2`]: crate::array::codec::bytes_to_bytes::gzip
[`numcodecs.fletcher32`]: crate::array::codec::bytes_to_bytes::fletcher32
[`zarrs.gdeflate`]: crate::array::codec::bytes_to_bytes::gdeflate

**Experimental codecs are recommended for evaluation only**.
They may change in future releases without maintaining backwards compatibilty.

Codec `name`s and aliases are configurable with [`Config::codec_map_mut`](config::Config::codec_map_mut).
`zarrs` will persist codec names if opening an existing array of creating an array from metadata.

Arrays created with `zarr-python` 3.x.x with codecs in the `numcodecs.zarr3` submodule are supported.
However, arrays must be written with `numcodecs` 0.15+.

