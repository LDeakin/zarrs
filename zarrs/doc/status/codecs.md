| Codec Type     | Default codec `name`               | Status       | Feature Flag* |
| -------------- | ---------------------------------- | ------------ | ------------- |
| Array to Array | [`transpose`]                      | Core         | **transpose** |
|                | [`numcodecs.bitround`]†            | Experimental | bitround      |
|                | [`numcodecs.fixedscaleoffset`]     | Experimental |               |
|                | [`zarrs.squeeze`]                  | Experimental |               |
| Array to Bytes | [`bytes`]                          | Core         |               |
|                | [`sharding_indexed`]               | Core         | **sharding**  |
|                | [`vlen-array`]                     | Experimental |               |
|                | [`vlen-bytes`]                     | Experimental |               |
|                | [`vlen-utf8`]                      | Experimental |               |
|                | [`numcodecs.pcodec`]               | Experimental | pcodec        |
|                | [`numcodecs.zfpy`]                 | Experimental | zfp           |
|                | [`packbits`]                       | Experimental |               |
|                | [`zarrs.vlen`]                     | Experimental |               |
|                | [`zarrs.vlen_v2`]                  | Experimental |               |
|                | [`zfp`]                            | Experimental | zfp           |
| Bytes to Bytes | [`blosc`]                          | Core         | **blosc**     |
|                | [`crc32c`]                         | Core         | **crc32c**    |
|                | [`gzip`]                           | Core         | **gzip**      |
|                | [`zstd`]                           | Experimental | **zstd**      |
|                | [`numcodecs.bz2`]                  | Experimental | bz2           |
|                | [`numcodecs.fletcher32`]           | Experimental | fletcher32    |
|                | [`numcodecs.shuffle`]              | Experimental |               |
|                | [`numcodecs.zlib`]                 | Experimental | zlib          |
|                | [`zarrs.gdeflate`]                 | Experimental | gdeflate      |

<sup>\* Bolded feature flags are part of the default set of features.</sup>
<sup>† `numcodecs.bitround` supports additional data types not supported by `zarr-python`/`numcodecs`</sup>

[ZEP0001]: https://zarr.dev/zeps/accepted/ZEP0001.html
[ZEP0002]: https://zarr.dev/zeps/accepted/ZEP0001.html
[zarr-specs #256]: https://github.com/zarr-developers/zarr-specs/pull/256

[`transpose`]: crate::array::codec::array_to_array::transpose
[`numcodecs.bitround`]: crate::array::codec::array_to_array::bitround
[`numcodecs.fixedscaleoffset`]: crate::array::codec::array_to_array::fixedscaleoffset
[`zarrs.squeeze`]: crate::array::codec::array_to_array::squeeze

[`bytes`]: crate::array::codec::array_to_bytes::bytes
[`vlen-array`]: crate::array::codec::array_to_bytes::vlen_array
[`vlen-bytes`]: crate::array::codec::array_to_bytes::vlen_bytes
[`vlen-utf8`]: crate::array::codec::array_to_bytes::vlen_utf8
[`sharding_indexed`]: crate::array::codec::array_to_bytes::sharding
[`numcodecs.pcodec`]: crate::array::codec::array_to_bytes::pcodec
[`numcodecs.zfpy`]: crate::array::codec::array_to_bytes::zfpy
[`packbits`]: crate::array::codec::array_to_bytes::packbits
[`zarrs.vlen`]: crate::array::codec::array_to_bytes::vlen
[`zarrs.vlen_v2`]: crate::array::codec::array_to_bytes::vlen_v2
[`zfp`]: crate::array::codec::array_to_bytes::zfp

[`blosc`]: crate::array::codec::bytes_to_bytes::blosc
[`crc32c`]: crate::array::codec::bytes_to_bytes::crc32c
[`gzip`]: crate::array::codec::bytes_to_bytes::gzip
[`zstd`]: crate::array::codec::bytes_to_bytes::zstd
[`numcodecs.bz2`]: crate::array::codec::bytes_to_bytes::gzip
[`numcodecs.fletcher32`]: crate::array::codec::bytes_to_bytes::fletcher32
[`numcodecs.shuffle`]: crate::array::codec::bytes_to_bytes::shuffle
[`numcodecs.zlib`]: crate::array::codec::bytes_to_bytes::zlib
[`zarrs.gdeflate`]: crate::array::codec::bytes_to_bytes::gdeflate

**Experimental codecs are recommended for evaluation only**.
They may change in future releases without maintaining backwards compatibilty.
These codecs have not been standardised, but many are fully compatible with other Zarr implementations.

Codec `name`s and aliases are configurable with [`Config::codec_aliases_v3_mut`](config::Config::codec_aliases_v3_mut) and [`Config::codec_aliases_v2_mut`](config::Config::codec_aliases_v2_mut).
`zarrs` will persist codec names if opening an existing array of creating an array from metadata.

`zarrs` supports arrays created with `zarr-python` 3.x.x with various `numcodecs.zarr3` codecs.
However, arrays must be written with `numcodecs` 0.15.1+.
