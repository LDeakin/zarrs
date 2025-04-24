| Codec Type     | Default codec `name`               | Specification                       | Feature Flag* |
| -------------- | ---------------------------------- | ----------------------------------- | ------------- |
| Array to Array | [`transpose`]                      | [Zarr V3.0 Transpose]               | **transpose** |
|                | [`numcodecs.fixedscaleoffset`]     | Experimental                        |               |
|                | [`numcodecs.bitround`]†            | Experimental                        | bitround      |
|                | [`zarrs.squeeze`]                  | Experimental                        |               |
| Array to Bytes | [`bytes`]                          | [Zarr V3.0 Bytes]                   |               |
|                | [`sharding_indexed`]               | [Zarr V3.0 Sharding]                | **sharding**  |
|                | [`vlen-array`]                     | Experimental                        |               |
|                | [`vlen-bytes`]                     | [zarr-extensions/codecs/vlen-bytes] |               |
|                | [`vlen-utf8`]                      | [zarr-extensions/codecs/vlen-utf8]  |               |
|                | [`numcodecs.pcodec`]               | Experimental                        | pcodec        |
|                | [`numcodecs.zfpy`]                 | Experimental                        | zfp           |
|                | [`packbits`]                       | [zarr-extensions/codecs/packbits]   |               |
|                | [`zarrs.vlen`]                     | Experimental                        |               |
|                | [`zarrs.vlen_v2`]                  | Experimental                        |               |
|                | [`zfp`]                            | [zarr-extensions/codecs/zfp]        | zfp           |
| Bytes to Bytes | [`blosc`]                          | [Zarr V3.0 Blosc]                   | **blosc**     |
|                | [`crc32c`]                         | [Zarr V3.0 CRC32C]                  | **crc32c**    |
|                | [`gzip`]                           | [Zarr V3.0 Gzip]                    | **gzip**      |
|                | [`zstd`]                           | [zarr-extensions/codecs/zstd]       | **zstd**      |
|                | [`numcodecs.bz2`]                  | Experimental                        | bz2           |
|                | [`numcodecs.fletcher32`]           | Experimental                        | fletcher32    |
|                | [`numcodecs.shuffle`]              | Experimental                        |               |
|                | [`numcodecs.zlib`]                 | Experimental                        | zlib          |
|                | [`zarrs.gdeflate`]                 | Experimental                        | gdeflate      |

<sup>\* Bolded feature flags are part of the default set of features.</sup>
<sup>† `numcodecs.bitround` supports additional data types not supported by `zarr-python`/`numcodecs`</sup>

[Zarr V3.0 Blosc]: https://zarr-specs.readthedocs.io/en/latest/v3/codecs/blosc/index.html
[Zarr V3.0 Bytes]: https://zarr-specs.readthedocs.io/en/latest/v3/codecs/bytes/index.html
[Zarr V3.0 CRC32C]: https://zarr-specs.readthedocs.io/en/latest/v3/codecs/crc32c/index.html
[Zarr V3.0 Gzip]: https://zarr-specs.readthedocs.io/en/latest/v3/codecs/gzip/index.html
[Zarr V3.0 Sharding]: https://zarr-specs.readthedocs.io/en/latest/v3/codecs/sharding-indexed/index.html
[Zarr V3.0 Transpose]: https://zarr-specs.readthedocs.io/en/latest/v3/codecs/transpose/index.html

[zarr-extensions/codecs/vlen-bytes]: https://github.com/zarr-developers/zarr-extensions/tree/main/codecs/vlen-bytes
[zarr-extensions/codecs/vlen-utf8]: https://github.com/zarr-developers/zarr-extensions/tree/main/codecs/vlen-utf8
[zarr-extensions/codecs/packbits]: https://github.com/zarr-developers/zarr-extensions/tree/main/codecs/packbits
[zarr-extensions/codecs/zfp]: https://github.com/zarr-developers/zarr-extensions/tree/main/codecs/zfp
[zarr-extensions/codecs/zstd]: https://github.com/zarr-developers/zarr-extensions/tree/main/codecs/zstd

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

Codecs have three potential statuses:
- *Core*: These are defined in the Zarr V3 specification and are fully supported.
- *Registered*: These are specified at <https://github.com/zarr-developers/zarr-extensions/> and are fully supported unless otherwise indicated.
- *Experimental*: These are **recommended for evaluation only**.
  - These codecs may have no formal specification or are pending registration at <https://github.com/zarr-developers/zarr-extensions/>.
  - These codecs may change in future releases without maintaining backwards compatibility.

Codec `name`s and aliases are configurable with [`Config::codec_aliases_v3_mut`](config::Config::codec_aliases_v3_mut) and [`Config::codec_aliases_v2_mut`](config::Config::codec_aliases_v2_mut).
`zarrs` will persist codec names if opening an existing array of creating an array from metadata.

`zarrs` supports arrays created with `zarr-python` 3.x.x with various `numcodecs.zarr3` codecs.
However, arrays must be written with `numcodecs` 0.15.1+.
