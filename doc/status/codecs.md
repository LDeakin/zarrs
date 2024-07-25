| Codec Type     | Codec<sup>†</sup>                                 | ZEP               | V3      | V2      | Feature Flag* |
| -------------- | ------------------------------------------------- | ----------------- | ------- | ------- | ------------- |
| Array to Array | [transpose]                                       | [ZEP0001]         | &check; |         | **transpose** |
|                | [bitround] (experimental)                         |                   | &check; |         | bitround      |
| Array to Bytes | [bytes]                                           | [ZEP0001]         | &check; |         |               |
|                | [sharding_indexed]                                | [ZEP0002]         | &check; |         | **sharding**  |
|                | [zfp] (experimental)                              |                   | &check; |         | zfp           |
|                | [pcodec] (experimental)                           |                   | &check; |         | pcodec        |
|                | [vlen] (experimental)                             |                   | &check; |         |               |
|                | [vlen_v2] (experimental)<br>`vlen-*` in Zarr V2   |                   | &check; | &check; |               |
| Bytes to Bytes | [blosc]                                           | [ZEP0001]         | &check; | &check; | **blosc**     |
|                | [gzip]                                            | [ZEP0001]         | &check; | &check; | **gzip**      |
|                | [crc32c]                                          | [ZEP0002]         | &check; |         | **crc32c**    |
|                | [zstd]                                            | [zarr-specs #256] | &check; |         | zstd          |
|                | [bz2] (experimental)                              |                   | &check; | &check; | bz2           |

<sup>\* Bolded feature flags are part of the default set of features.</sup>
<br>
<sup>† Experimental codecs are recommended for evaluation only.</sup>

[ZEP0001]: https://zarr.dev/zeps/accepted/ZEP0001.html
[ZEP0002]: https://zarr.dev/zeps/accepted/ZEP0001.html
[zarr-specs #256]: https://github.com/zarr-developers/zarr-specs/pull/256

[transpose]: crate::array::codec::array_to_array::transpose
[bitround]: (crate::array::codec::array_to_array::bitround)
[bytes]: crate::array::codec::array_to_bytes::bytes
[sharding_indexed]: crate::array::codec::array_to_bytes::sharding
[zfp]: crate::array::codec::array_to_bytes::zfp
[pcodec]: crate::array::codec::array_to_bytes::pcodec
[blosc]: crate::array::codec::bytes_to_bytes::blosc
[gzip]: crate::array::codec::bytes_to_bytes::gzip
[crc32c]: crate::array::codec::bytes_to_bytes::crc32c
[zstd]: crate::array::codec::bytes_to_bytes::zstd
[bz2]: crate::array::codec::bytes_to_bytes::bz2
[vlen]: crate::array::codec::array_to_bytes::vlen
[vlen_v2]: crate::array::codec::array_to_bytes::vlen_v2

The `"name"` of of experimental codecs in array metadata links the codec documentation in this crate.

| Experimental Codec | Name / URI                                               |
| ------------------ | -------------------------------------------------------- |
| `bitround`         | <https://codec.zarrs.dev/array_to_array/bitround>        |
| `zfp`              | <https://codec.zarrs.dev/array_to_bytes/zfp>             |
| `pcodec`           | <https://codec.zarrs.dev/array_to_bytes/pcodec>          |
| `bz2`              | <https://codec.zarrs.dev/bytes_to_bytes/bz2>             |
| `vlen`             | <https://codec.zarrs.dev/array_to_array/vlen>            |
| `vlen_v2` | <https://codec.zarrs.dev/array_to_array/zfp_interleaved> |
