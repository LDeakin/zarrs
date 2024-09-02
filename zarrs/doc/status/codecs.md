| Codec Type     | Codec              | ZEP               | V3      | V2      | Feature Flag* |
| -------------- | ------------------ | ----------------- | ------- | ------- | ------------- |
| Array to Array | [transpose]        | [ZEP0001]         | &check; |         | **transpose** |
| Array to Bytes | [bytes]            | [ZEP0001]         | &check; |         |               |
|                | [sharding_indexed] | [ZEP0002]         | &check; |         | **sharding**  |
| Bytes to Bytes | [blosc]            | [ZEP0001]         | &check; | &check; | **blosc**     |
|                | [gzip]             | [ZEP0001]         | &check; | &check; | **gzip**      |
|                | [crc32c]           | [ZEP0002]         | &check; |         | **crc32c**    |
|                | [zstd]             | [zarr-specs #256] | &check; | &check; | zstd          |

<sup>\* Bolded feature flags are part of the default set of features.</sup>

[ZEP0001]: https://zarr.dev/zeps/accepted/ZEP0001.html
[ZEP0002]: https://zarr.dev/zeps/accepted/ZEP0001.html
[zarr-specs #256]: https://github.com/zarr-developers/zarr-specs/pull/256

[transpose]: crate::array::codec::array_to_array::transpose
[bytes]: crate::array::codec::array_to_bytes::bytes
[sharding_indexed]: crate::array::codec::array_to_bytes::sharding
[blosc]: crate::array::codec::bytes_to_bytes::blosc
[gzip]: crate::array::codec::bytes_to_bytes::gzip
[crc32c]: crate::array::codec::bytes_to_bytes::crc32c
[zstd]: crate::array::codec::bytes_to_bytes::zstd
