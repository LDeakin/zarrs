| Codec Type     | Codec                                                             | ZEP                                                                 | Zarrs        | Feature Flag* |
| -------------- | ----------------------------------------------------------------- | ------------------------------------------------------------------- | ------------ | ------------- |
| Array to Array | [transpose](crate::array::codec::array_to_array::transpose)       | [ZEP0001](https://zarr.dev/zeps/accepted/ZEP0001.html)              | Full support | **transpose** |
|                | [bitround](crate::array::codec::array_to_array::bitround)         |                                                                     | Experimental | bitround      |
| Array to Bytes | [bytes](crate::array::codec::array_to_bytes::bytes)               | [ZEP0001](https://zarr.dev/zeps/accepted/ZEP0001.html)              |              |               |
|                | [sharding_indexed](crate::array::codec::array_to_bytes::sharding) | [ZEP0002](https://zarr.dev/zeps/accepted/ZEP0002.html)              | Full support | **sharding**  |
|                | [zfp](crate::array::codec::array_to_bytes::zfp)                   |                                                                     | Experimental | zfp           |
|                | [pcodec](crate::array::codec::array_to_bytes::pcodec)             |                                                                     | Experimental | pcodec        |
| Bytes to Bytes | [blosc](crate::array::codec::bytes_to_bytes::blosc)               | [ZEP0001](https://zarr.dev/zeps/accepted/ZEP0001.html)              | Full support | **blosc**     |
|                | [gzip](crate::array::codec::bytes_to_bytes::gzip)                 | [ZEP0001](https://zarr.dev/zeps/accepted/ZEP0001.html)              | Full support | **gzip**      |
|                | [crc32c](crate::array::codec::bytes_to_bytes::crc32c)             | [ZEP0002](https://zarr.dev/zeps/accepted/ZEP0002.html)              | Full support | **crc32c**    |
|                | [zstd](crate::array::codec::bytes_to_bytes::zstd)                 | [GitHub PR](https://github.com/zarr-developers/zarr-specs/pull/256) | Full support | zstd          |
|                | [bz2](crate::array::codec::bytes_to_bytes::bz2)                   |                                                                     | Experimental | bz2           |

\* Bolded feature flags are part of the default set of features.

<div class="warning">
Experimental codecs are for evaluation purposes only and should not be used in production.

 - They will not be supported by other Zarr V3 implementations until they have been standardised.
 - The `"name"` of of experimental codecs in array metadata is a link to either:
   - a Zarr ZEP draft or GitHub PR, or
   - the codec documentation in this crate.
</div>
