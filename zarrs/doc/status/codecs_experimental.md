Experimental codecs are recommended for evaluation only.
By default, the `"name"` of of experimental codecs in array metadata links the codec documentation in this crate.
This is configurable with [`Config::experimental_codec_names_mut`](config::Config::experimental_codec_names_mut).

| Codec Type     | Codec                    | ZEP or URI                                         | V3      | V2      | Feature Flag |
| -------------- | ------------------------ | -------------------------------------------------- | ------- | ------- | ------------ |
| Array to Array | [bitround]               | <https://codec.zarrs.dev/array_to_array/bitround>  | &check; | &check; | bitround     |
| Array to Bytes | [zfp]<br>zfpy (V2)       | <https://codec.zarrs.dev/array_to_bytes/zfp>       | &check; | &check; | zfp          |
|                | [pcodec]                 | <https://codec.zarrs.dev/array_to_bytes/pcodec>    | &check; | &check; | pcodec       |
|                | [vlen]                   | <https://codec.zarrs.dev/array_to_bytes/vlen>      | &check; |         |              |
|                | [vlen_v2]<br>vlen-* (V2) | <https://codec.zarrs.dev/array_to_bytes/vlen_v2>   | &check; | &check; |              |
| Bytes to Bytes | [bz2]                    | <https://codec.zarrs.dev/bytes_to_bytes/bz2>       | &check; | &check; | bz2          |
|                | [gdeflate]               | <https://codec.zarrs.dev/bytes_to_bytes/gdeflate>  | &check; |         | gdeflate     |

[bitround]: (crate::array::codec::array_to_array::bitround)
[zfp]: crate::array::codec::array_to_bytes::zfp
[pcodec]: crate::array::codec::array_to_bytes::pcodec
[vlen]: crate::array::codec::array_to_bytes::vlen
[vlen_v2]: crate::array::codec::array_to_bytes::vlen_v2
[bz2]: crate::array::codec::bytes_to_bytes::bz2
[gdeflate]: crate::array::codec::bytes_to_bytes::gdeflate
