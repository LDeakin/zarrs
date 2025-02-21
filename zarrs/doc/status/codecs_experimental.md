Experimental codecs are recommended for evaluation only and may have limited ecosystem compatibility.
By default, the `"name"` of of experimental codecs in array metadata links the codec documentation in this crate.
This is configurable with [`Config::experimental_codec_names_mut`](config::Config::experimental_codec_names_mut).

The experimental codecs with a &check; in the `V2` column below were developed with `zarr-python` 2.x.x compatibility.
They should also be compatible with `zarr-python` 3.x.x with `zarr_format=2` arrays.

Arrays created with `zarr-python` 3.x.x with codecs in the `numcodecs.zarr3` submodule are supported.
However, arrays must be written with `numcodecs` >0.14.1 which is unreleased at the time of writing.

By default, arrays encoded with `numcodecs.zarr3` codecs will fail to open because the `numcodecs.*` prefix for codec names is not supported by default.
This is intentional to encourage standardisation of some of these experimental codecs in the near future.
To enable support, the `numcodecs` codec names needs to be remapped to the identifier of the `zarrs` codec:
```rust,ignore
{
    use zarrs_metadata::v3::array::codec;
    let mut config = crate::config::global_config_mut();
    let experimental_codec_names = config.experimental_codec_names_mut();
    experimental_codec_names.insert(codec::zfp::IDENTIFIER.to_string(), "numcodecs.zfpy".to_string());
    experimental_codec_names.insert(codec::pcodec::IDENTIFIER.to_string(), "numcodecs.pcodec".to_string());
    experimental_codec_names.insert(codec::bz2::IDENTIFIER.to_string(), "numcodecs.bz2".to_string());
}
```

| Codec Type     | Codec                          | Default Name                                        | V3      | V2      | Feature Flag |
| -------------- | ------------------------------ | --------------------------------------------------- | ------- | ------- | ------------ |
| Array to Array | [codec_bitround]               | <https://codec.zarrs.dev/array_to_array/bitround>   | &check; | &check; | bitround     |
| Array to Bytes | [codec_zfp]                    | <https://codec.zarrs.dev/array_to_bytes/zfp>        | &check; |         | zfp          |
|                | [codec_zfpy]                   | zfpy                                                | ↑       | &check; | zfp          |
|                | [codec_pcodec]                 | <https://codec.zarrs.dev/array_to_bytes/pcodec>     | &check; | &check; | pcodec       |
|                | [codec_vlen]                   | <https://codec.zarrs.dev/array_to_bytes/vlen>       | &check; |         |              |
|                | [codec_vlen-array]             | <https://codec.zarrs.dev/array_to_bytes/vlen_array> | &check; | &check; |              |
|                | [codec_vlen-bytes]             | <https://codec.zarrs.dev/array_to_bytes/vlen_bytes> | &check; | &check; |              |
|                | [codec_vlen-utf8]              | <https://codec.zarrs.dev/array_to_bytes/vlen_utf8>  | &check; | &check; |              |
| Bytes to Bytes | [codec_bz2]                    | <https://codec.zarrs.dev/bytes_to_bytes/bz2>        | &check; | &check; | bz2          |
|                | [codec_gdeflate]               | <https://codec.zarrs.dev/bytes_to_bytes/gdeflate>   | &check; |         | gdeflate     |
|                | [codec_fletcher32]             | <https://codec.zarrs.dev/bytes_to_bytes/fletcher32> | &check; | &check; | fletcher32   |

[codec_bitround]: crate::array::codec::array_to_array::bitround
[codec_zfp]: crate::array::codec::array_to_bytes::zfp
[codec_zfpy]: https://numcodecs.readthedocs.io/en/latest/compression/zfpy.html
[codec_pcodec]: crate::array::codec::array_to_bytes::pcodec
[codec_vlen]: crate::array::codec::array_to_bytes::vlen
[codec_vlen-array]: crate::array::codec::array_to_bytes::vlen_array
[codec_vlen-bytes]: crate::array::codec::array_to_bytes::vlen_bytes
[codec_vlen-utf8]: crate::array::codec::array_to_bytes::vlen_utf8
[codec_bz2]: crate::array::codec::bytes_to_bytes::bz2
[codec_gdeflate]: crate::array::codec::bytes_to_bytes::gdeflate
[codec_fletcher32]: crate::array::codec::bytes_to_bytes::fletcher32
