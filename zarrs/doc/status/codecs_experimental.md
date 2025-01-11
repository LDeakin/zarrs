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
    let mut config = crate::config::global_config_mut();
    let experimental_codec_names = config.experimental_codec_names_mut();
    experimental_codec_names.insert("zfp".to_string(), "numcodecs.zfpy".to_string());
    experimental_codec_names.insert("pcodec".to_string(), "numcodecs.pcodec".to_string());
    experimental_codec_names.insert("bz2".to_string(), "numcodecs.bz2".to_string());
}
```

| Codec Type     | Codec                    | Default Name                                        | V3      | V2      | Feature Flag |
| -------------- | ------------------------ | --------------------------------------------------- | ------- | ------- | ------------ |
| Array to Array | [bitround]               | <https://codec.zarrs.dev/array_to_array/bitround>   | &check; | &check; | bitround     |
| Array to Bytes | [zfp]                    | <https://codec.zarrs.dev/array_to_bytes/zfp>        | &check; |         | zfp          |
|                | [zfpy]                   | zfpy                                                | ↑       | &check; | zfp          |
|                | [pcodec]                 | <https://codec.zarrs.dev/array_to_bytes/pcodec>     | &check; | &check; | pcodec       |
|                | [vlen]                   | <https://codec.zarrs.dev/array_to_bytes/vlen>       | &check; |         |              |
|                | [vlen-array]             | <https://codec.zarrs.dev/array_to_bytes/vlen_array> | &check; | &check; |              |
|                | [vlen-bytes]             | <https://codec.zarrs.dev/array_to_bytes/vlen_bytes> | &check; | &check; |              |
|                | [vlen-utf8]              | <https://codec.zarrs.dev/array_to_bytes/vlen_utf8>  | &check; | &check; |              |
| Bytes to Bytes | [bz2]                    | <https://codec.zarrs.dev/bytes_to_bytes/bz2>        | &check; | &check; | bz2          |
|                | [gdeflate]               | <https://codec.zarrs.dev/bytes_to_bytes/gdeflate>   | &check; |         | gdeflate     |
|                | [fletcher32]             | <https://codec.zarrs.dev/bytes_to_bytes/fletcher32> | &check; | &check; | fletcher32   |

[bitround]: (crate::array::codec::array_to_array::bitround)
[zfp]: crate::array::codec::array_to_bytes::zfp
[zfpy]: https://numcodecs.readthedocs.io/en/latest/compression/zfpy.html
[pcodec]: crate::array::codec::array_to_bytes::pcodec
[vlen]: crate::array::codec::array_to_bytes::vlen
[vlen-array]: crate::array::codec::array_to_bytes::vlen_array
[vlen-bytes]: crate::array::codec::array_to_bytes::vlen_bytes
[vlen-utf8]: crate::array::codec::array_to_bytes::vlen_utf8
[bz2]: crate::array::codec::bytes_to_bytes::bz2
[gdeflate]: crate::array::codec::bytes_to_bytes::gdeflate
[fletcher32]: crate::array::codec::bytes_to_bytes::fletcher32
