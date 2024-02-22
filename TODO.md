## TODO

- Reduce code duplication in tests
- Review documentation
- Increase test coverage
- Variable sized data type support (waiting on ZEP)
- URI support for stores [see ZEP0008](https://github.com/zarr-developers/zeps/pull/48)
- Implement `{Async}BytesPartialDecoderTraits::partial_decode_concat()` for each bytes-to-bytes codec
- **Breaking**: Remove default implementation for `{Async}BytesPartialDecoderTraits::partial_decode_concat()`
