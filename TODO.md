## TODO

- Add a simple API for transparently accessing inner chunks of a sharded zarr array. Cache the shard index for accessed chunks.
- Reduce code duplication in tests
- Review documentation
- Increase test coverage
- Variable sized data type support (waiting on ZEP)
- URI support for stores [see ZEP0008](https://github.com/zarr-developers/zeps/pull/48)
- Implement `{Async}BytesPartialDecoderTraits::partial_decode_concat()` for each bytes-to-bytes codec
- Async support for StorageValueIO for async zip store?
