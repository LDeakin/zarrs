## TODO

### Features/API
- Stabilise the async API
    - Improve async API performance
    - New async API supporting internal task spawning
        - Prototype in the [async_spawning](https://github.com/LDeakin/zarrs_tools/tree/async_spawning) branch
    - Use an io_uring backed filesystem store
        - Waiting for opendal to support [real async filesystem IO](https://github.com/apache/opendal/issues/4520)
    - Add `array_sharded_ext::{AsyncArrayShardedExt,AsyncArrayShardedReadableExt}`
    - Async support for `StorageValueIO` to enable `ZipStorageAdapter` with async stores
- Write ZEPs for experimental codecs and stabilise
  - Waiting on [the new zarr ZEP process](https://github.com/zarr-developers/zeps/pull/59)
  - `bitround`: easiest
  - `zfp`: most valuable
  - `pcodec`: too new/unstable?
- Variable sized data type support [#21](https://github.com/LDeakin/zarrs/issues/21)
  - Waiting on [draft ZEP](https://github.com/zarr-developers/zeps/pull/47)

### Maintenance/Code Quality
- Benchmark against other Zarr V3 implementations
  - Tracked in [LDeakin/zarrs_tools (benchmarks.md)](https://github.com/LDeakin/zarrs_tools/blob/main/docs/benchmarks.md)
- Increase test coverage
- Reduce code duplication in tests
- Review documentation

### Miscellaneous
- A logo for `zarrs`?
  - zar**rs** was chosen because it is short, clearly associated with the zarr format, and subtly includes rs (rust) in the name
