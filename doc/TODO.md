## TODO

### Features/API
- Stabilise the async API
    - Support internal task spawning
        - Prototype in the [async_spawning](https://github.com/LDeakin/zarrs_tools/tree/async_spawning) branch
    - Add `array_sharded_ext::{AsyncArrayShardedExt,AsyncArrayShardedReadableExt}`
    - Async support for `StorageValueIO` to enable `ZipStorageAdapter` with async stores
- Variable sized data type support [#21](https://github.com/LDeakin/zarrs/issues/21)
  - **Pending**: [draft ZEP](https://github.com/zarr-developers/zeps/pull/47)

### Experiments
- Test an io_uring backed filesystem store
    - **Pending**: OpenDAL [real async filesystem IO](https://github.com/apache/opendal/issues/4520)

### Maintenance/Code Quality
- Increase test coverage
- Reduce code duplication in tests
- Review documentation

### Write ZEPs?
- **Pending**: [the new Zarr ZEP process](https://github.com/zarr-developers/zeps/pull/59)
- Experimental codecs:
    - `bitround`: easiest
    - `zfp`: most valuable
    - `pcodec`: too new/unstable?
