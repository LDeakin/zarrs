## TODO

### Features/API
- Add array methods supporting efficient advanced indexing
    `Array::{store,retrieve}[_elements](indexer: impl Indexer)`
- Stabilise the async API
    - Support internal task spawning?
    - Add `array_sharded_ext::{AsyncArrayShardedExt,AsyncArrayShardedReadableExt}`
    - Async support for `StorageValueIO` to enable `ZipStorageAdapter` with async stores
- Remove most/all `_opt` methods when Rust [`import-trait-associated-functions`](https://github.com/rust-lang/rfcs/pull/3591) stabilises
- Use lending iterators where/if possible to avoid `Vec` allocations in iterators?

### Experiments
- Test an io_uring backed filesystem store
    - **Pending**: OpenDAL [real async filesystem IO](https://github.com/apache/opendal/issues/4520)

### Maintenance/Code Quality
- Increase test coverage
- Reduce code duplication in tests
- Review documentation
- Use `async_generic` to reduce `async` code duplication

### Write/Review ZEPs?
- **Pending**: [the new Zarr ZEP process](https://github.com/zarr-developers/zeps/pull/59)
- Experimental codecs:
    - `bz2`
    - `bitround`
    - `zfp`
    - `pcodec`: too new/unstable?
    - `vlen`/`vlen_v2`: ZEP0007
