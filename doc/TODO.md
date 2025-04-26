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
- Test an io_uring backed filesystem store
    - **Pending**: OpenDAL [real async filesystem IO](https://github.com/apache/opendal/issues/4520)
- Support additional registered extension points [zarr-developers/zarr-extensions]

### Maintenance/Code Quality
- Increase test coverage
- Use `async_generic` to reduce `async` code duplication
  - Reduce code duplication in async tests
- Conformance test suite for `zarrs`, `zarr-python`, `tensorstore`, `zarrita.js`, etc?

### Register experimental codecs at [zarr-developers/zarr-extensions]
  - `bitround` (integer data types)
  - `vlen`/`vlen_v2`: ZEP0007
  - `gdeflate`
  - `squeeze`

[zarr-developers/zarr-extensions]: https://github.com/zarr-developers/zarr-extensions
