# zarrs_object_store

[![Latest Version](https://img.shields.io/crates/v/zarrs_object_store.svg)](https://crates.io/crates/zarrs_object_store)
[![object_store 0.12](https://img.shields.io/badge/object__store-0.12-blue)](https://crates.io/crates/object_store)
[![zarrs_object_store documentation](https://docs.rs/zarrs_object_store/badge.svg)](https://docs.rs/zarrs_object_store)
![msrv](https://img.shields.io/crates/msrv/zarrs_object_store)
[![build](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml/badge.svg)](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml)

[`object_store`](https://crates.io/crates/object_store) store support for the [`zarrs`](https://crates.io/crates/zarrs) Rust crate.

```rust
use zarrs_storage::AsyncReadableWritableListableStorage;
use zarrs_object_store::AsyncObjectStore;

let options = object_store::ClientOptions::new().with_allow_http(true);
let store = object_store::http::HttpBuilder::new()
    .with_url("http://...")
    .with_client_options(options)
    .build()?;
let store: AsyncReadableWritableListableStorage =
    Arc::new(AsyncObjectStore::new(store));
```

## Version Compatibility Matrix
See [doc/version_compatibility_matrix.md](./doc/version_compatibility_matrix.md).

`object_store` is re-exported as a dependency of this crate, so it does not need to be specified as a direct dependency.

However, if `object_store` is a direct dependency, it is necessary to ensure that the version used by this crate is compatible.
This crate can depend on a range of semver-incompatible versions of `object_store`, and Cargo will not automatically choose a single version of `object_store` that satisfies all dependencies.
Use a precise cargo update to ensure compatibility.
For example, if this crate resolves to `object_store` 0.11.1 and your code uses 0.10.2:
```shell
cargo update --package object_store:0.11.1 --precise 0.10.2
```

## Licence
`zarrs_object_store` is licensed under either of
 - the Apache License, Version 2.0 [LICENSE-APACHE](./LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
 - the MIT license [LICENSE-MIT](./LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
