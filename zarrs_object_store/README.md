# zarrs_object_store

[![Latest Version](https://img.shields.io/crates/v/zarrs_object_store.svg)](https://crates.io/crates/zarrs_object_store)
[![object_store 0.11](https://img.shields.io/badge/object__store-0.11-blue)](https://crates.io/crates/object_store)
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

## Licence
`zarrs_object_store` is licensed under either of
 - the Apache License, Version 2.0 [LICENSE-APACHE](./LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
 - the MIT license [LICENSE-MIT](./LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
