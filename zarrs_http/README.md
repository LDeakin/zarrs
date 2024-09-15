# zarrs_http

[![Latest Version](https://img.shields.io/crates/v/zarrs_http.svg)](https://crates.io/crates/zarrs_http)
[![zarrs_http documentation](https://docs.rs/zarrs_http/badge.svg)](https://docs.rs/zarrs_http)
![msrv](https://img.shields.io/crates/msrv/zarrs_http)
[![build](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml/badge.svg)](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml)

A synchronous `http` store for the [`zarrs`](https://crates.io/crates/zarrs) Rust crate.

For asynchronous `HTTP` support, use [`zarrs_object_store`](https://crates.io/crates/zarrs_object_store) or [`zarrs_opendal`](https://crates.io/crates/zarrs_opendal).

```rust
use zarrs_storage::ReadableStorage;
use zarrs_http::HTTPStore;

let http_store: ReadableStorage = Arc::new(HTTPStore::new("http://...")?);
```

## Licence
`zarrs_http` is licensed under either of
 - the Apache License, Version 2.0 [LICENSE-APACHE](./LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
 - the MIT license [LICENSE-MIT](./LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
