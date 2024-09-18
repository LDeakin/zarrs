# zarrs_opendal

[![Latest Version](https://img.shields.io/crates/v/zarrs_opendal.svg)](https://crates.io/crates/zarrs_opendal)
[![opendal 0.50](https://img.shields.io/badge/opendal-0.50-blue)](https://crates.io/crates/opendal)
[![zarrs_opendal documentation](https://docs.rs/zarrs_opendal/badge.svg)](https://docs.rs/zarrs_opendal)
![msrv](https://img.shields.io/crates/msrv/zarrs_opendal)
[![build](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml/badge.svg)](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml)

[`opendal`](https://crates.io/crates/opendal) store support for the [`zarrs`](https://crates.io/crates/zarrs) Rust crate.

```rust
use zarrs_storage::AsyncReadableStorage;
use zarrs_opendal::AsyncOpendalStore;

let builder = opendal::services::Http::default().endpoint("http://...");
let operator = opendal::Operator::new(builder)?.finish();
let store: AsyncReadableStorage = Arc::new(AsyncOpendalStore::new(operator));
```

## Licence
`zarrs_opendal` is licensed under either of
 - the Apache License, Version 2.0 [LICENSE-APACHE](./LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
 - the MIT license [LICENSE-MIT](./LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
