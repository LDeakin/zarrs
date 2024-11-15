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

## Version Compatibility Matrix
See [doc/version_compatibility_matrix.md](./doc/version_compatibility_matrix.md).

`opendal` is re-exported as a dependency of this crate, so it does not need to be specified as a direct dependency.

However, if `opendal` is a direct dependency, it is necessary to ensure that the version used by this crate is compatible.
This crate can depend on a range of semver-incompatible versions of `opendal`, and Cargo will not automatically choose a single version of `opendal` that satisfies all dependencies.
Use a precise cargo update to ensure compatibility.
For example, if this crate resolves to `opendal` 0.50.2 and your code uses 0.49.2:
```shell
cargo update --package opendal:0.50.2 --precise 0.49.2
```

## Licence
`zarrs_opendal` is licensed under either of
 - the Apache License, Version 2.0 [LICENSE-APACHE](./LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
 - the MIT license [LICENSE-MIT](./LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
