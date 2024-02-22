# zarrs

[![Latest Version](https://img.shields.io/crates/v/zarrs.svg)](https://crates.io/crates/zarrs)
[![zarrs documentation](https://docs.rs/zarrs/badge.svg)](https://docs.rs/zarrs)
![msrv](https://img.shields.io/crates/msrv/zarrs)
[![build](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml/badge.svg)](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/LDeakin/zarrs/graph/badge.svg?token=OBKJQNAZPP)](https://codecov.io/gh/LDeakin/zarrs)

A rust library for the [Zarr V3](https://zarr.dev) storage format for multidimensional arrays and metadata.

Developed at the [Department of Materials Physics](https://physics.anu.edu.au/research/mp/), Australian National University, Canberra, Australia.

**zarrs is experimental and in limited production use. Use at your own risk! Correctness issues with past versions are [detailed here](https://docs.rs/zarrs/latest/zarrs/#correctness-issues-with-past-versions).**

- [API documentation (`docs.rs`)](https://docs.rs/zarrs/latest/zarrs/)
- [Changelog (`CHANGELOG.md`)](./CHANGELOG.md)
- [Examples (`./examples`)](./examples)

## Zarrs Ecosystem
- [zarrs-ffi](https://github.com/LDeakin/zarrs-ffi): A subset of zarrs exposed as a C API.
- [zarrs_tools](https://github.com/LDeakin/zarrs_tools): Various tools for creating and manipulating Zarr v3 data. Includes `zarrs` benchmarks.

## Licence
`zarrs` is licensed under either of
 - the Apache License, Version 2.0 [LICENSE-APACHE](./LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
 - the MIT license [LICENSE-MIT](./LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
