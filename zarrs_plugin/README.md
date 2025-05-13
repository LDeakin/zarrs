# zarrs_plugin

[![Latest Version](https://img.shields.io/crates/v/zarrs_plugin.svg)](https://crates.io/crates/zarrs_plugin)
[![zarrs_plugin documentation](https://docs.rs/zarrs_plugin/badge.svg)](https://docs.rs/zarrs_plugin)
![msrv](https://img.shields.io/crates/msrv/zarrs_plugin)
[![build](https://github.com/zarrs/zarrs/actions/workflows/ci.yml/badge.svg)](https://github.com/zarrs/zarrs/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/zarrs/zarrs/graph/badge.svg?component=zarrs_plugin)](https://codecov.io/gh/zarrs/zarrs)

The plugin API for the [`zarrs`](https://crates.io/crates/zarrs) Rust crate.

A `Plugin` creates concrete implementations of [Zarr V3 extension points](https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html#extension-points) from inputs.
Extension points include chunk grids, chunk key encodings, codecs, data types, and storage transformers.

In `zarrs`, plugins are registered at compile time using the [`inventory`](https://docs.rs/inventory/latest/inventory/) crate.
At runtime, a name matching function is applied to identify which registered plugin is associated with the metadata.
If a match is found, the plugin is created from the metadata and other relevant inputs.

## Licence
`zarrs_plugin` is licensed under either of
 - the Apache License, Version 2.0 [LICENSE-APACHE](./LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
 - the MIT license [LICENSE-MIT](./LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
