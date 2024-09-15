# zarrs_storage

[![Latest Version](https://img.shields.io/crates/v/zarrs_storage.svg)](https://crates.io/crates/zarrs_storage)
[![zarrs_storage documentation](https://docs.rs/zarrs_storage/badge.svg)](https://docs.rs/zarrs_storage)
![msrv](https://img.shields.io/crates/msrv/zarrs_storage)
[![build](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml/badge.svg)](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml)

The storage API for the [`zarrs`](https://crates.io/crates/zarrs) Rust crate.

A Zarr store is a system that can be used to store and retrieve data from a Zarr hierarchy.
For example: a filesystem, HTTP server, FTP server, Amazon S3 bucket, ZIP file, etc.
The Zarr V3 storage API is detailed here: <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#storage>.

This crate includes an in-memory store implementation. See [`zarrs` storage support](https://docs.rs/zarrs/latest/zarrs/index.html#storage-support) for a list of stores that implement the `zarrs_storage` API.

## Licence
`zarrs_storage` is licensed under either of
 - the Apache License, Version 2.0 [LICENSE-APACHE](./LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
 - the MIT license [LICENSE-MIT](./LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
