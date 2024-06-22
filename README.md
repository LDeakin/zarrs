# zar<u>*rs*</u>

[![Latest Version](https://img.shields.io/crates/v/zarrs.svg)](https://crates.io/crates/zarrs)
[![zarrs documentation](https://docs.rs/zarrs/badge.svg)](https://docs.rs/zarrs)
![msrv](https://img.shields.io/crates/msrv/zarrs)
[![downloads](https://img.shields.io/crates/d/zarrs)](https://crates.io/crates/zarrs)
[![build](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml/badge.svg)](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/LDeakin/zarrs/graph/badge.svg?token=OBKJQNAZPP)](https://codecov.io/gh/LDeakin/zarrs)

`zarrs` is a Rust library for the [Zarr](https://zarr.dev) storage format for multidimensional arrays and metadata. It supports:
 - [Zarr V3](https://zarr.readthedocs.io/en/stable/spec/v3.html).
 - [Zarr V2](https://zarr.readthedocs.io/en/stable/spec/v2.html) (a [V3 compatible subset](https://docs.rs/zarrs/latest/zarrs/#arrays-zarr-v3-and-zarr-v2)).

A changelog can be found [here](https://github.com/LDeakin/zarrs/blob/main/CHANGELOG.md).
Correctness issues with past versions are [detailed here](https://github.com/LDeakin/zarrs/blob/main/doc/correctness_issues.md).

Developed at the [Department of Materials Physics](https://physics.anu.edu.au/research/mp/), Australian National University, Canberra, Australia.

## Getting Started
- Review the [implementation status](https://docs.rs/zarrs/latest/zarrs/#implementation-status) to see supported [Zarr ZEPs](https://zarr.dev/zeps/) and Zarr extension points.
- View the [examples](https://github.com/LDeakin/zarrs/tree/main/examples).
- Read the [documentation](https://docs.rs/zarrs/latest/zarrs/). [`array::Array`](https://docs.rs/zarrs/latest/zarrs/array/struct.Array.html), [`storage`](https://docs.rs/zarrs/latest/zarrs/storage/index.html), and [`metadata`](https://docs.rs/zarrs/latest/zarrs/metadata/index.html) are good places to start.
- Check out [zarrs_tools](https://github.com/LDeakin/zarrs_tools) for real-world usage and [performance benchmarks](https://github.com/LDeakin/zarrs_tools/blob/main/docs/benchmarks.md) of `zarrs`.

## Example (Sync API)
```rust
let store_path: PathBuf = "/path/to/store".into();
let store: zarrs::storage::ReadableWritableListableStorage =
    Arc::new(zarrs::storage::store::FilesystemStore::new(&store_path)?);

let array_path: &str = "/group/array"; // /path/to/store/group/array
let array = zarrs::array::Array::new(store, array_path)?;

let chunk: ndarray::ArrayD<f32> = array.retrieve_chunk_ndarray(&[1, 0])?;
println!("Chunk [1,0] is:\n{chunk}");
```

## `zarrs` Ecosystem
- [zarrs_tools](https://github.com/LDeakin/zarrs_tools): Various tools for creating and manipulating Zarr v3 data. Includes `zarrs` benchmarks.
- [zarrs_ffi](https://github.com/LDeakin/zarrs_ffi): A subset of zarrs exposed as a C API.

## Licence
`zarrs` is licensed under either of
 - the Apache License, Version 2.0 [LICENSE-APACHE](./LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
 - the MIT license [LICENSE-MIT](./LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
