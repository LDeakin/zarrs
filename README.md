# zarrs

[![Latest Version](https://img.shields.io/crates/v/zarrs.svg)](https://crates.io/crates/zarrs)
[![zarrs documentation](https://docs.rs/zarrs/badge.svg)](https://docs.rs/zarrs)
![msrv](https://img.shields.io/crates/msrv/zarrs)
[![build](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml/badge.svg)](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/LDeakin/zarrs/graph/badge.svg?token=OBKJQNAZPP)](https://codecov.io/gh/LDeakin/zarrs)

A rust library for the [Zarr V3](https://zarr.dev) storage format for multidimensional arrays and metadata.

Developed at the [Department of Materials Physics](https://physics.anu.edu.au/research/mp/), Australian National University, Canberra, Australia.

**zarrs is experimental and in limited production use. Use at your own risk! Correctness issues with past versions are [detailed here](https://github.com/LDeakin/zarrs/blob/main/doc/correctness_issues.md).**

A changelog can be found [here](https://github.com/LDeakin/zarrs/blob/main/CHANGELOG.md).

## Getting Started
- View the [examples](https://github.com/LDeakin/zarrs/tree/main/examples).
- Read the [documentation](https://docs.rs/zarrs/latest/zarrs/). [`array::Array`](https://docs.rs/zarrs/latest/zarrs/array/struct.Array.html) and [`storage`](https://docs.rs/zarrs/latest/zarrs/storage/index.html) are good places to start.
- Check out [zarrs_tools](https://github.com/LDeakin/zarrs_tools) for real-world usage and performance benchmarks of `zarrs`.

## Example
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
