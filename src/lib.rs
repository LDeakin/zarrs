//! `zarrs` is Rust library for the [Zarr](https://zarr.dev) storage format for multidimensional arrays and metadata. It supports:
//! - [Zarr V3](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html), and
//! - A [V3 compatible subset](#arrays-zarr-v3-and-zarr-v2) of [Zarr V2](https://zarr-specs.readthedocs.io/en/latest/v2/v2.0.html).
//!
//! A changelog can be found [here](https://github.com/LDeakin/zarrs/blob/main/CHANGELOG.md).
//! Correctness issues with past versions are [detailed here](https://github.com/LDeakin/zarrs/blob/main/doc/correctness_issues.md).
//!
//! Developed at the [Department of Materials Physics](https://physics.anu.edu.au/research/mp/), Australian National University, Canberra, Australia.
//!
//! ## Getting Started
//! - Review the [implementation status](#implementation-status).
//! - View the [examples](https://github.com/LDeakin/zarrs/tree/main/examples).
//! - Read the [documentation](https://docs.rs/zarrs/latest/zarrs/). [`array::Array`], [`storage`], and [`metadata`] are good places to start.
//! - Check out [zarrs_tools](https://github.com/LDeakin/zarrs_tools) for various tools built upon this crate. Includes:
//!   - A reencoder that can change codecs, chunk shape, convert Zarr V2 to V3, etc.
//!   - Create a Zarr V3 [OME-Zarr](https://ngff.openmicroscopy.org/latest/) hierarchy from a Zarr array.
//!   - Transform arrays: crop, rescale, downsample, gradient magnitude, gaussian, noise filtering, etc.
//!   - Benchmarking tools and performance benchmarks of `zarrs`.
//!
//! ## Example
//! ```rust,ignore
//! # use std::path::PathBuf;
//! # use std::sync::Arc;
//! let store_path: PathBuf = "/path/to/store".into();
//! # let store_path: PathBuf = "tests/data/array_write_read.zarr".into();
//! let store: zarrs::storage::ReadableWritableListableStorage =
//!     Arc::new(zarrs::storage::store::FilesystemStore::new(&store_path)?);
//!
//! let array_path: &str = "/group/array"; // /path/to/store/group/array
//! let array = zarrs::array::Array::open(store, array_path)?;
//!
//! let chunk: ndarray::ArrayD<f32> = array.retrieve_chunk_ndarray(&[1, 0])?;
//! println!("Chunk [1,0] is:\n{chunk}");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! ## Implementation Status
//!
#![doc = include_str!("../doc/status/ZEPs.md")]
//!
//! ### Arrays (Zarr V3 and Zarr V2)
//! <details><summary>Data Types</summary>
//!
#![doc = include_str!("../doc/status/data_types.md")]
//! </details>
//! <details><summary>Codecs</summary>
//!
#![doc = include_str!("../doc/status/codecs.md")]
//! </details>
//! <details><summary>Codecs (Experimental)</summary>
//!
#![doc = include_str!("../doc/status/codecs_experimental.md")]
//! </details>
//! <details><summary>Chunk Grids</summary>
//!
#![doc = include_str!("../doc/status/chunk_grids.md")]
//! </details>
//! <details><summary>Chunk Key Encodings</summary>
//!
#![doc = include_str!("../doc/status/chunk_key_encodings.md")]
//! </details>
//!
//! ### Stores and Storage Transformers
//!
//! <details><summary>Stores and Storage Adapters</summary>
//!
#![doc = include_str!("../doc/status/stores.md")]
//! </details>
//! <details><summary>Storage Transformers</summary>
//!
#![doc = include_str!("../doc/status/storage_transformers.md")]
//! </details>
//!
//! ## Crate Features
//! #### Default
//!  - `ndarray`: [`ndarray`] utility functions for [`Array`](crate::array::Array).
//!  - Codecs: `blosc`, `gzip`, `transpose`, `zstd`, `sharding`, `crc32c`.
//!
//! #### Non-Default
//!  - `async`: an **experimental** asynchronous API for [`stores`](storage), [`Array`](crate::array::Array), and [`Group`](group::Group).
//!    - The async API is runtime-agnostic. This has some limitations that are detailed in the [`Array`](crate::array::Array) docs.
//!    - The async API is not as performant as the sync API.
//!  - Codecs: `bitround`, `bz2`, `pcodec`, `zfp`, `zstd`.
//!  - Stores: `http`, `object_store`, `opendal`, `zip`.
//!
//! ## Examples
//! Examples can be run with `cargo run --example <EXAMPLE_NAME>`.
//!  - Add `-- --usage-log` to see storage API calls during example execution.
//!  - Some examples require non-default features, which can be enabled with `--all-features` or `--features <FEATURES>`.
//!
//! #### Sync API Examples
//! [`array_write_read`](https://github.com/LDeakin/zarrs/blob/main/examples/array_write_read.rs),
//! [`array_write_read_ndarray`](https://github.com/LDeakin/zarrs/blob/main/examples/array_write_read_ndarray.rs),
//! [`sharded_array_write_read`](https://github.com/LDeakin/zarrs/blob/main/examples/sharded_array_write_read.rs),
//! [`rectangular_array_write_read`](https://github.com/LDeakin/zarrs/blob/main/examples/rectangular_array_write_read.rs),
//! [`zip_array_write_read`](https://github.com/LDeakin/zarrs/blob/main/examples/zip_array_write_read.rs),
//! [`http_array_read`](https://github.com/LDeakin/zarrs/blob/main/examples/http_array_read.rs).
//!
//! #### Async API Examples
//! [`async_array_write_read`](https://github.com/LDeakin/zarrs/blob/main/examples/async_array_write_read.rs),
//! [`async_http_array_read_object_store`](https://github.com/LDeakin/zarrs/blob/main/examples/async_http_array_read_object_store.rs),
//! [`async_http_array_read_opendal`](https://github.com/LDeakin/zarrs/blob/main/examples/async_http_array_read_opendal.rs).
//!
//! ## `zarrs` Ecosystem
//! - [zarrs_tools](https://github.com/LDeakin/zarrs_tools): Various tools for creating and manipulating Zarr V3 data.
//! - [zarrs_ffi](https://github.com/LDeakin/zarrs_ffi): A subset of `zarrs` exposed as a C API.
//!
//! ## Licence
//! `zarrs` is licensed under either of
//!  - the Apache License, Version 2.0 [LICENSE-APACHE](./LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
//!  - the MIT license [LICENSE-MIT](./LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.
//!
//! Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

#![warn(unused_variables)]
#![warn(dead_code)]
#![deny(missing_docs)]
// #![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![deny(clippy::missing_panics_doc)]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

pub mod array;
pub mod array_subset;
pub mod byte_range;
pub mod config;
pub mod group;
pub mod metadata;
pub mod node;
pub mod plugin;
pub mod storage;
pub mod version;

/// Get a mutable slice of the spare capacity in a vector.
unsafe fn vec_spare_capacity_to_mut_slice<T>(vec: &mut Vec<T>) -> &mut [T] {
    let spare_capacity = vec.spare_capacity_mut();
    unsafe {
        std::slice::from_raw_parts_mut(
            spare_capacity.as_mut_ptr().cast::<T>(),
            spare_capacity.len(),
        )
    }
}
