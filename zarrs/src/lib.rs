//! `zarrs` is Rust library for the [Zarr](https://zarr.dev) storage format for multidimensional arrays and metadata. It supports:
//! - [Zarr V3](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html), and
//! - A [V3 compatible subset](#implementation-status) of [Zarr V2](https://zarr-specs.readthedocs.io/en/latest/v2/v2.0.html).
//!
//! A changelog can be found [here](https://github.com/LDeakin/zarrs/blob/main/CHANGELOG.md).
//! Correctness issues with past versions are [detailed here](https://github.com/LDeakin/zarrs/blob/main/doc/correctness_issues.md).
//!
//! Developed at the [Department of Materials Physics](https://physics.anu.edu.au/research/mp/), Australian National University, Canberra, Australia.
//!
//! ## Getting Started
//! - Review the [implementation status](#implementation-status), [array support](#array-support), and [storage support](#storage-support).
//! - View the [examples](https://github.com/LDeakin/zarrs/tree/main/examples) and [the example below](#examples).
//! - Read the [documentation](https://docs.rs/zarrs/latest/zarrs/). [`array::Array`] is a good place to start.
//! - Check out [`zarrs` ecosystem](#zarrs-ecosystem).
//!
//! ## Implementation Status
//!
#![doc = include_str!("../doc/status/ZEPs.md")]
//!
//! `zarrs` has first-class Zarr V3 support and additionally supports a *compatible subset* of Zarr V2 data that:
//! - can be converted to V3 with only a metadata change, and
//! - uses array metadata that is recognised and supported for encoding/decoding.
//!
//! An existing V2 or V3 array can be opened with [`Array::open`](crate::array::Array::open).
//! A new array can be created from V2 or V3 metadata with [`Array::new_with_metadata`](crate::array::Array::new_with_metadata).
//! The [`ArrayBuilder`](crate::array::ArrayBuilder) only supports V3 array creation.
//!
//! `zarrs` supports forward conversion of Zarr V2 data to V3.
//! See ["Metadata Convert Version"](crate::config::Config#metadata-convert-version) and ["Metadata Erase Version"](crate::config::Config#metadata-erase-version) for information about manipulating the version of array/group metadata.
//!
//! ### Array Support
//!
//! <details><summary>Data Types</summary>
//!
#![doc = include_str!("../doc/status/data_types.md")]
//! </details>
//!
//! <details><summary>Codecs</summary>
//!
#![doc = include_str!("../doc/status/codecs.md")]
//! </details>
//!
//! <details><summary>Codecs (Experimental)</summary>
//!
#![doc = include_str!("../doc/status/codecs_experimental.md")]
//! </details>
//!
//! <details><summary>Chunk Grids</summary>
//!
#![doc = include_str!("../doc/status/chunk_grids.md")]
//! </details>
//!
//! <details><summary>Chunk Key Encodings</summary>
//!
#![doc = include_str!("../doc/status/chunk_key_encodings.md")]
//! </details>
//!
//! <details><summary>Storage Transformers</summary>
//!
#![doc = include_str!("../doc/status/storage_transformers.md")]
//! </details>
//!
//! ### Storage Support
//!
//! `zarrs` supports stores (filesystem, HTTP, S3, etc.) via crates implementing the [`zarrs_storage`] API.
//!
#![doc = include_str!("../doc/status/stores.md")]
//!
//! A huge range of storage backends are supported via the [`opendal`](https://docs.rs/opendal/latest/opendal/) and [`object_store`](https://docs.rs/opendal/latest/object_store/) crates.
//! The documentation for the [zarrs_opendal] and [zarrs_object_store] crates includes version compatibility matrices with `zarrs` and the associated storage backends.
//! These backends provide more feature complete HTTP stores than [zarrs_http].
//!
//! Asynchronous stores can be used in a synchronous context with the [`AsyncToSyncStorageAdapter`](crate::storage::storage_adapter::async_to_sync::AsyncToSyncStorageAdapter).
//!
//! ## Examples
#![cfg_attr(feature = "ndarray", doc = "```rust")]
#![cfg_attr(not(feature = "ndarray"), doc = "```rust,ignore")]
//! # use std::{path::PathBuf, sync::Arc};
//! use zarrs::group::GroupBuilder;
//! use zarrs::array::{ArrayBuilder, DataType, FillValue, ZARR_NAN_F32};
//! # #[cfg(feature = "gzip")]
//! use zarrs::array::codec::GzipCodec; // requires gzip feature
//! use zarrs::array_subset::ArraySubset;
//! use zarrs::storage::ReadableWritableListableStorage;
//! use zarrs::filesystem::FilesystemStore; // requires filesystem feature
//!
//! // Create a filesystem store
//! let store_path: PathBuf = "/path/to/hierarchy.zarr".into();
//! # let store_path: PathBuf = "tests/data/array_write_read.zarr".into();
//! let store: ReadableWritableListableStorage =
//!     Arc::new(FilesystemStore::new(&store_path)?);
//! # let store = Arc::new(zarrs::storage::store::MemoryStore::new());
//!
//! // Write the root group metadata
//! GroupBuilder::new()
//!     .build(store.clone(), "/")?
//!     // .attributes(...)
//!     .store_metadata()?;
//!
//! // Create a new V3 array using the array builder
//! let array = ArrayBuilder::new(
//!     vec![3, 4], // array shape
//!     DataType::Float32,
//!     vec![2, 2].try_into()?, // regular chunk shape (non-zero elements)
//!     FillValue::from(ZARR_NAN_F32),
//! )
//! .bytes_to_bytes_codecs(vec![
//! #     #[cfg(feature = "gzip")]
//!     Arc::new(GzipCodec::new(5)?),
//! ])
//! .dimension_names(["y", "x"].into())
//! .attributes(serde_json::json!({"Zarr V3": "is great"}).as_object().unwrap().clone())
//! .build(store.clone(), "/array")?; // /path/to/hierarchy.zarr/array
//!
//! // Store the array metadata
//! array.store_metadata()?;
//! println!("{}", serde_json::to_string_pretty(array.metadata())?);
//! // {
//! //     "zarr_format": 3,
//! //     "node_type": "array",
//! //     ...
//! // }
//!
//! // Perform some operations on the chunks
//! array.store_chunk_elements::<f32>(
//!     &[0, 1], // chunk index
//!     &[0.2, 0.3, 1.2, 1.3]
//! )?;
//! array.store_array_subset_ndarray::<f32, _>(
//!     &[1, 1], // array index (start of subset)
//!     ndarray::array![[-1.1, -1.2], [-2.1, -2.2]]
//! )?;
//! array.erase_chunk(&[1, 1])?;
//!
//! // Retrieve all array elements as an ndarray
//! let array_ndarray = array.retrieve_array_subset_ndarray::<f32>(&array.subset_all())?;
//! println!("{array_ndarray:4}");
//! // [[ NaN,  NaN,  0.2,  0.3],
//! //  [ NaN, -1.1, -1.2,  1.3],
//! //  [ NaN, -2.1,  NaN,  NaN]]
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
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
//! [`sync_http_array_read`](https://github.com/LDeakin/zarrs/blob/main/examples/sync_http_array_read.rs).
//!
//! #### Async API Examples
//! [`async_array_write_read`](https://github.com/LDeakin/zarrs/blob/main/examples/async_array_write_read.rs),
//! [`async_http_array_read`](https://github.com/LDeakin/zarrs/blob/main/examples/async_http_array_read.rs),
//!
//! ## Crate Features
//! #### Default
//!  - `filesystem`: Re-export `zarrs_filesystem` as `zarrs::filesystem`
//!  - `ndarray`: [`ndarray`] utility functions for [`Array`](crate::array::Array).
//!  - Codecs: `blosc`, `gzip`, `transpose`, `zstd`, `sharding`, `crc32c`.
//!
//! #### Non-Default
//!  - `async`: an **experimental** asynchronous API for [`stores`](storage), [`Array`](crate::array::Array), and [`Group`](group::Group).
//!    - The async API is runtime-agnostic. This has some limitations that are detailed in the [`Array`](crate::array::Array) docs.
//!    - The async API is not as performant as the sync API.
//!  - Codecs: `bitround`, `bz2`, `pcodec`, `zfp`, `zstd`.
//!
//! ## `zarrs` Ecosystem
#![doc = include_str!("../doc/ecosystem.md")]
//!
//! ## Licence
//! `zarrs` is licensed under either of
//!  - the Apache License, Version 2.0 [LICENSE-APACHE](https://docs.rs/crate/zarrs/latest/source/LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
//!  - the MIT license [LICENSE-MIT](https://docs.rs/crate/zarrs/latest/source/LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.
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
pub mod config;
pub mod group;
pub mod node;
pub mod plugin;
pub mod version;

pub use zarrs_metadata as metadata;
pub use zarrs_storage as storage;

#[cfg(feature = "filesystem")]
pub use zarrs_filesystem as filesystem;

// Re-export byte_range for compat with <17.0.0
pub use storage::byte_range;

/// Get a mutable slice of the spare capacity in a vector.
#[allow(dead_code)]
unsafe fn vec_spare_capacity_to_mut_slice<T>(vec: &mut Vec<T>) -> &mut [T] {
    let spare_capacity = vec.spare_capacity_mut();
    unsafe {
        std::slice::from_raw_parts_mut(
            spare_capacity.as_mut_ptr().cast::<T>(),
            spare_capacity.len(),
        )
    }
}
