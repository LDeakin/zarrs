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
//! - Review the [implementation status](#implementation-status).
//! - View the [examples](https://github.com/LDeakin/zarrs/tree/main/examples).
//! - Read the [documentation](https://docs.rs/zarrs/latest/zarrs/). [`array::Array`], [`storage`], and [`metadata`] are good places to start.
//! - Check out [zarrs_tools](https://github.com/LDeakin/zarrs_tools) for various tools built upon this crate. Includes:
//!   - A reencoder that can change codecs, chunk shape, convert Zarr V2 to V3, etc.
//!   - Create a Zarr V3 [OME-Zarr](https://ngff.openmicroscopy.org/latest/) hierarchy from a Zarr array.
//!   - Transform arrays: crop, rescale, downsample, gradient magnitude, gaussian, noise filtering, etc.
//!   - Benchmarking tools and performance benchmarks of `zarrs`.
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
//! `zarrs` supports a huge range of storage backends through the [`opendal`] and [`object_store`] crates.
//!
//! <details><summary>Stores and Storage Adapters</summary>
//!
#![doc = include_str!("../doc/status/stores.md")]
//! </details>
//!
//!
//! ## Examples
#![cfg_attr(feature = "ndarray", doc = "```rust")]
#![cfg_attr(not(feature = "ndarray"), doc = "```rust,ignore")]
//! # use std::{path::PathBuf, sync::Arc};
//! use zarrs::array::{ArrayBuilder, DataType, FillValue, ZARR_NAN_F32};
//! # #[cfg(feature = "gzip")]
//! use zarrs::array::codec::GzipCodec; // requires gzip feature
//! use zarrs::array_subset::ArraySubset;
//! use zarrs::storage::{ReadableWritableListableStorage, store::FilesystemStore};
//!
//! // Create a filesystem store
//! let store_path: PathBuf = "/path/to/store".into();
//! # let store_path: PathBuf = "tests/data/array_write_read.zarr".into();
//! let store: ReadableWritableListableStorage =
//!     Arc::new(FilesystemStore::new(&store_path)?);
//! # let store = Arc::new(zarrs::storage::store::MemoryStore::new());
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
//!     Box::new(GzipCodec::new(5)?),
//! ])
//! .dimension_names(["y", "x"].into())
//! .attributes(serde_json::json!({"Zarr V3": "is great"}).as_object().unwrap().clone())
//! .build(store.clone(), "/group/array")?; // /path/to/store/group/array
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
//!     &[1, 1], // array index
//!     ndarray::array![[-1.1, -1.2], [-2.1, -2.2]]
//! )?;
//! array.erase_chunk(&[1, 1])?;
//!
//! // Retrieve all array elements as an ndarray
//! let array_subset_all = ArraySubset::new_with_shape(array.shape().to_vec());
//! let array_ndarray = array.retrieve_array_subset_ndarray::<f32>(&array_subset_all)?;
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
//! [`http_array_read`](https://github.com/LDeakin/zarrs/blob/main/examples/http_array_read.rs).
//!
//! #### Async API Examples
//! [`async_array_write_read`](https://github.com/LDeakin/zarrs/blob/main/examples/async_array_write_read.rs),
//! [`async_http_array_read_object_store`](https://github.com/LDeakin/zarrs/blob/main/examples/async_http_array_read_object_store.rs),
//! [`async_http_array_read_opendal`](https://github.com/LDeakin/zarrs/blob/main/examples/async_http_array_read_opendal.rs).
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
