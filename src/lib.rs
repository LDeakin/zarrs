//! A rust library for the [Zarr V3](https://zarr.dev) storage format for multidimensional arrays and metadata.
//!
//! Developed at the [Department of Materials Physics](https://physics.anu.edu.au/research/mp/), Australian National University, Canberra, Australia.
//!
//! **zarrs is experimental and in limited production use. Use at your own risk! Correctness issues with past versions are [detailed here](#correctness-issues-with-past-versions).**
//!
//! A changelog can be found [here](https://github.com/LDeakin/zarrs/blob/main/CHANGELOG.md).
//!
//! The best docs to start with are [`array::Array`] and [`storage::store`].
//!
//! ## Implementation Status
//! - [x] [ZEP0001 - Zarr specification version 3](https://zarr.dev/zeps/accepted/ZEP0001.html).
//! - [x] [ZEP0002 - Sharding codec](https://zarr.dev/zeps/accepted/ZEP0002.html).
//! - [x] [ZEP0003 - Variable chunking](https://zarr.dev/zeps/draft/ZEP0003.html) ([draft](https://github.com/orgs/zarr-developers/discussions/52)).
//! - [x] Stores and storage adapters:
//!   - Sync:
//!     - [`FilesystemStore`](crate::storage::store::FilesystemStore).
//!     - [`MemoryStore`](crate::storage::store::MemoryStore).
//!     - [`HTTPStore`](crate::storage::store::HTTPStore).
//!     - [`ZipStorageAdapter`](crate::storage::storage_adapter::ZipStorageAdapter).
//!     - [`OpendalStore`](crate::storage::store::OpendalStore) (supports all [`opendal` services](https://docs.rs/opendal/latest/opendal/services/index.html) as [`opendal::BlockingOperator`]).
//!   - Async:
//!     - [`AsyncObjectStore`](crate::storage::store::AsyncObjectStore) (supports all [`object_store` stores](https://docs.rs/object_store/latest/object_store/index.html#modules)).
//!     - [`AsyncOpendalStore`](crate::storage::store::AsyncOpendalStore) (supports all [`opendal` services](https://docs.rs/opendal/latest/opendal/services/index.html) as [`opendal::Operator`]).
//! - [x] Data types: [core data types](crate::array::data_type::DataType), [raw bits](crate::array::data_type::DataType::RawBits), [float16](crate::array::data_type::DataType::Float16), [bfloat16](crate::array::data_type::DataType::BFloat16) [(spec issue)](https://github.com/zarr-developers/zarr-specs/issues/130).
//! - [x] Chunk grids: [regular](crate::array::chunk_grid::RegularChunkGrid), [rectangular](crate::array::chunk_grid::RectangularChunkGrid) ([draft](https://github.com/orgs/zarr-developers/discussions/52)).
//! - [x] Chunk key encoding: [default](crate::array::chunk_key_encoding::DefaultChunkKeyEncoding), [v2](crate::array::chunk_key_encoding::V2ChunkKeyEncoding).
//! - [x] Codecs:
//!   - Array to array: [transpose](crate::array::codec::array_to_array::transpose).
//!     - Experimental: [bitround](crate::array::codec::array_to_array::bitround).
//!   - Array to bytes: [bytes](crate::array::codec::array_to_bytes::bytes), [sharding indexed](crate::array::codec::array_to_bytes::sharding).
//!     - Experimental: [zfp](crate::array::codec::array_to_bytes::zfp), [pcodec](crate::array::codec::array_to_bytes::pcodec).
//!   - Bytes to bytes: [blosc](crate::array::codec::bytes_to_bytes::blosc), [gzip](crate::array::codec::bytes_to_bytes::gzip), [zstd](crate::array::codec::bytes_to_bytes::zstd) [(spec issue)](https://github.com/zarr-developers/zarr-specs/pull/256), [crc32c checksum](crate::array::codec::bytes_to_bytes::crc32c).
//!     - Experimental: [bz2](crate::array::codec::bytes_to_bytes::bz2).
//! - [x] Storage transformers: [usage log](crate::storage::storage_transformer::UsageLogStorageTransformer), [performance metrics](crate::storage::storage_transformer::PerformanceMetricsStorageTransformer).
//!
//! ## Crate Features
//! The following crate features are enabled by default:
//!  - `ndarray`: [`ndarray`] utility functions for [`Array`](crate::array::Array).
//!  - Codecs
//!    - `blosc`, `gzip`, `transpose`, `zstd`, `sharding`, `crc32c`.
//!
//! The following features are disabled by default:
//!  - `async`: an experimental asynchronous API for [`stores`](storage), [`Array`](crate::array::Array), and [Group](group::Group).
//!    - currently, this API is not as performant as the synchronous API
//!  - Codecs
//!    - `bitround`, `zfp`, `bz2`, `pcodec`.
//!  - Stores
//!    - `object_store`: support for [`object_store`] stores.
//!    - `opendal`: support for [`opendal`] stores.
//!
//! ## Examples
//! Examples can be run with `cargo run --example <EXAMPLE_NAME>`.
//!
//! Add `-- --usage-log` to see storage API calls during example execution.
//!
//! Some examples require non-default features, which can be enabled with `--features <FEATURES>`.
//!
//! #### Sync API
//! [`array_write_read`](https://github.com/LDeakin/zarrs/blob/main/examples/array_write_read.rs),
//! [`array_write_read_ndarray`](https://github.com/LDeakin/zarrs/blob/main/examples/array_write_read_ndarray.rs),
//! [`sharded_array_write_read`](https://github.com/LDeakin/zarrs/blob/main/examples/sharded_array_write_read.rs),
//! [`rectangular_array_write_read`](https://github.com/LDeakin/zarrs/blob/main/examples/rectangular_array_write_read.rs),
//! [`zip_array_write_read`](https://github.com/LDeakin/zarrs/blob/main/examples/zip_array_write_read.rs),
//! [`http_array_read`](https://github.com/LDeakin/zarrs/blob/main/examples/http_array_read.rs).
//!
//! #### Async API
//! [`async_array_write_read`](https://github.com/LDeakin/zarrs/blob/main/examples/async_array_write_read.rs),
//! [`async_http_array_read`](https://github.com/LDeakin/zarrs/blob/main/examples/async_http_array_read.rs).
//!
//! ## Zarrs Ecosystem
//! - [zarrs-ffi](https://github.com/LDeakin/zarrs-ffi): A subset of zarrs exposed as a C API.
//! - [zarrs_tools](https://github.com/LDeakin/zarrs_tools): Various tools for creating and manipulating Zarr v3 data.
//!
//! ## Correctness Issues with Past Versions
//! - Prior to zarrs [v0.11.5](https://github.com/LDeakin/zarrs/releases/tag/v0.11.5), arrays that used the `crc32c` codec have invalid chunk checksums
//!   - Such arrays will fail to be read by other zarr implementations that validate checksums
//!   - These arrays can be read by zarrs if the [validate checksums](crate::config::Config#validate-checksums) global configuration option is disabled
//! - From zarrs [v0.11.2](https://github.com/LDeakin/zarrs/releases/tag/v0.11.2)-[v0.11.3](https://github.com/LDeakin/zarrs/releases/tag/v0.11.3), the codec configuration of the `crc32c` codec or `bytes` codec (with unspecified endianness) does not conform to the zarr specification
//!   - Such arrays will likely fail to be read by other zarr implementations
//!   - Zarrs still supports reading these arrays, but this may become an error in a future release
//!   - Fixing these arrays only requires a simple metadata correction, e.g.
//!     - `sed -i -E "s/(^([ tab]+)\"(crc32c|bytes)\"(,?)$)/\2{ \"name\": \"\3\" }\4/" zarr.json`
//!
//! [zarrs_tools](https://github.com/LDeakin/zarrs_tools) v0.2.3+ can fix arrays with the above correctness issues with `zarrs_reencode`. Example:
//! ```bash
//! zarrs_reencode --ignore-checksums array.zarr array_fixed.zarr
//! ```
//!
//! ## Licence
//! zarrs is licensed under either of
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
#![cfg_attr(nightly, feature(doc_auto_cfg))]

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

/// Re-export [`bytemuck`].
pub use bytemuck;

/// Re-export [`bytes`].
pub use bytes;

/// Re-export [`dyn_clone`].
pub use dyn_clone;

/// Re-export [`half`].
pub use half;

/// Re-export [`serde_json`].
pub use serde_json;

#[cfg(feature = "ndarray")]
/// Re-export [`ndarray`].
pub use ndarray;

#[cfg(feature = "object_store")]
/// Re-export [`object_store`].
pub use object_store;

#[cfg(feature = "opendal")]
/// Re-export [`opendal`].
pub use opendal;

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
