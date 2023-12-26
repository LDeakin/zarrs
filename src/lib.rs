//! A rust library for the [Zarr V3](https://zarr.dev) storage format for multidimensional arrays and metadata.
//!
//! Developed at the [Department of Materials Physics](https://physics.anu.edu.au/research/mp/), Australian National University, Canberra, Australia.
//!
//! **zarrs is experimental and in limited production use. Use at your own risk!**
//!
//! A changelog can be found [here](https://github.com/LDeakin/zarrs/blob/main/CHANGELOG.md).
//!
//! The recommended places to start in the docs are the [`crate::array`] and [`crate::storage`] modules and their submodules.
//!
//! ## Stability
//! Zarrs will remain unstable as the API is refined and the Zarr V3 specification is finalised.
//!
//! ## Implementation Status
//! - [x] [ZEP0001 - Zarr specification version 3](https://zarr.dev/zeps/draft/ZEP0001.html).
//! - [x] [ZEP0002 - Sharding codec](https://zarr.dev/zeps/draft/ZEP0002.html) ([under review](https://github.com/zarr-developers/zarr-specs/issues/254)).
//! - [x] [ZEP0003 - Variable chunking](https://zarr.dev/zeps/draft/ZEP0003.html) ([draft](https://github.com/orgs/zarr-developers/discussions/52)).
//! - [x] Stores and storage adapters:
//!   - Sync:
//!     - [`FilesystemStore`](crate::storage::store::FilesystemStore).
//!     - [`MemoryStore`](crate::storage::store::MemoryStore).
//!     - [`HTTPStore`](crate::storage::store::HTTPStore).
//!     - [`ZipStorageAdapter`](crate::storage::storage_adapter::ZipStorageAdapter).
//!     - [`OpendalStore`](crate::storage::store::OpendalStore) (supports all [`opendal` services](https://docs.rs/opendal/latest/opendal/services/index.html)).
//!   - Async:
//!     - [`AsyncObjectStore`](crate::storage::store::AsyncObjectStore) (supports all [`object_store` stores](https://docs.rs/object_store/latest/object_store/index.html#modules)).
//!     - [`AsyncOpendalStore`](crate::storage::store::AsyncOpendalStore) (supports all [`opendal` services](https://docs.rs/opendal/latest/opendal/services/index.html)).
//! - [x] Data types: [core data types](crate::array::data_type::DataType), [raw bits](crate::array::data_type::DataType::RawBits), [float16](crate::array::data_type::DataType::Float16), [bfloat16](crate::array::data_type::DataType::BFloat16) [(spec issue)](https://github.com/zarr-developers/zarr-specs/issues/130).
//! - [x] Chunk grids: [regular](crate::array::chunk_grid::RegularChunkGrid), [rectangular](crate::array::chunk_grid::RectangularChunkGrid) ([draft](https://github.com/orgs/zarr-developers/discussions/52)).
//! - [x] Chunk key encoding: [default](crate::array::chunk_key_encoding::DefaultChunkKeyEncoding), [v2](crate::array::chunk_key_encoding::V2ChunkKeyEncoding).
//! - [x] Codecs:
//!   - array to array: [transpose](crate::array::codec::array_to_array::transpose), [bitround](crate::array::codec::array_to_array::bitround) (experimental).
//!   - array to bytes: [bytes](crate::array::codec::array_to_bytes::bytes) [(spec issue)](https://github.com/zarr-developers/zarr-specs/pull/263), [sharding indexed](crate::array::codec::array_to_bytes::sharding), [zfp](crate::array::codec::array_to_bytes::zfp) (experimental).
//!   - bytes to bytes: [blosc](crate::array::codec::bytes_to_bytes::blosc), [gzip](crate::array::codec::bytes_to_bytes::gzip), [zstd](crate::array::codec::bytes_to_bytes::zstd) [(spec issue)](https://github.com/zarr-developers/zarr-specs/pull/256), [crc32c checksum](crate::array::codec::bytes_to_bytes::crc32c).
//! - [x] Storage transformers: [usage log](crate::storage::storage_transformer::UsageLogStorageTransformer), [performance metrics](crate::storage::storage_transformer::PerformanceMetricsStorageTransformer).
//!
//! ## Crate Features
//! The following crate features are enabled by default:
//!  - `ndarray`: [`ndarray`] utility functions for [`Array`](crate::array::Array).
//!  - Codecs: `blosc`, `gzip`, `transpose`, `zstd`, `sharding`, `crc32c`.
//!
//! The following features are disabled by default:
//!  - Codecs: `bitround`, `zfp`.
//!  - `async`: asynchronous API for stores, arrays, and groups.
//!  - `object_store`: support for [`object_store`] stores.
//!  - `opendal`: support for [`opendal`] stores.
//!
//! ## Examples
//! Examples can be run with `cargo run --example EXAMPLE_NAME`.
//!
//! #### Sync API
//! - [`array_read_write`](https://github.com/LDeakin/zarrs/blob/main/examples/array_write_read.rs): create an array, write its metadata, write chunks in parallel, delete a chunk, read the whole array, read a chunk, and partially read a subset.
//! - [`sharded_array_read_write`](https://github.com/LDeakin/zarrs/blob/main/examples/sharded_array_write_read.rs): write and read a sharded array.
//! - [`rectangular_array_read_write`](https://github.com/LDeakin/zarrs/blob/main/examples/rectangular_array_write_read.rs): write and read an array with a rectangular chunk grid.
//! - [`zip_array_read_write`](https://github.com/LDeakin/zarrs/blob/main/examples/zip_array_write_read.rs): write an array to a filesystem, zip it, then read it from the zipped file.
//! - [`http_array_read`](https://github.com/LDeakin/zarrs/blob/main/examples/http_array_read.rs): read an array over HTTP.
//!
//! #### Async API
//! - [`async_array_read_write`](https://github.com/LDeakin/zarrs/blob/main/examples/array_write_read.rs).
//! - [`async_http_array_read`](https://github.com/LDeakin/zarrs/blob/main/examples/http_array_read.rs).
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
pub mod group;
pub mod metadata;
pub mod node;
pub mod plugin;
pub mod storage;
pub mod version;
