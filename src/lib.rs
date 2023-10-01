//! A rust library for the [Zarr V3](https://zarr.dev) storage format for multidimensional arrays and metadata.
//!
//! Developed at the [Department of Materials Physics](https://physics.anu.edu.au/research/mp/), Australian National University, Canberra, Australia.
//!
//! **zarrs and zarrs-ffi is experimental and in limited production use. Use at your own risk!**
//!
//! A subset of zarrs is exposed as a C API in the [zarrs-ffi](https://github.com/LDeakin/zarrs-ffi) crate.
//!
//! ## Features
//! All features are enabled by default.
//!  - Codecs: `blosc`, `gzip`, `transpose`, `zstd`, `sharding`, `crc32c`.
//!  - Data types: `raw_bits`, `float16`, `bfloat16`.
//!  - `ndarray`: adds [`ndarray`] utility functions to [`Array`](crate::array::Array).
//!
//! ## Implementation Status
//! - [x] [ZEP0001 - Zarr specification version 3](https://zarr.dev/zeps/draft/ZEP0001.html)
//! - [x] [ZEP0002 - Sharding codec](https://zarr.dev/zeps/draft/ZEP0002.html) ([under review](https://github.com/zarr-developers/zarr-specs/issues/254))
//! - [x] [ZEP0003 - Variable chunking](https://zarr.dev/zeps/draft/ZEP0003.html) ([draft](https://github.com/orgs/zarr-developers/discussions/52))
//! - [x] Stores: [`filesystem`](crate::storage::store::FilesystemStore), [`memory`](crate::storage::store::MemoryStore), [`zip`](crate::storage::storage_adapter::ZipStorageAdapter)
//! - [x] Data types: [core data types](crate::array::data_type::DataType), [`raw bits`](crate::array::data_type::RawBitsDataType), [`float16`](crate::array::data_type::Float16DataType), [`bfloat16`](crate::array::data_type::Bfloat16DataType) [(spec issue)](https://github.com/zarr-developers/zarr-specs/issues/130)
//! - [x] Chunk grids: [`regular`](crate::array::chunk_grid::RegularChunkGrid), [`rectangular`](crate::array::chunk_grid::RectangularChunkGrid) ([draft](https://github.com/orgs/zarr-developers/discussions/52))
//! - [x] Chunk key encoding: [`default`](crate::array::chunk_key_encoding::DefaultChunkKeyEncoding), [`v2`](crate::array::chunk_key_encoding::V2ChunkKeyEncoding)
//! - [x] Codecs: [`blosc`](crate::array::codec::BloscCodec), [`bytes`](crate::array::codec::BytesCodec) [(spec issue)](https://github.com/zarr-developers/zarr-specs/pull/263), [`gzip`](crate::array::codec::GzipCodec), [`transpose`](crate::array::codec::TransposeCodec), [`zstd`](crate::array::codec::ZstdCodec) [(spec issue)](https://github.com/zarr-developers/zarr-specs/pull/256), [`sharding`](crate::array::codec::ShardingCodec), [`crc32c checksum`](crate::array::codec::Crc32cCodec)
//! - [x] Storage transformers: [`usage_log`](crate::storage::storage_transformer::UsageLogStorageTransformer), [`performance_metrics`](crate::storage::storage_transformer::PerformanceMetricsStorageTransformer)
//!
//! ## Examples
//! - `array_read_write` demonstrates creating an array, writing its metadata, writing chunks in parallel, reading the whole array, reading a chunk, and partially reading a subset.
//! > `cargo run --example array_write_read`
//!
//! - `sharded_array_write_read` is similar to `array_read_write`, but with a sharded array.
//! > `cargo run --example sharded_array_write_read`
//!
//! - `rectangular_array_write_read` is similar to `array_read_write`, but with a rectangular chunk grid.
//! > `cargo run --example rectangular_array_write_read`
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

pub mod array;
pub mod array_subset;
pub mod byte_range;
pub mod group;
pub mod metadata;
pub mod node;
pub mod plugin;
pub mod storage;
pub mod version;
