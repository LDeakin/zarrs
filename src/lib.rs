//! A rust library for the [Zarr V3](https://zarr.dev) storage format for multidimensional arrays and metadata.
//!
//! Developed at the [Department of Materials Physics](https://physics.anu.edu.au/research/mp/), Australian National University, Canberra, Australia.
//!
//! **zarrs and zarrs-ffi is experimental and in limited production use. Use at your own risk!**
//!
//! A subset of zarrs is exposed as a C API in the [zarrs-ffi](https://github.com/LDeakin/zarrs-ffi) crate.
//!
//! ## Stability
//! Zarrs is unstable and will remain unstable in the near future.
//!  - The API surface for typical use (e.g. array reading and writing) *should* remain quite stable.
//!  - Breaking changes are more likely to impact custom codecs, stores, storage transformers, etc.
//!  - Zarr V3 has been approved but still has some pending revisions.
//!
//! A changelog can be found [here](https://github.com/LDeakin/zarrs/blob/main/CHANGELOG.md).
//!
//! ## Implementation Status
//! - [x] [ZEP0001 - Zarr specification version 3](https://zarr.dev/zeps/draft/ZEP0001.html)
//! - [x] [ZEP0002 - Sharding codec](https://zarr.dev/zeps/draft/ZEP0002.html) ([under review](https://github.com/zarr-developers/zarr-specs/issues/254))
//! - [x] [ZEP0003 - Variable chunking](https://zarr.dev/zeps/draft/ZEP0003.html) ([draft](https://github.com/orgs/zarr-developers/discussions/52))
//! - [x] Stores: [`filesystem`](crate::storage::store::FilesystemStore), [`memory`](crate::storage::store::MemoryStore), [`http`](crate::storage::store::HTTPStore), [`zip`](crate::storage::storage_adapter::ZipStorageAdapter)
//! - [x] Data types: [core data types](crate::array::data_type::DataType), [`raw bits`](crate::array::data_type::DataType::RawBits), [`float16`](crate::array::data_type::DataType::Float16), [`bfloat16`](crate::array::data_type::DataType::BFloat16) [(spec issue)](https://github.com/zarr-developers/zarr-specs/issues/130)
//! - [x] Chunk grids: [`regular`](crate::array::chunk_grid::RegularChunkGrid), [`rectangular`](crate::array::chunk_grid::RectangularChunkGrid) ([draft](https://github.com/orgs/zarr-developers/discussions/52))
//! - [x] Chunk key encoding: [`default`](crate::array::chunk_key_encoding::DefaultChunkKeyEncoding), [`v2`](crate::array::chunk_key_encoding::V2ChunkKeyEncoding)
//! - [x] Codecs:
//!   - array->array: [`transpose`](crate::array::codec::array_to_array::transpose), [`bitround`](crate::array::codec::array_to_array::bitround) (experimental)
//!   - array->bytes: [`bytes`](crate::array::codec::array_to_bytes::bytes) [(spec issue)](https://github.com/zarr-developers/zarr-specs/pull/263), [`sharding`](crate::array::codec::array_to_bytes::sharding), [`zfp`](crate::array::codec::array_to_bytes::zfp) (experimental)
//!   - bytes->bytes: [`blosc`](crate::array::codec::bytes_to_bytes::blosc), [`gzip`](crate::array::codec::bytes_to_bytes::gzip), [`zstd`](crate::array::codec::bytes_to_bytes::zstd) [(spec issue)](https://github.com/zarr-developers/zarr-specs/pull/256), [`crc32c checksum`](crate::array::codec::bytes_to_bytes::crc32c)
//! - [x] Storage transformers: [`usage_log`](crate::storage::storage_transformer::UsageLogStorageTransformer), [`performance_metrics`](crate::storage::storage_transformer::PerformanceMetricsStorageTransformer)
//!
//! ## Features
//! The following features are enabled by default:
//!  - Codecs: `blosc`, `gzip`, `transpose`, `zstd`, `sharding`, `crc32c`.
//!  - Stores: `http`, `zip`.
//!  - `ndarray`: adds [`ndarray`] utility functions to [`Array`](crate::array::Array).
//!
//! The following features are disabled by default:
//!  - Codecs:  `bitround`, `zfp`
//!
//! ## Examples
//! Examples can be run with `cargo run --example EXAMPLE_NAME`
//!
//! - [`array_read_write`](https://github.com/LDeakin/zarrs/blob/main/examples/array_write_read.rs): create an array, write its metadata, write chunks in parallel, delete a chunk, read the whole array, read a chunk, and partially read a subset.
//! - [`sharded_array_read_write`](https://github.com/LDeakin/zarrs/blob/main/examples/sharded_array_write_read.rs): write and read a sharded array.
//! - [`rectangular_array_read_write`](https://github.com/LDeakin/zarrs/blob/main/examples/rectangular_array_write_read.rs): write and read an array with a rectangular chunk grid.
//! - [`zip_array_read_write`](https://github.com/LDeakin/zarrs/blob/main/examples/zip_array_write_read.rs): write an array to a filesystem, zip it, then read it from the zipped file.
//! - [`http_array_read`](https://github.com/LDeakin/zarrs/blob/main/examples/http_array_read.rs): read an array over HTTP.
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

use std::mem::transmute;

use half::{bf16, f16};

pub mod array;
pub mod array_subset;
pub mod byte_range;
pub mod group;
pub mod metadata;
pub mod node;
pub mod plugin;
pub mod storage;
pub mod version;

// https://github.com/rust-lang/rust/issues/72447

/// The Zarr "NaN" fill value for a 64-bit IEEE 754 floating point number.
#[allow(clippy::unusual_byte_groupings)]
pub const ZARR_NAN_F64: f64 = unsafe {
    transmute::<u64, f64>(0b0_11111111111_1000000000000000000000000000000000000000000000000000)
};
// const ZARR_NAN_F64: f64 = f64::from_bits(0b0_11111111111_1000000000000000000000000000000000000000000000000000);

/// The Zarr "NaN" fill value for a 32-bit IEEE 754 floating point number.
#[allow(clippy::unusual_byte_groupings)]
pub const ZARR_NAN_F32: f32 =
    unsafe { transmute::<u32, f32>(0b0_11111111_10000000000000000000000) };
// const ZARR_NAN_F32: f32 = f32::from_bits(0b0_11111111_10000000000000000000000);

/// The Zarr "NaN" fill value for a 16-bit IEEE 754 floating point number.
pub const ZARR_NAN_F16: f16 = f16::NAN;

/// The Zarr "NaN" fill value for a 16-bit brain floating point number.
pub const ZARR_NAN_BF16: bf16 = bf16::NAN;

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn nan_representations() {
        assert_eq!(
            bf16::NAN.to_ne_bytes(),
            bf16::from_bits(0b0_11111111_1000000).to_ne_bytes()
        );
        assert_eq!(
            f16::NAN.to_ne_bytes(),
            f16::from_bits(0b0_11111_1000000000).to_ne_bytes()
        );
    }
}
