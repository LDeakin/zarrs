//! [Zarr](https://zarr-specs.readthedocs.io/) extensions metadata support for the [`zarrs`](https://docs.rs/zarrs/latest/zarrs/index.html) crate.
//!
//! This crate supports serialisation and deserialisation of Zarr V2 and V3 extensions metadata.
//!
//! This crate includes known metadata for Zarr V3 extension points (chunk grids, chunk key encodings, codecs, and data types), including:
//! - _Core_ extensions defined in the [Zarr V3 specification](https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html),
//! - _Registered_ extensions defined at [zarr-developers/zarr-extensions](https://github.com/zarr-developers/zarr-extensions/), and
//! - `numcodecs` codecs and _experimental_ extensions in `zarrs` that have yet to be registered.
//!
//! Functions for converting Zarr V2 to equivalent Zarr V3 metadata are included.

pub mod array;
pub mod group;

pub mod array_storage_transformer;
pub mod chunk_grid;
pub mod chunk_key_encoding;
pub mod codec;
pub mod data_type;

pub mod v2_to_v3;
