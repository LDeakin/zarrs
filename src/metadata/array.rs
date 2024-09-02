//! Zarr common array metadata.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#array-metadata>.

mod chunk_key_separator;
pub use chunk_key_separator::ChunkKeySeparator;

mod dimension_name;
pub use dimension_name::DimensionName;

/// The shape of an array.
pub type ArrayShape = Vec<u64>;

mod chunk_shape;
pub use chunk_shape::ChunkShape;

mod endianness;
pub use endianness::Endianness;
