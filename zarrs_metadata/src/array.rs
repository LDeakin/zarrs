//! Zarr common array metadata.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html#array-metadata>.

mod chunk_key_separator;
pub use chunk_key_separator::ChunkKeySeparator;

mod dimension_name;
pub use dimension_name::DimensionName;

/// An array shape.
pub type ArrayShape = Vec<u64>;

mod chunk_shape;
pub use chunk_shape::ChunkShape;

mod endianness;
pub use endianness::Endianness;
