//! Zarr array data type metadata.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html#array-metadata-data-type>.

mod numpy {
    pub(super) mod datetime64;
}

pub use numpy::datetime64::*;
